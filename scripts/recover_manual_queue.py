#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CI2 manual-queue recovery pipeline

Fast path for Bloomberg / FT:
1) Syndication first
2) Direct archive snapshot URL from row (if already present)
3) Diffbot on canonical URL
4) Diffbot on ONE AMP variant only (?output=amp)
5) Diffbot on Wayback latest pointer
6) RemovePaywall Selenium fallback

Default path for everything else:
1) Diffbot on original URL
2) Diffbot on ONE AMP variant
3) Diffbot on Wayback latest pointer
4) Syndicated copy via Google News RSS -> Diffbot
5) Direct archive snapshot URL from row (if already present)

Key design choices:
- lower acceptance threshold (closer to your old script)
- no repeated retries on weak HTTP 200 results
- retries only for 429 / 5xx / request exceptions
- reduced candidate explosion
- robust Chromium / ChromeDriver path detection

Colab:
1) mount drive in a notebook cell first
2) run:
   !python scripts/recover_manual_queue.py --limit 10
"""

import argparse
import html
import os
import random
import re
import shutil
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Optional Selenium fallback
try:
    from pyvirtualdisplay import Display
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Recover missing article summaries in manual queue")
    parser.add_argument(
        "--env-path",
        default="/content/drive/MyDrive/CI2/ci2_keys.env",
        help="Path to env file containing DIFFBOT_KEY / DIFFBOT_TOKEN"
    )
    parser.add_argument(
        "--input-file",
        default="/content/drive/MyDrive/CI2/db/qwass2/collector_manual_queue_20260317_042544.csv",
        help="Input CSV"
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Output CSV. If omitted, auto-generates next to input."
    )
    parser.add_argument("--limit", type=int, default=None, help="Only process first N rows")
    parser.add_argument("--sleep-between-rows", type=float, default=2.0)
    parser.add_argument("--jitter", type=float, default=1.0)
    parser.add_argument("--disable-selenium-fallback", action="store_true")
    return parser.parse_args()


# =========================================================
# CONFIG
# =========================================================

SHORT_SUMMARY_MAX = 50

# More permissive, like the old script
MIN_ACCEPT_CHARS = 200
MIN_ACCEPT_WORDS = 40

DIFFBOT_API = "https://api.diffbot.com/v3/article"
REQUEST_TIMEOUT_S = 45

# Reduced retries
DIFFBOT_MAX_RETRIES = 3
DIFFBOT_BACKOFF_BASE_S = 8
DIFFBOT_PER_CALL_SLEEP_S = 2.0

REMOVEPAYWALL_WAIT_AFTER_CLICK = 10
REMOVEPAYWALL_ARTICLE_MIN_WORDS = 50

ARCHIVE_SNAPSHOT_COLUMNS = [
    "archive_snapshot_url",
    "archive_url",
    "snapshot_url",
]

TARGET_DOMAIN_PATTERNS = (
    r"bloomberg\.com",
    r"\bft\.com\b",
    r"financialtimes\.com",
)

GOOD_SYNDICATION_DOMAINS = [
    "msn.com",
    "news.yahoo.com",
    "finance.yahoo.com",
    "marketscreener.com",
    "marketwatch.com",
    "seekingalpha.com",
    "theprint.in",
    "hindustantimes.com",
    "biztoc.com",
    "breakingviews.com",
    "reuters.com",
]

FT_PAYWALL_MARKERS = [
    "subscribe to unlock this article",
    "keep reading for $",
    "explore more offers",
    "standard digital",
    "premium digital",
    "print + premium digital",
    "terms & conditions apply",
    "discover all the plans",
    "ft professional",
    "why the ft",
    "see why over a million readers pay to read the financial times",
]

BLOOMBERG_JUNK_MARKERS = [
    "before it’s here, it’s on the bloomberg terminal",
    "before it's here, it's on the bloomberg terminal",
    "bloomberg the company & its products",
    "bloomberg anywhere login",
    "customer support",
]

ARCHIVE_CHALLENGE_MARKERS = [
    "one more step",
    "please complete the security check to access",
    "why do i have to complete a captcha",
    "what can i do to prevent this in the future",
]


# =========================================================
# HELPERS
# =========================================================

def safe_text(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def is_short_summary(summary: str) -> bool:
    return len(safe_text(summary)) < SHORT_SUMMARY_MAX


def is_bbg_or_ft(url: str, source: str = "") -> bool:
    u = safe_text(url).lower()
    s = safe_text(source).lower()
    if any(re.search(p, u) for p in TARGET_DOMAIN_PATTERNS):
        return True
    if s in {"bloomberg", "financial times", "ft"}:
        return True
    return False


def polite_row_sleep(base_sleep: float, jitter: float):
    time.sleep(base_sleep + random.uniform(0, jitter))


def good_article_text(text: str) -> bool:
    t = safe_text(text)
    return len(t) >= MIN_ACCEPT_CHARS and len(t.split()) >= MIN_ACCEPT_WORDS


def looks_like_ft_paywall(text: str) -> bool:
    t = safe_text(text).lower()
    hits = sum(1 for m in FT_PAYWALL_MARKERS if m in t)
    return hits >= 2 or ("subscribe" in t and "digital" in t and "financial times" in t)


def looks_like_bbg_junk(text: str) -> bool:
    t = safe_text(text).lower()
    return any(m in t for m in BLOOMBERG_JUNK_MARKERS)


def looks_like_archive_challenge(text: str) -> bool:
    t = safe_text(text).lower()
    return any(m in t for m in ARCHIVE_CHALLENGE_MARKERS)


def clean_extracted_text(text: str) -> str:
    t = safe_text(text)

    junk_phrases = [
        "Skip to content",
        "Sign In",
        "Subscribe",
        "Live TV",
        "Markets",
        "Opinion",
        "Get Alerts for:",
        "Submit a Tip",
        "Explore Offers",
        "Terms of Service",
        "Trademarks",
        "Advertise",
        "Help",
        "Made in NYC",
        "Gift this article",
        "Share feedback",
        "Get in Touch",
        "Bloomberg Terminal LEARN MORE",
    ]

    for junk in junk_phrases:
        t = t.replace(junk, "")

    t = re.sub(r"\n{3,}", "\n\n", t)
    t = re.sub(r"[ \t]+", " ", t)
    return t.strip()


def amp_variants(url: str) -> List[str]:
    """
    Keep only the most useful AMP variant to avoid candidate explosion.
    """
    url = safe_text(url)
    if not url:
        return []

    out = []
    if "output=amp" not in url:
        sep = "&" if "?" in url else "?"
        out.append(url + f"{sep}output=amp")
    return out


def wayback_latest(url: str) -> str:
    return f"https://web.archive.org/web/0/{url}"


def find_syndicated_url_by_title(title: str) -> str:
    title = safe_text(title)
    if not title:
        return ""

    title_q = urllib.parse.quote_plus(f'"{title}"')
    rss_url = f"https://news.google.com/rss/search?q={title_q}"

    try:
        r = requests.get(rss_url, timeout=20)
        r.raise_for_status()
        links = re.findall(r"<link>(.*?)</link>", r.text)
        urls = [html.unescape(u.strip()) for u in links[1:]]

        for d in GOOD_SYNDICATION_DOMAINS:
            for u in urls:
                if d in u:
                    return u

        for u in urls:
            if "ft.com" not in u and "bloomberg.com" not in u:
                return u

    except Exception as e:
        print(f"   ⚠️ Syndication lookup failed: {e}")

    return ""


def get_first_present(row: Dict, cols: List[str]) -> str:
    for c in cols:
        if c in row and safe_text(row[c]):
            return safe_text(row[c])
    return ""


# =========================================================
# DIFFBOT
# =========================================================

def fetch_diffbot_text_once(url: str, token: str) -> Tuple[str, int]:
    time.sleep(DIFFBOT_PER_CALL_SLEEP_S)

    params = {
        "token": token,
        "url": url,
        "timeout": 60000,
        "render": "true",
        "useCanonical": "false",
    }

    r = requests.get(DIFFBOT_API, params=params, timeout=REQUEST_TIMEOUT_S)
    status = r.status_code

    if status != 200:
        return "", status

    j = r.json()
    objs = j.get("objects") or []
    if not objs:
        return "", status

    obj = objs[0]
    text = safe_text(obj.get("text"))

    if not text and obj.get("html"):
        txt = re.sub("<[^>]+>", " ", obj["html"])
        text = re.sub(r"\s+", " ", txt).strip()

    return clean_extracted_text(text), status


def fetch_diffbot_text(url: str, token: str) -> str:
    """
    Important change: if Diffbot returns HTTP 200 with some text, do not keep retrying
    weak results forever. That behavior was causing 429 storms.
    """
    for attempt in range(1, DIFFBOT_MAX_RETRIES + 1):
        try:
            text, status = fetch_diffbot_text_once(url, token)

            if status == 200:
                if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
                    return text

                # Old-script-style behavior: if we got a 200 response with something,
                # move on instead of burning the API with more retries.
                return text

            if status in (429, 500, 502, 503, 504):
                wait = DIFFBOT_BACKOFF_BASE_S * (2 ** (attempt - 1))
                print(f"   ⚠️ Diffbot HTTP {status} for {url} — backoff {wait}s ({attempt}/{DIFFBOT_MAX_RETRIES})")
                time.sleep(wait)
                continue

            print(f"   ⚠️ Diffbot HTTP {status} for {url} — not retrying")
            return ""

        except requests.RequestException as e:
            wait = DIFFBOT_BACKOFF_BASE_S * (2 ** (attempt - 1))
            print(f"   ⚠️ Diffbot error {type(e).__name__} for {url} — backoff {wait}s ({attempt}/{DIFFBOT_MAX_RETRIES})")
            time.sleep(wait)

    return ""


# =========================================================
# ARCHIVE SNAPSHOT FETCH (manual final snapshot URL only)
# =========================================================

def fetch_archive_snapshot_text(snapshot_url: str) -> str:
    if not safe_text(snapshot_url):
        return ""

    try:
        r = requests.get(snapshot_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")

        text_block = soup.find(id="TEXT")
        if text_block:
            paragraphs = text_block.find_all("p")
        else:
            for tag in soup(["script", "style", "nav", "footer", "header", "form", "noscript"]):
                tag.decompose()
            paragraphs = soup.find_all("p")

        lines = [p.get_text(" ", strip=True) for p in paragraphs if len(p.get_text(" ", strip=True)) > 30]
        text = clean_extracted_text("\n\n".join(lines))

        if looks_like_archive_challenge(text):
            return ""

        return text

    except Exception as e:
        print(f"   ⚠️ Archive snapshot fetch failed: {e}")
        return ""


# =========================================================
# SELENIUM / REMOVEPAYWALL FALLBACK
# =========================================================

def build_chrome_driver():
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium dependencies are not available")

    display = Display(visible=0, size=(1920, 1080))
    display.start()

    chromium_candidates = [
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("google-chrome"),
    ]
    chromedriver_candidates = [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        shutil.which("chromedriver"),
    ]

    chromium_path = next((p for p in chromium_candidates if p and os.path.exists(p)), None)
    chromedriver_path = next((p for p in chromedriver_candidates if p and os.path.exists(p)), None)

    if not chromium_path:
        raise RuntimeError("Could not find chromium binary")
    if not chromedriver_path:
        raise RuntimeError("Could not find chromedriver binary")

    options = ChromeOptions()
    options.binary_location = chromium_path
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-software-rasterizer")

    service = ChromeService(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    return driver, display


def fetch_via_removepaywall(url: str) -> str:
    encoded = urllib.parse.quote_plus(url)
    rp_url = f"https://www.removepaywall.com/search?url={encoded}"

    driver, display = build_chrome_driver()

    try:
        driver.get(rp_url)
        time.sleep(3)

        btns = driver.find_elements(By.XPATH, "//button[contains(., 'Option 1')]")
        if not btns:
            return ""

        btns[0].click()
        time.sleep(REMOVEPAYWALL_WAIT_AFTER_CLICK)

        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[1])
            time.sleep(2)

        try:
            iframe = driver.find_element(By.TAG_NAME, "iframe")
            driver.switch_to.frame(iframe)
            time.sleep(1)
        except Exception:
            pass

        raw_text = driver.find_element(By.TAG_NAME, "body").text
        cleaned = clean_extracted_text(raw_text)

        if len(cleaned.split()) < REMOVEPAYWALL_ARTICLE_MIN_WORDS:
            return ""

        if looks_like_ft_paywall(cleaned) or looks_like_bbg_junk(cleaned):
            return ""

        return cleaned

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            display.stop()
        except Exception:
            pass


# =========================================================
# RECOVERY LOGIC
# =========================================================

def recover_article(
    url: str,
    title: str,
    source: str,
    token: str,
    archive_snapshot_url: str = "",
    use_selenium_fallback: bool = True,
) -> Tuple[str, str]:
    """
    Returns (text, method_used)
    """

    url = safe_text(url)
    title = safe_text(title)
    source = safe_text(source)

    bbg_or_ft = is_bbg_or_ft(url, source)

    # -------------------------------------------------
    # FAST PATH FOR BLOOMBERG / FT
    # -------------------------------------------------
    if bbg_or_ft:
        # 1) Syndication first
        alt = find_syndicated_url_by_title(title)
        if alt:
            print(f"   🔎 Fast-path syndicated copy: {alt}")
            text = fetch_diffbot_text(alt, token)
            if good_article_text(text):
                return text, f"syndicated:{alt}"

        # 2) Direct archive snapshot URL from row if present
        if archive_snapshot_url:
            print(f"   🗂️ Fast-path archive snapshot URL: {archive_snapshot_url}")
            text = fetch_archive_snapshot_text(archive_snapshot_url)
            if good_article_text(text):
                return text, f"archive:{archive_snapshot_url}"

        # 3) Canonical URL
        print(f"   🤖 Diffbot candidate: {url}")
        text = fetch_diffbot_text(url, token)
        if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
            return text, f"diffbot:{url}"

        # 4) Single AMP variant only
        for cand in amp_variants(url):
            print(f"   🤖 Diffbot AMP candidate: {cand}")
            text = fetch_diffbot_text(cand, token)
            if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
                return text, f"diffbot:{cand}"

        # 5) Wayback
        wb = wayback_latest(url)
        print(f"   🤖 Diffbot Wayback candidate: {wb}")
        text = fetch_diffbot_text(wb, token)
        if good_article_text(text):
            return text, f"diffbot:{wb}"

        # 6) Selenium fallback last
        if use_selenium_fallback:
            print("   ↩️ Falling back to RemovePaywall (Chromium)...")
            try:
                text = fetch_via_removepaywall(url)
                if good_article_text(text):
                    return text, "removepaywall"
            except Exception as e:
                print(f"   ⚠️ Selenium fallback failed: {e}")

        return "", ""

    # -------------------------------------------------
    # DEFAULT PATH FOR EVERYTHING ELSE
    # -------------------------------------------------
    candidates = [url] + amp_variants(url) + [wayback_latest(url)]

    for cand in candidates:
        print(f"   🤖 Diffbot candidate: {cand}")
        text = fetch_diffbot_text(cand, token)
        if good_article_text(text):
            return text, f"diffbot:{cand}"

    alt = find_syndicated_url_by_title(title)
    if alt:
        print(f"   🔎 Trying syndicated copy: {alt}")
        text = fetch_diffbot_text(alt, token)
        if good_article_text(text):
            return text, f"syndicated:{alt}"

    if archive_snapshot_url:
        print(f"   🗂️ Trying archive snapshot URL: {archive_snapshot_url}")
        text = fetch_archive_snapshot_text(archive_snapshot_url)
        if good_article_text(text):
            return text, f"archive:{archive_snapshot_url}"

    return "", ""


# =========================================================
# MAIN
# =========================================================

def main():
    args = parse_args()

    load_dotenv(args.env_path)
    token = os.getenv("DIFFBOT_KEY") or os.getenv("DIFFBOT_TOKEN")
    if not token:
        raise ValueError(f"❌ Missing DIFFBOT_KEY / DIFFBOT_TOKEN in {args.env_path}")

    print("✅ Diffbot token loaded:", token[:6] + "...")

    input_file = args.input_file
    if not args.output_file:
        stamp = datetime.now().strftime("%Y%m%d_%H%M")
        input_path = Path(input_file)
        output_file = str(input_path.with_name(f"{input_path.stem}_RECOVERED_{stamp}.csv"))
    else:
        output_file = args.output_file

    df = pd.read_csv(input_file)

    required = ["title", "url"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    if "summary" not in df.columns:
        df["summary"] = ""
    if "source" not in df.columns:
        df["source"] = ""
    if "recovery_method" not in df.columns:
        df["recovery_method"] = ""

    if args.limit is not None:
        df = df.head(args.limit).copy()

    print(f"Rows loaded: {len(df)}")

    mask_short = df["summary"].apply(is_short_summary)
    to_update_idx = df[mask_short].index
    print(f"Rows with summary < {SHORT_SUMMARY_MAX} chars: {len(to_update_idx)}")

    updated = 0

    for i in to_update_idx:
        row = df.loc[i].to_dict()

        url = safe_text(row.get("url"))
        title = safe_text(row.get("title"))
        source = safe_text(row.get("source"))
        before = safe_text(row.get("summary"))
        archive_snapshot_url = get_first_present(row, ARCHIVE_SNAPSHOT_COLUMNS)

        if not url:
            continue

        print(f"\n🔗 [{i}] {title}\n    {url}")
        print(f"   Before chars: {len(before)}")

        text, method = recover_article(
            url=url,
            title=title,
            source=source,
            token=token,
            archive_snapshot_url=archive_snapshot_url,
            use_selenium_fallback=not args.disable_selenium_fallback,
        )

        if good_article_text(text):
            df.at[i, "summary"] = text
            df.at[i, "recovery_method"] = method
            updated += 1
            print(f"   ✅ UPDATED → {title} | {method} | chars: {len(text)}")
        else:
            print(f"   ❌ Skipped (only {len(text)} chars)")

        polite_row_sleep(args.sleep_between_rows, args.jitter)

    df.to_csv(output_file, index=False)
    print(f"\n🎯 Updated rows: {updated} / {len(to_update_idx)}")
    print(f"💾 Saved output:\n   {output_file}")


if __name__ == "__main__":
    main()
