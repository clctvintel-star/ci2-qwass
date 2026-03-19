#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CI2 • Manual Queue Recovery (clean rebuilt version)

What it does
- reads a manual queue CSV
- targets rows where summary < 50 chars
- tries, in order:
    1) Google News RSS syndication by title (with redirect resolution)
    2) Diffbot on original URL
    3) Diffbot on AMP variants
    4) Diffbot on Wayback latest pointer
    5) direct archive snapshot URL already present in row
    6) RemovePaywall fallback via Selenium/Chromium for Bloomberg/FT only

Important
- Do NOT mount Drive inside this script.
- Mount Drive in Colab first, then run with !python.
- Default input is your FIRST PASS recovered file.
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
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Optional selenium imports
try:
    from pyvirtualdisplay import Display
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False


# =========================================================
# DEFAULT CONFIG
# =========================================================

DEFAULT_ENV_PATH = "/content/drive/MyDrive/CI2/ci2_keys.env"

# IMPORTANT: this is the correct SECOND-PASS input
DEFAULT_INPUT_FILE = (
    "/content/drive/MyDrive/CI2/db/qwass2/"
    "collector_manual_queue_20260317_042544_RECOVERED_20260318_1610.csv"
)

SHORT_SUMMARY_MAX = 50
MIN_ACCEPT_CHARS = 200
MIN_ACCEPT_WORDS = 40

USE_SELENIUM_FALLBACK_DEFAULT = True

DIFFBOT_API = "https://api.diffbot.com/v3/article"
REQUEST_TIMEOUT_S = 45
DIFFBOT_MAX_RETRIES = 3
DIFFBOT_BACKOFF_BASE_S = 8

REMOVEPAYWALL_WAIT_AFTER_CLICK = 10
REMOVEPAYWALL_ARTICLE_MIN_WORDS = 50

FALLBACK_DOMAIN_PATTERNS = (
    r"bloomberg\.com",
    r"\bft\.com\b",
    r"financialtimes\.com",
)

GOOD_SYNDICATION_DOMAINS = [
    "msn.com",
    "news.yahoo.com",
    "finance.yahoo.com",
    "marketscreener.com",
    "theprint.in",
    "hindustantimes.com",
    "biztoc.com",
    "marketwatch.com",
    "seekingalpha.com",
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

ARCHIVE_SNAPSHOT_COLUMNS = [
    "archive_snapshot_url",
    "archive_url",
    "snapshot_url",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Recover missing article summaries in manual queue")

    parser.add_argument(
        "--env-path",
        default=DEFAULT_ENV_PATH,
        help="Path to env file containing DIFFBOT_KEY / DIFFBOT_TOKEN",
    )
    parser.add_argument(
        "--input-file",
        default=DEFAULT_INPUT_FILE,
        help="Input CSV",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Output CSV. If omitted, auto-generates next to input.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N short-summary rows",
    )
    parser.add_argument(
        "--sleep-between-rows",
        type=float,
        default=2.0,
        help="Base sleep between rows",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=1.0,
        help="Random extra sleep added per row",
    )
    parser.add_argument(
        "--disable-selenium-fallback",
        action="store_true",
        help="Disable RemovePaywall Selenium fallback",
    )

    return parser.parse_args()


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
    if any(re.search(p, u) for p in FALLBACK_DOMAIN_PATTERNS):
        return True
    if s in {"bloomberg", "financial times", "ft"}:
        return True
    return False


def polite_row_sleep(base_sleep: float, jitter: float):
    time.sleep(base_sleep + random.uniform(0, jitter))


def clean_text(text: str) -> str:
    t = safe_text(text)
    t = t.replace("\xa0", " ")
    t = re.sub(r"\s+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    t = re.sub(r"[ \t]{2,}", " ", t)
    return t.strip()


def clean_extracted_text(text: str) -> str:
    t = clean_text(text)

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


def get_first_present(row: Dict, cols: List[str]) -> str:
    for c in cols:
        if c in row and safe_text(row[c]):
            return safe_text(row[c])
    return ""


def amp_variants(url: str) -> List[str]:
    """
    Keep a small candidate set. Enough to be useful without exploding requests.
    """
    url = safe_text(url)
    if not url:
        return []

    out = []

    if "output=amp" not in url:
        sep = "&" if "?" in url else "?"
        out.append(url + f"{sep}output=amp")

    if not url.endswith("/amp"):
        out.append(url.rstrip("/") + "/amp")

    # dedupe preserve order
    final = []
    seen = set()
    for u in out:
        if u not in seen:
            seen.add(u)
            final.append(u)
    return final


def wayback_latest(url: str) -> str:
    return f"https://web.archive.org/web/0/{url}"


# =========================================================
# GOOGLE NEWS RSS SYNDICATION
# =========================================================

def resolve_redirect_url(url: str) -> str:
    """
    Resolve Google News redirect links to the final publisher URL.
    Return empty string if it still lands on bare Google News.
    """
    url = safe_text(url)
    if not url:
        return ""

    try:
        session = requests.Session()
        r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        final_url = safe_text(r.url)

        if "news.google.com" in final_url and final_url.rstrip("/") in {
            "https://news.google.com",
            "https://news.google.com/",
        }:
            return ""

        return final_url
    except Exception:
        return ""


def find_syndicated_url_by_title(title: str) -> str:
    """
    Search Google News RSS by exact title, resolve redirects,
    prefer known syndicators, otherwise any non-Google resolved URL.
    """
    title = safe_text(title)
    if not title:
        return ""

    title_q = urllib.parse.quote_plus(f'"{title}"')
    rss_url = f"https://news.google.com/rss/search?q={title_q}"

    try:
        r = requests.get(rss_url, headers=HEADERS, timeout=20)
        r.raise_for_status()

        links = re.findall(r"<link>(.*?)</link>", r.text)
        raw_urls = [html.unescape(u.strip()) for u in links[1:]]  # skip channel link

        resolved = []
        for u in raw_urls:
            final_u = resolve_redirect_url(u) if "news.google.com" in u else u
            final_u = safe_text(final_u)
            if not final_u:
                continue
            if "news.google.com" in final_u:
                continue
            resolved.append(final_u)

        for d in GOOD_SYNDICATION_DOMAINS:
            for u in resolved:
                if d in u:
                    return u

        for u in resolved:
            if "news.google.com" not in u:
                return u

    except Exception as e:
        print(f"   ⚠️ RSS syndication search failed: {e}")

    return ""


# =========================================================
# DIFFBOT
# =========================================================

def fetch_diffbot_text_once(url: str, token: str) -> Tuple[str, int]:
    params = {
        "token": token,
        "url": url,
        "timeout": 60000,
        "render": "true",
        "useCanonical": "false",
    }
    r = requests.get(DIFFBOT_API, params=params, timeout=REQUEST_TIMEOUT_S)
    status = r.status_code

    if status == 200:
        j = r.json()
        objs = j.get("objects") or []
        if not objs:
            return "", status

        text = safe_text(objs[0].get("text"))
        if not text and objs[0].get("html"):
            txt = re.sub("<[^>]+>", " ", objs[0]["html"])
            text = re.sub(r"\s+", " ", txt).strip()

        return clean_extracted_text(text), status

    return "", status


def fetch_diffbot_text(url: str, token: str) -> str:
    """
    Retry only on 429 / 5xx / request exceptions.
    If Diffbot gives 200, return what it gave and let caller judge quality.
    """
    for attempt in range(1, DIFFBOT_MAX_RETRIES + 1):
        try:
            text, status = fetch_diffbot_text_once(url, token)

            if status == 200:
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


def best_text_via_diffbot(original_url: str, original_title: str, token: str) -> Tuple[str, str]:
    """
    Old-script style:
    1) syndicated copy by title
    2) original URL
    3) AMP variants
    4) Wayback pointer
    """
    if original_title:
        alt = find_syndicated_url_by_title(original_title)
        if alt:
            print(f"   🔎 Fast-path syndicated copy: {alt}")
            text = fetch_diffbot_text(alt, token)
            if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
                return text, f"syndicated:{alt}"

    candidates = [original_url] + amp_variants(original_url) + [wayback_latest(original_url)]

    for cand in candidates:
        label = "Diffbot candidate"
        if "output=amp" in cand or cand.endswith("/amp"):
            label = "Diffbot AMP candidate"
        elif "web.archive.org/web/0/" in cand:
            label = "Diffbot Wayback candidate"

        print(f"   🤖 {label}: {cand}")
        text = fetch_diffbot_text(cand, token)

        if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
            return text, f"diffbot:{cand}"

    return "", ""


# =========================================================
# ARCHIVE SNAPSHOT FETCH
# =========================================================

def fetch_archive_snapshot_text(snapshot_url: str) -> str:
    snapshot_url = safe_text(snapshot_url)
    if not snapshot_url:
        return ""

    try:
        r = requests.get(snapshot_url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        text_block = soup.find(id="TEXT")
        if text_block:
            paragraphs = text_block.find_all("p")
        else:
            for tag in soup(["script", "style", "noscript", "iframe", "svg", "nav", "footer", "header", "form"]):
                tag.decompose()
            paragraphs = soup.find_all("p")

        text = "\n\n".join(
            p.get_text(" ", strip=True)
            for p in paragraphs
            if len(p.get_text(" ", strip=True)) > 20
        )
        text = clean_extracted_text(text)

        if looks_like_archive_challenge(text):
            return ""

        return text

    except Exception as e:
        print(f"   ⚠️ Archive snapshot fetch failed: {e}")
        return ""


# =========================================================
# SELENIUM / REMOVEPAYWALL
# =========================================================

def find_existing_path(candidates: List[Optional[str]]) -> Optional[str]:
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None


def build_chrome_driver():
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium dependencies are not available")

    display = Display(visible=0, size=(1920, 1080))
    display.start()

    chromium_candidates = [
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
    ]
    chromedriver_candidates = [
        shutil.which("chromedriver"),
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
    ]

    chromium_path = find_existing_path(chromium_candidates)
    chromedriver_path = find_existing_path(chromedriver_candidates)

    if not chromium_path:
        raise RuntimeError(
            "Could not find chromium/google-chrome binary. "
            "Install chromium + chromedriver in Colab first."
        )
    if not chromedriver_path:
        raise RuntimeError(
            "Could not find chromedriver binary. "
            "Install chromedriver in Colab first."
        )

    options = ChromeOptions()
    options.binary_location = chromium_path
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-blink-features=AutomationControlled")

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

        header_trash = [
            "Skip to content",
            "Bloomberg the Company",
            "Sign In",
            "Subscribe",
            "Live TV",
            "Markets",
            "Opinion",
            "US Edition",
        ]
        footer_trash_starts = [
            "Get Alerts for:",
            "Submit a Tip",
            "Explore Offers",
            "Terms of Service",
            "Trademarks",
            "Advertise",
            "Subscribe now",
            "Help",
            "Made in NYC",
            "©",
        ]
        mid_trash = [
            "Photographer:",
            "Gift this article",
            "Share feedback",
            "Get in Touch",
            "Before it’s here, it’s on the Bloomberg Terminal",
            "Before it's here, it's on the Bloomberg Terminal",
            "Bloomberg Terminal LEARN MORE",
        ]

        for junk in header_trash:
            if junk in raw_text:
                raw_text = raw_text.split(junk, 1)[-1]

        for junk in footer_trash_starts:
            if junk in raw_text:
                raw_text = raw_text.split(junk, 1)[0]

        for junk in mid_trash:
            raw_text = raw_text.replace(junk, "")

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
# MAIN
# =========================================================

def main():
    args = parse_args()

    load_dotenv(args.env_path)
    token = os.getenv("DIFFBOT_KEY") or os.getenv("DIFFBOT_TOKEN")
    if not token:
        raise ValueError(f"❌ Missing DIFFBOT_KEY / DIFFBOT_TOKEN in {args.env_path}")

    print("✅ Diffbot token loaded:", token[:6] + "...")
    print(f"📥 Input file: {args.input_file}")

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

    mask_short = df["summary"].apply(is_short_summary)
    to_update_idx = list(df[mask_short].index)

    if args.limit is not None:
        to_update_idx = to_update_idx[:args.limit]

    print(f"Rows loaded: {len(df)}")
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

        text, method = best_text_via_diffbot(url, title, token)

        if not good_article_text(text) and archive_snapshot_url:
            print(f"   🗂️ Trying archive snapshot URL: {archive_snapshot_url}")
            archive_text = fetch_archive_snapshot_text(archive_snapshot_url)
            if good_article_text(archive_text):
                text = archive_text
                method = f"archive:{archive_snapshot_url}"

        if (
            not good_article_text(text)
            and USE_SELENIUM_FALLBACK_DEFAULT
            and not args.disable_selenium_fallback
            and is_bbg_or_ft(url, source)
        ):
            print("   ↩️ Falling back to RemovePaywall (Chromium)...")
            try:
                selenium_text = fetch_via_removepaywall(url) or ""
                if looks_like_ft_paywall(selenium_text):
                    selenium_text = ""
                if good_article_text(selenium_text):
                    text = selenium_text
                    method = "removepaywall"
            except Exception as e:
                print(f"   ⚠️ Selenium fallback failed: {e}")

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
