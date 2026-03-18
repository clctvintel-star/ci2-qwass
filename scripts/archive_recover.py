# ============================================
# CI2 • Bloomberg / FT article recovery pipeline
# Diffbot -> AMP -> Wayback -> FT syndication -> optional archive snapshot -> RemovePaywall
# ============================================

# ---------- System / Python deps ----------
!apt-get -qq update
!apt-get -qq install -y chromium chromium-driver
!pip -q install python-dotenv pandas openpyxl requests selenium pyvirtualdisplay beautifulsoup4 lxml

# ---------- Imports ----------
import os
import re
import time
import html
import random
import urllib.parse
import requests
import pandas as pd

from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv
from google.colab import drive
from bs4 import BeautifulSoup

from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService


# =========================================================
# 0) MOUNT + ENV
# =========================================================
drive.mount('/content/drive', force_remount=True)

ENV_PATH = "/content/drive/MyDrive/CI2/ci2_keys.env"
load_dotenv(ENV_PATH)

DIFFBOT_TOKEN = os.getenv("DIFFBOT_KEY") or os.getenv("DIFFBOT_TOKEN")
if not DIFFBOT_TOKEN:
    raise ValueError("❌ Missing DIFFBOT_KEY / DIFFBOT_TOKEN in /content/drive/MyDrive/CI2/ci2_keys.env")

print("✅ Diffbot token loaded:", DIFFBOT_TOKEN[:6] + "...")


# =========================================================
# 1) CONFIG
# =========================================================
INPUT_FILE = "/content/drive/MyDrive/CI2/WORKING.hedge_fund_news_Millennium_20250901_0306_with_700w_summaries.xlsx"
STAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = f"/content/drive/MyDrive/CI2/WORKING.hedge_fund_news_Millennium_20250901_0306_RECOVERED_{STAMP}.xlsx"

SHORT_SUMMARY_MAX = 50
MIN_ACCEPT_CHARS = 350
MIN_ACCEPT_WORDS = 80

DIFFBOT_API = "https://api.diffbot.com/v3/article"
REQUEST_TIMEOUT_S = 45

# Diffbot retries/backoff
DIFFBOT_MAX_RETRIES = 4
DIFFBOT_BACKOFF_BASE_S = 8
DIFFBOT_PER_CALL_SLEEP_S = 3.0

# Row pacing
SLEEP_BETWEEN_ROWS_BASE = 8.0
SLEEP_BETWEEN_ROWS_JITTER = 3.0

# Selenium fallback
USE_SELENIUM_FALLBACK = True
REMOVEPAYWALL_WAIT_AFTER_CLICK = 10
REMOVEPAYWALL_ARTICLE_MIN_WORDS = 80

# Optional direct archive snapshot columns (final archive URLs only, e.g. https://archive.ph/abc123)
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
    "over a million readers pay to read the financial times",
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
# 2) HELPERS
# =========================================================
def safe_text(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
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


def polite_row_sleep():
    time.sleep(SLEEP_BETWEEN_ROWS_BASE + random.uniform(0, SLEEP_BETWEEN_ROWS_JITTER))


def good_article_text(text: str) -> bool:
    t = safe_text(text)
    return len(t) >= MIN_ACCEPT_CHARS and len(t.split()) >= MIN_ACCEPT_WORDS


def looks_like_ft_paywall(text: str) -> bool:
    t = safe_text(text).lower()
    hits = sum(1 for m in FT_PAYWALL_MARKERS if m in t)
    return hits >= 2 or ("subscribe" in t and "financial times" in t and "digital" in t)


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


def amp_variants(url: str):
    url = safe_text(url)
    if not url:
        return []

    out = []

    if "output=amp" not in url:
        sep = "&" if "?" in url else "?"
        out.append(url + f"{sep}output=amp")

    if not url.endswith("/amp"):
        out.append(url.rstrip("/") + "/amp")

    if "://www." in url:
        out.append(url.replace("://www.", "://amp."))

    seen = set()
    final = []
    for u in out:
        if u not in seen:
            seen.add(u)
            final.append(u)
    return final


def wayback_latest(url: str):
    return f"https://web.archive.org/web/0/{url}"


def find_syndicated_url_by_title(title: str) -> str:
    title = safe_text(title)
    if not title:
        return ""

    title_q = quote_plus(f'"{title}"')
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


def get_first_present(row, cols):
    for c in cols:
        if c in row and safe_text(row[c]):
            return safe_text(row[c])
    return ""


# =========================================================
# 3) DIFFBOT
# =========================================================
def fetch_diffbot_text_once(url: str):
    time.sleep(DIFFBOT_PER_CALL_SLEEP_S)

    params = {
        "token": DIFFBOT_TOKEN,
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


def fetch_diffbot_text(url: str) -> str:
    for attempt in range(1, DIFFBOT_MAX_RETRIES + 1):
        try:
            text, status = fetch_diffbot_text_once(url)

            if status == 200:
                # good result
                if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
                    return text

                # partial / weak result — retry because these services are flaky
                if attempt < DIFFBOT_MAX_RETRIES:
                    wait = DIFFBOT_BACKOFF_BASE_S * attempt
                    print(f"   ↻ Diffbot partial/weak result for {url} — retrying in {wait}s ({attempt}/{DIFFBOT_MAX_RETRIES})")
                    time.sleep(wait)
                    continue

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
# 4) ARCHIVE SNAPSHOT FETCH (manual final snapshot URL only)
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
# 5) SELENIUM / REMOVEPAYWALL FALLBACK
# =========================================================
def build_chrome_driver():
    display = Display(visible=0, size=(1920, 1080))
    display.start()

    options = ChromeOptions()
    options.binary_location = "/usr/bin/chromium"

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-software-rasterizer")

    service = ChromeService(executable_path="/usr/bin/chromedriver")
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
# 6) RECOVERY LOGIC
# =========================================================
def recover_article(url: str, title: str, source: str, archive_snapshot_url: str = ""):
    """
    Returns (text, method_used)
    """
    url = safe_text(url)
    title = safe_text(title)
    source = safe_text(source)

    # A) FT: try syndication first, because FT live URLs often just burn Diffbot calls
    if "ft.com" in url or "financialtimes.com" in url:
        alt = find_syndicated_url_by_title(title)
        if alt:
            print(f"   🔎 FT syndicated copy first: {alt}")
            text = fetch_diffbot_text(alt)
            if good_article_text(text):
                return text, f"syndicated:{alt}"

    # B) Direct Diffbot chain
    candidates = [url] + amp_variants(url) + [wayback_latest(url)]
    for cand in candidates:
        print(f"   🤖 Diffbot candidate: {cand}")
        text = fetch_diffbot_text(cand)

        if good_article_text(text) and not looks_like_ft_paywall(text) and not looks_like_bbg_junk(text):
            return text, f"diffbot:{cand}"

    # C) Syndication fallback for everyone else
    alt = find_syndicated_url_by_title(title)
    if alt:
        print(f"   🔎 Trying syndicated copy: {alt}")
        text = fetch_diffbot_text(alt)
        if good_article_text(text):
            return text, f"syndicated:{alt}"

    # D) Manual archive snapshot URL if present in workbook
    if archive_snapshot_url:
        print(f"   🗂️ Trying archive snapshot URL: {archive_snapshot_url}")
        text = fetch_archive_snapshot_text(archive_snapshot_url)
        if good_article_text(text):
            return text, f"archive:{archive_snapshot_url}"

    # E) Selenium fallback for Bloomberg / FT only
    if USE_SELENIUM_FALLBACK and is_bbg_or_ft(url, source):
        print("   ↩️ Falling back to RemovePaywall (Chromium)...")
        try:
            text = fetch_via_removepaywall(url)
            if good_article_text(text):
                return text, "removepaywall"
        except Exception as e:
            print(f"   ⚠️ Selenium fallback failed: {e}")

    return "", ""


# =========================================================
# 7) LOAD + RUN
# =========================================================
df = pd.read_excel(INPUT_FILE)

required = ["date", "time", "utc", "title", "url", "source", "author1", "author2", "summary"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

col_order = list(df.columns)

# add method column if missing
if "recovery_method" not in df.columns:
    df["recovery_method"] = ""

mask_short = df["summary"].apply(is_short_summary)
to_update_idx = df[mask_short].index

print(f"Total rows: {len(df)}")
print(f"Rows with summary < {SHORT_SUMMARY_MAX} chars: {len(to_update_idx)}")

updated = 0

for i in to_update_idx:
    url = safe_text(df.at[i, "url"])
    title = safe_text(df.at[i, "title"])
    source = safe_text(df.at[i, "source"])
    before = safe_text(df.at[i, "summary"])

    if not url:
        continue

    archive_snapshot_url = get_first_present(df.loc[i].to_dict(), ARCHIVE_SNAPSHOT_COLUMNS)

    print(f"\n🔗 [{i}] {title}\n    {url}")
    print(f"   Before chars: {len(before)}")

    text, method = recover_article(
        url=url,
        title=title,
        source=source,
        archive_snapshot_url=archive_snapshot_url,
    )

    if good_article_text(text):
        df.at[i, "summary"] = text
        df.at[i, "recovery_method"] = method
        updated += 1
        print(f"   ✅ UPDATED → {title} | {method} | chars: {len(text)}")
    else:
        print(f"   ❌ Skipped (only {len(text)} chars)")

    polite_row_sleep()

print(f"\n🎯 Updated rows: {updated} / {len(to_update_idx)}")

# preserve original order plus recovery_method at end if it was newly added
if "recovery_method" not in col_order:
    col_order = col_order + ["recovery_method"]

df = df[col_order]
df.to_excel(OUTPUT_FILE, index=False)

print(f"\n💾 Saved updated workbook:\n   {OUTPUT_FILE}")
