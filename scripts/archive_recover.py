# ============================================
# CI2 Article Recovery Pipeline
# Diffbot → AMP → Wayback → Syndication
# ============================================

import os
import re
import time
import random
import requests
import pandas as pd

from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv

# ---------------- CONFIG ----------------

ENV_PATH = "/content/drive/MyDrive/CI2/ci2_keys.env"
INPUT_FILE = "/content/drive/MyDrive/CI2/input.xlsx"

STAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = f"/content/drive/MyDrive/CI2/output_{STAMP}.xlsx"

MIN_ACCEPT_CHARS = 300

DIFFBOT_API = "https://api.diffbot.com/v3/article"
DIFFBOT_RETRIES = 4
DIFFBOT_BACKOFF = 6

SLEEP_BASE = 6
SLEEP_JITTER = 4

GOOD_SYNDICATION = [
    "msn.com",
    "finance.yahoo.com",
    "news.yahoo.com",
    "marketscreener.com",
    "marketwatch.com",
    "seekingalpha.com"
]

# ---------------- AUTH ----------------

load_dotenv(ENV_PATH)
DIFFBOT_TOKEN = os.getenv("DIFFBOT_KEY") or os.getenv("DIFFBOT_TOKEN")

if not DIFFBOT_TOKEN:
    raise RuntimeError("Missing Diffbot token")

# ---------------- HELPERS ----------------

def polite_sleep():
    time.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))


def amp_variants(url):

    c = []

    if "output=amp" not in url:
        sep = "&" if "?" in url else "?"
        c.append(url + f"{sep}output=amp")

    if not url.endswith("/amp"):
        c.append(url.rstrip("/") + "/amp")

    return c


def wayback_variant(url):
    return f"https://web.archive.org/web/0/{url}"


# ---------------- DIFFBOT ----------------

def diffbot_call(url):

    params = {
        "token": DIFFBOT_TOKEN,
        "url": url,
        "timeout": 60000,
        "render": "true"
    }

    r = requests.get(DIFFBOT_API, params=params, timeout=45)

    if r.status_code == 200:

        j = r.json()
        objs = j.get("objects", [])

        if objs:

            text = (objs[0].get("text") or "").strip()

            return text

    return ""


def diffbot_with_retry(url):

    for attempt in range(1, DIFFBOT_RETRIES + 1):

        polite_sleep()

        try:

            text = diffbot_call(url)

            if len(text) > MIN_ACCEPT_CHARS:
                return text

        except Exception:
            pass

        wait = DIFFBOT_BACKOFF * attempt

        print(f"Retry {attempt} waiting {wait}s")

        time.sleep(wait)

    return ""


# ---------------- GOOGLE NEWS RSS ----------------

def find_syndicated(title):

    if not title:
        return ""

    query = quote_plus(f'"{title}"')

    rss = f"https://news.google.com/rss/search?q={query}"

    try:

        r = requests.get(rss, timeout=20)

        links = re.findall(r"<link>(.*?)</link>", r.text)

        links = links[1:]

        for domain in GOOD_SYNDICATION:

            for url in links:

                if domain in url:
                    return url

        for url in links:

            if "ft.com" not in url:
                return url

    except Exception:
        pass

    return ""


# ---------------- PIPELINE ----------------

def recover_article(url, title):

    print("\nTrying:", url)

    candidates = []

    candidates.append(url)

    candidates += amp_variants(url)

    candidates.append(wayback_variant(url))

    for candidate in candidates:

        print("Diffbot:", candidate)

        text = diffbot_with_retry(candidate)

        if len(text) > MIN_ACCEPT_CHARS:

            print("Success via:", candidate)

            return text

    print("Trying syndication...")

    alt = find_syndicated(title)

    if alt:

        print("Syndicated:", alt)

        text = diffbot_with_retry(alt)

        if len(text) > MIN_ACCEPT_CHARS:

            return text

    return ""


# ---------------- MAIN ----------------

df = pd.read_excel(INPUT_FILE)

updated = 0

for i, row in df.iterrows():

    summary = str(row.get("summary", "")).strip()

    if len(summary) > 50:
        continue

    url = str(row.get("url", "")).strip()
    title = str(row.get("title", "")).strip()

    if not url:
        continue

    text = recover_article(url, title)

    if len(text) > MIN_ACCEPT_CHARS:

        df.at[i, "summary"] = text

        updated += 1

        print("UPDATED:", title)

    else:

        print("FAILED:", title)

print("Updated rows:", updated)

df.to_excel(OUTPUT_FILE, index=False)

print("Saved:", OUTPUT_FILE)
