#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CI2 • Manual Queue Recovery (CLEAN VERSION)

Second-pass recovery script.

Pipeline priority:
1) Google News syndication (VERY high yield)
2) Diffbot (URL → AMP → Wayback)
3) Archive snapshot (if present)
4) Selenium fallback (optional, only BBG/FT)

Outputs a NEW file. Never overwrites source.
"""

import os
import re
import time
import random
import html
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


# =========================================================
# CONFIG
# =========================================================

ENV_PATH = "/content/drive/MyDrive/CI2/ci2_keys.env"
INPUT_FILE = "/content/drive/MyDrive/CI2/db/qwass2/collector_manual_queue_20260317_042544_RECOVERED_20260318_1610.csv"

STAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = f"/content/drive/MyDrive/CI2/db/qwass2/collector_manual_queue_PASS2_{STAMP}.csv"

SHORT_SUMMARY_MAX = 50
MIN_ACCEPT_CHARS = 200
MIN_ACCEPT_WORDS = 40

DIFFBOT_API = "https://api.diffbot.com/v3/article"
REQUEST_TIMEOUT = 45

SLEEP = 2.5
JITTER = 1.5

GOOD_SYNDICATION_DOMAINS = [
    "msn.com", "news.yahoo.com", "finance.yahoo.com",
    "marketscreener.com", "marketwatch.com",
    "seekingalpha.com", "reuters.com"
]

TARGET_DOMAINS = ["bloomberg.com", "ft.com", "financialtimes.com"]


# =========================================================
# UTILS
# =========================================================

def safe(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except:
        pass
    return str(x).strip()


def is_short(text):
    return len(safe(text)) < SHORT_SUMMARY_MAX


def good(text):
    t = safe(text)
    return len(t) >= MIN_ACCEPT_CHARS and len(t.split()) >= MIN_ACCEPT_WORDS


def sleep():
    time.sleep(SLEEP + random.uniform(0, JITTER))


# =========================================================
# GOOGLE NEWS SYNDICATION
# =========================================================

def find_syndicated(title):
    if not title:
        return ""

    q = urllib.parse.quote_plus(f'"{title}"')
    url = f"https://news.google.com/rss/search?q={q}"

    try:
        r = requests.get(url, timeout=20)
        links = re.findall(r"<link>(.*?)</link>", r.text)

        urls = [html.unescape(u) for u in links[1:]]

        # Prefer known syndicators
        for d in GOOD_SYNDICATION_DOMAINS:
            for u in urls:
                if d in u:
                    return u

        # fallback
        for u in urls:
            if "bloomberg.com" not in u and "ft.com" not in u:
                return u

    except Exception as e:
        print("   ⚠️ RSS failed:", e)

    return ""


# =========================================================
# DIFFBOT
# =========================================================

load_dotenv(ENV_PATH)
TOKEN = os.getenv("DIFFBOT_KEY") or os.getenv("DIFFBOT_TOKEN")

if not TOKEN:
    raise ValueError("❌ Missing Diffbot key")


def diffbot(url):
    try:
        params = {
            "token": TOKEN,
            "url": url,
            "render": "true",
            "timeout": 60000
        }

        r = requests.get(DIFFBOT_API, params=params, timeout=REQUEST_TIMEOUT)

        if r.status_code != 200:
            return ""

        j = r.json()
        objs = j.get("objects") or []
        if not objs:
            return ""

        text = safe(objs[0].get("text"))

        if not text and objs[0].get("html"):
            text = re.sub("<[^>]+>", " ", objs[0]["html"])

        return re.sub(r"\s+", " ", text).strip()

    except Exception:
        return ""


def amp(url):
    if "?" in url:
        return url + "&output=amp"
    return url + "?output=amp"


def wayback(url):
    return f"https://web.archive.org/web/0/{url}"


# =========================================================
# ARCHIVE SNAPSHOT
# =========================================================

def fetch_archive(url):
    if not url:
        return ""

    try:
        r = requests.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "lxml")

        for tag in soup(["script","style","nav","footer"]):
            tag.decompose()

        ps = soup.find_all("p")
        text = "\n".join(p.get_text(" ", strip=True) for p in ps if len(p.text) > 40)

        return re.sub(r"\s+", " ", text)

    except:
        return ""


# =========================================================
# CORE RECOVERY
# =========================================================

def recover(row):
    url = safe(row["url"])
    title = safe(row["title"])
    source = safe(row.get("source"))

    # ---- 1. Syndication (HUGE WIN RATE)
    alt = find_syndicated(title)
    if alt:
        print("   🔎 syndicated:", alt)
        text = diffbot(alt)
        if good(text):
            return text, f"syndicated:{alt}"

    # ---- 2. Diffbot canonical
    text = diffbot(url)
    if good(text):
        return text, f"diffbot:{url}"

    # ---- 3. AMP
    text = diffbot(amp(url))
    if good(text):
        return text, f"diffbot:amp"

    # ---- 4. Wayback
    text = diffbot(wayback(url))
    if good(text):
        return text, f"diffbot:wayback"

    # ---- 5. Archive snapshot (if exists)
    for c in ["archive_snapshot_url", "archive_url", "snapshot_url"]:
        if c in row and safe(row[c]):
            print("   🗂 archive:", row[c])
            text = fetch_archive(row[c])
            if good(text):
                return text, f"archive:{row[c]}"

    return "", ""


# =========================================================
# MAIN
# =========================================================

def main():
    print("✅ Diffbot loaded")

    df = pd.read_csv(INPUT_FILE)

    if "summary" not in df:
        df["summary"] = ""
    if "recovery_method" not in df:
        df["recovery_method"] = ""

    mask = df["summary"].apply(is_short)
    idxs = df[mask].index

    print("Rows needing recovery:", len(idxs))

    wins = 0

    for i in idxs:
        row = df.loc[i]

        print(f"\n[{i}] {row['title'][:80]}")
        print("   before:", len(safe(row["summary"])))

        text, method = recover(row)

        if good(text):
            df.at[i, "summary"] = text
            df.at[i, "recovery_method"] = method
            wins += 1
            print("   ✅ WIN:", method, "|", len(text))
        else:
            print("   ❌ miss")

        sleep()

    df.to_csv(OUTPUT_FILE, index=False)

    print("\n======================")
    print("WINS:", wins)
    print("TOTAL:", len(idxs))
    print("Saved:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
