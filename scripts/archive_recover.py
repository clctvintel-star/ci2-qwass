#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
archive_recover.py

Manual archive recovery for hard URLs (Bloomberg, FT, Reuters etc).

Key behavior
------------
1. Try archive /newest/ endpoint
2. If no snapshot, try archive search form
3. If still no result, try headline search fallback
4. Only accept real snapshot URLs (archive.ph/ABC123 etc)
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse, urljoin

import requests
import trafilatura
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


ARCHIVE_HOSTS = [
    "archive.today",
    "archive.ph",
    "archive.is",
    "archive.vn",
    "archive.li",
    "archive.md",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}


SNAPSHOT_RE = re.compile(
    r"https?://archive\.(?:today|ph|is|li|md|vn)/[A-Za-z0-9]{4,}$",
    re.I,
)


# ------------------------------------------------------------
# DATA STRUCT
# ------------------------------------------------------------

@dataclass
class RecoverResult:

    input_url: str
    normalized_input_url: str
    live_canonical_url: str
    archive_snapshot_url: str
    archive_host_used: str

    page_title: str
    article_title: str
    byline: str
    date: str

    text: str
    word_count: int
    extraction_status: str

    notes: List[str]


# ------------------------------------------------------------
# URL UTIL
# ------------------------------------------------------------

def normalize(url: str) -> str:
    return url.strip()


def slug_tokens(url: str):

    path = urlparse(url).path.lower()

    parts = re.split(r"[^a-z0-9]+", path)

    stop = {
        "news",
        "feature",
        "features",
        "article",
        "the",
        "and",
        "with",
        "from",
        "2026",
    }

    return [p for p in parts if len(p) > 3 and p not in stop]


# ------------------------------------------------------------
# CANONICAL
# ------------------------------------------------------------

def fetch_canonical(url):

    try:

        r = requests.get(url, headers=HEADERS, timeout=15)

        soup = BeautifulSoup(r.text, "lxml")

        tag = soup.find("link", rel="canonical")

        if tag and tag.get("href"):
            return tag["href"]

        return r.url

    except Exception:

        return url


# ------------------------------------------------------------
# PLAYWRIGHT HELPERS
# ------------------------------------------------------------

def goto(page, url):

    page.goto(url, wait_until="domcontentloaded", timeout=30000)

    page.wait_for_timeout(1000)


def snapshot_links(page):

    links = []

    for a in page.locator("a").all():

        href = a.get_attribute("href") or ""

        href = urljoin(page.url, href)

        if SNAPSHOT_RE.match(href):

            links.append(href)

    return links


def resolve_snapshot(page):

    url = page.url

    if SNAPSHOT_RE.match(url):
        return url

    links = snapshot_links(page)

    if links:
        return links[0]

    return None


# ------------------------------------------------------------
# ARCHIVE STRATEGIES
# ------------------------------------------------------------

def try_newest(page, host, url):

    target = f"https://{host}/newest/{url}"

    goto(page, target)

    return resolve_snapshot(page)


def try_search(page, host, url):

    goto(page, f"https://{host}/")

    inputs = page.locator("input[type=text]").all()

    if not inputs:
        return None

    box = inputs[-1]

    box.fill(url)

    box.press("Enter")

    page.wait_for_timeout(2000)

    return resolve_snapshot(page)


def try_headline(page, host, url):

    tokens = slug_tokens(url)

    if not tokens:
        return None

    q = "+".join(tokens[:6])

    search = f"https://{host}/?q={q}"

    goto(page, search)

    return resolve_snapshot(page)


def resolve_archive(page, url):

    for host in ARCHIVE_HOSTS:

        snap = try_newest(page, host, url)

        if snap:
            return snap, host

    for host in ARCHIVE_HOSTS:

        snap = try_search(page, host, url)

        if snap:
            return snap, host

    for host in ARCHIVE_HOSTS:

        snap = try_headline(page, host, url)

        if snap:
            return snap, host

    raise RuntimeError("No archive snapshot found")


# ------------------------------------------------------------
# EXTRACTION
# ------------------------------------------------------------

def clean_html(html):

    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    return str(soup)


def extract_text(html, url):

    html = clean_html(html)

    meta = trafilatura.extract_metadata(html, default_url=url)

    title = meta.title if meta else ""
    author = meta.author if meta else ""
    date = meta.date if meta else ""

    text = trafilatura.extract(html)

    if not text:

        soup = BeautifulSoup(html, "lxml")

        text = soup.get_text("\n")

    return title, author, date, text


# ------------------------------------------------------------
# RECOVERY
# ------------------------------------------------------------

def recover(url, show_browser=False):

    notes = []

    url = normalize(url)

    canonical = fetch_canonical(url)

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=not show_browser)

        ctx = browser.new_context(user_agent=USER_AGENT)

        page = ctx.new_page()

        snapshot, host = resolve_archive(page, canonical)

        goto(page, snapshot)

        html = page.content()

        page_title = page.title()

        browser.close()

    title, author, date, text = extract_text(html, canonical)

    wc = len(text.split())

    if wc > 150:
        status = "ok"
    elif wc > 0:
        status = "partial"
    else:
        status = "failed"

    return RecoverResult(
        url,
        url,
        canonical,
        snapshot,
        host,
        page_title,
        title,
        author,
        date,
        text,
        wc,
        status,
        notes,
    )


# ------------------------------------------------------------
# OUTPUT
# ------------------------------------------------------------

def save(result, outdir):

    outdir.mkdir(parents=True, exist_ok=True)

    slug = urlparse(result.live_canonical_url).path.split("/")[-1]

    slug = re.sub(r"[^A-Za-z0-9]+", "_", slug)

    j = outdir / f"{slug}.json"
    t = outdir / f"{slug}.txt"

    with open(j, "w") as f:
        json.dump(asdict(result), f, indent=2)

    with open(t, "w") as f:
        f.write(result.text)

    return j, t


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main():

    ap = argparse.ArgumentParser()

    ap.add_argument("--url")

    ap.add_argument("--show-browser", action="store_true")

    ap.add_argument("--output-dir", default="archive_recover_output")

    args = ap.parse_args()

    if not args.url:

        print("Need --url")

        sys.exit(1)

    r = recover(args.url, args.show_browser)

    j, t = save(r, Path(args.output_dir))

    print(json.dumps(asdict(r), indent=2))


if __name__ == "__main__":
    main()
