#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/archive_recover.py

Manual recovery tool for hard article URLs using archive.is / archive.today sister sites.

Designed for:
- Bloomberg
- FT
- Reuters Pro / blocked cases
- other pages where live extraction failed or was too thin
"""

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
import trafilatura
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


ARCHIVE_HOSTS = [
    "archive.today",
    "archive.ph",
    "archive.is",
    "archive.vn",
    "archive.li",
    "archive.md",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

DROP_QUERY_KEYS = {
    "embedded-checkout",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
}

SNAPSHOT_URL_RE = re.compile(
    r"^https?://archive\.(?:is|today|ph|li|md|vn)/[A-Za-z0-9]{4,}",
    re.IGNORECASE,
)


# --------------------------------------------------------
# DATA STRUCTURES
# --------------------------------------------------------

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


# --------------------------------------------------------
# URL NORMALIZATION
# --------------------------------------------------------

def normalize_url(url: str) -> str:
    url = (url or "").strip()

    parsed = urlparse(url)

    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    cleaned = []

    for k, v in parse_qsl(parsed.query, keep_blank_values=True):

        if k.lower() in DROP_QUERY_KEYS or k.lower().startswith("utm_"):
            continue

        cleaned.append((k, v))

    query = urlencode(cleaned) if cleaned else ""

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", query, ""))


def drop_query_and_fragment(url: str) -> str:
    p = urlparse(url)

    path = p.path.rstrip("/") or "/"

    return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))


# --------------------------------------------------------
# VARIANT BUILDING
# --------------------------------------------------------

def with_http_https_variants(url: str) -> List[str]:

    p = urlparse(url)

    path = p.path or "/"

    return [
        urlunparse(("https", p.netloc, path, "", "", "")),
        urlunparse(("http", p.netloc, path, "", "", "")),
    ]


def with_www_variants(url: str) -> List[str]:

    p = urlparse(url)

    host = p.netloc.lower()

    path = p.path or "/"

    hosts = {host}

    if host.startswith("www."):
        hosts.add(host[4:])
    else:
        hosts.add("www." + host)

    return [urlunparse((p.scheme or "https", h, path, "", "", "")) for h in hosts]


def build_lookup_candidates(input_url: str, live_canonical_url: str) -> List[str]:

    seeds = [
        input_url,
        normalize_url(input_url),
        live_canonical_url,
        normalize_url(live_canonical_url),
    ]

    candidates = []
    seen = set()

    for seed in seeds:

        if not seed:
            continue

        variants = [seed]

        variants += with_http_https_variants(seed)

        more = []

        for v in variants:
            more += with_www_variants(v)

        variants += more

        for v in variants:

            v = drop_query_and_fragment(normalize_url(v))

            if v not in seen:

                seen.add(v)

                candidates.append(v)

    return candidates


# --------------------------------------------------------
# CANONICAL DETECTION
# --------------------------------------------------------

def fetch_live_canonical(url: str) -> Optional[str]:

    try:

        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)

        soup = BeautifulSoup(r.text, "lxml")

        canonical = soup.find("link", rel=lambda x: x and "canonical" in x)

        if canonical and canonical.get("href"):

            return normalize_url(urljoin(r.url, canonical["href"]))

        return normalize_url(r.url)

    except Exception:

        return None


# --------------------------------------------------------
# PLAYWRIGHT HELPERS
# --------------------------------------------------------

def safe_goto(page, url: str, timeout_ms=30000):

    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

    page.wait_for_timeout(1200)


def choose_search_form_input(page):

    inputs = page.locator("input[type='text']").all()

    visible = []

    for i in inputs:

        try:

            if i.is_visible():
                visible.append(i)

        except Exception:
            pass

    if visible:

        return visible[-1]

    return None


# --------------------------------------------------------
# SNAPSHOT RESOLUTION
# --------------------------------------------------------

def collect_snapshot_candidates(page):

    anchors = page.locator("a").all()

    results = []

    for a in anchors:

        href = a.get_attribute("href") or ""

        if SNAPSHOT_URL_RE.match(href):

            results.append(href)

    return results


def resolve_snapshot_from_current_page(page):

    url = page.url

    if SNAPSHOT_URL_RE.match(url):

        return url

    candidates = collect_snapshot_candidates(page)

    if candidates:

        return candidates[0]

    return None


# --------------------------------------------------------
# ARCHIVE SEARCH
# --------------------------------------------------------

def try_direct_newest(page, host, candidate):

    url = f"https://{host}/newest/{candidate}"

    safe_goto(page, url)

    snap = resolve_snapshot_from_current_page(page)

    if snap:

        return snap

    return None


def try_search_form(page, host, candidate):

    safe_goto(page, f"https://{host}/")

    inp = choose_search_form_input(page)

    if not inp:

        return None

    inp.fill(candidate)

    inp.press("Enter")

    page.wait_for_timeout(2000)

    snap = resolve_snapshot_from_current_page(page)

    return snap


def resolve_latest_snapshot(page, input_url, live_canonical_url):

    candidates = build_lookup_candidates(input_url, live_canonical_url)

    for c in candidates:

        for host in ARCHIVE_HOSTS:

            snap = try_direct_newest(page, host, c)

            if snap:

                return snap, host

    for c in candidates:

        for host in ARCHIVE_HOSTS:

            snap = try_search_form(page, host, c)

            if snap:

                return snap, host

    raise RuntimeError("No archive snapshot found")


# --------------------------------------------------------
# EXTRACTION
# --------------------------------------------------------

def clean_archive_html(html):

    soup = BeautifulSoup(html, "lxml")

    for t in soup(["script", "style", "noscript"]):
        t.decompose()

    return str(soup)


def extract_reader_text(html, url_hint):

    html = clean_archive_html(html)

    meta = trafilatura.extract_metadata(html, default_url=url_hint)

    title = meta.title if meta else ""

    author = meta.author if meta else ""

    date = meta.date if meta else ""

    text = trafilatura.extract(html)

    if not text:

        soup = BeautifulSoup(html, "lxml")

        text = soup.get_text("\n")

    return title or "", author or "", date or "", text or ""


# --------------------------------------------------------
# MAIN RECOVERY
# --------------------------------------------------------

def recover_from_archive(url, show_browser=False):

    notes = []

    normalized = normalize_url(url)

    canonical = fetch_live_canonical(normalized) or normalized

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=not show_browser)

        ctx = browser.new_context(user_agent=DEFAULT_HEADERS["User-Agent"])

        page = ctx.new_page()

        snapshot_url, host = resolve_latest_snapshot(page, normalized, canonical)

        safe_goto(page, snapshot_url)

        html = page.content()

        page_title = page.title()

        browser.close()

    title, byline, date, text = extract_reader_text(html, canonical)

    wc = len(text.split())

    if wc >= 150:

        status = "ok"

    elif wc > 0:

        status = "partial"

    else:

        status = "failed"

    return RecoverResult(
        url,
        normalized,
        canonical,
        snapshot_url,
        host,
        page_title,
        title,
        byline,
        date,
        text,
        wc,
        status,
        notes,
    )


# --------------------------------------------------------
# OUTPUT
# --------------------------------------------------------

def save_single_outputs(result: RecoverResult, out_dir: Path):

    out_dir.mkdir(parents=True, exist_ok=True)

    slug = urlparse(result.live_canonical_url).path.split("/")[-1]

    slug = re.sub(r"[^A-Za-z0-9]+", "_", slug)[:120]

    j = out_dir / f"{slug}.json"

    t = out_dir / f"{slug}.txt"

    with open(j, "w") as f:

        json.dump(asdict(result), f, indent=2)

    with open(t, "w") as f:

        f.write(result.text)

    return j, t


# --------------------------------------------------------
# CLI
# --------------------------------------------------------

def main():

    ap = argparse.ArgumentParser()

    ap.add_argument("--url")

    ap.add_argument("--show-browser", action="store_true")

    ap.add_argument("--output-dir", default="archive_recover_output")

    args = ap.parse_args()

    if not args.url:

        print("Need --url")

        sys.exit(1)

    r = recover_from_archive(args.url, args.show_browser)

    j, t = save_single_outputs(r, Path(args.output_dir))

    print(json.dumps(asdict(r), indent=2))


if __name__ == "__main__":
    main()
