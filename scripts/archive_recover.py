#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/archive_recover.py

Browser-driven archive article fetcher for archive.ph / archive.today / archive.is.

What it does:
1. Opens archive resolver for the target URL
2. Waits for the real snapshot page to load
3. Clicks "Webpage" if present
4. Extracts article text from the rendered HTML
5. Saves JSON + TXT output

Important:
- Uses NON-headless Chromium because archive/Cloudflare often blocks headless browsers
- Intended for manual/small-batch recovery, not huge parallel bulk jobs
"""

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
import trafilatura
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


ARCHIVE_HOSTS = [
    "archive.ph",
    "archive.today",
    "archive.is",
    "archive.li",
    "archive.md",
    "archive.vn",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

REAL_SNAPSHOT_RE = re.compile(
    r"^https?://archive\.(?:ph|today|is|li|md|vn)/[A-Za-z0-9]{4,}(?:[#?].*)?$",
    re.IGNORECASE,
)

CLOUDFLARE_PATTERNS = [
    "What can I do to prevent this in the future?",
    "Checking if the site connection is secure",
    "Please enable cookies",
    "DDoS protection by",
    "Verify you are human",
]


@dataclass
class RecoverResult:
    input_url: str
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


def safe_slug(s: str) -> str:
    s = s or "article"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:140]


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    return str(soup)


def looks_like_cloudflare(html: str, title: str) -> bool:
    blob = f"{title}\n{html}"
    return any(pat.lower() in blob.lower() for pat in CLOUDFLARE_PATTERNS)


def extract_article(html: str, url_hint: str) -> Tuple[str, str, str, str]:
    cleaned_html = clean_html(html)

    meta = trafilatura.extract_metadata(cleaned_html, default_url=url_hint)

    article_title = ""
    byline = ""
    date = ""

    if meta:
        article_title = getattr(meta, "title", "") or ""
        byline = getattr(meta, "author", "") or ""
        date = getattr(meta, "date", "") or ""

    text = trafilatura.extract(
        cleaned_html,
        url=url_hint,
        include_comments=False,
        include_tables=False,
        include_images=False,
        favor_precision=True,
        deduplicate=True,
    )

    if not text:
        soup = BeautifulSoup(cleaned_html, "lxml")
        text = soup.get_text("\n", strip=True)

    return article_title.strip(), byline.strip(), date.strip(), (text or "").strip()


def try_click_webpage_tab(page, notes: List[str]) -> None:
    for label in ["Webpage", "webpage"]:
        try:
            loc = page.locator(f"text={label}").first
            if loc.is_visible():
                loc.click(timeout=4000)
                page.wait_for_timeout(1500)
                notes.append("Clicked Webpage tab.")
                return
        except Exception:
            pass


def resolve_snapshot_via_browser(page, url: str, host: str, notes: List[str]) -> Optional[str]:
    resolver = f"https://{host}/?run=1&url={url}"
    notes.append(f"Trying resolver: {resolver}")

    page.goto(resolver, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(5000)

    current = page.url
    title = page.title() or ""
    html = page.content()

    # Best case: resolver lands directly on snapshot
    if REAL_SNAPSHOT_RE.match(current):
        notes.append(f"Resolver landed on snapshot: {current}")
        return current

    # Sometimes resolver lands on results page; click first snapshot link
    try:
        anchors = page.locator("a[href*='archive.']").all()
    except Exception:
        anchors = []

    ranked = []
    for a in anchors:
        href = a.get_attribute("href") or ""
        text = (a.inner_text() or "").strip()
        if REAL_SNAPSHOT_RE.match(href):
            score = 100
            if text:
                score += 10
            ranked.append((score, href))

    if ranked:
        ranked.sort(reverse=True)
        snap = ranked[0][1]
        notes.append(f"Resolver page exposed snapshot link: {snap}")
        return snap

    if looks_like_cloudflare(html, title):
        notes.append(f"Blocked by Cloudflare/interstitial on {host}.")
    else:
        notes.append(f"Resolver did not reach snapshot on {host}. Current URL: {current}")

    return None


def fetch_article_from_archive(url: str, show_browser: bool = True) -> RecoverResult:
    notes: List[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not show_browser,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
            slow_mo=150,
        )

        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 1200},
            locale="en-US",
            java_script_enabled=True,
        )

        page = context.new_page()

        snapshot_url = None
        host_used = None

        for host in ARCHIVE_HOSTS:
            snap = resolve_snapshot_via_browser(page, url, host, notes)
            if snap:
                snapshot_url = snap
                host_used = host
                break

        if not snapshot_url:
            browser.close()
            raise RuntimeError("Could not resolve an archive snapshot. Archive likely blocked automation in this run.")

        page.goto(snapshot_url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(3000)

        try_click_webpage_tab(page, notes)
        page.wait_for_timeout(1500)

        page_title = page.title() or ""
        html = page.content()

        if looks_like_cloudflare(html, page_title):
            browser.close()
            raise RuntimeError("Reached archive, but Cloudflare/interstitial page was returned instead of article HTML.")

        browser.close()

    article_title, byline, date, text = extract_article(html, snapshot_url)
    word_count = len(text.split()) if text else 0

    if word_count >= 150:
        status = "ok"
    elif word_count > 0:
        status = "partial"
        notes.append(f"Low word count: {word_count}")
    else:
        status = "failed"
        notes.append("No extracted text returned.")

    return RecoverResult(
        input_url=url,
        archive_snapshot_url=snapshot_url,
        archive_host_used=host_used or "",
        page_title=page_title.strip(),
        article_title=article_title,
        byline=byline,
        date=date,
        text=text,
        word_count=word_count,
        extraction_status=status,
        notes=notes,
    )


def save_outputs(result: RecoverResult, output_dir: Path) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    slug = safe_slug(urlparse(result.input_url).path.split("/")[-1] or "article")

    json_path = output_dir / f"{slug}.json"
    txt_path = output_dir / f"{slug}.txt"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(asdict(result), f, ensure_ascii=False, indent=2)

    with open(txt_path, "w", encoding="utf-8") as f:
        if result.article_title:
            f.write(result.article_title + "\n\n")
        if result.byline:
            f.write(f"Byline: {result.byline}\n")
        if result.date:
            f.write(f"Date: {result.date}\n")
        f.write(f"Snapshot: {result.archive_snapshot_url}\n")
        f.write(f"Host: {result.archive_host_used}\n")
        f.write(f"Word count: {result.word_count}\n\n")
        f.write(result.text or "")

    return json_path, txt_path


def main():
    parser = argparse.ArgumentParser(description="Archive article fetcher")
    parser.add_argument("--url", required=True, help="Article URL to recover from archive")
    parser.add_argument("--output-dir", default="archive_recover_output")
    parser.add_argument("--show-browser", action="store_true", help="Show browser window; recommended for archive sites")
    args = parser.parse_args()

    result = fetch_article_from_archive(args.url, show_browser=args.show_browser or True)
    json_path, txt_path = save_outputs(result, Path(args.output_dir))

    summary = {
        "input_url": result.input_url,
        "archive_snapshot_url": result.archive_snapshot_url,
        "archive_host_used": result.archive_host_used,
        "page_title": result.page_title,
        "article_title": result.article_title,
        "byline": result.byline,
        "date": result.date,
        "word_count": result.word_count,
        "extraction_status": result.extraction_status,
        "json_output": str(json_path),
        "txt_output": str(txt_path),
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("\n--- NOTES ---")
    for note in result.notes:
        print(f"- {note}")


if __name__ == "__main__":
    main()
