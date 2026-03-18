#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


ARCHIVE_SITES = [
    "https://archive.is",
    "https://archive.ph",
    "https://archive.today",
    "https://archive.li",
    "https://archive.md",
    "https://archive.vn",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SNAPSHOT_RE = re.compile(
    r"^https?://archive\.(?:is|ph|today|li|md|vn)/[A-Za-z0-9]{4,}(?:[#?].*)?$",
    re.IGNORECASE,
)

INTERNAL_SNAPSHOT_RE = re.compile(r"^/[A-Za-z0-9]{4,}$")
META_REFRESH_RE = re.compile(r"url=(.*)$", re.IGNORECASE)


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


def is_snapshot_url(url: str) -> bool:
    return bool(url and SNAPSHOT_RE.match(url))


def clean_url_candidate(u: str) -> str:
    return (u or "").strip()


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def candidate_score(original_url: str, href: str, link_text: str) -> int:
    score = 0
    target = original_url.lower()
    href_l = href.lower()
    text_l = (link_text or "").lower()

    if is_snapshot_url(href):
        score += 100

    target_path = urlparse(original_url).path.lower().rstrip("/")
    if target_path and target_path in href_l:
        score += 50
    if target_path and target_path in text_l:
        score += 50

    slug_parts = [p for p in re.split(r"[^a-z0-9]+", target_path) if len(p) >= 4]
    overlap = sum(1 for p in slug_parts if p in href_l or p in text_l)
    score += overlap * 5

    return score


def extract_snapshot_from_html(site: str, html: str, original_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1. meta refresh
    refresh = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if refresh:
        content = refresh.get("content", "")
        m = META_REFRESH_RE.search(content)
        if m:
            target = m.group(1).strip().strip("'\"")
            target = urljoin(site, target)
            if is_snapshot_url(target):
                return target

    # 2. scan all links
    ranked: List[Tuple[int, str]] = []

    for link in soup.find_all("a", href=True):
        href = link["href"].strip()
        text = link.get_text(" ", strip=True)

        if INTERNAL_SNAPSHOT_RE.match(href):
            href = urljoin(site, href)
        elif href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin(site, href)

        if not href.startswith("http"):
            continue

        score = candidate_score(original_url, href, text)
        if score > 0:
            ranked.append((score, href))

    if ranked:
        ranked.sort(reverse=True)
        return ranked[0][1]

    return None


def try_direct_path(session: requests.Session, site: str, original_url: str, notes: List[str]) -> Optional[str]:
    url = f"{site}/{original_url}"
    notes.append(f"Trying direct path: {url}")
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
        if is_snapshot_url(r.url):
            return r.url
        snap = extract_snapshot_from_html(site, r.text, original_url)
        if snap:
            return snap
    except Exception as e:
        notes.append(f"direct path failed on {site}: {e}")
    return None


def try_newest(session: requests.Session, site: str, original_url: str, notes: List[str]) -> Optional[str]:
    url = f"{site}/newest/{original_url}"
    notes.append(f"Trying newest: {url}")
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
        if is_snapshot_url(r.url):
            return r.url
        snap = extract_snapshot_from_html(site, r.text, original_url)
        if snap:
            return snap
    except Exception as e:
        notes.append(f"newest failed on {site}: {e}")
    return None


def try_url_query(session: requests.Session, site: str, original_url: str, notes: List[str]) -> Optional[str]:
    encoded = quote(original_url, safe="")
    url = f"{site}/?url={encoded}"
    notes.append(f"Trying ?url=: {url}")
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
        if is_snapshot_url(r.url):
            return r.url
        snap = extract_snapshot_from_html(site, r.text, original_url)
        if snap:
            return snap
    except Exception as e:
        notes.append(f"?url= failed on {site}: {e}")
    return None


def resolve_snapshot(original_url: str) -> Tuple[str, str, List[str]]:
    notes: List[str] = []
    session = get_session()

    url = clean_url_candidate(original_url)

    for site in ARCHIVE_SITES:
        snap = try_direct_path(session, site, url, notes)
        if snap:
            notes.append(f"Resolved via direct path on {site}")
            return snap, site, notes
        time.sleep(0.7)

    for site in ARCHIVE_SITES:
        snap = try_newest(session, site, url, notes)
        if snap:
            notes.append(f"Resolved via newest on {site}")
            return snap, site, notes
        time.sleep(0.7)

    for site in ARCHIVE_SITES:
        snap = try_url_query(session, site, url, notes)
        if snap:
            notes.append(f"Resolved via ?url= on {site}")
            return snap, site, notes
        time.sleep(0.7)

    raise RuntimeError("No snapshot found on archive.* mirrors")


def extract_text_from_snapshot(snapshot_url: str, notes: List[str]) -> Tuple[str, str, str, str, str]:
    session = get_session()
    r = session.get(snapshot_url, timeout=30, allow_redirects=True)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    page_title = ""
    if soup.title:
        page_title = soup.title.get_text(" ", strip=True)

    # title / author / date best-effort
    article_title = ""
    byline = ""
    date = ""

    # Prefer archive main text block
    text_block = soup.find(id="TEXT")
    if text_block:
        notes.append("Using #TEXT block")
        paragraphs = text_block.find_all("p")
        if not article_title:
            h1 = text_block.find(["h1", "h2"])
            if h1:
                article_title = h1.get_text(" ", strip=True)
    else:
        notes.append("Falling back to generic paragraph extraction")
        for tag in soup(["script", "style", "nav", "footer", "header", "form", "noscript"]):
            tag.decompose()
        paragraphs = soup.find_all("p")

    # metadata fallback
    if not article_title:
        ogt = soup.find("meta", attrs={"property": "og:title"})
        if ogt and ogt.get("content"):
            article_title = ogt["content"].strip()

    author_meta = soup.find("meta", attrs={"name": re.compile(r"author", re.I)})
    if author_meta and author_meta.get("content"):
        byline = author_meta["content"].strip()

    date_meta = (
        soup.find("meta", attrs={"property": re.compile(r"article:published_time", re.I)})
        or soup.find("meta", attrs={"name": re.compile(r"pubdate|date", re.I)})
    )
    if date_meta and date_meta.get("content"):
        date = date_meta["content"].strip()

    lines = []
    for p in paragraphs:
        txt = p.get_text(" ", strip=True)
        if len(txt) >= 30:
            lines.append(txt)

    text = "\n\n".join(lines).strip()

    return page_title, article_title, byline, date, text


def save_outputs(result: RecoverResult, output_dir: Path) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    slug = urlparse(result.input_url).path.split("/")[-1] or "article"
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", slug)[:140]

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
    parser = argparse.ArgumentParser(description="Recover article text from archive.* mirrors")
    parser.add_argument("url", help="Original article URL")
    parser.add_argument("--output-dir", default="archive_recover_output")
    args = parser.parse_args()

    snapshot_url, host_used, notes = resolve_snapshot(args.url)
    page_title, article_title, byline, date, text = extract_text_from_snapshot(snapshot_url, notes)

    word_count = len(text.split()) if text else 0
    if word_count >= 150:
        status = "ok"
    elif word_count > 0:
        status = "partial"
        notes.append(f"Low word count: {word_count}")
    else:
        status = "failed"
        notes.append("No extracted text returned")

    result = RecoverResult(
        input_url=args.url,
        archive_snapshot_url=snapshot_url,
        archive_host_used=host_used,
        page_title=page_title,
        article_title=article_title,
        byline=byline,
        date=date,
        text=text,
        word_count=word_count,
        extraction_status=status,
        notes=notes,
    )

    json_path, txt_path = save_outputs(result, Path(args.output_dir))

    print(json.dumps(
        {
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
        },
        ensure_ascii=False,
        indent=2,
    ))
    print("\n--- NOTES ---")
    for note in notes:
        print(f"- {note}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
