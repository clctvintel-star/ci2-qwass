#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/archive_recover.py

Manual archive recovery tool for hard article URLs using archive.today / archive.is mirrors.

Designed for:
- Bloomberg
- FT
- Reuters Pro / blocked cases
- other pages where live extraction failed or was too thin

This version uses the working browser flow:
1. open archive home
2. use the LOWER blue "search saved snapshots" form
3. search by exact URL / variants
4. click best result
5. click Webpage tab if present
6. extract text from archived page

Install:
    pip install requests beautifulsoup4 lxml trafilatura playwright pandas
    playwright install chromium
"""

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
import trafilatura
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


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
    "cmpid",
    "srnd",
    "leadsource",
    "mkt_tok",
    "mc_cid",
    "mc_eid",
    "ref",
    "source",
}

# Real snapshot URL: archive.xx/<id>
REAL_SNAPSHOT_RE = re.compile(
    r"^https?://archive\.(?:today|ph|is|vn|li|md)/[A-Za-z0-9]{4,}(?:[#?].*)?$",
    re.IGNORECASE,
)


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


# -------------------------------------------------------------------
# URL HELPERS
# -------------------------------------------------------------------

def normalize_url(url: str) -> str:
    url = (url or "").strip()
    parsed = urlparse(url)

    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    cleaned_pairs = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        kl = k.lower()
        if kl in DROP_QUERY_KEYS or kl.startswith("utm_"):
            continue
        cleaned_pairs.append((k, v))

    query = urlencode(cleaned_pairs, doseq=True) if cleaned_pairs else ""

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", query, ""))


def drop_query_and_fragment(url: str) -> str:
    parsed = urlparse((url or "").strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", "", ""))


def with_http_https_variants(url: str) -> List[str]:
    parsed = urlparse(url)
    path = parsed.path or "/"
    return [
        urlunparse(("https", parsed.netloc, path, "", "", "")),
        urlunparse(("http", parsed.netloc, path, "", "", "")),
    ]


def with_www_variants(url: str) -> List[str]:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    host = parsed.netloc.lower()
    path = parsed.path or "/"

    hosts = {host}
    if host.startswith("www."):
        hosts.add(host[4:])
    else:
        hosts.add("www." + host)

    return [urlunparse((scheme, h, path, "", "", "")) for h in hosts]


def get_slug_tokens(url: str) -> List[str]:
    path = urlparse(url).path.lower()
    parts = re.split(r"[^a-z0-9]+", path)

    stop = {
        "news", "feature", "features", "article", "articles", "www", "com", "amp",
        "the", "and", "for", "with", "from", "that", "this", "have", "been",
        "2023", "2024", "2025", "2026", "2027", "2028",
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
    }

    return [p for p in parts if p and len(p) >= 4 and p not in stop]


def build_lookup_candidates(input_url: str, live_canonical_url: str) -> List[str]:
    seeds: List[str] = []

    for u in [
        input_url,
        normalize_url(input_url),
        live_canonical_url,
        normalize_url(live_canonical_url),
    ]:
        if u:
            seeds.append(u)
            seeds.append(drop_query_and_fragment(u))

    candidates: List[str] = []
    seen = set()

    for seed in seeds:
        if not seed:
            continue

        variants = [seed]
        variants.extend(with_http_https_variants(seed))

        more = []
        for v in variants:
            more.extend(with_www_variants(v))
        variants.extend(more)

        for v in variants:
            v = drop_query_and_fragment(normalize_url(v))
            if v and v not in seen:
                seen.add(v)
                candidates.append(v)

    return candidates


# -------------------------------------------------------------------
# LIVE CANONICAL DETECTION
# -------------------------------------------------------------------

def fetch_live_canonical(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )

        html = resp.text
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        canonical = soup.find("link", attrs={"rel": lambda x: x and "canonical" in x})
        if canonical and canonical.get("href"):
            href = canonical["href"].strip()
            return normalize_url(urljoin(resp.url, href))

        og_url = soup.find("meta", attrs={"property": "og:url"})
        if og_url and og_url.get("content"):
            href = og_url["content"].strip()
            return normalize_url(urljoin(resp.url, href))

        return normalize_url(resp.url)

    except Exception:
        return None


# -------------------------------------------------------------------
# PLAYWRIGHT HELPERS
# -------------------------------------------------------------------

def text_from_locator(locator) -> str:
    try:
        return (locator.inner_text() or "").strip()
    except Exception:
        return ""


def safe_attr(locator, attr: str) -> str:
    try:
        return locator.get_attribute(attr) or ""
    except Exception:
        return ""


def safe_goto(page, url: str, timeout_ms: int = 30000) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(1200)


def is_real_snapshot_url(url: str) -> bool:
    return bool(url and REAL_SNAPSHOT_RE.match(url))


def choose_search_form_input(page):
    """
    Pick the LOWER blue search form, not the top red save form.
    """
    selectors = [
        "input[placeholder='query']",
        "form input[placeholder='query']",
        "input[name='q']",
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            count = loc.count()
            for i in range(count):
                candidate = loc.nth(i)
                if candidate.is_visible():
                    return candidate
        except Exception:
            pass

    # Fallback: choose the lowest visible text input
    try:
        inputs = page.locator("input[type='text']").all()
    except Exception:
        inputs = []

    visible = []
    for inp in inputs:
        try:
            if inp.is_visible():
                box = inp.bounding_box()
                if box:
                    visible.append((inp, box["y"]))
        except Exception:
            continue

    if visible:
        visible.sort(key=lambda x: x[1], reverse=True)
        return visible[0][0]

    return None


# -------------------------------------------------------------------
# RESULT PAGE SCORING / CLICKTHROUGH
# -------------------------------------------------------------------

def candidate_score(target_url: str, href_abs: str, text: str) -> int:
    target_norm = drop_query_and_fragment(normalize_url(target_url))
    target_path = urlparse(target_norm).path.rstrip("/").lower()
    target_tokens = set(get_slug_tokens(target_norm))

    blob = f"{href_abs}\n{text}".lower()
    blob_tokens = set(re.split(r"[^a-z0-9]+", blob))

    score = 0

    if is_real_snapshot_url(href_abs):
        score += 20

    if target_path and target_path in blob:
        score += 30

    overlap = len(target_tokens.intersection(blob_tokens))
    score += overlap * 4

    if overlap >= 3:
        score += 10

    if "bloomberg" in blob and "bloomberg" in target_norm:
        score += 3

    if "ft" in blob and "ft.com" in target_norm:
        score += 3

    return score


def collect_snapshot_candidates_from_page(page, target_url: str, notes: List[str]) -> List[Tuple[str, str, int]]:
    candidates: List[Tuple[str, str, int]] = []

    current = page.url
    if is_real_snapshot_url(current):
        candidates.append((current, page.title() or "", 1000))

    try:
        anchors = page.locator("a").all()
    except Exception:
        anchors = []

    for a in anchors:
        href = safe_attr(a, "href").strip()
        text = text_from_locator(a).strip()
        if not href:
            continue

        href_abs = urljoin(page.url, href)
        score = candidate_score(target_url, href_abs, text)

        if score > 0:
            candidates.append((href_abs, text, score))

    dedup = {}
    for href, text, score in candidates:
        if href not in dedup or score > dedup[href][1]:
            dedup[href] = (text, score)

    out = [(href, dedup[href][0], dedup[href][1]) for href in dedup]
    out.sort(key=lambda x: x[2], reverse=True)

    if out:
        notes.append(f"Found {len(out)} candidate(s) on page; best score={out[0][2]}: {out[0][0]}")
    else:
        notes.append("No usable snapshot candidates found on page.")

    return out


def resolve_snapshot_from_current_page(page, target_url: str, notes: List[str]) -> Optional[str]:
    current = page.url.strip()

    if is_real_snapshot_url(current):
        notes.append(f"Current page is real snapshot: {current}")
        return current

    candidates = collect_snapshot_candidates_from_page(page, target_url, notes)
    for href, _, _ in candidates:
        if is_real_snapshot_url(href):
            notes.append(f"Resolved snapshot from page candidates: {href}")
            return href

    return None


def click_best_result_if_needed(page, target_url: str, notes: List[str]) -> Optional[str]:
    """
    On archive search results page, click the best result if we are not already at a snapshot.
    """
    snap = resolve_snapshot_from_current_page(page, target_url, notes)
    if snap:
        return snap

    try:
        anchors = page.locator("a").all()
    except Exception:
        anchors = []

    ranked = []
    for a in anchors:
        href = safe_attr(a, "href").strip()
        text = text_from_locator(a).strip()
        if not href:
            continue

        href_abs = urljoin(page.url, href)
        score = candidate_score(target_url, href_abs, text)
        if score > 0:
            ranked.append((a, href_abs, score, text))

    ranked.sort(key=lambda x: x[2], reverse=True)

    if not ranked:
        notes.append("No clickable ranked results found.")
        return None

    best_anchor, best_href, best_score, best_text = ranked[0]
    notes.append(f"Clicking best result score={best_score}: {best_href}")

    try:
        best_anchor.click(timeout=5000)
        page.wait_for_timeout(1800)
    except Exception:
        try:
            safe_goto(page, best_href, timeout_ms=30000)
        except Exception as e:
            notes.append(f"Failed to open best result: {e}")
            return None

    return resolve_snapshot_from_current_page(page, target_url, notes)


# -------------------------------------------------------------------
# ARCHIVE SEARCH STRATEGIES
# -------------------------------------------------------------------

def try_search_form(page, host: str, candidate_url: str, notes: List[str]) -> Optional[str]:
    base = f"https://{host}/"
    notes.append(f"Trying archive SEARCH form on {base} with: {candidate_url}")

    try:
        safe_goto(page, base, timeout_ms=30000)

        search_input = choose_search_form_input(page)
        if search_input is None:
            notes.append(f"Could not identify lower search input on {host}")
            return None

        search_input.fill(candidate_url)
        search_input.press("Enter")
        page.wait_for_timeout(2500)

        snap = click_best_result_if_needed(page, candidate_url, notes)
        if snap:
            return snap

    except PlaywrightTimeoutError:
        notes.append(f"Timeout using search form on {host} | {candidate_url}")
    except Exception as e:
        notes.append(f"Error using search form on {host} | {candidate_url}: {e}")

    return None


def try_headline_search(page, host: str, target_url: str, notes: List[str]) -> Optional[str]:
    tokens = get_slug_tokens(target_url)
    if not tokens:
        return None

    query = " ".join(tokens[:6])
    base = f"https://{host}/"
    notes.append(f"Trying headline fallback on {base} with: {query}")

    try:
        safe_goto(page, base, timeout_ms=30000)

        search_input = choose_search_form_input(page)
        if search_input is None:
            notes.append(f"Could not identify lower search input on {host} for headline fallback")
            return None

        search_input.fill(query)
        search_input.press("Enter")
        page.wait_for_timeout(2500)

        snap = click_best_result_if_needed(page, target_url, notes)
        if snap:
            return snap

    except PlaywrightTimeoutError:
        notes.append(f"Timeout using headline fallback on {host} | {query}")
    except Exception as e:
        notes.append(f"Error using headline fallback on {host} | {query}: {e}")

    return None


def resolve_latest_snapshot(page, input_url: str, live_canonical_url: str, notes: List[str]) -> Tuple[str, str]:
    candidates = build_lookup_candidates(input_url, live_canonical_url)
    notes.append(f"Built {len(candidates)} lookup candidates.")

    # Pass 1: exact URL / normalized / host variants through archive search flow
    for candidate in candidates:
        for host in ARCHIVE_HOSTS:
            snapshot = try_search_form(page, host, candidate, notes)
            if snapshot:
                return snapshot, host

    # Pass 2: headline/slug search fallback
    for host in ARCHIVE_HOSTS:
        snapshot = try_headline_search(page, host, live_canonical_url or input_url, notes)
        if snapshot:
            return snapshot, host

    raise RuntimeError("Could not resolve a latest archive snapshot from any archive host.")


# -------------------------------------------------------------------
# SNAPSHOT PAGE EXTRACTION
# -------------------------------------------------------------------

def click_webpage_tab_if_present(page, notes: List[str]) -> None:
    for txt in ["Webpage", "webpage"]:
        try:
            tab = page.locator(f"text={txt}").first
            if tab and tab.is_visible():
                tab.click(timeout=3000)
                page.wait_for_timeout(1200)
                notes.append("Clicked Webpage tab.")
                return
        except Exception:
            pass


def clean_archive_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    bad_text_patterns = [
        r"Saved from",
        r"All snapshots from host",
        r"history",
        r"prior",
        r"next",
        r"share",
        r"download \.zip",
        r"report bug",
        r"Buy me a coffee",
        r"My url is alive and I want to archive its content",
        r"I want to search the archive for saved snapshots",
    ]
    regex = re.compile("|".join(bad_text_patterns), re.IGNORECASE)

    for node in soup.find_all(string=regex):
        parent = node.parent
        if parent and parent.name in {"div", "span", "td", "p", "small", "header", "section"}:
            try:
                parent.decompose()
            except Exception:
                pass

    return str(soup)


def extract_reader_text(html: str, url_hint: str) -> Tuple[str, str, str, str]:
    cleaned_html = clean_archive_html(html)

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

    if text:
        return article_title.strip(), byline.strip(), date.strip(), text.strip()

    soup = BeautifulSoup(cleaned_html, "lxml")
    body = soup.body or soup
    raw_text = body.get_text("\n", strip=True)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    return article_title.strip(), byline.strip(), date.strip(), raw_text.strip()


# -------------------------------------------------------------------
# MAIN RECOVERY
# -------------------------------------------------------------------

def recover_from_archive(url: str, show_browser: bool = False) -> RecoverResult:
    notes: List[str] = []

    normalized_input = normalize_url(url)
    notes.append(f"Normalized input URL: {normalized_input}")

    live_canonical = fetch_live_canonical(normalized_input)
    if live_canonical:
        notes.append(f"Live canonical discovered: {live_canonical}")
    else:
        live_canonical = normalized_input
        notes.append("Could not discover live canonical URL; using normalized input URL.")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not show_browser,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            viewport={"width": 1400, "height": 1200},
        )
        page = context.new_page()

        snapshot_url, host_used = resolve_latest_snapshot(
            page,
            normalized_input,
            live_canonical,
            notes,
        )
        notes.append(f"Resolved latest snapshot: {snapshot_url}")

        safe_goto(page, snapshot_url, timeout_ms=45000)

        click_webpage_tab_if_present(page, notes)
        page.wait_for_timeout(1200)

        page_title = page.title() or ""
        html = page.content()

        browser.close()

    article_title, byline, date, text = extract_reader_text(html, url_hint=live_canonical)
    word_count = len(text.split()) if text else 0

    if word_count >= 150:
        status = "ok"
    elif word_count > 0:
        status = "partial"
        notes.append(f"Low word count: {word_count}. Capture may be partial or extraction weak.")
    else:
        status = "failed"
        notes.append("No extracted text returned.")

    return RecoverResult(
        input_url=url,
        normalized_input_url=normalized_input,
        live_canonical_url=live_canonical,
        archive_snapshot_url=snapshot_url,
        archive_host_used=host_used,
        page_title=page_title.strip(),
        article_title=article_title,
        byline=byline,
        date=date,
        text=text,
        word_count=word_count,
        extraction_status=status,
        notes=notes,
    )


# -------------------------------------------------------------------
# OUTPUT
# -------------------------------------------------------------------

def safe_slug(s: str) -> str:
    s = s or "item"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:120]


def save_single_outputs(result: RecoverResult, output_dir: Path) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = safe_slug(urlparse(result.live_canonical_url or result.normalized_input_url).path.split("/")[-1] or "article")

    json_path = output_dir / f"{slug}.json"
    txt_path = output_dir / f"{slug}.txt"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(asdict(result), f, ensure_ascii=False, indent=2)

    with open(txt_path, "w", encoding="utf-8") as f:
        if result.article_title:
            f.write(result.article_title.strip() + "\n\n")
        if result.byline:
            f.write(f"Byline: {result.byline.strip()}\n")
        if result.date:
            f.write(f"Date: {result.date.strip()}\n")
        f.write(f"Archive Snapshot: {result.archive_snapshot_url}\n")
        f.write(f"Canonical URL: {result.live_canonical_url}\n")
        f.write(f"Word Count: {result.word_count}\n\n")
        f.write(result.text or "")

    return json_path, txt_path


def guess_url_column(df: pd.DataFrame) -> str:
    candidates = [
        "url",
        "article_url",
        "link",
        "source_url",
        "original_url",
        "target_url",
    ]
    lower_map = {c.lower(): c for c in df.columns}

    for c in candidates:
        if c in lower_map:
            return lower_map[c]

    for c in df.columns:
        if "url" in c.lower():
            return c

    raise ValueError(f"Could not find a URL column. Columns were: {list(df.columns)}")


def domain_matches(url: str, domains: List[str]) -> bool:
    if not domains:
        return True
    netloc = urlparse(url).netloc.lower()
    return any(d.lower() in netloc for d in domains)


def process_csv_queue(
    input_csv: Path,
    output_csv: Path,
    output_dir: Path,
    domains: List[str],
    limit: Optional[int],
    show_browser: bool,
    sleep_seconds: float,
    save_json_sidecars: bool,
) -> None:
    df = pd.read_csv(input_csv)
    url_col = guess_url_column(df)

    work_df = df.copy()
    work_df = work_df[work_df[url_col].notna()].copy()
    work_df[url_col] = work_df[url_col].astype(str).str.strip()
    work_df = work_df[work_df[url_col] != ""].copy()

    if domains:
        work_df = work_df[work_df[url_col].apply(lambda x: domain_matches(x, domains))].copy()

    if limit is not None:
        work_df = work_df.head(limit).copy()

    results = []
    total = len(work_df)

    if total == 0:
        print("No matching rows to process.")
        return

    print(f"Processing {total} row(s) from {input_csv}")
    print(f"Using URL column: {url_col}")

    for i, (_, row) in enumerate(work_df.iterrows(), start=1):
        url = row[url_col]
        print("=" * 100)
        print(f"[{i}/{total}] {url}")

        try:
            result = recover_from_archive(url, show_browser=show_browser)

            out_row = row.to_dict()
            out_row["archive_input_url"] = result.input_url
            out_row["archive_normalized_input_url"] = result.normalized_input_url
            out_row["archive_live_canonical_url"] = result.live_canonical_url
            out_row["archive_snapshot_url"] = result.archive_snapshot_url
            out_row["archive_host_used"] = result.archive_host_used
            out_row["archive_page_title"] = result.page_title
            out_row["archive_article_title"] = result.article_title
            out_row["archive_byline"] = result.byline
            out_row["archive_date"] = result.date
            out_row["archive_text"] = result.text
            out_row["archive_word_count"] = result.word_count
            out_row["archive_extraction_status"] = result.extraction_status
            out_row["archive_notes"] = " | ".join(result.notes)
            results.append(out_row)

            print(
                f"OK    status={result.extraction_status} "
                f"wc={result.word_count} snapshot={result.archive_snapshot_url}"
            )

            if save_json_sidecars:
                row_dir = output_dir / "json_sidecars"
                row_dir.mkdir(parents=True, exist_ok=True)
                sidecar_name = f"row_{i}.json"
                with open(row_dir / sidecar_name, "w", encoding="utf-8") as f:
                    json.dump(asdict(result), f, ensure_ascii=False, indent=2)

        except Exception as e:
            out_row = row.to_dict()
            out_row["archive_input_url"] = url
            out_row["archive_normalized_input_url"] = normalize_url(url)
            out_row["archive_live_canonical_url"] = ""
            out_row["archive_snapshot_url"] = ""
            out_row["archive_host_used"] = ""
            out_row["archive_page_title"] = ""
            out_row["archive_article_title"] = ""
            out_row["archive_byline"] = ""
            out_row["archive_date"] = ""
            out_row["archive_text"] = ""
            out_row["archive_word_count"] = 0
            out_row["archive_extraction_status"] = "failed"
            out_row["archive_notes"] = f"ERROR: {e}"
            results.append(out_row)

            print(f"FAIL  {e}")

        if sleep_seconds > 0 and i < total:
            time.sleep(sleep_seconds)

    out_df = pd.DataFrame(results)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_csv, index=False)
    print(f"\nSaved recovered rows to: {output_csv}")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Manual archive recovery tool for hard article URLs.")
    parser.add_argument("--url", help="Single URL to recover from archive.")
    parser.add_argument("--input-csv", help="CSV file containing manual queue rows.")
    parser.add_argument("--output-csv", help="Output CSV path for recovered rows.")
    parser.add_argument("--output-dir", default="archive_recover_output", help="Directory for side outputs.")
    parser.add_argument("--domains", nargs="*", default=[], help="Optional domain filters, e.g. bloomberg.com ft.com")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows processed in CSV mode.")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Sleep between CSV rows.")
    parser.add_argument("--show-browser", action="store_true", help="Show Playwright browser for debugging.")
    parser.add_argument("--save-json-sidecars", action="store_true", help="Save one JSON sidecar per recovered row in CSV mode.")
    args = parser.parse_args()

    if not args.url and not args.input_csv:
        print("ERROR: pass either --url or --input-csv", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir)

    if args.url:
        result = recover_from_archive(args.url, show_browser=args.show_browser)
        json_path, txt_path = save_single_outputs(result, output_dir)

        summary = {
            "input_url": result.input_url,
            "normalized_input_url": result.normalized_input_url,
            "live_canonical_url": result.live_canonical_url,
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
        return

    if args.input_csv and not args.output_csv:
        print("ERROR: in CSV mode you must pass --output-csv", file=sys.stderr)
        sys.exit(1)

    process_csv_queue(
        input_csv=Path(args.input_csv),
        output_csv=Path(args.output_csv),
        output_dir=output_dir,
        domains=args.domains,
        limit=args.limit,
        show_browser=args.show_browser,
        sleep_seconds=args.sleep_seconds,
        save_json_sidecars=args.save_json_sidecars,
    )


if __name__ == "__main__":
    main()
