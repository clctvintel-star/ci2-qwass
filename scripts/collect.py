import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import yaml
from dotenv import load_dotenv
from serpapi import GoogleSearch


# =========================================================
# CLI
# =========================================================

parser = argparse.ArgumentParser(description="CI2 QWASS discovery collector")
parser.add_argument("--firm", type=str, default=None, help="Collect only one canonical firm name")
parser.add_argument("--start-date", type=str, default=None, help="Override start date YYYY-MM-DD")
parser.add_argument("--end-date", type=str, default=None, help="Override end date YYYY-MM-DD")
parser.add_argument("--mode", type=str, default="incremental", choices=["incremental", "backfill"])
parser.add_argument("--primary-pages", type=int, default=3, help="Max pages for primary query per window")
parser.add_argument("--secondary-pages", type=int, default=1, help="Max pages for secondary query per window")
parser.add_argument("--results-per-page", type=int, default=100, help="Requested results per page")
parser.add_argument("--sleep-seconds", type=float, default=1.2, help="Delay between SerpAPI calls")
parser.add_argument("--window", type=str, default="month", choices=["month", "halfmonth"])
args = parser.parse_args()


# =========================================================
# CONFIG LOADERS
# =========================================================

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


PATHS = load_yaml("config/paths.yaml")
FIRMS_CONFIG = load_yaml("config/firms.yaml")

DRIVE_ROOT = PATHS["ci2"]["drive_root"]
QWASS_DB = PATHS["projects"]["qwass2"]["db"]

if "keys" in PATHS and "env_file" in PATHS["keys"]:
    ENV_FILE_REL = PATHS["keys"]["env_file"]
elif "paths" in PATHS and "keys_env" in PATHS["paths"]:
    ENV_FILE_REL = PATHS["paths"]["keys_env"]
else:
    ENV_FILE_REL = "ci2_keys.env"

CORPUS_PATH = Path(DRIVE_ROOT) / QWASS_DB / "combined_ultra_raw.csv"
APPEND_DIR = Path(DRIVE_ROOT) / QWASS_DB
ENV_PATH = (
    Path(DRIVE_ROOT) / ENV_FILE_REL
    if not str(ENV_FILE_REL).startswith("/content/")
    else Path(ENV_FILE_REL)
)

STAMP = pd.Timestamp.now("UTC").strftime("%Y%m%d_%H%M%S")
APPEND_PATH = APPEND_DIR / f"collector_append_{STAMP}.csv"
REPORT_PATH = APPEND_DIR / f"collector_report_{STAMP}.json"

DEFAULT_BACKFILL_START = pd.Timestamp("2018-01-01")
OVERLAP_DAYS = 7

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "oref",
}

CANONICAL_COLUMNS = [
    "article_id",
    "date",
    "time",
    "utc",
    "title",
    "url",
    "normalized_url",
    "source",
    "author1",
    "author2",
    "summary",
    "summary_source",
    "retrieved_snippet",
    "snippet_engine",
    "fund_name",
    "collected_at",
    "query_text",
    "query_window_start",
    "query_window_end",
    "was_updated",
]

CATEGORY_QUALIFIER = {
    "hedge_fund": "hedge fund",
    "trading_firm": "trading firm",
    "asset_manager": "asset manager",
}

SECONDARY_TRIGGER_THRESHOLD = 5
DUPLICATE_HEAVY_PAGE_THRESHOLD = 0.85
EARLY_STOP_EMPTY_PAGES = 1


# =========================================================
# DATA STRUCTURES
# =========================================================

@dataclass
class FirmPlan:
    canonical: str
    category: str
    primary_query: str
    secondary_queries: List[str]


# =========================================================
# HELPERS
# =========================================================

def load_env() -> str:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"Missing env file: {ENV_PATH}")

    load_dotenv(ENV_PATH)
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError(f"SERPAPI_API_KEY not found in env file: {ENV_PATH}")
    return api_key


def normalize_text(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def normalize_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""

    parsed = urlparse(url.strip())

    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = parsed.path.rstrip("/")
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    filtered_pairs = [(k, v) for k, v in query_pairs if k not in TRACKING_PARAMS]
    query = urlencode(filtered_pairs, doseq=True)

    return urlunparse((scheme, netloc, path, "", query, ""))


def make_article_id(normalized_url: str, title: str, source: str, date_value: str) -> str:
    base = normalized_url if normalized_url else f"{normalize_text(title)}|{normalize_text(source)}|{str(date_value).strip()}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def split_date(raw_date) -> Tuple[str, str, str]:
    if not raw_date:
        return "", "", ""
    parts = [p.strip() for p in str(raw_date).split(",")]
    date_part = parts[0] if len(parts) > 0 else ""
    time_part = parts[1] if len(parts) > 1 else ""
    utc_part = parts[2] if len(parts) > 2 else ""
    return date_part, time_part, utc_part


def add_fallback_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["title"].fillna("").astype(str).map(normalize_text)
        + "|"
        + df["source"].fillna("").astype(str).map(normalize_text)
        + "|"
        + df["date"].fillna("").astype(str).str.strip()
    )


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.reindex(columns=CANONICAL_COLUMNS)


def normalize_fund_name(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip()
    definitions = FIRMS_CONFIG.get("firm_definitions", {})
    for canonical, meta in definitions.items():
        aliases = [canonical] + list(meta.get("aliases_safe", []))
        normalized_aliases = {normalize_text(a) for a in aliases}
        if normalize_text(value) in normalized_aliases:
            return canonical
    return value


def canonical_stem(value: str) -> str:
    s = normalize_text(value)
    s = re.sub(r"\b(llc|l\.l\.c\.|lp|l\.p\.|ltd|inc|plc|group|management|capital)\b", "", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = " ".join(s.split())
    return s


def quoted(text: str) -> str:
    return f'"{text}"'


def build_firm_plan(canonical: str, meta: dict) -> FirmPlan:
    category = meta.get("category", "")
    qualifier = CATEGORY_QUALIFIER.get(category, "").strip()

    primary_query = quoted(canonical)
    if qualifier:
        primary_query = f'{quoted(canonical)} {qualifier}'

    aliases = [a for a in meta.get("aliases_safe", []) if str(a).strip()]
    seen_stems = {canonical_stem(canonical)}
    secondary_queries: List[str] = []

    scored_aliases = []
    for alias in aliases:
        alias_clean = str(alias).strip()
        if not alias_clean:
            continue
        stem = canonical_stem(alias_clean)
        if not stem:
            continue
        extra_words = max(0, len(stem.split()) - len(canonical_stem(canonical).split()))
        scored_aliases.append((extra_words, len(alias_clean), alias_clean, stem))

    scored_aliases.sort(key=lambda x: (x[0], x[1]), reverse=True)

    for _, _, alias_clean, stem in scored_aliases:
        if stem in seen_stems:
            continue
        seen_stems.add(stem)
        secondary_queries.append(quoted(alias_clean))
        if len(secondary_queries) >= 2:
            break

    return FirmPlan(
        canonical=canonical,
        category=category,
        primary_query=primary_query,
        secondary_queries=secondary_queries,
    )


def active_firm_plans(selected_firm: Optional[str] = None) -> List[FirmPlan]:
    core = FIRMS_CONFIG.get("firms", {}).get("core", [])
    definitions = FIRMS_CONFIG.get("firm_definitions", {})

    plans: List[FirmPlan] = []
    for canonical in core:
        if selected_firm and canonical != selected_firm:
            continue
        meta = definitions.get(canonical)
        if not meta:
            raise ValueError(f"Missing firm_definitions entry for core firm: {canonical}")
        plans.append(build_firm_plan(canonical, meta))

    if selected_firm and not plans:
        raise ValueError(f"Selected firm not found in firms.core: {selected_firm}")

    return plans


# =========================================================
# CORPUS / DEDUPE / INCREMENTAL
# =========================================================

def load_existing_corpus(corpus_path: Path) -> pd.DataFrame:
    if not corpus_path.exists():
        print(f"⚠️ Corpus not found at {corpus_path}")
        print("⚠️ Starting with empty corpus")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.read_csv(corpus_path)
    print(f"✅ Loaded existing corpus: {len(df)} rows")

    if "fund_name" in df.columns:
        df["fund_name_canonical"] = df["fund_name"].astype(str).apply(normalize_fund_name)
    else:
        df["fund_name_canonical"] = ""

    if "normalized_url" not in df.columns:
        df["normalized_url"] = df.get("url", "").fillna("").astype(str).apply(normalize_url)

    if "article_id" not in df.columns:
        df["article_id"] = [
            make_article_id(nu, t, s, d)
            for nu, t, s, d in zip(
                df["normalized_url"].astype(str),
                df.get("title", "").fillna("").astype(str),
                df.get("source", "").fillna("").astype(str),
                df.get("date", "").fillna("").astype(str),
            )
        ]

    df["_fallback_key"] = add_fallback_key(df)
    return df


def compute_incremental_window(df: pd.DataFrame, firm_name: str, mode: str):
    if args.start_date and args.end_date:
        return pd.Timestamp(args.start_date), pd.Timestamp(args.end_date), False

    if mode == "backfill":
        start_date = pd.Timestamp(args.start_date) if args.start_date else DEFAULT_BACKFILL_START
        end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.today().normalize()
        return start_date, end_date, False

    if df.empty or "date" not in df.columns:
        start_date = pd.Timestamp(args.start_date) if args.start_date else DEFAULT_BACKFILL_START
        end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.today().normalize()
        return start_date, end_date, False

    fund_col = "fund_name_canonical" if "fund_name_canonical" in df.columns else "fund_name"
    firm_df = df[df[fund_col] == firm_name].copy()

    if firm_df.empty:
        start_date = pd.Timestamp(args.start_date) if args.start_date else DEFAULT_BACKFILL_START
        end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.today().normalize()
        return start_date, end_date, False

    firm_df["date"] = pd.to_datetime(firm_df["date"], errors="coerce")
    firm_df = firm_df[firm_df["date"].notna()]

    if firm_df.empty:
        start_date = pd.Timestamp(args.start_date) if args.start_date else DEFAULT_BACKFILL_START
        end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.today().normalize()
        return start_date, end_date, False

    max_date = firm_df["date"].max().normalize()
    start_date = pd.Timestamp(args.start_date) if args.start_date else (max_date - pd.Timedelta(days=OVERLAP_DAYS))
    end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.today().normalize()

    return start_date, end_date, True


def month_windows(start_date: pd.Timestamp, end_date: pd.Timestamp) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    current = pd.Timestamp(start_date.year, start_date.month, 1)
    end_anchor = pd.Timestamp(end_date.year, end_date.month, 1)

    while current <= end_anchor:
        month_start = max(current, start_date)
        month_end = min(current + pd.offsets.MonthEnd(1), end_date)
        yield month_start, month_end
        current = current + pd.offsets.MonthBegin(1)


def halfmonth_windows(start_date: pd.Timestamp, end_date: pd.Timestamp) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    for m_start, m_end in month_windows(start_date, end_date):
        split = pd.Timestamp(m_start.year, m_start.month, 15)
        first_end = min(split, m_end)
        if m_start <= first_end:
            yield m_start, first_end
        second_start = first_end + pd.Timedelta(days=1)
        if second_start <= m_end:
            yield second_start, m_end


def iter_windows(start_date: pd.Timestamp, end_date: pd.Timestamp) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    if args.window == "halfmonth":
        yield from halfmonth_windows(start_date, end_date)
    else:
        yield from month_windows(start_date, end_date)


# =========================================================
# SERPAPI
# =========================================================

def collect_page(query: str, window_start: pd.Timestamp, window_end: pd.Timestamp, api_key: str, page: int) -> List[dict]:
    mm_dd_yyyy = lambda d: f"{d.month:02d}/{d.day:02d}/{d.year}"
    tbs = f"cdr:1,cd_min:{mm_dd_yyyy(window_start)},cd_max:{mm_dd_yyyy(window_end)}"

    params = {
        "engine": "google_news",
        "q": query,
        "api_key": api_key,
        "tbm": "nws",
        "tbs": tbs,
        "num": args.results_per_page,
        "start": page * args.results_per_page,
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    return results.get("news_results", [])


def normalize_news_result(news: dict, firm_name: str, query_text: str, window_start: pd.Timestamp, window_end: pd.Timestamp) -> dict:
    d, t, u = split_date(news.get("date"))
    title = str(news.get("title") or "").strip()
    url = str(news.get("link") or "").strip()
    normalized_url = normalize_url(url)

    snippet = news.get("snippet") or news.get("description") or ""
    source_info = news.get("source")
    source_name, author1, author2 = "", "", ""

    if isinstance(source_info, dict):
        source_name = source_info.get("name", "")
        authors = source_info.get("authors")
        if isinstance(authors, list):
            author1 = authors[0] if len(authors) > 0 else ""
            author2 = authors[1] if len(authors) > 1 else ""
        elif authors:
            author1 = str(authors)
    elif source_info is not None:
        source_name = str(source_info)

    article_id = make_article_id(normalized_url, title, source_name, d)

    return {
        "article_id": article_id,
        "date": d,
        "time": t,
        "utc": u,
        "title": title,
        "url": url,
        "normalized_url": normalized_url,
        "source": source_name,
        "author1": author1,
        "author2": author2,
        "summary": snippet,
        "summary_source": "google_news",
        "retrieved_snippet": "",
        "snippet_engine": "",
        "fund_name": firm_name,
        "collected_at": pd.Timestamp.now("UTC").isoformat(),
        "query_text": query_text,
        "query_window_start": str(window_start.date()),
        "query_window_end": str(window_end.date()),
        "was_updated": False,
    }


def page_duplicate_ratio(raw_results: int, accepted_results: int) -> float:
    if raw_results <= 0:
        return 0.0
    return 1.0 - (accepted_results / raw_results)


def collect_query_window(
    firm_name: str,
    query: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    api_key: str,
    max_pages: int,
    existing_article_ids: Set[str],
    existing_norm_urls: Set[str],
    existing_fallbacks: Set[str],
    seen_run_article_ids: Set[str],
    seen_run_norm_urls: Set[str],
    seen_run_fallbacks: Set[str],
) -> Tuple[List[dict], dict]:
    accepted_rows: List[dict] = []
    stats = {
        "firm": firm_name,
        "query": query,
        "window_start": str(window_start.date()),
        "window_end": str(window_end.date()),
        "pages_attempted": 0,
        "raw_results": 0,
        "accepted_results": 0,
        "duplicate_results": 0,
        "empty_pages": 0,
        "saturated": False,
        "stopped_reason": "",
    }

    duplicate_heavy_streak = 0

    for page in range(max_pages):
        raw = collect_page(query, window_start, window_end, api_key, page)
        stats["pages_attempted"] += 1

        if not raw:
            stats["empty_pages"] += 1
            stats["stopped_reason"] = "no_results"
            break

        raw_count = len(raw)
        page_accepted = 0

        if raw_count >= args.results_per_page:
            stats["saturated"] = True

        for news in raw:
            row = normalize_news_result(news, firm_name, query, window_start, window_end)

            if not row["title"] or not row["url"]:
                stats["duplicate_results"] += 1
                continue

            fallback = (
                normalize_text(row["title"])
                + "|"
                + normalize_text(row["source"])
                + "|"
                + str(row["date"]).strip()
            )

            is_dup = (
                (row["article_id"] and row["article_id"] in existing_article_ids)
                or (row["article_id"] and row["article_id"] in seen_run_article_ids)
                or (row["normalized_url"] and row["normalized_url"] in existing_norm_urls)
                or (row["normalized_url"] and row["normalized_url"] in seen_run_norm_urls)
                or (fallback and fallback in existing_fallbacks)
                or (fallback and fallback in seen_run_fallbacks)
            )

            if is_dup:
                stats["duplicate_results"] += 1
                continue

            accepted_rows.append(row)
            page_accepted += 1

            seen_run_article_ids.add(row["article_id"])
            if row["normalized_url"]:
                seen_run_norm_urls.add(row["normalized_url"])
            seen_run_fallbacks.add(fallback)

        stats["raw_results"] += raw_count
        stats["accepted_results"] += page_accepted

        dup_ratio = page_duplicate_ratio(raw_count, page_accepted)

        print(
            f"      Page {page + 1}: raw={raw_count} accepted={page_accepted} dup_ratio={dup_ratio:.2f}",
            flush=True,
        )

        if page_accepted == 0 and dup_ratio >= DUPLICATE_HEAVY_PAGE_THRESHOLD:
            duplicate_heavy_streak += 1
        else:
            duplicate_heavy_streak = 0

        if duplicate_heavy_streak >= EARLY_STOP_EMPTY_PAGES:
            stats["stopped_reason"] = "duplicate_heavy_page"
            break

        if raw_count < args.results_per_page:
            stats["stopped_reason"] = "short_page"
            break

        time.sleep(args.sleep_seconds)

    if not stats["stopped_reason"]:
        stats["stopped_reason"] = "page_limit"

    return accepted_rows, stats


# =========================================================
# MAIN
# =========================================================

def main():
    APPEND_DIR.mkdir(parents=True, exist_ok=True)

    api_key = load_env()
    plans = active_firm_plans(args.firm)
    corpus_df = load_existing_corpus(CORPUS_PATH)

    existing_article_ids = set(corpus_df["article_id"].fillna("").astype(str)) if not corpus_df.empty else set()
    existing_norm_urls = set(corpus_df["normalized_url"].fillna("").astype(str)) if not corpus_df.empty else set()
    existing_fallbacks = set(corpus_df["_fallback_key"].fillna("").astype(str)) if not corpus_df.empty else set()

    seen_run_article_ids: Set[str] = set()
    seen_run_norm_urls: Set[str] = set()
    seen_run_fallbacks: Set[str] = set()

    collected_rows: List[dict] = []
    report = {
        "created_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "mode": args.mode,
        "window_mode": args.window,
        "firm_filter": args.firm,
        "primary_pages": args.primary_pages,
        "secondary_pages": args.secondary_pages,
        "results_per_page": args.results_per_page,
        "append_path": str(APPEND_PATH),
        "corpus_path": str(CORPUS_PATH),
        "firm_reports": [],
        "summary": {},
    }

    print("\n=== CI2 CALL-AWARE DISCOVERY COLLECTOR ===")
    print(f"Master corpus (read-only): {CORPUS_PATH}")
    print(f"Append output:             {APPEND_PATH}")
    print(f"Report output:             {REPORT_PATH}")

    for plan in plans:
        start_date, end_date, has_history = compute_incremental_window(corpus_df, plan.canonical, args.mode)
        history_label = "incremental" if has_history else "backfill"

        firm_report = {
            "firm": plan.canonical,
            "history_label": history_label,
            "window_start": str(start_date.date()),
            "window_end": str(end_date.date()),
            "primary_query": plan.primary_query,
            "secondary_queries": plan.secondary_queries,
            "windows": [],
        }

        print(f"\n{plan.canonical} [{history_label}]")
        print(f"  Window: {start_date.date()} → {end_date.date()}")
        print(f"  Primary: {plan.primary_query}")
        if plan.secondary_queries:
            print(f"  Secondary: {plan.secondary_queries}")

        for window_start, window_end in iter_windows(start_date, end_date):
            window_meta = {
                "window_start": str(window_start.date()),
                "window_end": str(window_end.date()),
                "queries_run": [],
            }

            print(f"    Window: {window_start.date()} → {window_end.date()}")

            primary_rows, primary_stats = collect_query_window(
                firm_name=plan.canonical,
                query=plan.primary_query,
                window_start=window_start,
                window_end=window_end,
                api_key=api_key,
                max_pages=args.primary_pages,
                existing_article_ids=existing_article_ids,
                existing_norm_urls=existing_norm_urls,
                existing_fallbacks=existing_fallbacks,
                seen_run_article_ids=seen_run_article_ids,
                seen_run_norm_urls=seen_run_norm_urls,
                seen_run_fallbacks=seen_run_fallbacks,
            )
            collected_rows.extend(primary_rows)
            window_meta["queries_run"].append(primary_stats)

            if primary_stats["accepted_results"] < SECONDARY_TRIGGER_THRESHOLD and plan.secondary_queries:
                for secondary_query in plan.secondary_queries:
                    print(f"      Secondary rescue: {secondary_query}")
                    secondary_rows, secondary_stats = collect_query_window(
                        firm_name=plan.canonical,
                        query=secondary_query,
                        window_start=window_start,
                        window_end=window_end,
                        api_key=api_key,
                        max_pages=args.secondary_pages,
                        existing_article_ids=existing_article_ids,
                        existing_norm_urls=existing_norm_urls,
                        existing_fallbacks=existing_fallbacks,
                        seen_run_article_ids=seen_run_article_ids,
                        seen_run_norm_urls=seen_run_norm_urls,
                        seen_run_fallbacks=seen_run_fallbacks,
                    )
                    collected_rows.extend(secondary_rows)
                    window_meta["queries_run"].append(secondary_stats)

            firm_report["windows"].append(window_meta)

        report["firm_reports"].append(firm_report)

    if not collected_rows:
        print("\nNo net-new rows collected.")
        report["summary"] = {
            "raw_candidate_rows": 0,
            "append_rows": 0,
            "status": "empty",
        }
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        return

    new_df = pd.DataFrame(collected_rows)
    new_df = reorder_columns(new_df)

    before_internal = len(new_df)
    new_df["_fallback_key"] = add_fallback_key(new_df)
    new_df = new_df.drop_duplicates(subset=["article_id"], keep="first")
    new_df = new_df.loc[~new_df["_fallback_key"].isin(existing_fallbacks)].copy()
    after_internal = len(new_df)
    new_df = new_df.drop(columns=["_fallback_key"], errors="ignore")

    new_df.to_csv(APPEND_PATH, index=False)

    total_saturated = 0
    total_calls = 0
    for firm_report in report["firm_reports"]:
        for window in firm_report["windows"]:
            for q in window["queries_run"]:
                total_calls += q["pages_attempted"]
                if q["saturated"]:
                    total_saturated += 1

    report["summary"] = {
        "raw_candidate_rows": before_internal,
        "append_rows": after_internal,
        "api_calls_estimated": total_calls,
        "saturated_query_windows": total_saturated,
        "status": "ok",
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n=== COLLECTION SUMMARY ===")
    print(f"Collected candidate rows: {before_internal}")
    print(f"Append rows saved:        {after_internal}")
    print(f"Estimated API calls:      {total_calls}")
    print(f"Saturated windows:        {total_saturated}")
    print(f"✅ Saved append file:     {APPEND_PATH}")
    print(f"✅ Saved report file:     {REPORT_PATH}")
    print("✅ Master corpus remains untouched.")


if __name__ == "__main__":
    main()    "query_window_start",
    "query_window_end",
    "was_updated",
]

FUND_NAME_NORMALIZATION = {
    "Citadel hedge fund": "Citadel",
    "Millennium hedge fund": "Millennium",
    "Point72 hedge fund": "Point72",
    "D. E. Shaw hedge fund": "D. E. Shaw",
    "D.E. Shaw": "D. E. Shaw",
    "DE Shaw hedge fund": "D. E. Shaw",
    "Two Sigma hedge fund": "Two Sigma",
    "Balyasny Asset Management hedge": "Balyasny",
    "Schonfeld Strategic Advisors he": "Schonfeld",
    "ExodusPoint Capital hedge fund": "ExodusPoint",
    "Jane Street firm": "Jane Street",
    "Hudson River Trading firm": "Hudson River Trading",
    "Jump Trading firm": "Jump Trading",
    "'Jump Trading'": "Jump Trading",
    "Citadel": "Citadel",
    "Millennium": "Millennium",
    "Point72": "Point72",
    "D. E. Shaw": "D. E. Shaw",
    "Two Sigma": "Two Sigma",
    "Balyasny": "Balyasny",
    "Schonfeld": "Schonfeld",
    "ExodusPoint": "ExodusPoint",
    "Jane Street": "Jane Street",
    "Hudson River Trading": "Hudson River Trading",
    "Jump Trading": "Jump Trading",
}


def load_env() -> str:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"Missing env file: {ENV_PATH}")

    load_dotenv(ENV_PATH)
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise ValueError(f"SERPAPI_API_KEY not found in env file: {ENV_PATH}")

    return api_key


def normalize_fund_name(value: str) -> str:
    value = str(value).strip()
    return FUND_NAME_NORMALIZATION.get(value, value)


def normalize_text(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def normalize_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""

    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = parsed.path.rstrip("/")
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    filtered_pairs = [(k, v) for k, v in query_pairs if k not in TRACKING_PARAMS]
    query = urlencode(filtered_pairs, doseq=True)

    return urlunparse((scheme, netloc, path, "", query, ""))


def make_article_id(normalized_url: str, title: str, source: str, date_value: str) -> str:
    base = normalized_url if normalized_url else f"{normalize_text(title)}|{normalize_text(source)}|{str(date_value).strip()}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def split_date(raw_date):
    if not raw_date:
        return "", "", ""
    parts = [p.strip() for p in str(raw_date).split(",")]
    date_part = parts[0] if len(parts) > 0 else ""
    time_part = parts[1] if len(parts) > 1 else ""
    utc_part = parts[2] if len(parts) > 2 else ""
    return date_part, time_part, utc_part


def add_fallback_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["title"].fillna("").astype(str).map(normalize_text)
        + "|"
        + df["source"].fillna("").astype(str).map(normalize_text)
        + "|"
        + df["date"].fillna("").astype(str).str.strip()
    )


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.reindex(columns=CANONICAL_COLUMNS)


def load_existing_corpus(corpus_path: Path) -> pd.DataFrame:
    if not corpus_path.exists():
        print(f"⚠️ Corpus not found at {corpus_path}")
        print("⚠️ Starting with empty corpus")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.read_csv(corpus_path)
    print(f"✅ Loaded existing corpus: {len(df)} rows")

    if "fund_name" in df.columns:
        df["fund_name_canonical"] = df["fund_name"].astype(str).apply(normalize_fund_name)
    else:
        df["fund_name_canonical"] = ""

    if "normalized_url" not in df.columns:
        df["normalized_url"] = df["url"].fillna("").astype(str).apply(normalize_url)

    if "article_id" not in df.columns:
        df["article_id"] = [
            make_article_id(nu, t, s, d)
            for nu, t, s, d in zip(
                df["normalized_url"].astype(str),
                df["title"].fillna("").astype(str),
                df["source"].fillna("").astype(str),
                df["date"].fillna("").astype(str),
            )
        ]

    return df


def compute_incremental_window(df: pd.DataFrame, firm_name: str):
    if df.empty or "date" not in df.columns:
        return DEFAULT_BACKFILL_START, pd.Timestamp.today().normalize(), False

    fund_col = "fund_name_canonical" if "fund_name_canonical" in df.columns else "fund_name"
    firm_df = df[df[fund_col] == firm_name].copy()

    if firm_df.empty:
        return DEFAULT_BACKFILL_START, pd.Timestamp.today().normalize(), False

    firm_df["date"] = pd.to_datetime(firm_df["date"], errors="coerce")
    firm_df = firm_df[firm_df["date"].notna()]

    if firm_df.empty:
        return DEFAULT_BACKFILL_START, pd.Timestamp.today().normalize(), False

    max_date = firm_df["date"].max().normalize()
    start_date = max_date - pd.Timedelta(days=OVERLAP_DAYS)
    end_date = pd.Timestamp.today().normalize()

    return start_date, end_date, True


def month_windows(start_date: pd.Timestamp, end_date: pd.Timestamp):
    current = pd.Timestamp(start_date.year, start_date.month, 1)
    end_anchor = pd.Timestamp(end_date.year, end_date.month, 1)

    while current <= end_anchor:
        month_start = max(current, start_date)
        month_end = min(current + pd.offsets.MonthEnd(1), end_date)
        yield month_start, month_end
        current = current + pd.offsets.MonthBegin(1)


def collect_google_news(query: str, window_start: pd.Timestamp, window_end: pd.Timestamp, api_key: str):
    rows = []

    tbs = (
        f"cdr:1,"
        f"cd_min:{window_start.month:02d}/{window_start.day:02d}/{window_start.year},"
        f"cd_max:{window_end.month:02d}/{window_end.day:02d}/{window_end.year}"
    )

    params_base = {
        "engine": "google_news",
        "q": query,
        "api_key": api_key,
        "tbm": "nws",
        "tbs": tbs,
        "num": RESULTS_PER_PAGE,
    }

    for page in range(MAX_PAGES_PER_QUERY):
        params = dict(params_base)
        params["start"] = page * RESULTS_PER_PAGE

        search = GoogleSearch(params)
        results = search.get_dict()
        news_results = results.get("news_results", [])

        if not news_results:
            break

        rows.extend(news_results)
        time.sleep(SLEEP_SECONDS)

    return rows


def normalize_news_result(news, firm_name, query_text, window_start, window_end):
    d, t, u = split_date(news.get("date"))
    title = str(news.get("title") or "").strip()
    url = str(news.get("link") or "").strip()
    normalized_url = normalize_url(url)

    snippet = news.get("snippet") or news.get("description") or ""
    source_info = news.get("source")
    source_name, author1, author2 = "", "", ""

    if isinstance(source_info, dict):
        source_name = source_info.get("name", "")
        authors = source_info.get("authors")
        if isinstance(authors, list):
            author1 = authors[0] if len(authors) > 0 else ""
            author2 = authors[1] if len(authors) > 1 else ""
        elif authors:
            author1 = str(authors)
    elif source_info is not None:
        source_name = str(source_info)

    article_id = make_article_id(normalized_url, title, source_name, d)

    return {
        "article_id": article_id,
        "date": d,
        "time": t,
        "utc": u,
        "title": title,
        "url": url,
        "normalized_url": normalized_url,
        "source": source_name,
        "author1": author1,
        "author2": author2,
        "summary": snippet,
        "summary_source": "google_news",
        "retrieved_snippet": "",
        "snippet_engine": "",
        "fund_name": firm_name,
        "collected_at": pd.Timestamp.utcnow().isoformat(),
        "query_text": query_text,
        "query_window_start": str(window_start.date()),
        "query_window_end": str(window_end.date()),
        "was_updated": False,
    }


def dedupe_new_rows(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return new_df

    new_df = new_df.copy()
    new_df["_fallback_key"] = add_fallback_key(new_df)
    new_df = new_df.drop_duplicates(subset=["article_id"], keep="first")

    if existing_df.empty:
        return new_df.drop(columns=["_fallback_key"], errors="ignore")

    existing = existing_df.copy()
    existing["_fallback_key"] = add_fallback_key(existing)

    existing_article_ids = set(existing["article_id"].fillna("").astype(str))
    existing_norm_urls = set(existing["normalized_url"].fillna("").astype(str))
    existing_fallbacks = set(existing["_fallback_key"].fillna("").astype(str))

    keep_mask = []
    for _, row in new_df.iterrows():
        article_id = str(row["article_id"] or "")
        norm_url = str(row["normalized_url"] or "")
        fallback = str(row["_fallback_key"] or "")

        is_dup = (
            (article_id and article_id in existing_article_ids)
            or (norm_url and norm_url in existing_norm_urls)
            or (fallback and fallback in existing_fallbacks)
        )
        keep_mask.append(not is_dup)

    out = new_df.loc[keep_mask].copy()
    return out.drop(columns=["_fallback_key"], errors="ignore")


def main():
    APPEND_DIR.mkdir(parents=True, exist_ok=True)

    api_key = load_env()
    config = load_firms_config("config/firms.yaml")
    query_map = build_discovery_queries(config)
    corpus_df = load_existing_corpus(CORPUS_PATH)

    print("\n=== CI2 LIVE COLLECTOR ===")
    print(f"Master corpus: {CORPUS_PATH}")
    print(f"Append output: {APPEND_PATH}")

    collected_rows = []

    for firm, queries in query_map.items():
        start_date, end_date, has_history = compute_incremental_window(corpus_df, firm)
        history_label = "incremental" if has_history else "full backfill"

        print(f"\n{firm} [{history_label}]")
        print(f"  Window: {start_date.date()} → {end_date.date()}")

        for query in queries:
            print(f"  Query: {query}")

            for window_start, window_end in month_windows(start_date, end_date):
                print(f"    Month: {window_start.date()} → {window_end.date()}")

                raw_results = collect_google_news(
                    query=query,
                    window_start=window_start,
                    window_end=window_end,
                    api_key=api_key,
                )

                print(f"      Raw results: {len(raw_results)}")

                for news in raw_results:
                    row = normalize_news_result(
                        news=news,
                        firm_name=firm,
                        query_text=query,
                        window_start=window_start,
                        window_end=window_end,
                    )
                    if row["title"] and row["url"]:
                        collected_rows.append(row)

    if not collected_rows:
        print("\nNo rows collected from SerpAPI.")
        return

    new_df = pd.DataFrame(collected_rows)
    new_df = reorder_columns(new_df)

    before_internal = len(new_df)
    new_df = new_df.drop_duplicates(subset=["article_id"], keep="first")
    after_internal = len(new_df)

    deduped_df = dedupe_new_rows(new_df, corpus_df)
    append_count = len(deduped_df)

    print("\n=== COLLECTION SUMMARY ===")
    print(f"Collected raw normalized rows: {before_internal}")
    print(f"After internal dedupe: {after_internal}")
    print(f"Net-new rows for append file: {append_count}")

    if append_count == 0:
        print("No net-new rows found. Master corpus remains untouched.")
        return

    deduped_df.to_csv(APPEND_PATH, index=False)

    print(f"✅ Saved append file: {APPEND_PATH}")
    print("✅ Master corpus remains untouched.")


if __name__ == "__main__":
    main()
