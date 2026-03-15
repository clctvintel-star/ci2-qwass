import hashlib
import os
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import yaml
from dotenv import load_dotenv
from serpapi import GoogleSearch

from query_helper import load_firms_config, build_discovery_queries


# =========================================================
# CONFIG
# =========================================================

def load_paths_config(path: str = "config/paths.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


PATHS = load_paths_config()

DRIVE_ROOT = PATHS["ci2"]["drive_root"]
QWASS_DB = PATHS["projects"]["qwass2"]["db"]

# Try a couple of likely places for env path in your paths.yaml
if "keys" in PATHS and "env_file" in PATHS["keys"]:
    ENV_FILE_REL = PATHS["keys"]["env_file"]
elif "paths" in PATHS and "keys_env" in PATHS["paths"]:
    ENV_FILE_REL = PATHS["paths"]["keys_env"]
else:
    ENV_FILE_REL = "ci2_keys.env"

CORPUS_PATH = Path(DRIVE_ROOT) / QWASS_DB / "combined_ultra_raw.csv"
APPEND_DIR = Path(DRIVE_ROOT) / QWASS_DB
ENV_PATH = Path(DRIVE_ROOT) / ENV_FILE_REL if not str(ENV_FILE_REL).startswith("/content/") else Path(ENV_FILE_REL)

STAMP = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
APPEND_PATH = APPEND_DIR / f"collector_append_{STAMP}.csv"

DEFAULT_BACKFILL_START = pd.Timestamp("2018-01-01")
OVERLAP_DAYS = 7

RESULTS_PER_PAGE = 10
MAX_PAGES_PER_QUERY = 5
SLEEP_SECONDS = 1.5

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid", "oref"
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

# Only needed for legacy master corpus naming issues
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


# =========================================================
# CORPUS / WINDOWS
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


# =========================================================
# SERPAPI COLLECTION
# =========================================================

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


# =========================================================
# DEDUPE
# =========================================================

def dedupe_new_rows(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return new_df

    new_df = new_df.copy()
    new_df["_fallback_key"] = add_fallback_key(new_df)

    # internal dedupe first
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


# =========================================================
# MAIN
# =========================================================

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
    main()        end_date = pd.Timestamp.today().normalize()
        return start_date, end_date, False

    firm_df["date"] = pd.to_datetime(firm_df["date"], errors="coerce")
    firm_df = firm_df[firm_df["date"].notna()]

    if firm_df.empty:
        start_date = DEFAULT_BACKFILL_START
        end_date = pd.Timestamp.today().normalize()
        return start_date, end_date, False

    max_date = firm_df["date"].max().normalize()
    start_date = max_date - pd.Timedelta(days=OVERLAP_DAYS)
    end_date = pd.Timestamp.today().normalize()

    return start_date, end_date, True


def main():
    config = load_firms_config("config/firms.yaml")
    query_map = build_discovery_queries(config)
    corpus_df = load_existing_corpus(CORPUS_PATH)

    print("\n=== CI2 COLLECTOR PLAN ===")
    print(f"Corpus path: {CORPUS_PATH}")

    for firm, queries in query_map.items():
        start_date, end_date, has_history = compute_incremental_window(corpus_df, firm)

        history_label = "incremental" if has_history else "full backfill"
        print(f"\n{firm} [{history_label}]")
        print(f"  Window: {start_date.date()} → {end_date.date()}")
        print("  Queries:")
        for q in queries:
            print(f"    - {q}")


if __name__ == "__main__":
    main()
