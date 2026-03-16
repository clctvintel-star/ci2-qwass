import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import anthropic
import pandas as pd
import requests
import trafilatura
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from newspaper import Article


# =========================================================
# CLI
# =========================================================

parser = argparse.ArgumentParser(description="CI2 QWASS enricher")
parser.add_argument("--input", type=str, required=True, help="Path to collector append CSV")
parser.add_argument("--output-dir", type=str, default=None, help="Directory for enriched outputs")
parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Delay between network calls")
parser.add_argument("--llm-sleep-seconds", type=float, default=0.4, help="Delay between Claude relevance calls")
parser.add_argument("--max-rows", type=int, default=None, help="Optional max rows for testing")
args = parser.parse_args()


# =========================================================
# CONFIG
# =========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path: str) -> dict:
    with open(REPO_ROOT / path, "r", encoding="utf-8") as f:
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

ENV_PATH = (
    Path(DRIVE_ROOT) / ENV_FILE_REL
    if not str(ENV_FILE_REL).startswith("/content/")
    else Path(ENV_FILE_REL)
)

INPUT_PATH = Path(args.input)
STAMP = pd.Timestamp.now("UTC").strftime("%Y%m%d_%H%M%S")

DEFAULT_OUTPUT_DIR = Path(args.output_dir) if args.output_dir else (Path(DRIVE_ROOT) / QWASS_DB)
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ENRICHED_PATH = DEFAULT_OUTPUT_DIR / f"collector_enriched_{STAMP}.csv"
MANUAL_QUEUE_PATH = DEFAULT_OUTPUT_DIR / f"collector_manual_queue_{STAMP}.csv"
REPORT_PATH = DEFAULT_OUTPUT_DIR / f"collector_enrich_report_{STAMP}.json"

CANONICAL_INPUT_COLUMNS = [
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

NEW_COLUMNS = [
    "relevance_decision",
    "relevance_confidence",
    "relevance_reason",
    "enrich_status",
    "word_count",
    "full_text_source",
    "boilerplate_stripped",
    "manual_review_flag",
]

OUTPUT_COLUMNS = CANONICAL_INPUT_COLUMNS + NEW_COLUMNS

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://news.google.com/",
}

# User logic:
# <200 words -> likely just summary / too short
# 200-400 -> ambiguous / often still not good enough
# >400 -> probably full article or close enough
MIN_WORDS_CLEAR_FAILURE = 150
MIN_WORDS_PARTIAL = 350
MIN_WORDS_STRONG_SUCCESS = 350

RELEVANCE_MODEL = "claude-haiku-4-5"
RELEVANCE_MAX_TOKENS = 180

CLAUDE_RETRY_ATTEMPTS = 3
REQUEST_TIMEOUT = 30

BOILERPLATE_PATTERNS = [
    r"subscribe now.*",
    r"sign up for.*newsletter.*",
    r"share this article.*",
    r"follow us on .*",
    r"all rights reserved.*",
    r"copyright\s+\d{4}.*",
    r"advertisement",
    r"recommended stories.*",
    r"read more:.*",
    r"gift this article.*",
    r"save article.*",
    r"listen to this article.*",
    r"create a free account.*",
    r"already have an account\?.*",
    r"to continue reading.*",
    r"register now.*",
    r"unlock this article.*",
    r"explore more offers.*",
    r"terms & conditions apply.*",
    r"before it.?s here, it.?s on the bloomberg terminal.*",
    r"make sense of the markets.*",
    r"jump to comments.*",
    r"sign in.*",
    r"skip to content.*",
    r"share on facebook.*",
    r"share on twitter.*",
    r"copy link.*",
    r"this story has been shared.*",
]

MANUAL_REVIEW_DOMAINS = [
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "barrons.com",
    "economist.com",
]


# =========================================================
# ENV / CLIENTS
# =========================================================

def setup_env() -> anthropic.Anthropic:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"Missing env file: {ENV_PATH}")

    load_dotenv(ENV_PATH)

    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_api_key:
        raise ValueError(f"ANTHROPIC_API_KEY not found in {ENV_PATH}")

    return anthropic.Anthropic(api_key=anthropic_api_key)


# =========================================================
# FIRM ALIAS HELPERS
# =========================================================

def normalize_text(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def get_aliases_for_firm(fund_name: str) -> List[str]:
    definitions = FIRMS_CONFIG.get("firm_definitions", {})
    meta = definitions.get(fund_name, {})
    aliases = [fund_name] + list(meta.get("aliases_safe", []))
    aliases = [a.strip() for a in aliases if str(a).strip()]
    deduped = []
    seen = set()
    for a in aliases:
        key = normalize_text(a)
        if key not in seen:
            seen.add(key)
            deduped.append(a)
    return deduped


# =========================================================
# LOAD / WORD COUNTS
# =========================================================

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in CANONICAL_INPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    for col in NEW_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def word_count(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return len(re.findall(r"\b\S+\b", text))


def should_trust_existing_text(summary_text: str, summary_source: str) -> bool:
    wc = word_count(summary_text)
    src = str(summary_source or "").strip().lower()

    if wc >= MIN_WORDS_STRONG_SUCCESS and src not in {"google_news", ""}:
        return True

    if wc >= 600:
        return True

    return False


def classify_text_length(wc: int) -> str:
    if wc < MIN_WORDS_CLEAR_FAILURE:
        return "too_short"
    if wc < MIN_WORDS_PARTIAL:
        return "partial"
    return "strong"


# =========================================================
# CLAUDE RELEVANCE GATE
# =========================================================

def parse_relevance_response(text: str) -> Tuple[str, float, str]:
    decision = "UNCERTAIN"
    confidence = 0.5
    reason = "[no reason]"

    if not text:
        return decision, confidence, reason

    d_match = re.search(r"Decision:\s*(KEEP|DROP|UNCERTAIN)", text, flags=re.IGNORECASE)
    c_match = re.search(r"Confidence:\s*([-+]?\d*\.?\d+)", text, flags=re.IGNORECASE)
    r_match = re.search(r"Reason:\s*(.*)", text, flags=re.IGNORECASE | re.DOTALL)

    if d_match:
        decision = d_match.group(1).upper()
    if c_match:
        confidence = float(c_match.group(1))
        confidence = max(0.0, min(1.0, confidence))
    if r_match:
        reason = r_match.group(1).strip()

    return decision, confidence, reason


def build_relevance_prompt(fund_name: str, aliases: List[str], title: str, snippet: str, source: str, url: str) -> str:
    alias_text = ", ".join(aliases)

    return f"""You are validating Google News discovery results for hedge fund and trading-firm research.

Task:
Decide whether this result is actually about, or at least explicitly mentions, the firm "{fund_name}" or one of its aliases.

Important rules:
- KEEP if the title/snippet plausibly refers to the correct firm, even if the mention seems minor, peripheral, or not important.
- KEEP if the result appears to be about a subsidiary, affiliate, or closely tied entity that is included in the alias list.
- KEEP if the result is a roundup, hiring story, legal story, performance story, or market story that mentions the firm.
- DROP only if this is clearly a false positive, wrong entity, unrelated person/place/thing, or obvious name collision.
- Use UNCERTAIN only when you genuinely cannot tell from the title/snippet/URL.

Do NOT judge whether the article is central enough, important enough, or worth scoring. Only judge whether it is actually referring to this firm.

Examples of DROP:
- "Jane Street" meaning a literal street
- "millennium" meaning the era, not Millennium Management
- a person named Schonfeld unrelated to the hedge fund
- a company/article clearly about some other entity with a similar name

Return exactly this format:
Decision: KEEP or DROP or UNCERTAIN
Confidence: <0.0 to 1.0>
Reason: <one short sentence>

Firm: {fund_name}
Aliases: {alias_text}
Title: {title}
Snippet: {snippet}
Source: {source}
URL: {url}
""".strip()


def call_claude_relevance(
    client: anthropic.Anthropic,
    fund_name: str,
    aliases: List[str],
    title: str,
    snippet: str,
    source: str,
    url: str,
) -> Tuple[str, float, str]:
    prompt = build_relevance_prompt(
        fund_name=fund_name,
        aliases=aliases,
        title=title,
        snippet=snippet,
        source=source,
        url=url,
    )

    last_error = None
    for attempt in range(1, CLAUDE_RETRY_ATTEMPTS + 1):
        try:
            response = client.messages.create(
                model=RELEVANCE_MODEL,
                max_tokens=RELEVANCE_MAX_TOKENS,
                temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            parts = []
            for block in response.content:
                if getattr(block, "type", None) == "text":
                    parts.append(block.text)
            text = "\n".join(parts).strip()
            return parse_relevance_response(text)
        except Exception as e:
            last_error = e
            print(f"⚠️ Relevance gate attempt {attempt}/{CLAUDE_RETRY_ATTEMPTS} failed: {e}", flush=True)
            time.sleep(2.0)

    print(f"⚠️ Relevance gate fallback due to repeated failure: {last_error}", flush=True)
    return "UNCERTAIN", 0.3, "Model call failed; defaulting to uncertain."


# =========================================================
# FETCH / EXTRACTION
# =========================================================

def fetch_html(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200 and resp.text:
            return resp.text
    except Exception:
        return None
    return None


def trafilatura_extract_from_url(url: str) -> str:
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_links=False,
            favor_precision=True,
            deduplicate=True,
        )
        return (text or "").strip()
    except Exception:
        return ""


def trafilatura_extract_from_html(html_text: str) -> str:
    try:
        text = trafilatura.extract(
            html_text,
            include_comments=False,
            include_links=False,
            favor_precision=True,
            deduplicate=True,
        )
        return (text or "").strip()
    except Exception:
        return ""


def newspaper_extract(url: str, html_text: Optional[str] = None) -> str:
    try:
        art = Article(url, language="en")
        if html_text:
            art.set_html(html_text)
            art.parse()
        else:
            art.download()
            art.parse()
        return (art.text or "").strip()
    except Exception:
        return ""


def wayback_url(url: str) -> str:
    return f"https://web.archive.org/web/0/{quote(url, safe=':/?&=%')}"


def get_domain(url: str) -> str:
    m = re.search(r"https?://([^/]+)", str(url).strip().lower())
    return m.group(1).replace("www.", "") if m else ""


# =========================================================
# CLEANUP
# =========================================================

def clean_html_to_text(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = re.sub(r"\n{2,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def strip_boilerplate(text: str) -> Tuple[str, bool]:
    if not text:
        return "", False

    original = text
    cleaned = text

    for pattern in BOILERPLATE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE | re.DOTALL)

    lines = [ln.strip() for ln in cleaned.splitlines()]
    filtered = []
    for ln in lines:
        if not ln:
            continue
        low = ln.lower()
        if len(ln.split()) <= 3 and low in {
            "share", "subscribe", "advertisement", "newsletter", "sign in", "read more"
        }:
            continue
        filtered.append(ln)

    cleaned = "\n".join(filtered)
    cleaned = re.sub(r"\n{2,}", "\n\n", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned).strip()

    changed = cleaned != original
    return cleaned, changed


# =========================================================
# ENRICHMENT CORE
# =========================================================

def try_extract_full_text(url: str) -> Tuple[str, str]:
    # 1) direct trafilatura from URL
    text = trafilatura_extract_from_url(url)
    if text:
        return text, "trafilatura"

    # 2) fetch HTML then trafilatura/newspaper/raw
    html_text = fetch_html(url)
    if html_text:
        text2 = trafilatura_extract_from_html(html_text)
        if text2:
            return text2, "trafilatura_html"

        text3 = newspaper_extract(url, html_text=html_text)
        if text3:
            return text3, "newspaper_html"

        text4 = clean_html_to_text(html_text)
        if text4:
            return text4, "raw_html_text"

    # 3) direct newspaper
    text5 = newspaper_extract(url)
    if text5:
        return text5, "newspaper"

    # 4) wayback fallback
    wb_url = wayback_url(url)
    text6 = trafilatura_extract_from_url(wb_url)
    if text6:
        return text6, "wayback_trafilatura"

    wb_html = fetch_html(wb_url)
    if wb_html:
        text7 = trafilatura_extract_from_html(wb_html)
        if text7:
            return text7, "wayback_trafilatura_html"

        text8 = newspaper_extract(wb_url, html_text=wb_html)
        if text8:
            return text8, "wayback_newspaper_html"

    return "", ""


# =========================================================
# OUTPUT HELPERS
# =========================================================

def make_manual_row(out: Dict, domain: str) -> Dict:
    return {
        "article_id": out.get("article_id", ""),
        "fund_name": out.get("fund_name", ""),
        "title": out.get("title", ""),
        "url": out.get("url", ""),
        "source": out.get("source", ""),
        "summary": out.get("summary", ""),
        "relevance_decision": out.get("relevance_decision", ""),
        "relevance_confidence": out.get("relevance_confidence", ""),
        "relevance_reason": out.get("relevance_reason", ""),
        "enrich_status": out.get("enrich_status", ""),
        "domain": domain,
        "hard_domain_flag": domain in MANUAL_REVIEW_DOMAINS,
        "word_count": out.get("word_count", 0),
        "full_text_source": out.get("full_text_source", ""),
    }


# =========================================================
# MAIN
# =========================================================

def main():
    client = setup_env()

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)
    df = ensure_columns(df)

    if args.max_rows is not None:
        df = df.head(args.max_rows).copy()

    rows_loaded = len(df)
    print(f"✅ Loaded append rows: {rows_loaded}", flush=True)
    print(f"Input:  {INPUT_PATH}", flush=True)
    print(f"Output: {ENRICHED_PATH}", flush=True)

    enriched_rows: List[Dict] = []
    manual_rows: List[Dict] = []

    report = {
        "created_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "input_file": str(INPUT_PATH),
        "enriched_file": str(ENRICHED_PATH),
        "manual_queue_file": str(MANUAL_QUEUE_PATH),
        "rows_loaded": rows_loaded,
        "relevance_counts": {"KEEP": 0, "DROP": 0, "UNCERTAIN": 0},
        "status_counts": {},
    }

    for idx, row in df.iterrows():
        out = row.to_dict()

        fund_name = str(out.get("fund_name", "")).strip()
        aliases = get_aliases_for_firm(fund_name)

        title = str(out.get("title", "") or "").strip()
        snippet = str(out.get("summary", "") or "").strip()
        source = str(out.get("source", "") or "").strip()
        url = str(out.get("url", "") or "").strip()
        summary_source = str(out.get("summary_source", "") or "").strip()

        existing_wc = word_count(snippet)
        domain = get_domain(url)

        decision, confidence, reason = call_claude_relevance(
            client=client,
            fund_name=fund_name,
            aliases=aliases,
            title=title,
            snippet=snippet,
            source=source,
            url=url,
        )

        out["relevance_decision"] = decision
        out["relevance_confidence"] = confidence
        out["relevance_reason"] = reason

        report["relevance_counts"][decision] = report["relevance_counts"].get(decision, 0) + 1

        if decision == "DROP":
            out["enrich_status"] = "dropped_relevance_gate"
            out["word_count"] = existing_wc
            out["full_text_source"] = ""
            out["boilerplate_stripped"] = False
            out["manual_review_flag"] = False
            enriched_rows.append(out)
            print(f"🗑️ DROP [{idx+1}/{rows_loaded}] {title[:90]}", flush=True)
            time.sleep(args.llm_sleep_seconds)
            continue

        # Trust existing text only if it is genuinely substantial and not just a Google snippet
        if should_trust_existing_text(snippet, summary_source):
            cleaned, changed = strip_boilerplate(snippet)
            out["summary"] = cleaned
            out["summary_source"] = out.get("summary_source") or "existing"
            out["retrieved_snippet"] = out.get("retrieved_snippet") or ""
            out["snippet_engine"] = out.get("snippet_engine") or ""
            out["was_updated"] = bool(changed)
            out["enrich_status"] = "kept_existing_long_text"
            out["word_count"] = word_count(cleaned)
            out["full_text_source"] = "existing_summary"
            out["boilerplate_stripped"] = changed
            out["manual_review_flag"] = False
            enriched_rows.append(out)
            print(f"✅ KEEP EXISTING [{idx+1}/{rows_loaded}] wc={out['word_count']} | {title[:90]}", flush=True)
            time.sleep(args.llm_sleep_seconds)
            continue

        text, source_label = try_extract_full_text(url)

        if text:
            cleaned, changed = strip_boilerplate(text)
            cleaned_wc = word_count(cleaned)
            strength = classify_text_length(cleaned_wc)

            out["summary"] = cleaned
            out["summary_source"] = source_label
            out["retrieved_snippet"] = ""
            out["snippet_engine"] = source_label
            out["was_updated"] = True
            out["word_count"] = cleaned_wc
            out["full_text_source"] = source_label
            out["boilerplate_stripped"] = changed

            if strength == "strong":
                out["enrich_status"] = f"success_{source_label}"
                out["manual_review_flag"] = False
                enriched_rows.append(out)
                print(f"✅ ENRICHED [{idx+1}/{rows_loaded}] {source_label} wc={cleaned_wc} | {title[:90]}", flush=True)
            else:
                out["enrich_status"] = f"manual_review_needed_{source_label}_{strength}"
                out["manual_review_flag"] = True
                enriched_rows.append(out)
                manual_rows.append(make_manual_row(out, domain))
                print(f"⚠️ PARTIAL [{idx+1}/{rows_loaded}] {source_label} wc={cleaned_wc} | {title[:90]}", flush=True)
        else:
            out["enrich_status"] = "manual_review_needed_failed_extraction"
            out["word_count"] = existing_wc
            out["full_text_source"] = ""
            out["boilerplate_stripped"] = False
            out["manual_review_flag"] = True

            if not snippet:
                out["summary"] = ""

            enriched_rows.append(out)
            manual_rows.append(make_manual_row(out, domain))
            print(f"⚠️ MANUAL [{idx+1}/{rows_loaded}] {domain} | {title[:90]}", flush=True)

        time.sleep(args.sleep_seconds)

    enriched_df = pd.DataFrame(enriched_rows)
    enriched_df = ensure_columns(enriched_df)
    enriched_df = enriched_df.reindex(columns=OUTPUT_COLUMNS)

    manual_df = pd.DataFrame(manual_rows)

    status_counts = enriched_df["enrich_status"].fillna("").astype(str).value_counts().to_dict()
    report["status_counts"] = status_counts
    report["manual_queue_count"] = len(manual_df)

    enriched_df.to_csv(ENRICHED_PATH, index=False)

    if len(manual_df) > 0:
        manual_df.to_csv(MANUAL_QUEUE_PATH, index=False)
    else:
        pd.DataFrame(columns=[
            "article_id",
            "fund_name",
            "title",
            "url",
            "source",
            "summary",
            "relevance_decision",
            "relevance_confidence",
            "relevance_reason",
            "enrich_status",
            "domain",
            "hard_domain_flag",
            "word_count",
            "full_text_source",
        ]).to_csv(MANUAL_QUEUE_PATH, index=False)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n=== ENRICHMENT SUMMARY ===", flush=True)
    print(f"Rows loaded:          {rows_loaded}", flush=True)
    print(f"KEEP:                 {report['relevance_counts'].get('KEEP', 0)}", flush=True)
    print(f"DROP:                 {report['relevance_counts'].get('DROP', 0)}", flush=True)
    print(f"UNCERTAIN:            {report['relevance_counts'].get('UNCERTAIN', 0)}", flush=True)
    print(f"Manual queue count:   {len(manual_df)}", flush=True)
    print(f"Saved enriched file:  {ENRICHED_PATH}", flush=True)
    print(f"Saved manual queue:   {MANUAL_QUEUE_PATH}", flush=True)
    print(f"Saved report:         {REPORT_PATH}", flush=True)


if __name__ == "__main__":
    main()
