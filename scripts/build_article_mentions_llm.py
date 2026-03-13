import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from google import genai
from google.genai import types

from scripts.env import get_project_paths, get_keys_env_path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIRMS_CONFIG_PATH = REPO_ROOT / "config" / "firms.yaml"
PROMPT_VERSION = "article_mentions_llm_v4"
DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

MIN_WORDS_FOR_VERIFICATION = 200
LIKELY_FULL_ARTICLE_WORDS = 500
AUTOSAVE_EVERY = 50


# ---------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------

def load_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing env file: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip('"').strip("'")


# ---------------------------------------------------------------------
# FIRMS
# ---------------------------------------------------------------------

def load_firms() -> list[str]:
    if not FIRMS_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing firms config: {FIRMS_CONFIG_PATH}")

    with open(FIRMS_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    if isinstance(cfg.get("firms"), list):
        firms = cfg["firms"]

    elif isinstance(cfg.get("firms"), dict):
        firms = cfg["firms"].get("core", [])

    elif isinstance(cfg.get("core"), list):
        firms = cfg["core"]

    else:
        raise ValueError(
            "Could not find firm universe in config/firms.yaml"
        )

    firms = [str(x).strip() for x in firms if str(x).strip()]

    if not firms:
        raise ValueError("Firm universe is empty")

    return firms


# ---------------------------------------------------------------------
# TEXT UTILS
# ---------------------------------------------------------------------

def word_count(text: str) -> int:
    text = (text or "").strip()

    if not text:
        return 0

    return len(re.findall(r"\b\w+\b", text))


def derive_text_completeness(row: pd.Series) -> str:
    title = str(row.get("title", "") or "").strip()
    summary = str(row.get("summary", "") or "").strip()

    wc = word_count(summary)

    if title and not summary:
        return "title_only"

    if wc < MIN_WORDS_FOR_VERIFICATION:
        return "short_snippet"

    if wc <= LIKELY_FULL_ARTICLE_WORDS:
        return "summary_or_short_article"

    return "likely_full_article"


def is_summary_too_short(summary: str) -> bool:
    return word_count(summary) < MIN_WORDS_FOR_VERIFICATION


# ---------------------------------------------------------------------
# PROMPT
# ---------------------------------------------------------------------

def build_prompt(article_id, title, summary, text_completeness, firms):

    universe = "\n".join(f"- {f}" for f in firms)

    return f"""
You are a careful financial-news annotator working on hedge fund media coverage.

Your task is to identify which firms from the provided universe are mentioned and classify them as CENTRAL or PERIPHERAL.

Definitions:

CENTRAL
The firm is a primary subject of the article.  
The article materially discusses the firm’s activity, strategy, performance, leadership, legal issues, hiring, departures, launches, closures, investments, or transactions.

PERIPHERAL
The firm is mentioned meaningfully but the article is not primarily about it.

Do not output firms not actually mentioned.

Be conservative.

---

Important rule: ranking / roundup articles

If the article is primarily a ranking, leaderboard, or list of many firms
(e.g. "largest hedge funds", "richest managers", "top funds", "billionaire portfolios"),
then firms should generally be PERIPHERAL.

Even if a firm receives a short paragraph within the ranking.

---

False positives to avoid

"prices jumped" ≠ Jump Trading  
"Hudson River" ≠ Hudson River Trading  
"HRT" may refer to something unrelated  

---

FIRM UNIVERSE
{universe}

ARTICLE_ID: {article_id}
TEXT_COMPLETENESS: {text_completeness}

TITLE:
{title}

ARTICLE TEXT:
{summary}

Return JSON only:

{{
 "article_id": "{article_id}",
 "mentions": [
   {{
     "firm": "<canonical firm name>",
     "mention_type": "CENTRAL" or "PERIPHERAL",
     "evidence_text": "<short quote from article>",
     "model_confidence": "HIGH" or "MEDIUM" or "LOW"
   }}
 ]
}}

If no firms from the universe appear:

{{"article_id": "{article_id}", "mentions": []}}
""".strip()


# ---------------------------------------------------------------------
# JSON EXTRACTION
# ---------------------------------------------------------------------

def extract_json(text):

    text = text.strip()

    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)

    except json.JSONDecodeError:

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)

        if not match:
            raise

        return json.loads(match.group(0))


# ---------------------------------------------------------------------
# NORMALIZE
# ---------------------------------------------------------------------

def normalize_mentions(data, firms):

    valid_firms = set(firms)
    mentions = data.get("mentions", [])

    cleaned = []

    for item in mentions:

        if not isinstance(item, dict):
            continue

        firm = str(item.get("firm", "")).strip()
        mention_type = str(item.get("mention_type", "")).upper()
        evidence = str(item.get("evidence_text", "")).strip()
        conf = str(item.get("model_confidence", "")).upper()

        if firm not in valid_firms:
            continue

        if mention_type not in {"CENTRAL", "PERIPHERAL"}:
            continue

        cleaned.append(
            dict(
                firm=firm,
                mention_type=mention_type,
                evidence_text=evidence,
                model_confidence=conf
            )
        )

    # dedupe
    best = {}

    for row in cleaned:

        f = row["firm"]

        if f not in best:
            best[f] = row
            continue

        if best[f]["mention_type"] == "PERIPHERAL" and row["mention_type"] == "CENTRAL":
            best[f] = row

    return list(best.values())


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--project", default="qwass2")
    parser.add_argument("--input-name", default="master_articles.csv")
    parser.add_argument("--output-name", default="article_mentions_llm_v1.csv")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--model", default=DEFAULT_MODEL)

    args = parser.parse_args()

    keys_path = get_keys_env_path()
    load_env_file(keys_path)

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")

    if not api_key:
        raise EnvironmentError("Missing Gemini API key")

    firms = load_firms()

    paths = get_project_paths(args.project)
    db_dir = Path(paths["db"])

    db_dir.mkdir(parents=True, exist_ok=True)

    input_path = db_dir / args.input_name
    output_path = db_dir / args.output_name

    master_articles = pd.read_csv(input_path)

    master_articles["title"] = master_articles["title"].fillna("")
    master_articles["summary"] = master_articles["summary"].fillna("")
    master_articles["url"] = master_articles["url"].fillna("")

    subset = master_articles.iloc[args.offset : args.offset + args.limit].copy()

    print(f"Loaded {len(master_articles)} articles")
    print(f"Batch size: {len(subset)}")
    print(f"Model: {args.model}")

    # reuse Gemini client (important speed improvement)
    client = genai.Client(api_key=api_key)

    timestamp = datetime.now(timezone.utc).isoformat()

    existing_rows = []
    processed_article_ids = set()

    if output_path.exists():

        existing_df = pd.read_csv(output_path)

        existing_rows = existing_df.to_dict("records")

        processed_article_ids = set(existing_df["article_id"].astype(str))

        print(f"Loaded {len(existing_rows)} previous rows")

    else:
        print("Starting fresh")

    out_rows = existing_rows.copy()

    processed_count = 0
    skipped_count = 0

    for _, row in subset.iterrows():

        article_id = str(row["article_id"])

        if article_id in processed_article_ids:
            skipped_count += 1
            continue

        title = str(row["title"])
        summary = str(row["summary"])
        url = str(row["url"])

        text_completeness = derive_text_completeness(row)
        summary_wc = word_count(summary)

        prompt = build_prompt(
            article_id,
            title,
            summary,
            text_completeness,
            firms
        )

        try:

            response = client.models.generate_content(
                model=args.model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json"
                )
            )

            data = extract_json(response.text)

            mentions = normalize_mentions(data, firms)

            if not mentions:

                out_rows.append(dict(
                    article_id=article_id,
                    original_url=url,
                    firm="",
                    mention_type="",
                    evidence_text="",
                    model_confidence="",
                    text_completeness=text_completeness,
                    summary_word_count=summary_wc,
                    model_name=args.model,
                    prompt_version=PROMPT_VERSION,
                    classified_at=timestamp,
                    status="no_mentions",
                    error_message=""
                ))

            else:

                for m in mentions:

                    out_rows.append(dict(
                        article_id=article_id,
                        original_url=url,
                        firm=m["firm"],
                        mention_type=m["mention_type"],
                        evidence_text=m["evidence_text"],
                        model_confidence=m["model_confidence"],
                        text_completeness=text_completeness,
                        summary_word_count=summary_wc,
                        model_name=args.model,
                        prompt_version=PROMPT_VERSION,
                        classified_at=timestamp,
                        status="ok",
                        error_message=""
                    ))

        except Exception as e:

            out_rows.append(dict(
                article_id=article_id,
                original_url=url,
                firm="",
                mention_type="",
                evidence_text="",
                model_confidence="",
                text_completeness=text_completeness,
                summary_word_count=summary_wc,
                model_name=args.model,
                prompt_version=PROMPT_VERSION,
                classified_at=timestamp,
                status="error",
                error_message=str(e)
            ))

            print(f"Error on {article_id}: {e}")

        processed_article_ids.add(article_id)
        processed_count += 1

        if processed_count % AUTOSAVE_EVERY == 0:

            pd.DataFrame(out_rows).to_csv(output_path, index=False)

            print(
                f"AUTOSAVE → {processed_count} new articles | "
                f"{len(out_rows)} rows total"
            )

    out_df = pd.DataFrame(out_rows)

    out_df.to_csv(output_path, index=False)

    print("\nBuild complete")
    print(f"Processed: {processed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Total rows: {len(out_df)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
