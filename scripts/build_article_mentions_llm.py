import argparse
import json
import os
import re
import time
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

MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing env file: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip('"').strip("'")


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
            "Could not find firm universe in config/firms.yaml. "
            "Expected one of: firms: [...], firms: {core: [...]}, or core: [...]."
        )

    firms = [str(x).strip() for x in firms if str(x).strip()]
    if not firms:
        raise ValueError("Firm universe is empty in config/firms.yaml")

    return firms


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


def build_prompt(
    article_id: str,
    title: str,
    summary: str,
    text_completeness: str,
    firms: list[str],
) -> str:
    universe = "\n".join(f"- {firm}" for firm in firms)

    return f"""
You are a careful financial-news annotator working on financial firm media coverage.

Your task is to identify which firms from the provided universe are clearly mentioned in the article and classify each mentioned firm as either CENTRAL or PERIPHERAL.

Core definitions:
- CENTRAL = the firm is one of the main subjects of the article itself. The article materially discusses the firm’s actions, strategy, leadership, business activity, results, transactions, legal issues, hiring, departures, launches, closures, investments, or other substantive developments.
- PERIPHERAL = the firm is mentioned meaningfully, but the article is not mainly about that firm.
- Do not output firms that are not actually mentioned.
- Be conservative. If a firm is not clearly mentioned in the narrative text, omit it.
- Use only the provided article text.
- Output valid JSON only.

Important distinction:
A firm is NOT CENTRAL if it appears only as:
- an industry example
- one item in a list of firms
- a competitor or peer
- a former employer of a person discussed
- a client example for another company
- a contextual reference to the industry
- a passing comparison point
- a logo, image caption, photo credit, chart label, sidebar, page furniture, or stock-image description
- a generic “firms like X, Y, Z” or “including X, Y, Z” construction
- a broad roster of clients, investors, employers, or market participants

If a firm appears only in those contexts, classify it as PERIPHERAL, not CENTRAL.

Ranking / roundup rule:
- If the article is primarily a ranking, leaderboard, roundup, listicle, "biggest firms" piece, richest-person list, awards list, or broad survey article covering many firms in parallel, then the firms should usually be classified as PERIPHERAL, not CENTRAL.
- In those articles, CENTRAL should be used only if one firm is clearly the main focus of the article rather than one entry in a broader list.
- If several firms are each given short profile blurbs as part of one larger comparison piece, classify them as PERIPHERAL.
- A firm is not CENTRAL merely because the article gives a short descriptive paragraph, biographical note, AUM figure, founder detail, or performance summary as part of a broader multi-firm roundup.

Additional guidance:
- CENTRAL usually means the article could reasonably be described as being about that firm.
- Typically an article will have 0–2 CENTRAL firms. If more than 2 firms seem CENTRAL, reconsider carefully.
- If the article mainly discusses a person, recruiter, conference, technology trend, market theme, or another company outside the firm universe, then firms from the universe are usually PERIPHERAL unless one of them is itself a main subject.
- Do not infer firms from ambiguous acronyms, similar words, or non-firm uses of names.

Be especially careful about false positives such as:
- "prices jumped" ≠ Jump Trading
- "Hudson River" ≠ Hudson River Trading
- "HRT" may refer to something unrelated
- "Citadel alumni" or "military academy" may not refer to the financial firm

FIRM UNIVERSE:
{universe}

ARTICLE_ID: {article_id}
TEXT_COMPLETENESS: {text_completeness}

TITLE:
{title}

ARTICLE TEXT:
{summary}

Return a JSON object with this exact structure:

{{
  "article_id": "{article_id}",
  "mentions": [
    {{
      "firm": "<canonical firm name from universe>",
      "mention_type": "CENTRAL" or "PERIPHERAL",
      "evidence_text": "<short excerpt from the article supporting the classification>",
      "model_confidence": "HIGH" or "MEDIUM" or "LOW"
    }}
  ]
}}

Rules:
- Only include firms from the universe that are clearly mentioned in the narrative text.
- If no firms from the universe are clearly mentioned in the narrative text, return:
  {{"article_id": "{article_id}", "mentions": []}}
- Do not include NOT_MENTIONED firms.
- Do not output any text outside the JSON object.
""".strip()


def extract_json(text: str) -> dict[str, Any]:
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


def normalize_mentions(data: dict[str, Any], firms: list[str]) -> list[dict[str, str]]:
    valid_firms = set(firms)
    mentions = data.get("mentions", [])

    cleaned = []
    for item in mentions:
        if not isinstance(item, dict):
            continue

        firm = str(item.get("firm", "")).strip()
        mention_type = str(item.get("mention_type", "")).strip().upper()
        evidence_text = str(item.get("evidence_text", "")).strip()
        model_confidence = str(item.get("model_confidence", "")).strip().upper()

        if firm not in valid_firms:
            continue
        if mention_type not in {"CENTRAL", "PERIPHERAL"}:
            continue
        if model_confidence not in {"HIGH", "MEDIUM", "LOW"}:
            model_confidence = ""

        cleaned.append(
            {
                "firm": firm,
                "mention_type": mention_type,
                "evidence_text": evidence_text,
                "model_confidence": model_confidence,
            }
        )

    deduped: dict[str, dict[str, str]] = {}
    for row in cleaned:
        firm = row["firm"]
        if firm not in deduped:
            deduped[firm] = row
            continue

        if deduped[firm]["mention_type"] == "PERIPHERAL" and row["mention_type"] == "CENTRAL":
            deduped[firm] = row

    return list(deduped.values())


def call_gemini(prompt: str, model_name: str, client: genai.Client) -> dict[str, Any]:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            return extract_json(response.text)

        except Exception as e:
            last_error = e
            print(f"Retry {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)

    raise last_error


def main():
    parser = argparse.ArgumentParser(
        description="Build ARTICLE_MENTIONS_LLM from master_articles.csv using Gemini."
    )
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
        raise EnvironmentError("Need GOOGLE_API_KEY or GEMINI_API_KEY in environment")

    client = genai.Client(api_key=api_key)

    firms = load_firms()
    paths = get_project_paths(args.project)
    db_dir = Path(paths["db"])
    db_dir.mkdir(parents=True, exist_ok=True)

    input_path = db_dir / args.input_name
    output_path = db_dir / args.output_name

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")
    if args.offset < 0:
        raise ValueError("--offset must be >= 0")

    print(f"Loading master articles: {input_path}")
    master_articles = pd.read_csv(input_path)
    master_articles["title"] = master_articles["title"].fillna("")
    master_articles["summary"] = master_articles["summary"].fillna("")
    master_articles["url"] = master_articles["url"].fillna("")

    subset = master_articles.iloc[args.offset : args.offset + args.limit].copy()

    print(f"Loaded {len(master_articles)} total articles")
    print(f"Running batch on {len(subset)} rows (offset={args.offset}, limit={args.limit})")
    print(f"Model: {args.model}")
    print(f"Firm universe size: {len(firms)}")

    timestamp = datetime.now(timezone.utc).isoformat()

    existing_rows: list[dict[str, Any]] = []
    processed_article_ids = set()

    if output_path.exists():
        existing_df = pd.read_csv(output_path)
        existing_rows = existing_df.to_dict("records")
        processed_article_ids = set(existing_df["article_id"].astype(str))
        print(f"Loaded {len(existing_rows)} existing rows")
        print(f"Found {len(processed_article_ids)} already-processed article_ids")
    else:
        print("No existing output file found, starting fresh.")

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
        original_url = str(row["url"])
        text_completeness = derive_text_completeness(row)
        summary_word_count = word_count(summary)
        summary_too_short = is_summary_too_short(summary)

        prompt = build_prompt(
            article_id=article_id,
            title=title,
            summary=summary,
            text_completeness=text_completeness,
            firms=firms,
        )

        try:
            data = call_gemini(prompt=prompt, model_name=args.model, client=client)
            mentions = normalize_mentions(data, firms)

            if not mentions:
                out_rows.append(
                    {
                        "article_id": article_id,
                        "original_url": original_url,
                        "firm": "",
                        "mention_type": "",
                        "evidence_text": "",
                        "model_confidence": "",
                        "text_completeness": text_completeness,
                        "summary_word_count": summary_word_count,
                        "summary_too_short": summary_too_short,
                        "model_name": args.model,
                        "prompt_version": PROMPT_VERSION,
                        "classified_at": timestamp,
                        "status": "no_mentions",
                        "error_message": "",
                    }
                )
            else:
                for m in mentions:
                    out_rows.append(
                        {
                            "article_id": article_id,
                            "original_url": original_url,
                            "firm": m["firm"],
                            "mention_type": m["mention_type"],
                            "evidence_text": m["evidence_text"],
                            "model_confidence": m["model_confidence"],
                            "text_completeness": text_completeness,
                            "summary_word_count": summary_word_count,
                            "summary_too_short": summary_too_short,
                            "model_name": args.model,
                            "prompt_version": PROMPT_VERSION,
                            "classified_at": timestamp,
                            "status": "ok",
                            "error_message": "",
                        }
                    )

        except Exception as e:
            out_rows.append(
                {
                    "article_id": article_id,
                    "original_url": original_url,
                    "firm": "",
                    "mention_type": "",
                    "evidence_text": "",
                    "model_confidence": "",
                    "text_completeness": text_completeness,
                    "summary_word_count": summary_word_count,
                    "summary_too_short": summary_too_short,
                    "model_name": args.model,
                    "prompt_version": PROMPT_VERSION,
                    "classified_at": timestamp,
                    "status": "error",
                    "error_message": str(e),
                }
            )
            print(f"Error on article_id={article_id}: {e}")

        processed_count += 1
        processed_article_ids.add(article_id)

        if processed_count % AUTOSAVE_EVERY == 0:
            autosave_df = pd.DataFrame(out_rows)
            autosave_df.to_csv(output_path, index=False)
            print(
                f"AUTOSAVE: {processed_count} new articles processed → "
                f"{len(autosave_df)} total rows written"
            )

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(output_path, index=False)

    print("\nBuild complete.")
    print(f"Articles in requested batch: {len(subset)}")
    print(f"New articles processed this run: {processed_count}")
    print(f"Already-processed articles skipped: {skipped_count}")
    print(f"Total output rows written: {len(out_df)}")
    print(f"Saved to: {output_path}")
    print("\nStatus counts:")
    print(out_df["status"].value_counts(dropna=False))


if __name__ == "__main__":
    main()
