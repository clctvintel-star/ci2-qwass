# scripts/build_article_mentions_llm.py

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from scripts.env import get_project_paths, get_keys_env_path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIRMS_CONFIG_PATH = REPO_ROOT / "config" / "firms.yaml"
PROMPT_VERSION = "article_mentions_llm_v1"


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

    # Supports:
    # 1) firms: [ ... ]
    # 2) firms: { core: [ ... ], ... }
    # 3) top-level core: [ ... ]
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


def derive_text_completeness(row: pd.Series) -> str:
    title = str(row.get("title", "") or "").strip()
    summary = str(row.get("summary", "") or "").strip()

    if title and not summary:
        return "title_only"

    if title and summary:
        if len(summary) > 4000:
            return "full_text"
        if len(summary) > 1500:
            return "extended_summary"
        return "title_plus_summary"

    return "title_only"


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

Definitions:
- CENTRAL = the firm is a primary subject of the story, named in the headline or discussed as a main actor, focus, or target.
- PERIPHERAL = the firm is mentioned meaningfully but is not a main focus of the story.
- Do not output firms that are not actually mentioned.
- Do not infer firms from people, industries, ambiguous acronyms, or similar words unless the article text clearly refers to the firm.
- Be conservative. If a firm is not clearly mentioned, omit it.
- Use only the provided article text.
- Output valid JSON only.

Be especially careful about false positives such as:
- "prices jumped" ≠ Jump Trading
- "Hudson River" ≠ Hudson River Trading
- "HRT" may refer to something unrelated
- "Citadel alumni" or "military academy" may not refer to the financial firm
- lists of firms may imply PERIPHERAL mention, not CENTRAL

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
- Only include firms from the universe that are clearly mentioned in the text.
- If no firms from the universe are clearly mentioned, return:
  {{"article_id": "{article_id}", "mentions": []}}
- Do not include NOT_MENTIONED firms.
- Do not output any text outside the JSON object.
""".strip()


def extract_json(text: str) -> dict[str, Any]:
    text = text.strip()

    # Remove fenced code block if present
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

    # Deduplicate same firm if model repeats it; prefer CENTRAL over PERIPHERAL
    deduped: dict[str, dict[str, str]] = {}
    for row in cleaned:
        firm = row["firm"]
        if firm not in deduped:
            deduped[firm] = row
            continue

        if deduped[firm]["mention_type"] == "PERIPHERAL" and row["mention_type"] == "CENTRAL":
            deduped[firm] = row

    return list(deduped.values())


def call_gemini(prompt: str, model_name: str) -> dict[str, Any]:
    try:
        import google.generativeai as genai
    except ImportError as e:
        raise ImportError(
            "google-generativeai is not installed. In Colab run: "
            "!pip install -q google-generativeai"
        ) from e

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("Need GOOGLE_API_KEY or GEMINI_API_KEY in environment")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
        },
    )

    return extract_json(response.text)


def main():
    parser = argparse.ArgumentParser(
        description="Build ARTICLE_MENTIONS_LLM from master_articles.csv using Gemini."
    )
    parser.add_argument(
        "--project",
        default="qwass2",
        help="Project key from config/paths.yaml (default: qwass2)",
    )
    parser.add_argument(
        "--input-name",
        default="master_articles.csv",
        help="Input CSV filename in db dir",
    )
    parser.add_argument(
        "--output-name",
        default="article_mentions_llm_v1.csv",
        help="Output CSV filename in db dir",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Number of articles to classify (default: 25 for pilot)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Start row offset in master_articles (default: 0)",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash"),
        help="Gemini model name",
    )
    args = parser.parse_args()

    keys_path = get_keys_env_path()
    load_env_file(keys_path)

    firms = load_firms()
    paths = get_project_paths(args.project)
    db_dir = Path(paths["db"])
    db_dir.mkdir(parents=True, exist_ok=True)

    input_path = db_dir / args.input_name
    output_path = db_dir / args.output_name

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    print(f"Loading master articles: {input_path}")
    master_articles = pd.read_csv(input_path)

    if args.offset < 0:
        raise ValueError("--offset must be >= 0")

    subset = master_articles.iloc[args.offset : args.offset + args.limit].copy()
    subset["title"] = subset["title"].fillna("")
    subset["summary"] = subset["summary"].fillna("")

    print(f"Loaded {len(master_articles)} total articles")
    print(f"Running pilot on {len(subset)} rows (offset={args.offset}, limit={args.limit})")
    print(f"Model: {args.model}")
    print(f"Firm universe size: {len(firms)}")

    out_rows: list[dict[str, Any]] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for _, row in subset.iterrows():
        article_id = str(row["article_id"])
        title = str(row.get("title", "") or "")
        summary = str(row.get("summary", "") or "")
        text_completeness = derive_text_completeness(row)

        prompt = build_prompt(
            article_id=article_id,
            title=title,
            summary=summary,
            text_completeness=text_completeness,
            firms=firms,
        )

        try:
            data = call_gemini(prompt=prompt, model_name=args.model)
            mentions = normalize_mentions(data, firms)

            if not mentions:
                out_rows.append(
                    {
                        "article_id": article_id,
                        "firm": "",
                        "mention_type": "",
                        "evidence_text": "",
                        "model_confidence": "",
                        "text_completeness": text_completeness,
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
                            "firm": m["firm"],
                            "mention_type": m["mention_type"],
                            "evidence_text": m["evidence_text"],
                            "model_confidence": m["model_confidence"],
                            "text_completeness": text_completeness,
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
                    "firm": "",
                    "mention_type": "",
                    "evidence_text": "",
                    "model_confidence": "",
                    "text_completeness": text_completeness,
                    "model_name": args.model,
                    "prompt_version": PROMPT_VERSION,
                    "classified_at": timestamp,
                    "status": "error",
                    "error_message": str(e),
                }
            )
            print(f"Error on article_id={article_id}: {e}")

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(output_path, index=False)

    print("\nBuild complete.")
    print(f"Articles processed: {len(subset)}")
    print(f"Output rows written: {len(out_df)}")
    print(f"Saved to: {output_path}")
    print("\nStatus counts:")
    print(out_df["status"].value_counts(dropna=False))


if __name__ == "__main__":
    main()
