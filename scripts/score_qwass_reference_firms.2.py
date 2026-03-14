# ============================================
# CI2 • QWASS Reference-Firm Solomon Scorer
# ============================================

import os
import re
import time
import hashlib
import argparse
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from dotenv import load_dotenv
import anthropic
from google import genai
from google.genai import types
from openai import OpenAI


# =========================================================
# 0) ARGUMENTS / CONFIG
# =========================================================

parser = argparse.ArgumentParser(description="Score QWASS reference-firm rows.")
parser.add_argument("--start", type=int, default=0, help="Start row index (inclusive)")
parser.add_argument("--end", type=int, default=None, help="End row index (exclusive)")
args = parser.parse_args()

START = args.start
END = args.end

INPUT_FILE = "/content/drive/MyDrive/CI2/db/qwass2/qwass_scoring_input.csv"
OUTPUT_DIR = "/content/drive/MyDrive/CI2/db/qwass2"
ENV_PATH = "ci2_keys.env"

STAMP = datetime.now().strftime("%Y%m%d_%H%M")
SLICE_TAG = f"{START}_{END if END is not None else 'end'}"

OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"qwass_reference_scored_{SLICE_TAG}_{STAMP}.csv")
OUTPUT_XLSX = os.path.join(OUTPUT_DIR, f"qwass_reference_scored_{SLICE_TAG}_{STAMP}.xlsx")

# Model stack
GEMINI_MODEL = "gemini-2.5-pro"
CLAUDE_MODEL = "claude-sonnet-4-5"
OPENAI_TIEBREAKER_MODEL = "gpt-5.4"

# Temperatures
PRIMARY_TEMPERATURE = 0.2
TIEBREAKER_TEMPERATURE = 0.3

# Runtime controls
AUTOSAVE_EVERY = 25
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5
SOLOMON_THRESHOLD = 0.30

theme_keywords = [
    "performance", "award", "fine", "layoff", "hired", "fired",
    "departure", "scandal", "sec", "investigation", "recruiting",
    "dominant", "elite", "redemption", "resign", "poach", "mocking"
]


# =========================================================
# 1) PATHS / PROMPTS
# =========================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
PROMPTS_DIR = REPO_ROOT / "prompts"

PRIMARY_PROMPT_PATH = PROMPTS_DIR / "qwass_primary_prompt.txt"
TIEBREAKER_PROMPT_PATH = PROMPTS_DIR / "qwass_tiebreaker_prompt.txt"


def load_prompt(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing prompt file: {path}")
    return path.read_text(encoding="utf-8")


PRIMARY_PROMPT_TEMPLATE = load_prompt(PRIMARY_PROMPT_PATH)
TIEBREAKER_PROMPT_TEMPLATE = load_prompt(TIEBREAKER_PROMPT_PATH)


# =========================================================
# 2) SETUP
# =========================================================

def setup_env():
    if not os.path.isfile(ENV_PATH):
        raise FileNotFoundError(f".env not found at {ENV_PATH}")

    load_dotenv(ENV_PATH)

    openai_api_key = os.getenv("OPENAI_API_KEY")
    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    google_api_key = os.getenv("GOOGLE_API_KEY")

    if not openai_api_key:
        raise ValueError("❌ OPENAI_API_KEY not found")
    if not anthropic_api_key:
        raise ValueError("❌ ANTHROPIC_API_KEY not found")
    if not google_api_key:
        raise ValueError("❌ GOOGLE_API_KEY not found")

    print("✅ API keys loaded", flush=True)

    openai_client = OpenAI(api_key=openai_api_key)
    anthropic_client = anthropic.Anthropic(api_key=anthropic_api_key)
    google_client = genai.Client(api_key=google_api_key)

    return openai_client, anthropic_client, google_client


def make_row_id(article_id: str, reference_firm: str) -> str:
    s = f"{article_id}|{reference_firm}"
    return hashlib.md5(s.encode()).hexdigest()


def extract_theme_mentions(justification_text: str) -> str:
    just_lower = (justification_text or "").lower()
    return ", ".join(kw for kw in theme_keywords if kw in just_lower)


# =========================================================
# 3) PARSERS
# =========================================================

def parse_primary_response(text):
    sentiment = 0.0
    confidence = 0.0
    justification = "[no output]"

    if not text:
        return sentiment, confidence, justification

    s_match = re.search(r"Sentiment:\s*([-+]?\d*\.?\d+)", text, flags=re.IGNORECASE)
    c_match = re.search(r"Confidence:\s*([-+]?\d*\.?\d+)", text, flags=re.IGNORECASE)
    j_match = re.search(r"Justification:\s*(.*)", text, flags=re.IGNORECASE | re.DOTALL)

    if s_match:
        sentiment = float(s_match.group(1))
    if c_match:
        confidence = float(c_match.group(1))
    if j_match:
        justification = j_match.group(1).strip()

    sentiment = max(min(sentiment, 1.0), -1.0)
    confidence = max(min(confidence, 1.0), 0.0)

    if re.fullmatch(r"[-+]?\d*\.?\d+", justification):
        justification = "[invalid justification]"

    return sentiment, confidence, justification


def parse_tiebreaker_response(text):
    sentiment = 0.0
    confidence = 0.0
    justification = "[no tiebreaker output]"

    if not text:
        return sentiment, confidence, justification

    s_match = re.search(r"Tiebreaker Sentiment:\s*([-+]?\d*\.?\d+)", text, flags=re.IGNORECASE)
    c_match = re.search(r"Tiebreaker Confidence:\s*([-+]?\d*\.?\d+)", text, flags=re.IGNORECASE)
    j_match = re.search(r"Justification:\s*(.*)", text, flags=re.IGNORECASE | re.DOTALL)

    if s_match:
        sentiment = float(s_match.group(1))
    if c_match:
        confidence = float(c_match.group(1))
    if j_match:
        justification = j_match.group(1).strip()

    sentiment = max(min(sentiment, 1.0), -1.0)
    confidence = max(min(confidence, 1.0), 0.0)

    if re.fullmatch(r"[-+]?\d*\.?\d+", justification):
        justification = "[invalid tiebreaker justification]"

    return sentiment, confidence, justification


# =========================================================
# 4) MODEL CALLS
# =========================================================

def call_gemini(prompt, google_client):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = google_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=PRIMARY_TEMPERATURE,
                ),
            )
            return (response.text or "").strip()
        except Exception as e:
            last_error = e
            print(f"⚠️ Gemini attempt {attempt}/{MAX_RETRIES} failed: {e}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    print(f"❌ Gemini failed after retries: {last_error}", flush=True)
    return None


def call_claude(prompt, anthropic_client):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = anthropic_client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1600,
                temperature=PRIMARY_TEMPERATURE,
                messages=[{"role": "user", "content": prompt}],
            )
            parts = []
            for block in response.content:
                if getattr(block, "type", None) == "text":
                    parts.append(block.text)
            return "\n".join(parts).strip()
        except Exception as e:
            last_error = e
            print(f"⚠️ Claude attempt {attempt}/{MAX_RETRIES} failed: {e}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    print(f"❌ Claude failed after retries: {last_error}", flush=True)
    return None


def call_openai_tiebreaker(prompt, openai_client):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = openai_client.responses.create(
                model=OPENAI_TIEBREAKER_MODEL,
                input=prompt,
                temperature=TIEBREAKER_TEMPERATURE,
                max_output_tokens=1600,
            )
            return (response.output_text or "").strip()
        except Exception as e:
            last_error = e
            print(f"⚠️ Tiebreaker attempt {attempt}/{MAX_RETRIES} failed: {e}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)
    print(f"❌ Tiebreaker failed after retries: {last_error}", flush=True)
    return None


def call_primary_models_in_parallel(prompt, google_client, anthropic_client):
    with ThreadPoolExecutor(max_workers=2) as executor:
        gemini_future = executor.submit(call_gemini, prompt, google_client)
        claude_future = executor.submit(call_claude, prompt, anthropic_client)

        gemini_text = gemini_future.result()
        claude_text = claude_future.result()

    return gemini_text, claude_text


# =========================================================
# 5) PROMPT BUILDERS
# =========================================================

def build_primary_prompt(reference_firm, mention_type, title, summary):
    return PRIMARY_PROMPT_TEMPLATE.format(
        reference_firm=reference_firm,
        mention_type=mention_type,
        title=title,
        summary=summary,
    )


def build_tiebreaker_prompt(
    reference_firm,
    mention_type,
    title,
    summary,
    gemini_wass,
    claude_wass,
    gemini_text,
    claude_text,
):
    return TIEBREAKER_PROMPT_TEMPLATE.format(
        reference_firm=reference_firm,
        mention_type=mention_type,
        title=title,
        summary=summary,
        gemini_wass=gemini_wass,
        claude_wass=claude_wass,
        gemini_text=gemini_text or "",
        claude_text=claude_text or "",
    )


# =========================================================
# 6) LOAD INPUT / RESUME
# =========================================================

def load_input_df():
    if not os.path.isfile(INPUT_FILE):
        raise FileNotFoundError(f"❌ Input file not found: {INPUT_FILE}")

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_FILE)

    required_cols = [
        "article_id",
        "reference_firm",
        "mention_type",
        "title",
        "summary",
        "original_url",
        "date",
        "source",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns in input: {missing}")

    for col in ["title", "summary", "original_url", "source", "mention_type", "reference_firm"]:
        df[col] = df[col].fillna("").astype(str)

    df["row_id"] = df.apply(
        lambda r: make_row_id(str(r["article_id"]), str(r["reference_firm"])),
        axis=1,
    )

    df = df.iloc[START:END].copy()

    print(
        f"✅ Loaded scoring input rows: {len(df)} "
        f"(slice {START}:{END if END is not None else 'end'})",
        flush=True,
    )
    return df


def load_existing_results():
    existing_rows = []
    processed_row_ids = set()

    if os.path.isfile(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        existing_rows = existing_df.to_dict("records")
        processed_row_ids = set(existing_df["row_id"].astype(str))
        print(f"📂 Loaded existing output rows: {len(existing_rows)}", flush=True)
        print(f"🔁 Found already processed row_ids: {len(processed_row_ids)}", flush=True)
    else:
        print("🆕 No existing output file found, starting fresh.", flush=True)

    return existing_rows, processed_row_ids


# =========================================================
# 7) MAIN LOOP
# =========================================================

def score_rows(df, openai_client, anthropic_client, google_client, existing_rows, processed_row_ids):
    results = existing_rows.copy()
    processed_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        row_id = str(row["row_id"])

        if row_id in processed_row_ids:
            skipped_count += 1
            continue

        article_id = str(row["article_id"])
        reference_firm = str(row["reference_firm"]).strip()
        mention_type = str(row["mention_type"]).strip().upper()
        title = str(row["title"]).strip()
        summary = str(row["summary"]).strip()
        original_url = str(row["original_url"]).strip()
        date = row["date"]
        source = str(row["source"]).strip()

        prompt = build_primary_prompt(reference_firm, mention_type, title, summary)

        gemini_text, claude_text = call_primary_models_in_parallel(
            prompt=prompt,
            google_client=google_client,
            anthropic_client=anthropic_client,
        )

        gs, gc, gjust = parse_primary_response(gemini_text or "")
        cs, cc, cjust = parse_primary_response(claude_text or "")

        gw = round(gs * gc, 3)
        cw = round(cs * cc, 3)
        diff = abs(gw - cw)

        disagreement_flag = diff > SOLOMON_THRESHOLD
        tiebreaker_flag = False
        tiebreaker_sentiment = None
        tiebreaker_confidence = None
        tiebreaker_wass = None
        tiebreaker_justification = ""

        if disagreement_flag:
            tiebreaker_flag = True

            tiebreaker_prompt = build_tiebreaker_prompt(
                reference_firm=reference_firm,
                mention_type=mention_type,
                title=title,
                summary=summary,
                gemini_wass=gw,
                claude_wass=cw,
                gemini_text=gemini_text or "",
                claude_text=claude_text or "",
            )

            tb_text = call_openai_tiebreaker(tiebreaker_prompt, openai_client)
            ts, tc, tjust = parse_tiebreaker_response(tb_text or "")

            tiebreaker_sentiment = ts
            tiebreaker_confidence = tc
            tiebreaker_wass = round(ts * tc, 3)
            tiebreaker_justification = tjust

            final_sentiment = ts
            final_confidence = tc
            final_wass = tiebreaker_wass
        else:
            final_sentiment = round((gs + cs) / 2, 4)
            final_confidence = round((gc + cc) / 2, 4)
            final_wass = round((gw + cw) / 2, 3)

        theme_flags = extract_theme_mentions(
            " ".join([gjust or "", cjust or "", tiebreaker_justification or ""])
        )

        out_row = {
            "row_id": row_id,
            "article_id": article_id,
            "reference_firm": reference_firm,
            "mention_type": mention_type,
            "date": date,
            "source": source,
            "title": title,
            "summary": summary,
            "original_url": original_url,
            "gemini_model": GEMINI_MODEL,
            "claude_model": CLAUDE_MODEL,
            "openai_tiebreaker_model": OPENAI_TIEBREAKER_MODEL,
            "gemini_sentiment": gs,
            "gemini_confidence": gc,
            "gemini_wass": gw,
            "gemini_justification": gjust,
            "claude_sentiment": cs,
            "claude_confidence": cc,
            "claude_wass": cw,
            "claude_justification": cjust,
            "disagreement_flag": disagreement_flag,
            "tiebreaker_flag": tiebreaker_flag,
            "tiebreaker_sentiment": tiebreaker_sentiment,
            "tiebreaker_confidence": tiebreaker_confidence,
            "tiebreaker_wass": tiebreaker_wass,
            "tiebreaker_justification": tiebreaker_justification,
            "final_sentiment": final_sentiment,
            "final_confidence": final_confidence,
            "final_wass": final_wass,
            "theme_flags": theme_flags,
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

        results.append(out_row)
        processed_row_ids.add(row_id)
        processed_count += 1

        print(f"\n➡️ {processed_count} | {reference_firm} | {mention_type} | {title[:90]}", flush=True)
        print(f"🔮 Gemini | S={gs:.2f} | C={gc:.2f} | WASS={gw:.3f}", flush=True)
        print(f"🧠 GJust  | {gjust[:250]}", flush=True)
        print(f"📚 Claude | S={cs:.2f} | C={cc:.2f} | WASS={cw:.3f}", flush=True)
        print(f"🧠 CJust  | {cjust[:250]}", flush=True)

        if disagreement_flag:
            print(
                f"⚖️ Tiebreaker | S={tiebreaker_sentiment:.2f} | "
                f"C={tiebreaker_confidence:.2f} | WASS={tiebreaker_wass:.3f}",
                flush=True,
            )
            print(f"🧠 TJust     | {tiebreaker_justification[:250]}", flush=True)

        if processed_count % AUTOSAVE_EVERY == 0:
            autosave_df = pd.DataFrame(results)
            autosave_df.to_csv(OUTPUT_CSV, index=False)
            print(
                f"📂 Autosaved after {processed_count} new rows → "
                f"{len(autosave_df)} total rows",
                flush=True,
            )

    return results, processed_count, skipped_count


# =========================================================
# 8) FINAL SAVE
# =========================================================

def final_save(results):
    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_CSV, index=False)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="scored_reference_qwass", index=False)

        top_10 = out_df.sort_values(by="final_wass", ascending=False).head(10)
        bottom_10 = out_df.sort_values(by="final_wass", ascending=True).head(10)

        top_10.to_excel(writer, sheet_name="top_10_final_wass", index=False)
        bottom_10.to_excel(writer, sheet_name="bottom_10_final_wass", index=False)

    return out_df


# =========================================================
# 9) MAIN
# =========================================================

def main():
    openai_client, anthropic_client, google_client = setup_env()
    df = load_input_df()
    existing_rows, processed_row_ids = load_existing_results()

    results, processed_count, skipped_count = score_rows(
        df=df,
        openai_client=openai_client,
        anthropic_client=anthropic_client,
        google_client=google_client,
        existing_rows=existing_rows,
        processed_row_ids=processed_row_ids,
    )

    out_df = final_save(results)

    print("\n✅ DONE", flush=True)
    print(f"Slice processed: {START}:{END if END is not None else 'end'}", flush=True)
    print(f"New rows processed this run: {processed_count}", flush=True)
    print(f"Already-processed rows skipped: {skipped_count}", flush=True)
    print(f"Total output rows: {len(out_df)}", flush=True)
    print(f"CSV saved to:  {OUTPUT_CSV}", flush=True)
    print(f"XLSX saved to: {OUTPUT_XLSX}", flush=True)


if __name__ == "__main__":
    main()
