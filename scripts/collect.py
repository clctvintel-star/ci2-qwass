import os

import pandas as pd

import yaml

def load_paths_config(path="config/paths.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

from query_helper import load_firms_config, build_discovery_queries


paths_config = load_paths_config()
drive_root = paths_config["ci2"]["drive_root"]
qwass_db = paths_config["projects"]["qwass2"]["db"]

from pathlib import Path

CORPUS_PATH = str(Path(drive_root) / qwass_db / "combined_ultra_raw.csv")

DEFAULT_BACKFILL_START = pd.Timestamp("2018-01-01")
OVERLAP_DAYS = 7


def load_existing_corpus(corpus_path: str) -> pd.DataFrame:
    if not os.path.isfile(corpus_path):
        print(f"⚠️ Corpus not found at {corpus_path}")
        print("⚠️ Starting with empty corpus")
        return pd.DataFrame(columns=[
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
        ])

    df = pd.read_csv(corpus_path)
    print(f"✅ Loaded existing corpus: {len(df)} rows")
    return df


def compute_incremental_window(df: pd.DataFrame, firm_name: str):
    if df.empty or "fund_name" not in df.columns or "date" not in df.columns:
        start_date = DEFAULT_BACKFILL_START
        end_date = pd.Timestamp.today().normalize()
        return start_date, end_date, False

    firm_df = df[df["fund_name"] == firm_name].copy()

    if firm_df.empty:
        start_date = DEFAULT_BACKFILL_START
        end_date = pd.Timestamp.today().normalize()
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

    for firm, queries in query_map.items():
        start_date, end_date, has_history = compute_incremental_window(corpus_df, firm)

        history_label = "incremental" if has_history else "full backfill"
        print(f"\n{firm} [{history_label}]")
        print(f"  Window: {start_date.date()} → {end_date.date()}")
        print("  Queries:")
        for q in queries:
            print(f"    - {q}")


if __name__ == "__main__":
    main()        "created_utc": datetime.now(timezone.utc).isoformat(),
        "out_dir": str(out_dir),
        "files": {},
        "notes": "V1 micro-collect: no API calls yet; proves folder + file writes work.",
    }

    # Minimal story schema (you’ll expand later)
    fieldnames = [
        "id",
        "firm",
        "month",
        "title",
        "source",
        "published_utc",
        "url",
        "snippet",
    ]

    rows = [
        {
            "id": f"dummy-{stamp}",
            "firm": firm,
            "month": month,
            "title": f"[DUMMY] Micro collect for {firm} {month}",
            "source": "ci2-smoketest",
            "published_utc": datetime.now(timezone.utc).isoformat(),
            "url": "",
            "snippet": "This is a placeholder row to validate the new architecture.",
        }
    ]

    csv_path = out_dir / f"stories_{safe_slug(firm)}_{month}_{stamp}.csv"
    manifest_path = out_dir / f"manifest_{stamp}.json"

    write_csv(csv_path, rows, fieldnames)
    manifest["files"]["stories_csv"] = str(csv_path)

    write_manifest(manifest_path, manifest)
    manifest["files"]["manifest_json"] = str(manifest_path)

    print("✅ Micro collect complete")
    print("Output dir:", out_dir)
    print("CSV:", csv_path.name)
    print("Manifest:", manifest_path.name)


if __name__ == "__main__":
    main()
