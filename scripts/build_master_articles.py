# scripts/build_master_articles.py

import argparse
import hashlib
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import pandas as pd

from scripts.env import get_project_paths


def normalize_url(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    try:
        parsed = urlparse(str(url).strip())
        query_params = [
            (k, v) for k, v in parse_qsl(parsed.query)
            if not k.lower().startswith("utm_")
        ]
        cleaned_query = urlencode(query_params)
        normalized = parsed._replace(query=cleaned_query, fragment="")
        return urlunparse(normalized).rstrip("/")
    except Exception:
        return str(url).strip()


def make_article_id(row):
    norm_url = row["normalized_url"]
    if norm_url:
        return hashlib.md5(norm_url.encode("utf-8")).hexdigest()
    fallback = f"{row.get('title', '')}|{row.get('date', '')}"
    return hashlib.md5(fallback.encode("utf-8")).hexdigest()


def main():
    parser = argparse.ArgumentParser(
        description="Build master_articles.csv from combined_ultra_raw.csv"
    )
    parser.add_argument(
        "--project",
        default="qwass2",
        help="Project key from config/paths.yaml (default: qwass2)",
    )
    parser.add_argument(
        "--input-name",
        default="combined_ultra_raw.csv",
        help="Input CSV filename in db dir",
    )
    parser.add_argument(
        "--output-name",
        default="master_articles.csv",
        help="Output CSV filename in db dir",
    )
    args = parser.parse_args()

    project_paths = get_project_paths(args.project)
    db_dir = Path(project_paths["db"])
    db_dir.mkdir(parents=True, exist_ok=True)

    input_path = db_dir / args.input_name
    output_path = db_dir / args.output_name

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    print(f"Loading: {input_path}")
    combined = pd.read_csv(input_path)

    combined["normalized_url"] = combined["url"].apply(normalize_url)
    combined["article_id"] = combined.apply(make_article_id, axis=1)

    master_articles = (
        combined[
            [
                "article_id",
                "date",
                "title",
                "url",
                "normalized_url",
                "source",
                "author1",
                "author2",
                "summary",
                "was_updated",
                "title_clean",
                "summary_clean",
            ]
        ]
        .sort_values(
            by="summary",
            key=lambda s: s.fillna("").str.len(),
            ascending=False,
        )
        .drop_duplicates("article_id")
    )

    master_articles = master_articles.loc[
        :, ~master_articles.columns.str.contains(r"\.1$")
    ]

    master_articles.to_csv(output_path, index=False)

    print("\nBuild complete.")
    print(f"Combined rows: {len(combined)}")
    print(f"Unique article_id: {combined['article_id'].nunique()}")
    print(f"MASTER_ARTICLES rows: {len(master_articles)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
