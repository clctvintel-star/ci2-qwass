# scripts/build_combined_ultra_raw.py

import argparse
from pathlib import Path
import pandas as pd

from scripts.env import get_project_paths


DEFAULT_ULTRA_PATH = "/content/drive/MyDrive/CI2/QWASS/ultra.qwass/ULTRA.Qwass.8.25.xlsx"


def main():
    parser = argparse.ArgumentParser(
        description="Build combined_ultra_raw.csv by stacking all ULTRA workbook tabs."
    )
    parser.add_argument(
        "--project",
        default="qwass2",
        help="Project key from config/paths.yaml (default: qwass2)",
    )
    parser.add_argument(
        "--input-xlsx",
        default=DEFAULT_ULTRA_PATH,
        help="Absolute path to ULTRA workbook in Drive",
    )
    parser.add_argument(
        "--output-name",
        default="combined_ultra_raw.csv",
        help="Output CSV filename (default: combined_ultra_raw.csv)",
    )

    args = parser.parse_args()

    project_paths = get_project_paths(args.project)
    db_dir = Path(project_paths["db"])
    db_dir.mkdir(parents=True, exist_ok=True)

    input_path = Path(args.input_xlsx)
    if not input_path.exists():
        raise FileNotFoundError(f"ULTRA workbook not found: {input_path}")

    print(f"Loading workbook: {input_path}")

    xls = pd.ExcelFile(input_path)
    print(f"Found {len(xls.sheet_names)} sheets:")
    print(xls.sheet_names)

    all_tabs = []

    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        df["source_tab_firm"] = sheet
        all_tabs.append(df)

    combined = pd.concat(all_tabs, ignore_index=True)

    output_path = db_dir / args.output_name
    combined.to_csv(output_path, index=False)

    print("\nBuild complete.")
    print(f"Rows: {len(combined)}")
    print(f"Columns: {list(combined.columns)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
