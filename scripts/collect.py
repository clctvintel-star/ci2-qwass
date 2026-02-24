# scripts/collect.py
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.env import get_project_paths


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def safe_slug(s: str) -> str:
    # simple filesystem-safe slug (keep letters/numbers/._-)
    out = []
    for ch in s.strip():
        if ch.isalnum() or ch in ("-", "_", ".", " "):
            out.append(ch)
    return "".join(out).strip().replace(" ", "_")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_manifest(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    ap = argparse.ArgumentParser(description="CI2 QWASS2 collector (V1 micro-collect).")
    ap.add_argument("--project", default="qwass2", help="Project key in config/paths.yaml (default: qwass2)")
    ap.add_argument("--firm", required=True, help='Firm name, e.g. "Citadel"')
    ap.add_argument("--month", required=True, help='Month in YYYY-MM, e.g. "2025-01"')
    args = ap.parse_args()

    project = args.project.strip()
    firm = args.firm.strip()
    month = args.month.strip()

    # Resolve canonical Drive output paths via scripts/env.py
    paths = get_project_paths(project)
    out_root: Path = Path(paths["outputs"])

    # Folder convention: outputs/<project>/<Firm>/<YYYY-MM>/
    out_dir = out_root / safe_slug(firm) / month
    ensure_dir(out_dir)

    stamp = utc_stamp()

    # --- V1: dummy output proving the pipeline writes ---
    manifest = {
        "project": project,
        "firm": firm,
        "month": month,
        "created_utc": datetime.now(timezone.utc).isoformat(),
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
