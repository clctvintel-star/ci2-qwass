# scripts/collect.py
import argparse
import json
from datetime import datetime
from pathlib import Path

def load_yaml(path: Path) -> dict:
    import yaml
    with open(path, "r") as f:
        return yaml.safe_load(f)

def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def load_paths_config() -> dict:
    cfg_path = repo_root() / "config" / "paths.yaml"
    if not cfg_path.exists():
        raise SystemExit(f"❌ Missing config file: {cfg_path}")
    return load_yaml(cfg_path)

def resolve_project_paths(project: str) -> dict:
    cfg = load_paths_config()

    if "ci2" not in cfg or "drive_root" not in cfg["ci2"]:
        raise SystemExit("❌ config/paths.yaml must include: ci2: { drive_root: ... }")

    if "projects" not in cfg or project not in cfg["projects"]:
        valid = list(cfg.get("projects", {}).keys())
        raise SystemExit(f"❌ Unknown project '{project}'. Valid: {valid}")

    drive_root = Path(cfg["ci2"]["drive_root"])
    proj = cfg["projects"][project]

    # required keys
    for k in ["db", "outputs"]:
        if k not in proj:
            raise SystemExit(f"❌ config/paths.yaml projects.{project} missing '{k}'")

    db_path = drive_root / proj["db"]
    out_path = drive_root / proj["outputs"]

    return {"db": db_path, "outputs": out_path}

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="CI2 QWASS2 collector (V1 micro-collect)")
    parser.add_argument("--project", default="qwass2", help="Project key in config/paths.yaml (default: qwass2)")
    parser.add_argument("--firm", required=True, help="Firm name (e.g., Citadel)")
    parser.add_argument("--month", required=True, help="Month in YYYY-MM format (e.g., 2025-01)")
    parser.add_argument("--notes", default="", help="Optional notes to store in the manifest")
    args = parser.parse_args()

    paths = resolve_project_paths(args.project)

    # Output structure: outputs/qwass2/<firm>/<YYYY-MM>/
    out_dir = Path(paths["outputs"]) / args.firm / args.month
    ensure_dir(out_dir)

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    manifest = {
        "project": args.project,
        "firm": args.firm,
        "month": args.month,
        "created_utc": datetime.utcnow().isoformat() + "Z",
        "notes": args.notes,
        "repo": str(repo_root()),
        "resolved_paths": {
            "db": str(paths["db"]),
            "outputs": str(paths["outputs"]),
            "this_run_dir": str(out_dir),
        },
        "version": "v1-micro-collect",
    }

    manifest_path = out_dir / f"manifest_{stamp}.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # Create an empty "collected" CSV placeholder to prove file I/O
    csv_path = out_dir / f"stories_{args.firm}_{args.month}_{stamp}.csv"
    csv_path.write_text("id,title,source,published_at,url\n", encoding="utf-8")

    print("✅ MICRO COLLECT COMPLETE")
    print("  wrote:", manifest_path)
    print("  wrote:", csv_path)

if __name__ == "__main__":
    main()
