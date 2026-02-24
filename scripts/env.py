# scripts/env.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import os

import yaml

# Repo root = .../ci2-qwass
REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "paths.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML structure in {path} (expected a mapping at top level).")
    return data


def load_paths_config() -> Dict[str, Any]:
    """
    Loads config/paths.yaml and returns it as a dict.
    """
    return _load_yaml(CONFIG_PATH)


def get_project_paths(project: str) -> Dict[str, Path]:
    """
    Returns resolved Drive paths for a given project, e.g.:
      get_project_paths("qwass2") -> {"db": Path(...), "outputs": Path(...)}
    """
    cfg = load_paths_config()

    if "ci2" not in cfg or not isinstance(cfg["ci2"], dict):
        raise KeyError("paths.yaml missing top-level key: ci2")

    if "projects" not in cfg or not isinstance(cfg["projects"], dict):
        raise KeyError("paths.yaml missing top-level key: projects")

    ci2_cfg = cfg["ci2"]
    projects_cfg = cfg["projects"]

    drive_root = ci2_cfg.get("drive_root")
    if not drive_root:
        raise KeyError("paths.yaml missing: ci2.drive_root")

    drive_root = Path(drive_root)

    if project not in projects_cfg:
        valid = ", ".join(sorted(projects_cfg.keys()))
        raise ValueError(f"Unknown project '{project}'. Valid options: {valid}")

    proj_cfg = projects_cfg[project]
    if not isinstance(proj_cfg, dict):
        raise ValueError(f"Invalid project config for '{project}' (expected a mapping).")

    for k in ("db", "outputs"):
        if k not in proj_cfg or not proj_cfg[k]:
            raise KeyError(f"Project '{project}' missing '{k}' in paths.yaml")

    db_path = drive_root / proj_cfg["db"]
    out_path = drive_root / proj_cfg["outputs"]

    return {"db": db_path, "outputs": out_path}


def get_keys_env_path() -> Path:
    """
    Returns the absolute path to the .env file that holds API keys.

    Priority:
    1) CI2_KEYS_ENV environment variable (lets you override in Colab)
    2) config/paths.yaml -> keys.env_file (relative to drive_root, or absolute if you set it that way)
    3) default: /content/drive/MyDrive/CI2/ci2_keys.env
    """
    # 1) Explicit override
    override = os.getenv("CI2_KEYS_ENV")
    if override:
        return Path(override)

    cfg = load_paths_config()

    # 2) YAML-configured location
    drive_root = None
    if "ci2" in cfg and isinstance(cfg["ci2"], dict):
        drive_root = cfg["ci2"].get("drive_root")

    env_file = None
    if "keys" in cfg and isinstance(cfg["keys"], dict):
        env_file = cfg["keys"].get("env_file")

    if drive_root and env_file:
        env_file_path = Path(env_file)
        # If env_file is absolute, use it; else resolve under drive_root
        return env_file_path if env_file_path.is_absolute() else Path(drive_root) / env_file_path

    # 3) Sensible default
    return Path("/content/drive/MyDrive/CI2/ci2_keys.env")
