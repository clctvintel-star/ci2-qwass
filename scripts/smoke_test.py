"""
CI2 Smoke Test
Verifies:
1. Google Drive is mounted
2. Required project directories exist
3. Keys file exists and loads
"""

import os
from pathlib import Path


# =========================
# CONFIG — EDIT IF NEEDED
# =========================

# Base Drive path (Colab)
DRIVE_ROOT = Path("/content/drive/MyDrive")

# CI2 project structure
CI2_ROOT = DRIVE_ROOT / "CI2"

DB_ROOT = CI2_ROOT / "db"
OUTPUTS_ROOT = CI2_ROOT / "outputs"

PROJECTS = ["qwass2", "scum2", "werk2", "dorian2"]

# Keys file
DEFAULT_KEYS_ENV = "/content/drive/MyDrive/CI2/ci2_keys.env"


# =========================
# UTILITIES
# =========================

def load_env_file(path):
    """Load .env file into dict without printing secrets."""
    env_vars = {}

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()

    return env_vars


# =========================
# SMOKE TEST
# =========================

def run_smoke_test():
    print("🚬 CI2 Smoke Test Starting...\n")

    # 1️⃣ Check Drive mount
    if not DRIVE_ROOT.exists():
        raise SystemExit("❌ Google Drive not mounted. Run drive.mount('/content/drive')")

    print("✅ Google Drive mounted")

    # 2️⃣ Check base folders
    required_paths = [CI2_ROOT, DB_ROOT, OUTPUTS_ROOT]

    for proj in PROJECTS:
        required_paths.append(DB_ROOT / proj)
        required_paths.append(OUTPUTS_ROOT / proj)

    missing = [p for p in required_paths if not p.exists()]

    if missing:
        print("❌ Missing directories:")
        for m in missing:
            print("   -", m)
        raise SystemExit("\nCreate missing folders in Drive and re-run.")

    print("✅ CI2 directory structure exists")

    # 3️⃣ Check keys file EXISTS before loading
    env_path = os.getenv("CI2_KEYS_ENV", DEFAULT_KEYS_ENV)

    if not Path(env_path).exists():
        raise SystemExit(f"❌ Keys file not found: {env_path}")

    # 4️⃣ Load keys safely
    env_vars = load_env_file(env_path)

    if not env_vars:
        raise SystemExit("❌ Keys file loaded but appears empty")

    key_names = sorted(env_vars.keys())

    print(f"✅ Keys file found: {env_path}")
    print(f"✅ Keys present ({len(key_names)}): {key_names}")

    print("\n🎉 SMOKE TEST PASSED — system ready.\n")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    run_smoke_test()
