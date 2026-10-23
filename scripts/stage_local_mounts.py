#!/usr/bin/env python3
"""Stage authorized project bind mounts when a VM cannot access the checkout."""
import hashlib
import shutil
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MOUNTS = ("backend/app", "frontend", "infrastructure_runtime", "migrations", "ml-models/artifacts", "monitoring")


def main() -> None:
    env = ROOT / ".env"
    if not env.exists():
        raise SystemExit("Run python3 scripts/setup_local.py first")
    parent = Path.home() / ".local/share/streaming-analytics"
    parent.mkdir(parents=True, exist_ok=True)
    destination = Path(tempfile.mkdtemp(prefix="dev-", dir=parent))
    ignored = shutil.ignore_patterns("node_modules", "dist", ".venv", "__pycache__",
                                    ".env", ".env.local", "*.tsbuildinfo")
    count = 0
    for relative in MOUNTS:
        source = ROOT / relative
        target = destination / relative
        shutil.copytree(source, target, ignore=ignored)
        for staged in target.rglob("*"):
            if staged.is_file():
                original = source / staged.relative_to(target)
                if hashlib.sha256(staged.read_bytes()).digest() != hashlib.sha256(original.read_bytes()).digest():
                    raise RuntimeError(f"Staged file differs: {original}")
                count += 1
    lines = env.read_text().splitlines()
    setting = f"DEV_BIND_ROOT={destination}"
    if any(line.startswith("DEV_BIND_ROOT=") for line in lines):
        lines = [setting if line.startswith("DEV_BIND_ROOT=") else line for line in lines]
    else:
        lines.append(setting)
    env.write_text("\n".join(lines) + "\n")
    print(f"Staged and SHA-256 verified {count} files at {destination}")
    print("Updated only DEV_BIND_ROOT in private .env. Credentials and existing snapshots preserved.")
    print("Application edits require restaging and container recreation; this is a source snapshot.")


if __name__ == "__main__":
    main()
