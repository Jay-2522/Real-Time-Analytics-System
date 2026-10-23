from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]


def test_one_event_lineage() -> None:
    subprocess.run(
        ["bash", str(ROOT / "scripts" / "lineage_check.sh")],
        cwd=ROOT,
        check=True,
        timeout=360,
    )
