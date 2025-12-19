from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def run(cmd: list[str]) -> None:
    print("\n$", " ".join(cmd))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> None:
    root = Path(".").resolve()

    # 1) check sample inputs exist
    raw = root / "data/sample/rer_raw/2025-11-15.csv"
    if not raw.exists():
        raise SystemExit(f"Missing sample raw: {raw}")

    # 2) run enrichment (uses existing sample weather file)
    run([sys.executable, "scripts/enrich_sample_raw.py", "--date", "2025-11-15"])


if __name__ == "__main__":
    main()
