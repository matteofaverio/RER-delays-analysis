import argparse
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

from idf_rer.config import load_prim_config
from idf_rer.prim_client import PrimClient
from idf_rer.polling_pipeline import poll_once


def main() -> None:
    load_dotenv(find_dotenv(usecwd=True), override=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-sec", type=int, default=300)
    ap.add_argument("--iterations", type=int, default=1)
    ap.add_argument("--raw-dir", type=str, default="data/rer_raw")
    ap.add_argument("--lead-horizon-sec", type=int, default=600)
    ap.add_argument("--outliers-csv", type=str, default="data/debug/snapshot_outliers.csv")
    args = ap.parse_args()

    cfg = load_prim_config()
    client = PrimClient(api_key=cfg.api_key)

    raw_dir = Path(args.raw_dir)
    outliers = Path(args.outliers_csv) if args.outliers_csv else None

    for _ in range(int(args.iterations)):
        out = poll_once(
            client=client,
            url=cfg.estimated_timetable_url,
            raw_dir=raw_dir,
            lead_horizon_s=int(args.lead_horizon_sec),
            outliers_path=outliers,
        )
        print(f"OK: appended -> {out}")


if __name__ == "__main__":
    main()
