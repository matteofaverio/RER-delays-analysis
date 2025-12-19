from __future__ import annotations

import argparse
from pathlib import Path

from weather.build_hourly_panel import HourlyPanelConfig, build_hourly_panel


def main() -> None:
    ap = argparse.ArgumentParser(description="Build station×hour panel from merged daily files.")
    ap.add_argument("--input-dir", default="data/derived/merged", help="Directory containing merged_*.csv files.")
    ap.add_argument("--output-csv", default="data/derived/hourly_panel.csv", help="Output CSV path.")
    ap.add_argument("--min-polls", type=int, default=1, help="Minimum polls per station-hour to keep.")
    ap.add_argument("--keep-date", action="store_true", help="Keep the date column (otherwise drop it).")
    args = ap.parse_args()

    cfg = HourlyPanelConfig(
        input_dir=Path(args.input_dir),
        output_csv=Path(args.output_csv),
        min_polls_per_hour=int(args.min_polls),
        keep_date_column=bool(args.keep_date),
    )
    build_hourly_panel(cfg)
    print(str(cfg.output_csv))


if __name__ == "__main__":
    main()
