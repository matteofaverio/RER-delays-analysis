from __future__ import annotations

import argparse
from pathlib import Path

from weather.build_daily_weather import WeatherDailyPaths, build_daily_weather


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch one day of hourly weather for all stations (Open-Meteo archive).")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--stations-csv", default="data/derived/stations.csv", help="Station catalog with lat/lon.")
    ap.add_argument("--out-dir", default="data/weather", help="Output directory for daily weather CSV.")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing output file.")
    ap.add_argument("--batch-size", type=int, default=50, help="Batch size for multi-coordinate API calls.")
    args = ap.parse_args()

    paths = WeatherDailyPaths(stations_csv=Path(args.stations_csv), out_dir=Path(args.out_dir))
    out = build_daily_weather(args.date, paths, batch_size=args.batch_size, overwrite=bool(args.overwrite))
    print(str(out))


if __name__ == "__main__":
    main()
