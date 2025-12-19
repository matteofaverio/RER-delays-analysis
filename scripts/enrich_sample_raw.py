from __future__ import annotations

import argparse
from pathlib import Path

from weather.merge_weather_delays import merge_daily_raw_with_weather


def _pick_weather_file(weather_dir: Path, date: str) -> Path:
    """
    Accept both:
      - YYYY-MM-DD_weather.csv
      - YYYY-MM-DD weather.csv
    """
    a = weather_dir / f"{date}_weather.csv"
    b = weather_dir / f"{date} weather.csv"
    if a.exists():
        return a
    if b.exists():
        return b
    raise FileNotFoundError(f"No weather file found for {date} in {weather_dir} (tried '{a.name}' and '{b.name}').")


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich one-day sample raw RER panel with stop index + weather.")
    ap.add_argument("--date", required=True, help="Service day in YYYY-MM-DD format (e.g., 2025-11-15).")
    ap.add_argument("--sample-dir", default="data/sample", help="Root folder for sample inputs.")
    ap.add_argument("--derived-dir", default="data/derived", help="Folder containing rer_stop_index.csv and stations.csv.")
    ap.add_argument("--out-dir", default="data/sample/merged", help="Output folder for merged sample.")
    ap.add_argument("--max-unmapped-share", type=float, default=0.40, help="Fail if unmapped stops exceed this share.")
    ap.add_argument("--keep-unmapped-stops", action="store_true", help="Keep unmapped stop rows (do not drop).")
    args = ap.parse_args()

    sample_dir = Path(args.sample_dir)
    derived_dir = Path(args.derived_dir)

    raw_path = sample_dir / "rer_raw" / f"{args.date}.csv"
    weather_path = _pick_weather_file(sample_dir / "weather", args.date)
    out_path = Path(args.out_dir) / f"merged_{args.date}.csv"

    stop_index = derived_dir / "rer_stop_index.csv"
    stations = derived_dir / "stations.csv"

    df = merge_daily_raw_with_weather(
        raw_csv=raw_path,
        stop_index_csv=stop_index,
        stations_csv=stations,
        weather_csv=weather_path,
        out_csv=out_path,
        drop_unmapped_stops=(not args.keep_unmapped_stops),
        max_unmapped_share=float(args.max_unmapped_share),
    )

    print(f"Wrote: {out_path}")
    print(f"Rows: {len(df)} | Cols: {len(df.columns)}")
    weather_cov = df["temperature_2m"].notna().mean() if "temperature_2m" in df.columns and len(df) else 0.0
    print(f"Weather coverage (temperature_2m non-null): {weather_cov:.1%}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
