#!/usr/bin/env python3
"""
Enrich a raw RER polling day with station metadata (GTFS mapping),
and optionally merge hourly weather.

Default paths are aligned with the repo layout:
- data/sample/rer_raw/YYYY-MM-DD.csv
- data/derived/rer_stop_index.csv
- data/derived/stations.csv
- data/sample/weather/YYYY-MM-DD_weather.csv
- data/sample/merged/merged_YYYY-MM-DD.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from weather.merge_weather_delays import merge_delays_with_weather, WeatherMergeSpec


_STOP_CODE_RE = re.compile(r"(\d+)")


def extract_numeric_stop_code(stop_id: str) -> Optional[str]:
    """
    Extract the numeric component from IDFM stop identifiers.

    Examples:
      STIF:StopArea:SP:43044:  -> 43044
      STIF:StopPoint:Q:474053: -> 474053
    """
    if stop_id is None or (isinstance(stop_id, float) and pd.isna(stop_id)):
        return None
    s = str(stop_id)
    matches = _STOP_CODE_RE.findall(s)
    return matches[-1] if matches else None


def enrich_raw_day(
    raw_path: Path,
    rer_stop_index_path: Path,
    stations_path: Path,
) -> pd.DataFrame:
    raw = pd.read_csv(raw_path, dtype=str)
    needed = {"poll_at_utc", "stop_id", "line_code", "mean_delay_s", "n"}
    missing = sorted(needed - set(raw.columns))
    if missing:
        raise ValueError(f"raw file missing columns: {missing}")

    raw["stop_code"] = raw["stop_id"].map(extract_numeric_stop_code)

    stop_index = pd.read_csv(rer_stop_index_path, dtype=str)
    if "quay_code" not in stop_index.columns:
        raise ValueError("rer_stop_index.csv must contain a 'quay_code' column")

    stations = pd.read_csv(stations_path, dtype=str)
    if "monomodal_code" not in stations.columns or "station_code" not in stations.columns:
        raise ValueError("stations.csv must contain 'monomodal_code' and 'station_code'")

    # stop_code -> stop_index(quay_code) to get monomodal_code + coordinates + zone/name
    merged = raw.merge(
        stop_index,
        left_on="stop_code",
        right_on="quay_code",
        how="left",
        suffixes=("", "_idx"),
    )

    # monomodal_code -> stations to get station_code and line flags, etc.
    merged = merged.merge(
        stations,
        on="monomodal_code",
        how="left",
        suffixes=("", "_stations"),
    )

    # keep tidy
    if "poll_at_local" in merged.columns:
        merged["poll_at_local"] = merged["poll_at_local"].astype(str)

    return merged


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich one sample raw day and optionally merge weather.")
    ap.add_argument("--day", default="2025-11-15", help="Service day (YYYY-MM-DD).")
    ap.add_argument("--raw", default=None, help="Path to raw day CSV (overrides --day).")
    ap.add_argument("--rer-stop-index", default="data/derived/rer_stop_index.csv")
    ap.add_argument("--stations", default="data/derived/stations.csv")
    ap.add_argument("--weather", default=None, help="Path to daily weather CSV (optional).")
    ap.add_argument("--out", default=None, help="Output path for merged CSV.")
    args = ap.parse_args()

    day = args.day
    raw_path = Path(args.raw) if args.raw else Path(f"data/sample/rer_raw/{day}.csv")
    out_path = Path(args.out) if args.out else Path(f"data/sample/merged/merged_{day}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rer_stop_index_path = Path(args.rer_stop_index)
    stations_path = Path(args.stations)

    df = enrich_raw_day(raw_path, rer_stop_index_path, stations_path)

    if args.weather:
        weather_path = Path(args.weather)
    else:
        weather_path = Path(f"data/sample/weather/{day}_weather.csv")

    if weather_path.exists():
        w = pd.read_csv(weather_path)
        spec = WeatherMergeSpec(poll_time_col="poll_at_utc", station_col="station_code", weather_time_col="weather_time_utc")
        df = merge_delays_with_weather(df, w, spec=spec, how="left")

    df.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
