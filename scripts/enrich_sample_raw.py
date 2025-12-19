#!/usr/bin/env python3
"""
Enrich a one-day sample of RER delay polling data with station metadata and hourly weather.

Inputs (defaults):
  - data/sample/rer_raw/YYYY-MM-DD.csv
  - data/sample/weather/YYYY-MM-DD_weather.csv
  - data/derived/rer_stop_index.csv
  - data/derived/stations.csv

Output (default):
  - data/sample/merged/merged_YYYY-MM-DD.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from weather.merge_weather_delays import merge_daily_raw_with_weather


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path)


def _ensure_station_code(
    raw: pd.DataFrame,
    *,
    stop_index: pd.DataFrame,
    stations: pd.DataFrame,
    stop_id_col: str = "stop_id",
) -> pd.DataFrame:
    """
    Ensure raw contains a station_code column.

    Strategy:
      raw.stop_id  -> extract numeric token -> build "IDFM:<num>"  (fallback: keep if already IDFM:*)
      join with stop_index on stop_id_idfm  -> monomodal_code
      join with stations on monomodal_code  -> station_code (+ optional metadata)
    """
    if "station_code" in raw.columns and raw["station_code"].notna().any():
        return raw

    if stop_id_col not in raw.columns:
        raise ValueError(f"raw is missing required column '{stop_id_col}'")

    if "stop_id_idfm" not in stop_index.columns or "monomodal_code" not in stop_index.columns:
        raise ValueError("rer_stop_index.csv must contain columns: stop_id_idfm, monomodal_code")

    if "monomodal_code" not in stations.columns or "station_code" not in stations.columns:
        raise ValueError("stations.csv must contain columns: monomodal_code, station_code")

    raw = raw.copy()
    stop_index = stop_index.copy()
    stations = stations.copy()

    def to_idfm(s: str) -> str | None:
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return None
        s = str(s)
        if s.startswith("IDFM:"):
            return s
        # typical patterns: STIF:StopArea:SP:43044: or STIF:StopPoint:Q:474053:
        m = re.search(r"(\d{4,})", s)
        return f"IDFM:{m.group(1)}" if m else None

    raw["stop_id_idfm"] = raw[stop_id_col].map(to_idfm)

    # Deduplicate mapping tables to avoid many-to-many merges
    stop_index["stop_id_idfm"] = stop_index["stop_id_idfm"].astype(str)
    stop_index["monomodal_code"] = stop_index["monomodal_code"].astype(str)
    stop_index = stop_index.drop_duplicates(subset=["stop_id_idfm"], keep="first")

    stations["monomodal_code"] = stations["monomodal_code"].astype(str)
    stations = stations.drop_duplicates(subset=["monomodal_code"], keep="first")

    raw = raw.merge(
        stop_index[["stop_id_idfm", "monomodal_code"]],
        on="stop_id_idfm",
        how="left",
        validate="m:1",
    )

    raw = raw.merge(
        stations[
            [
                "monomodal_code",
                "station_code",
                "stop_name",
                "stop_lat",
                "stop_lon",
                "zone_id",
            ]
        ],
        on="monomodal_code",
        how="left",
        validate="m:1",
    )

    n_missing = int(raw["station_code"].isna().sum())
    if n_missing > 0:
        examples = raw.loc[raw["station_code"].isna(), stop_id_col].dropna().astype(str).head(10).tolist()
        raise ValueError(
            "Could not map some stop_id values to station_code. "
            f"Missing station_code for {n_missing} rows. Examples: {examples}"
        )

    return raw


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a merged one-day sample (raw delays + station metadata + weather).")
    ap.add_argument("--date", required=True, help="Service day (YYYY-MM-DD).")

    ap.add_argument("--raw", default=None, help="Path to sample raw CSV. Default: data/sample/rer_raw/<date>.csv")
    ap.add_argument("--weather", default=None, help="Path to sample weather CSV. Default: data/sample/weather/<date>_weather.csv")
    ap.add_argument("--stop-index", default="data/derived/rer_stop_index.csv", help="Path to rer_stop_index.csv")
    ap.add_argument("--stations", default="data/derived/stations.csv", help="Path to stations.csv")

    ap.add_argument("--out", default=None, help="Output path. Default: data/sample/merged/merged_<date>.csv")
    ap.add_argument("--tolerance", default="59min", help="Max time delta for weather match (e.g., 30min, 59min).")
    args = ap.parse_args()

    date = args.date
    raw_path = Path(args.raw) if args.raw else Path(f"data/sample/rer_raw/{date}.csv")
    weather_path = Path(args.weather) if args.weather else Path(f"data/sample/weather/{date}_weather.csv")
    out_path = Path(args.out) if args.out else Path(f"data/sample/merged/merged_{date}.csv")

    raw = _read_csv(raw_path)
    weather = _read_csv(weather_path)
    stop_index = _read_csv(Path(args.stop_index))
    stations = _read_csv(Path(args.stations))

    raw = _ensure_station_code(raw, stop_index=stop_index, stations=stations)

    merged = merge_daily_raw_with_weather(
        raw,
        weather,
        station_col="station_code",
        raw_time_col="poll_at_utc",
        weather_time_col="weather_time_utc",
        tolerance=str(args.tolerance),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Rows: {len(merged):,} | Cols: {merged.shape[1]}")
    # quick sanity: how many rows got weather attached
    weather_cols = [c for c in merged.columns if c in ("temperature_2m", "precipitation", "wind_speed_10m")]
    if weather_cols:
        n_ok = int(merged[weather_cols[0]].notna().sum())
        print(f"Rows with weather match: {n_ok:,} / {len(merged):,}")


if __name__ == "__main__":
    main()
