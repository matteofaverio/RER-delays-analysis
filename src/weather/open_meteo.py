"""
Open-Meteo client and CLI utilities.

This module fetches hourly weather variables for a set of stations
(latitude/longitude) and writes a tidy CSV suitable for joining with
polling-derived delay panels.

Default endpoint: https://api.open-meteo.com/v1/forecast
No API key is required for the public Open-Meteo API.

Output columns:
- station_code
- stop_name (if available)
- stop_lat, stop_lon
- weather_time_utc (timezone-aware)
- temperature_2m
- precipitation
- wind_speed_10m
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import httpx
import numpy as np
import pandas as pd


DEFAULT_BASE_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_HOURLY_VARS = ("temperature_2m", "precipitation", "wind_speed_10m")


def _require_columns(df: pd.DataFrame, cols: Sequence[str], ctx: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{ctx}: missing required columns: {missing}")


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


@dataclass
class OpenMeteoClient:
    base_url: str = DEFAULT_BASE_URL
    timeout_s: float = 30.0
    retries: int = 3
    backoff_s: float = 0.75

    def _get(self, params: dict) -> dict:
        headers = {"User-Agent": "idf-rer-delays/0.1 (+github; educational)"}
        timeout = httpx.Timeout(self.timeout_s)
        last_err: Optional[Exception] = None

        for attempt in range(self.retries):
            if attempt > 0:
                time.sleep(self.backoff_s * (2 ** (attempt - 1)))
            try:
                with httpx.Client(timeout=timeout, headers=headers) as client:
                    r = client.get(self.base_url, params=params)
                    r.raise_for_status()
                    return r.json()
            except Exception as e:
                last_err = e

        raise RuntimeError(f"Open-Meteo request failed after {self.retries} attempts: {last_err}")

    def fetch_hourly_for_point(
        self,
        lat: float,
        lon: float,
        date: str,
        hourly_vars: Sequence[str] = DEFAULT_HOURLY_VARS,
    ) -> pd.DataFrame:
        """
        Fetch hourly weather for a single lat/lon and a single date (UTC day).

        Parameters
        ----------
        lat, lon : float
            Coordinates.
        date : str
            YYYY-MM-DD. The request uses start_date=end_date=date with timezone=UTC.
        hourly_vars : sequence of str
            Open-Meteo hourly variables.

        Returns
        -------
        pd.DataFrame with columns:
            weather_time_utc + requested variables
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "timezone": "UTC",
            "start_date": date,
            "end_date": date,
            "hourly": ",".join(hourly_vars),
        }

        data = self._get(params)
        hourly = (data or {}).get("hourly", {}) or {}
        times = hourly.get("time", []) or []

        out = pd.DataFrame({"weather_time_utc": pd.to_datetime(times, utc=True, errors="coerce")})
        for v in hourly_vars:
            values = hourly.get(v, None)
            if values is None:
                out[v] = np.nan
            else:
                out[v] = pd.to_numeric(pd.Series(values), errors="coerce")

        out.dropna(subset=["weather_time_utc"], inplace=True)
        return out


def build_daily_station_weather(
    stations: pd.DataFrame,
    date: str,
    hourly_vars: Sequence[str] = DEFAULT_HOURLY_VARS,
    limit: Optional[int] = None,
    sleep_s: float = 0.0,
    client: Optional[OpenMeteoClient] = None,
) -> pd.DataFrame:
    """
    Build an hourly weather panel for all stations for a given date.

    Expected station columns:
    - station_code (recommended)
    - stop_lat
    - stop_lon
    - stop_name (optional)
    """
    _require_columns(stations, ["stop_lat", "stop_lon"], "stations dataframe")

    client = client or OpenMeteoClient()

    df = stations.copy()
    if limit is not None:
        df = df.head(int(limit)).copy()

    # Normalize column presence
    if "station_code" not in df.columns:
        df["station_code"] = ""
    if "stop_name" not in df.columns:
        df["stop_name"] = ""

    rows: List[pd.DataFrame] = []

    for i, r in df.reset_index(drop=True).iterrows():
        lat = _to_float(r["stop_lat"])
        lon = _to_float(r["stop_lon"])
        if not np.isfinite(lat) or not np.isfinite(lon):
            continue

        w = client.fetch_hourly_for_point(lat=lat, lon=lon, date=date, hourly_vars=hourly_vars)
        w.insert(0, "stop_lon", lon)
        w.insert(0, "stop_lat", lat)
        w.insert(0, "stop_name", r.get("stop_name", ""))
        w.insert(0, "station_code", r.get("station_code", ""))

        rows.append(w)

        if sleep_s > 0:
            time.sleep(float(sleep_s))

        if (i + 1) % 25 == 0:
            print(f"[open_meteo] fetched {i+1}/{len(df)} stations", file=sys.stderr)

    if not rows:
        return pd.DataFrame(
            columns=["station_code", "stop_name", "stop_lat", "stop_lon", "weather_time_utc", *hourly_vars]
        )

    out = pd.concat(rows, ignore_index=True)
    # Stable ordering
    out.sort_values(["station_code", "weather_time_utc"], inplace=True)
    return out


def _read_stations(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Keep only columns we need, if present
    keep = [c for c in ["station_code", "stop_name", "stop_lat", "stop_lon"] if c in df.columns]
    if not keep:
        raise ValueError(f"{path}: no usable columns found (expected stop_lat/stop_lon at minimum).")
    df = df[keep].copy()
    return df


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Fetch daily (hourly) weather for stations using Open-Meteo.")
    ap.add_argument("--stations", required=True, help="Path to stations.csv (must contain stop_lat, stop_lon).")
    ap.add_argument("--date", required=True, help="Date in YYYY-MM-DD (UTC day).")
    ap.add_argument("--out", required=True, help="Output CSV path.")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of stations (for smoke tests).")
    ap.add_argument("--batch-size", type=int, default=250, help="Write to disk every N stations.")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between station requests (seconds).")
    ap.add_argument(
        "--hourly",
        default=",".join(DEFAULT_HOURLY_VARS),
        help="Comma-separated hourly variables (Open-Meteo names).",
    )
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Open-Meteo endpoint.")
    ap.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds.")
    ap.add_argument("--retries", type=int, default=3, help="HTTP retries.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    stations_path = Path(args.stations)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stations = _read_stations(stations_path)
    hourly_vars = tuple([s.strip() for s in str(args.hourly).split(",") if s.strip()])

    client = OpenMeteoClient(
        base_url=str(args.base_url),
        timeout_s=float(args.timeout),
        retries=int(args.retries),
    )

    # Chunked processing to keep memory low and make progress visible.
    df = stations.copy()
    if args.limit is not None:
        df = df.head(int(args.limit)).copy()

    batch_size = max(1, int(args.batch_size))
    written = False

    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start : start + batch_size].copy()
        panel = build_daily_station_weather(
            stations=chunk,
            date=str(args.date),
            hourly_vars=hourly_vars,
            limit=None,
            sleep_s=float(args.sleep),
            client=client,
        )

        mode = "w" if not written else "a"
        header = not written
        panel.to_csv(out_path, index=False, mode=mode, header=header)
        written = True

        print(
            f"[open_meteo] wrote {min(start+batch_size, len(df))}/{len(df)} stations to {out_path}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
