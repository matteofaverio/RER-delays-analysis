from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from weather.open_meteo import OpenMeteoArchiveClient, OpenMeteoHourlyRequest


@dataclass(frozen=True)
class WeatherDailyPaths:
    stations_csv: Path
    out_dir: Path


def _load_stations(stations_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(stations_csv)
    required = {"station_code", "stop_lat", "stop_lon"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"stations_csv missing columns: {missing}")

    # Keep minimal, clean rows
    out = df.copy()
    out["station_code"] = out["station_code"].astype(str).str.strip()
    out["stop_lat"] = pd.to_numeric(out["stop_lat"], errors="coerce")
    out["stop_lon"] = pd.to_numeric(out["stop_lon"], errors="coerce")
    out = out.dropna(subset=["station_code", "stop_lat", "stop_lon"])
    out = out[(out["stop_lat"] != 0.0) & (out["stop_lon"] != 0.0)]
    out = out.drop_duplicates(subset=["station_code"]).reset_index(drop=True)
    return out


def build_daily_weather(
    day: str,
    paths: WeatherDailyPaths,
    *,
    batch_size: int = 50,
    overwrite: bool = False,
    variables: Sequence[str] = ("temperature_2m", "precipitation", "wind_speed_10m"),
) -> Path:
    """
    Fetch one day of hourly weather for all stations in stations_csv.

    Output schema:
      station_code, stop_name (optional), stop_lat, stop_lon,
      weather_time_utc, temperature_2m, precipitation, wind_speed_10m
    """
    paths.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = paths.out_dir / f"{day}_weather.csv"
    if out_path.exists() and not overwrite:
        return out_path

    stations = _load_stations(paths.stations_csv)
    has_name = "stop_name" in stations.columns

    client = OpenMeteoArchiveClient(timeout_s=60.0)
    req = OpenMeteoHourlyRequest(start_date=day, end_date=day, variables=variables, timezone="UTC")

    rows: List[Dict] = []

    for start in range(0, len(stations), int(batch_size)):
        chunk = stations.iloc[start : start + int(batch_size)].reset_index(drop=True)
        coords: List[Tuple[float, float]] = list(zip(chunk["stop_lat"].tolist(), chunk["stop_lon"].tolist()))
        payloads = client.fetch_hourly_batch(coords, req)

        if len(payloads) != len(chunk):
            # Be conservative: skip the whole chunk if API returns mismatched payload size.
            continue

        for i, payload in enumerate(payloads):
            hourly = payload.get("hourly") or {}
            times = hourly.get("time") or []
            if not times:
                continue

            # Extract series (same length as times)
            series = {v: hourly.get(v) or [] for v in variables}
            n = min(len(times), *(len(series[v]) for v in variables))

            st = chunk.iloc[i]
            for k in range(n):
                row = {
                    "station_code": st["station_code"],
                    "stop_lat": float(st["stop_lat"]),
                    "stop_lon": float(st["stop_lon"]),
                    "weather_time_utc": times[k],
                }
                if has_name:
                    row["stop_name"] = st["stop_name"]

                for v in variables:
                    row[v] = series[v][k]
                rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        # Still create an empty, well-formed file
        cols = ["station_code", "stop_lat", "stop_lon", "weather_time_utc", *variables]
        if "stop_name" in stations.columns:
            cols.insert(1, "stop_name")
        df = pd.DataFrame(columns=cols)

    # Normalize types + sort
    df["weather_time_utc"] = pd.to_datetime(df["weather_time_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["weather_time_utc"])
    df = df.sort_values(["station_code", "weather_time_utc"]).reset_index(drop=True)

    df.to_csv(out_path, index=False)
    return out_path
