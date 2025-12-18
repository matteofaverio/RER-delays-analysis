"""
Utilities to merge delay polling outputs with hourly weather.

The merge logic is deliberately conservative:
- poll_at_utc is floored to the hour (UTC) -> weather_time_utc
- merge on (station_code, weather_time_utc) when possible
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import pandas as pd


@dataclass(frozen=True)
class WeatherMergeSpec:
    poll_time_col: str = "poll_at_utc"
    station_col: str = "station_code"
    weather_time_col: str = "weather_time_utc"


def _ensure_utc(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    return dt


def add_weather_time_utc(
    df: pd.DataFrame,
    poll_time_col: str = "poll_at_utc",
    weather_time_col: str = "weather_time_utc",
) -> pd.DataFrame:
    """
    Adds weather_time_utc = floor(poll_at_utc to the hour), in UTC.
    """
    out = df.copy()
    poll_dt = _ensure_utc(out[poll_time_col])
    out[weather_time_col] = poll_dt.dt.floor("H")
    return out


def merge_delays_with_weather(
    delays_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    spec: WeatherMergeSpec = WeatherMergeSpec(),
    how: str = "left",
) -> pd.DataFrame:
    """
    Merge delays with weather.

    If weather_df has a station_code column, merge on (station_code, weather_time_utc).
    Otherwise merge on weather_time_utc only.
    """
    if spec.poll_time_col not in delays_df.columns:
        raise ValueError(f"Missing {spec.poll_time_col} in delays_df")

    d = add_weather_time_utc(delays_df, poll_time_col=spec.poll_time_col, weather_time_col=spec.weather_time_col)

    w = weather_df.copy()
    if spec.weather_time_col not in w.columns:
        # allow Open-Meteo "time" naming if a user passes it directly
        if "time" in w.columns:
            w[spec.weather_time_col] = pd.to_datetime(w["time"], utc=True, errors="coerce")
        else:
            raise ValueError(f"Missing {spec.weather_time_col} in weather_df")

    w[spec.weather_time_col] = _ensure_utc(w[spec.weather_time_col]).dt.floor("H")

    keys = [spec.weather_time_col]
    if spec.station_col in d.columns and spec.station_col in w.columns:
        keys = [spec.station_col, spec.weather_time_col]

    merged = d.merge(w, on=keys, how=how, suffixes=("", "_weather"))
    return merged
