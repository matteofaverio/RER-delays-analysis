"""
Open-Meteo archive client (hourly historical weather).

This module is intentionally small and explicit:
- fetches hourly weather for one or many coordinates
- returns tidy pandas DataFrames with UTC timestamps

API docs (high level):
- https://open-meteo.com/
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import time
import requests
import pandas as pd


DEFAULT_HOURLY_VARS = ("temperature_2m", "precipitation", "wind_speed_10m")


@dataclass(frozen=True)
class OpenMeteoArchiveClient:
    """
    Minimal client for Open-Meteo archive endpoint.

    Notes:
    - No API key required
    - Supports batching multiple lat/lon pairs via comma-separated query params
    """
    base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    timeout_s: int = 60
    max_retries: int = 3
    backoff_s: float = 1.0

    def fetch_hourly(
        self,
        latitudes: Sequence[float],
        longitudes: Sequence[float],
        day: str,
        hourly_vars: Sequence[str] = DEFAULT_HOURLY_VARS,
        timezone: str = "UTC",
    ) -> List[dict]:
        """
        Fetch hourly weather for a list of coordinates for a single day.

        Returns:
            A list of response dicts (one per coordinate).
        """
        if len(latitudes) != len(longitudes):
            raise ValueError("latitudes and longitudes must have the same length")
        if len(latitudes) == 0:
            return []

        params = {
            "latitude": ",".join(str(x) for x in latitudes),
            "longitude": ",".join(str(x) for x in longitudes),
            "start_date": day,
            "end_date": day,
            "hourly": ",".join(hourly_vars),
            "timezone": timezone,
        }

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                r = requests.get(self.base_url, params=params, timeout=self.timeout_s)
                r.raise_for_status()
                payload = r.json()
                # For one coordinate the API returns a dict, for many it returns a list.
                return payload if isinstance(payload, list) else [payload]
            except Exception as e:
                last_err = e
                if attempt + 1 < self.max_retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
        raise RuntimeError(f"Open-Meteo request failed after retries: {last_err}")

    @staticmethod
    def to_hourly_frame(
        responses: Sequence[dict],
        station_codes: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """
        Convert Open-Meteo responses into a tidy DataFrame with columns:
        - station_code (optional)
        - weather_time_utc (timezone-aware UTC timestamp)
        - weather variables
        """
        rows = []
        if station_codes is not None and len(station_codes) != len(responses):
            raise ValueError("station_codes length must match number of responses")

        for i, resp in enumerate(responses):
            hourly = resp.get("hourly") or {}
            times = hourly.get("time") or []
            if not times:
                continue

            df = pd.DataFrame({"weather_time_utc": pd.to_datetime(times, utc=True, errors="coerce")})
            for k, v in hourly.items():
                if k == "time":
                    continue
                df[k] = v

            if station_codes is not None:
                df.insert(0, "station_code", station_codes[i])

            rows.append(df)

        if not rows:
            return pd.DataFrame(columns=["weather_time_utc", *DEFAULT_HOURLY_VARS])

        out = pd.concat(rows, ignore_index=True)
        out = out.dropna(subset=["weather_time_utc"]).sort_values(["weather_time_utc"] + (["station_code"] if "station_code" in out.columns else []))
        return out.reset_index(drop=True)
