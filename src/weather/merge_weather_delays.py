"""
Merge RER delay panel (raw polling aggregates) with:
- GTFS-derived stop index (quay/platform -> station metadata)
- per-station hourly weather (Open-Meteo)

The goal is to produce a clean, analysis-ready table without duplicated columns
and with a stable join key:
    (station_code, weather_time_utc=floor(poll_at_utc to hour))

Expected inputs
---------------
raw_csv:
    data/sample/rer_raw/YYYY-MM-DD.csv
    columns include: poll_at_utc, poll_at_local, stop_id, line_code, mean_delay_s, ...

stop_index_csv:
    data/derived/rer_stop_index.csv
    columns include: quay_code, station_code, stop_name, stop_lat, stop_lon, zone_id, ...

stations_csv:
    data/derived/stations.csv
    used only for a compatibility mapping:
        weather files may contain numeric "station_code" (e.g., 790),
        which corresponds to stations.old_station_code.

weather_csv:
    data/sample/weather/YYYY-MM-DD_weather.csv
    columns include: station_code, weather_time_utc, temperature_2m, precipitation, wind_speed_10m
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


_STOP_ID_RE = re.compile(r":(?:Q|SP):(\d+):")
_IDFM_RE = re.compile(r"IDFM:(\d+)")


def _extract_quay_code(stop_id: object) -> Optional[str]:
    """
    Extract numeric quay/platform code from SIRI stop identifiers.

    Examples:
      STIF:StopPoint:Q:491414: -> 491414
      STIF:StopArea:SP:43044:  -> 43044
      IDFM:472963              -> 472963
    """
    if stop_id is None or (isinstance(stop_id, float) and pd.isna(stop_id)):
        return None
    s = str(stop_id)

    m = _STOP_ID_RE.search(s)
    if m:
        return m.group(1)

    m = _IDFM_RE.search(s)
    if m:
        return m.group(1)

    # Last resort: take the last numeric token if present
    m = re.findall(r"\d+", s)
    if m:
        return m[-1]

    return None


def _read_csv(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return pd.read_csv(p, dtype=str)


def _to_utc(ts: pd.Series) -> pd.Series:
    # utc=True makes tz-naive timestamps interpreted as UTC, and keeps tz-aware in UTC.
    return pd.to_datetime(ts, errors="coerce", utc=True)


def _normalize_weather_station_code(weather: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    """
    Make weather.station_code compatible with our canonical station_code.

    Some legacy weather files store the numeric "old_station_code" as station_code
    (e.g., "790" for Villepinte). If detected, map it back to the 3-letter station_code.
    """
    if "station_code" not in weather.columns:
        raise ValueError("weather_csv must contain a 'station_code' column.")

    w = weather.copy()
    w["station_code"] = w["station_code"].astype(str).str.strip()

    if "old_station_code" not in stations.columns or "station_code" not in stations.columns:
        return w

    st = stations.copy()
    st["old_station_code"] = st["old_station_code"].astype(str).str.strip()
    st["station_code"] = st["station_code"].astype(str).str.strip()

    mapping = (
        st.loc[st["old_station_code"].notna() & (st["old_station_code"] != ""), ["old_station_code", "station_code"]]
        .drop_duplicates()
        .set_index("old_station_code")["station_code"]
        .to_dict()
    )

    # Heuristic: if most codes look numeric, apply mapping where possible
    numeric_share = w["station_code"].str.fullmatch(r"\d+").mean()
    if numeric_share >= 0.30:
        w["station_code"] = w["station_code"].map(lambda x: mapping.get(x, x))

    return w


def merge_daily_raw_with_weather(
    *,
    raw_csv: str | Path,
    stop_index_csv: str | Path,
    stations_csv: str | Path,
    weather_csv: str | Path,
    out_csv: Optional[str | Path] = None,
    drop_unmapped_stops: bool = True,
    max_unmapped_share: float = 0.40,
) -> pd.DataFrame:
    """
    Merge a daily raw RER panel with stop/station metadata and hourly weather.

    Parameters
    ----------
    drop_unmapped_stops:
        If True, drop rows whose stop_id cannot be mapped to station_code.
    max_unmapped_share:
        If share of unmapped rows exceeds this threshold, raise (usually join-key mismatch).
    """
    raw = _read_csv(raw_csv)
    stop_index = _read_csv(stop_index_csv)
    stations = _read_csv(stations_csv)
    weather = _read_csv(weather_csv)

    # --- parse timestamps in raw
    if "poll_at_utc" not in raw.columns:
        raise ValueError("raw_csv must contain 'poll_at_utc'.")
    raw["poll_at_utc"] = _to_utc(raw["poll_at_utc"])

    # --- build quay_code join key
    raw["quay_code"] = raw["stop_id"].map(_extract_quay_code).astype(str)
    raw.loc[raw["quay_code"].isin(["None", "nan"]), "quay_code"] = pd.NA

    stop_index["quay_code"] = stop_index["quay_code"].astype(str).str.strip()

    # Keep only the columns we want from the stop index to avoid noisy duplicates.
    stop_keep = [
        "quay_code",
        "stop_id_idfm",
        "monomodal_stop_id",
        "monomodal_code",
        "stop_name",
        "parent_station",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "location_type",
        "station_code",
    ]
    stop_keep = [c for c in stop_keep if c in stop_index.columns]
    stop_index_small = stop_index[stop_keep].copy()

    df = raw.merge(stop_index_small, on="quay_code", how="left", validate="m:1")

    # --- unmapped diagnostics
    missing_station = df["station_code"].isna()
    missing_share = float(missing_station.mean()) if len(df) else 0.0
    if missing_share > max_unmapped_share:
        examples = df.loc[missing_station, "stop_id"].dropna().astype(str).head(10).tolist()
        raise ValueError(
            f"Stop-to-station mapping incomplete: missing station_code for {int(missing_station.sum())} rows "
            f"({missing_share:.1%}). Example stop_id: {examples}. "
            "This usually means the stop_id parsing/join key does not match your rer_stop_index.csv."
        )
    if drop_unmapped_stops and missing_share > 0:
        df = df.loc[~missing_station].copy()

    # --- weather parsing and normalization
    weather = _normalize_weather_station_code(weather, stations)

    if "weather_time_utc" not in weather.columns:
        raise ValueError("weather_csv must contain 'weather_time_utc'.")

    weather["weather_time_utc"] = _to_utc(weather["weather_time_utc"])

    # Keep only weather variables needed for analysis (avoid stop_name duplication).
    weather_keep = ["station_code", "weather_time_utc", "temperature_2m", "precipitation", "wind_speed_10m"]
    weather_keep = [c for c in weather_keep if c in weather.columns]
    weather_small = weather[weather_keep].copy()

    # Align to hour
    df["weather_time_utc"] = df["poll_at_utc"].dt.floor("h")

    df = df.merge(
        weather_small,
        on=["station_code", "weather_time_utc"],
        how="left",
        validate="m:1",
    )

    # Optional output
    if out_csv is not None:
        out_path = Path(out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)

    return df
