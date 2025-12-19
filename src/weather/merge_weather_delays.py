# src/weather/merge_weather_delays.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd

PathLike = Union[str, Path]

_PAT_Q_SP = re.compile(r":(?:Q|SP):(\d+):")   # STIF:StopPoint:Q:491414: / STIF:StopArea:SP:43044:
_PAT_IDFM = re.compile(r"IDFM:(\d+)")


def _as_path(p: PathLike) -> Path:
    return p if isinstance(p, Path) else Path(p)


def _read_csv(path: PathLike, **kwargs) -> pd.DataFrame:
    p = _as_path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    return pd.read_csv(p, **kwargs)


def _extract_digits(stop_id: str) -> Optional[str]:
    if stop_id is None or (isinstance(stop_id, float) and pd.isna(stop_id)):
        return None
    s = str(stop_id)

    m = _PAT_Q_SP.search(s)
    if m:
        return str(int(m.group(1)))

    m = _PAT_IDFM.search(s)
    if m:
        return str(int(m.group(1)))

    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    return str(int(digits[-1]))


def _pick_weather_file(weather_dir: Path, date: str) -> Path:
    candidates = [
        weather_dir / f"{date}_weather.csv",
        weather_dir / f"{date} weather.csv",
        weather_dir / f"{date}-weather.csv",
        weather_dir / f"{date}.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("Weather file not found. Tried: " + ", ".join(str(c) for c in candidates))


def _best_stop_index_join(
    raw_unique: pd.DataFrame,
    stop_index: pd.DataFrame,
) -> Tuple[str, str, float]:
    """
    Choose which join key to use between raw and stop_index.

    raw_unique must contain:
      - quay_code (digits as string)
      - stop_id_idfm (IDFM:<digits>)

    stop_index may contain one or more of:
      - quay_code
      - stop_id_idfm
    """
    candidates = []
    if "quay_code" in stop_index.columns:
        candidates.append(("quay_code", "quay_code"))
    if "stop_id_idfm" in stop_index.columns:
        candidates.append(("stop_id_idfm", "stop_id_idfm"))

    if not candidates:
        raise ValueError(
            "rer_stop_index.csv must contain either 'quay_code' or 'stop_id_idfm' to join with raw stop_id."
        )

    best = ("", "", -1.0)
    for raw_col, idx_col in candidates:
        idx_keys = stop_index[[idx_col]].dropna().astype(str).drop_duplicates()
        probe = raw_unique[[raw_col]].dropna().astype(str).drop_duplicates()
        merged = probe.merge(idx_keys, left_on=raw_col, right_on=idx_col, how="left", indicator=True)
        hit_rate = (merged["_merge"] == "both").mean() if len(merged) else 0.0
        if hit_rate > best[2]:
            best = (raw_col, idx_col, float(hit_rate))

    return best


def merge_daily_raw_with_weather(
    *,
    raw_csv: PathLike,
    stop_index_csv: PathLike,
    stations_csv: PathLike,
    weather_csv: Optional[PathLike] = None,
    weather_dir: Optional[PathLike] = None,
    date: Optional[str] = None,
    out_csv: Optional[PathLike] = None,
    max_unmapped_share: float = 0.05,
    drop_unmapped_stops: bool = False,
) -> pd.DataFrame:
    """
    Merge one service-day raw file with hourly weather.

    Mapping strategy:
    - Parse digits from raw stop_id into quay_code
    - Build stop_id_idfm = 'IDFM:<digits>'
    - Automatically choose the best join key against rer_stop_index.csv
      (quay_code vs stop_id_idfm) based on match-rate.

    Missing mapping handling:
    - If drop_unmapped_stops=True: drop unmapped rows.
    - Else: allow up to max_unmapped_share; above that raise an error.
    """
    raw_path = _as_path(raw_csv)
    stop_index_path = _as_path(stop_index_csv)
    stations_path = _as_path(stations_csv)

    if weather_csv is not None:
        weather_path = _as_path(weather_csv)
    else:
        if weather_dir is None or date is None:
            raise ValueError("Provide either weather_csv, or (weather_dir and date).")
        weather_path = _pick_weather_file(_as_path(weather_dir), date)

    out_path = _as_path(out_csv) if out_csv is not None else None

    raw = _read_csv(raw_path, dtype={"stop_id": str, "line_code": str})
    stop_index = _read_csv(stop_index_path, dtype=str)
    stations = _read_csv(stations_path, dtype=str)
    weather = _read_csv(weather_path, dtype={"station_code": str})

    # --- raw join keys ---
    raw["quay_code"] = raw["stop_id"].map(_extract_digits).astype("string")
    raw["stop_id_idfm"] = raw["quay_code"].map(lambda x: f"IDFM:{x}" if pd.notna(x) else pd.NA).astype("string")

    raw_unique = raw[["quay_code", "stop_id_idfm"]].copy()
    raw_key, idx_key, hit_rate = _best_stop_index_join(raw_unique, stop_index)

    meta_cols = [
        c for c in [
            "quay_code", "stop_id_idfm",
            "monomodal_stop_id", "monomodal_code",
            "stop_name", "zone_id", "stop_lat", "stop_lon",
            "station_code",
        ]
        if c in stop_index.columns
    ]
    stop_small = stop_index[meta_cols].drop_duplicates(subset=[idx_key])

    df = raw.merge(stop_small, left_on=raw_key, right_on=idx_key, how="left")

    # --- station_code enrichment from stations.csv if needed ---
    if "station_code" not in df.columns:
        df["station_code"] = pd.NA

    if df["station_code"].isna().any():
        # primary: monomodal_stop_id
        if "monomodal_stop_id" in df.columns and "monomodal_stop_id" in stations.columns:
            st = stations[["station_code", "monomodal_stop_id"]].drop_duplicates(subset=["monomodal_stop_id"])
            df = df.merge(st, on="monomodal_stop_id", how="left", suffixes=("", "_st"))
            if "station_code_st" in df.columns:
                df["station_code"] = df["station_code"].fillna(df["station_code_st"])
                df.drop(columns=["station_code_st"], inplace=True)

        # fallback: monomodal_code
        if df["station_code"].isna().any() and ("monomodal_code" in df.columns) and ("monomodal_code" in stations.columns):
            st2 = stations[["station_code", "monomodal_code"]].drop_duplicates(subset=["monomodal_code"])
            df = df.merge(st2, on="monomodal_code", how="left", suffixes=("", "_st2"))
            if "station_code_st2" in df.columns:
                df["station_code"] = df["station_code"].fillna(df["station_code_st2"])
                df.drop(columns=["station_code_st2"], inplace=True)

    # --- unmapped handling ---
    missing = df["station_code"].isna()
    n_missing = int(missing.sum())
    if n_missing > 0:
        share = n_missing / max(1, len(df))
        if drop_unmapped_stops:
            df = df.loc[~missing].copy()
        else:
            if share > float(max_unmapped_share):
                examples = df.loc[missing, "stop_id"].head(10).tolist()
                raise ValueError(
                    "Stop-to-station mapping incomplete: "
                    f"missing station_code for {n_missing} rows ({share:.1%}). "
                    f"Example stop_id: {examples}. "
                    f"Join tried: raw.{raw_key} -> stop_index.{idx_key} (match-rate on keys ≈ {hit_rate:.1%}). "
                    "This usually means the stop_id parsing/join key does not match rer_stop_index.csv, "
                    "or rer_stop_index.csv was built from a different GTFS snapshot."
                )

    # --- weather merge (hourly) ---
    df["poll_at_utc"] = pd.to_datetime(df["poll_at_utc"], utc=True, errors="coerce")
    df["weather_time_utc"] = df["poll_at_utc"].dt.floor("H")
    weather["weather_time_utc"] = pd.to_datetime(weather["weather_time_utc"], utc=True, errors="coerce")

    df = df.merge(
        weather,
        on=["station_code", "weather_time_utc"],
        how="left",
        validate="m:1",
    )

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)

    return df
