from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd


@dataclass(frozen=True)
class HourlyPanelConfig:
    merged_dir: Path = Path("merged_datasets")
    out_csv: Path = Path("hourly_panel.csv")
    metrics: List[str] = None
    min_polls_per_hour: int = 1
    blacklist_stations: List[str] = None

    def __post_init__(self):
        object.__setattr__(self, "metrics", self.metrics or ["mean_delay_s", "precipitation", "temperature_2m", "wind_speed_10m"])
        object.__setattr__(self, "blacklist_stations", self.blacklist_stations or [])


def _load_one(file: Path, cfg: HourlyPanelConfig) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(file)
        if len(df.columns) < 2:
            df = pd.read_csv(file, sep="\t")
    except Exception:
        try:
            df = pd.read_csv(file, sep="\t")
        except Exception:
            return None

    # normalize common "mean" suffix columns
    df = df.rename(
        columns={
            "mean_delay_s_mean": "mean_delay_s",
            "precipitation_mean": "precipitation",
            "temperature_2m_mean": "temperature_2m",
            "wind_speed_10m_mean": "wind_speed_10m",
        }
    )

    if "stop_name" in df.columns and cfg.blacklist_stations:
        df = df[~df["stop_name"].isin(cfg.blacklist_stations)].copy()

    time_col = "poll_at_local" if "poll_at_local" in df.columns else "poll_at_utc"
    if time_col not in df.columns:
        return None

    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col]).copy()

    df["dt_local"] = df[time_col].dt.tz_convert("Europe/Paris")
    df["date"] = df["dt_local"].dt.date
    df["hour"] = df["dt_local"].dt.hour

    keep = ["station_code", "date", "hour"] + [m for m in cfg.metrics if m in df.columns]
    return df[keep].copy()


def build_hourly_panel(cfg: HourlyPanelConfig) -> Path:
    files = sorted(cfg.merged_dir.glob("merged_*.csv"))
    chunks: List[pd.DataFrame] = []
    for f in files:
        df = _load_one(f, cfg)
        if df is not None and not df.empty:
            chunks.append(df)

    if not chunks:
        raise RuntimeError(f"No usable files found in {cfg.merged_dir}")

    raw = pd.concat(chunks, ignore_index=True)

    grp = raw.groupby(["station_code", "date", "hour"])
    agg = {m: "mean" for m in cfg.metrics if m in raw.columns}
    agg["_obs"] = ("station_code", "count")

    panel = grp.agg(**agg).reset_index().rename(columns={"_obs": "obs_count"})
    panel = panel[panel["obs_count"] >= cfg.min_polls_per_hour].copy()

    # drop stations with always-zero delay (optional hygiene)
    if "mean_delay_s" in panel.columns:
        tot = panel.groupby("station_code")["mean_delay_s"].sum()
        bad = tot[tot == 0].index
        if len(bad) > 0:
            panel = panel[~panel["station_code"].isin(bad)].copy()

    panel = panel.drop(columns=["date"])
    panel = panel.sort_values(["station_code", "hour"]).copy()

    for c in ["mean_delay_s", "temperature_2m", "wind_speed_10m"]:
        if c in panel.columns:
            panel[c] = panel[c].round(2)

    cfg.out_csv.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(cfg.out_csv, index=False)
    return cfg.out_csv
