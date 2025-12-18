from typing import Optional, Tuple
from pathlib import Path
from datetime import time as dtime

import pandas as pd
import numpy as np

from .prim_client import PrimClient
from .siri_flatten import flatten_estimated_timetable


RER_LINES = ["RER A", "RER B", "RER C", "RER D", "RER E"]


def _service_day(local_ts: pd.Timestamp, cutoff: dtime = dtime(2, 30)) -> str:
    """
    Assign a poll to a service-day:
    local time < 02:30 -> previous calendar day
    else              -> current calendar day
    """
    if local_ts.time() < cutoff:
        day = (local_ts - pd.Timedelta(days=1)).date()
    else:
        day = local_ts.date()
    return day.isoformat()


def build_event_table(payload: dict) -> pd.DataFrame:
    rows = flatten_estimated_timetable(payload)
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # enforce expected columns
    for col in [
        "snapshot_at_utc","stop_id","stop_name","line_code","direction","destination",
        "scheduled_time_utc","rt_time_utc","delay_seconds","lead_time_seconds","vehicle_journey_id"
    ]:
        if col not in df.columns:
            df[col] = np.nan

    # normalize
    df["line_code"] = df["line_code"].astype(str).str.strip()
    df = df[df["line_code"].isin(RER_LINES)].copy()

    return df


def apply_lead_time_filter(df: pd.DataFrame, horizon_s: int = 600) -> pd.DataFrame:
    if df.empty:
        return df
    lead = pd.to_numeric(df["lead_time_seconds"], errors="coerce")
    # keep near-future arrivals only
    return df[(lead >= 0) & (lead <= float(horizon_s))].copy()


def write_outliers(df: pd.DataFrame, out_path: Path, abs_cap_s: int = 3600) -> None:
    if df.empty:
        return
    delay = pd.to_numeric(df["delay_seconds"], errors="coerce")
    mask = delay.abs() >= float(abs_cap_s)
    if mask.any():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.loc[mask].to_csv(out_path, index=False)


def aggregate_poll(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """
    Aggregate one snapshot into one row per (stop_id, line_code):
      mean_delay_s, mean_lateness_s, n, n_neg, n_pos
    """
    if df.empty:
        # keep timestamps explicit
        poll_utc = pd.Timestamp.utcnow().tz_localize("UTC")
        poll_local = poll_utc.tz_convert("Europe/Paris")
        cols = [
            "poll_at_utc","poll_at_local","stop_id","line_code",
            "mean_delay_s","mean_lateness_s","n","n_neg","n_pos"
        ]
        return pd.DataFrame(columns=cols), poll_utc, poll_local

    poll_utc = pd.to_datetime(df["snapshot_at_utc"].iloc[0], utc=True, errors="coerce")
    if pd.isna(poll_utc):
        poll_utc = pd.Timestamp.utcnow().tz_localize("UTC")
    poll_local = poll_utc.tz_convert("Europe/Paris")

    x = df.copy()
    x["delay_seconds"] = pd.to_numeric(x["delay_seconds"], errors="coerce")
    x = x.dropna(subset=["delay_seconds"])

    if x.empty:
        cols = [
            "poll_at_utc","poll_at_local","stop_id","line_code",
            "mean_delay_s","mean_lateness_s","n","n_neg","n_pos"
        ]
        return pd.DataFrame(columns=cols), poll_utc, poll_local

    def mean_lateness(s: pd.Series) -> float:
        return s.clip(lower=0).mean()

    g = (
        x.groupby(["stop_id", "line_code"], as_index=False)["delay_seconds"]
          .agg(
              mean_delay_s="mean",
              mean_lateness_s=mean_lateness,
              n="count",
              n_neg=lambda s: int((s < 0).sum()),
              n_pos=lambda s: int((s > 0).sum()),
          )
    )

    g["poll_at_utc"] = poll_utc.isoformat()
    g["poll_at_local"] = poll_local.isoformat()

    # tidy + rounding
    g["mean_delay_s"] = g["mean_delay_s"].astype(float).round(3)
    g["mean_lateness_s"] = g["mean_lateness_s"].astype(float).round(3)
    g["n"] = g["n"].astype(int)

    cols = [
        "poll_at_utc","poll_at_local","stop_id","line_code",
        "mean_delay_s","mean_lateness_s","n","n_neg","n_pos"
    ]
    return g[cols], poll_utc, poll_local


def append_daily_raw(rows: pd.DataFrame, raw_dir: Path, poll_local: pd.Timestamp) -> Optional[Path]:
    if rows is None or rows.empty:
        return None

    day = _service_day(poll_local)
    raw_dir.mkdir(parents=True, exist_ok=True)
    out = raw_dir / f"{day}.csv"

    key = ["poll_at_utc","stop_id","line_code"]

    if out.exists():
        cur = pd.read_csv(out, dtype=str)
        merged = pd.concat([cur, rows.astype(str)], ignore_index=True)
        merged.drop_duplicates(subset=key, keep="last", inplace=True)
        merged.sort_values(key, inplace=True)
        merged.to_csv(out, index=False)
    else:
        rows.sort_values(key).to_csv(out, index=False)

    return out


def poll_once(
    client: PrimClient,
    url: str,
    raw_dir: Path,
    lead_horizon_s: int = 600,
    outliers_path: Optional[Path] = None,
) -> Optional[Path]:
    payload = client.get_json(url)
    events = build_event_table(payload)
    events = apply_lead_time_filter(events, horizon_s=lead_horizon_s)

    if outliers_path is not None:
        write_outliers(events, outliers_path)

    agg, _, poll_local = aggregate_poll(events)
    return append_daily_raw(agg, raw_dir=raw_dir, poll_local=poll_local)
