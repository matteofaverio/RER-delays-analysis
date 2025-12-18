from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from .config import Settings
from .prim_client import fetch_estimated_timetable_json
from .siri_flatten import flatten_estimated_timetable


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _service_day(local_ts: pd.Timestamp, cutoff_h: int, cutoff_m: int) -> str:
    """
    Assign a poll timestamp to a service day (YYYY-MM-DD) with a cutoff at 02:30 local time.
    """
    local_ts = local_ts.tz_convert("Europe/Paris")
    cutoff = local_ts.normalize() + pd.Timedelta(hours=cutoff_h, minutes=cutoff_m)
    day = (local_ts - pd.Timedelta(days=1)).date() if local_ts < cutoff else local_ts.date()
    return day.isoformat()


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _isoformat_with_colon_offset(s: pd.Series) -> pd.Series:
    # +0100 -> +01:00
    return s.dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
        r"(\+|\-)(\d{2})(\d{2})$", r"\1\2:\3", regex=True
    )


def build_rer_events(settings: Settings) -> pd.DataFrame:
    """
    One PRIM snapshot -> flattened events -> RER subset within lead-time horizon.
    """
    payload = fetch_estimated_timetable_json(settings)
    df = flatten_estimated_timetable(payload)

    if df.empty:
        return df

    # Keep only RER (line_code starts with "RER ")
    df["line_code"] = df["line_code"].astype(str)
    df = df[df["line_code"].str.startswith("RER ", na=False)].copy()

    # Keep only events with delay and scheduled timestamp
    df = df[df["delay_seconds"].notna() & df["scheduled_time_utc"].notna()].copy()

    # Lead-time filter: scheduled within next 10 minutes
    df = df[df["lead_time_seconds"].between(0, settings.lead_time_horizon_s)].copy()

    return df


def aggregate_poll(df_events: pd.DataFrame, tz_local: str = "Europe/Paris") -> pd.DataFrame:
    """
    Aggregate a snapshot into one row per (stop_id, line_code).
    """
    if df_events.empty:
        return pd.DataFrame(
            columns=[
                "poll_at_utc", "poll_at_local",
                "stop_id", "line_code",
                "mean_delay_s", "mean_lateness_s",
                "n", "n_neg", "n_pos",
            ]
        )

    poll_at_utc = df_events["snapshot_at_utc"].dropna()
    poll_at_utc = poll_at_utc.iloc[0] if not poll_at_utc.empty else pd.to_datetime(_utc_now(), utc=True)
    poll_at_local = poll_at_utc.tz_convert(tz_local)

    def mean_lateness(s: pd.Series) -> float:
        return s.clip(lower=0).mean()

    g = (
        df_events.groupby(["stop_id", "line_code"], as_index=False)["delay_seconds"]
        .agg(
            mean_delay_s="mean",
            mean_lateness_s=mean_lateness,
            n="count",
            n_neg=lambda s: int((s < 0).sum()),
            n_pos=lambda s: int((s > 0).sum()),
        )
    )

    g["poll_at_utc"] = poll_at_utc
    g["poll_at_local"] = poll_at_local
    g["mean_delay_s"] = g["mean_delay_s"].round(3)
    g["mean_lateness_s"] = g["mean_lateness_s"].round(3)
    g["n"] = g["n"].astype(int)

    # Serialize timestamps as ISO strings (stable in CSV)
    g["poll_at_utc"] = g["poll_at_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    g["poll_at_local"] = _isoformat_with_colon_offset(pd.to_datetime(g["poll_at_local"]))

    return g[
        ["poll_at_utc", "poll_at_local", "stop_id", "line_code",
         "mean_delay_s", "mean_lateness_s", "n", "n_neg", "n_pos"]
    ]


def append_raw_poll_rows(rows: pd.DataFrame, settings: Settings) -> Optional[Path]:
    """
    Append poll rows to data/rer_raw/<service_day>.csv with idempotency on (poll_at_utc, stop_id, line_code).
    """
    if rows is None or rows.empty:
        return None

    poll_local = pd.to_datetime(rows["poll_at_local"].iloc[0], errors="coerce")
    if poll_local.tzinfo is None:
        poll_local = poll_local.tz_localize(settings.tz_local)
    else:
        poll_local = poll_local.tz_convert(settings.tz_local)

    day = _service_day(poll_local, settings.service_day_cutoff_h, settings.service_day_cutoff_m)
    out_path = settings.rer_raw_dir / f"{day}.csv"
    settings.rer_raw_dir.mkdir(parents=True, exist_ok=True)

    key_cols = ["poll_at_utc", "stop_id", "line_code"]

    if out_path.exists():
        cur = pd.read_csv(out_path, dtype={"stop_id": str, "line_code": str})
        merged = pd.concat([cur, rows], ignore_index=True)
        merged.drop_duplicates(subset=key_cols, keep="last", inplace=True)
        merged.sort_values(key_cols, inplace=True)
        merged.to_csv(out_path, index=False)
    else:
        rows.sort_values(key_cols, inplace=True)
        rows.to_csv(out_path, index=False)

    return out_path


def rebuild_daily_from_raw(raw_path: Path, settings: Settings, bin_sec: int) -> Path:
    """
    Build data/rer_daily/<day>.csv by binning in local time and using train-count weighted averages.
    """
    df = pd.read_csv(raw_path, dtype={"stop_id": str, "line_code": str})
    out_path = settings.rer_daily_dir / raw_path.name
    settings.rer_daily_dir.mkdir(parents=True, exist_ok=True)

    if df.empty:
        df.to_csv(out_path, index=False)
        return out_path

    df["poll_at_utc"] = pd.to_datetime(df["poll_at_utc"], utc=True, errors="coerce")
    df["poll_at_local"] = pd.to_datetime(df["poll_at_local"], errors="coerce")

    df["mean_delay_s"] = pd.to_numeric(df["mean_delay_s"], errors="coerce")
    df["mean_lateness_s"] = pd.to_numeric(df["mean_lateness_s"], errors="coerce")
    df["n"] = pd.to_numeric(df["n"], errors="coerce").fillna(0).astype(int)
    df["n_neg"] = pd.to_numeric(df["n_neg"], errors="coerce").fillna(0).astype(int)
    df["n_pos"] = pd.to_numeric(df["n_pos"], errors="coerce").fillna(0).astype(int)

    freq = f"{int(bin_sec)}S"
    bin_start = df["poll_at_local"].dt.tz_convert(settings.tz_local).dt.floor(freq)
    df["poll_bin_start_local_iso"] = _isoformat_with_colon_offset(bin_start)

    if bin_sec % 60 == 0:
        df["poll_bin_local"] = bin_start.dt.strftime("%Y-%m-%d %H:%M")
    else:
        df["poll_bin_local"] = bin_start.dt.strftime("%Y-%m-%d %H:%M:%S")

    df["w_delay"] = df["mean_delay_s"] * df["n"]
    df["w_late"] = df["mean_lateness_s"] * df["n"]

    gb = df.groupby(["poll_bin_local", "poll_bin_start_local_iso", "stop_id", "line_code"], as_index=False).agg(
        sum_w_delay=("w_delay", "sum"),
        sum_w_late=("w_late", "sum"),
        n=("n", "sum"),
        n_neg=("n_neg", "sum"),
        n_pos=("n_pos", "sum"),
        last_poll_at_utc=("poll_at_utc", "max"),
        last_poll_at_local=("poll_at_local", "max"),
    )

    gb["mean_delay_s"] = (gb["sum_w_delay"] / gb["n"]).replace([np.inf, -np.inf], np.nan).round(3)
    gb["mean_lateness_s"] = (gb["sum_w_late"] / gb["n"]).replace([np.inf, -np.inf], np.nan).round(3)

    gb = gb[
        ["poll_bin_local", "poll_bin_start_local_iso", "stop_id", "line_code",
         "mean_delay_s", "mean_lateness_s", "n", "n_neg", "n_pos",
         "last_poll_at_utc", "last_poll_at_local"]
    ].sort_values(["poll_bin_local", "stop_id", "line_code"])

    gb.to_csv(out_path, index=False)
    return out_path


def run_one_poll(settings: Settings, bin_sec: int) -> Tuple[Optional[Path], Optional[Path]]:
    """
    End-to-end: fetch snapshot -> filter -> aggregate -> append RAW -> rebuild DAILY.
    """
    events = build_rer_events(settings)
    rows = aggregate_poll(events, tz_local=settings.tz_local)
    raw_path = append_raw_poll_rows(rows, settings=settings)
    daily_path = rebuild_daily_from_raw(raw_path, settings=settings, bin_sec=bin_sec) if raw_path else None
    return raw_path, daily_path
