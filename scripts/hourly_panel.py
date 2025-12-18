from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class HourlyPanelConfig:
    input_dir: Path
    output_csv: Path
    # Expected columns in merged daily files
    time_col_candidates: Sequence[str] = ("poll_at_local", "poll_at_utc")
    station_col: str = "station_code"
    # Metrics to aggregate (means). If a column is missing, it is skipped.
    metrics: Sequence[str] = ("mean_delay_s", "precipitation", "temperature_2m", "wind_speed_10m")
    min_polls_per_hour: int = 1
    keep_date_column: bool = False


def _find_time_col(df: pd.DataFrame, candidates: Sequence[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"None of the time columns {list(candidates)} found in dataframe.")


def _read_csv_robust(path: Path) -> pd.DataFrame:
    # merged files should be CSV; a small fallback helps with accidental TSV exports.
    try:
        df = pd.read_csv(path)
        if df.shape[1] <= 1:
            df = pd.read_csv(path, sep="\t")
        return df
    except Exception:
        return pd.read_csv(path, sep="\t")


def build_hourly_panel(cfg: HourlyPanelConfig) -> pd.DataFrame:
    if not cfg.input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {cfg.input_dir}")

    files = sorted(cfg.input_dir.glob("merged_*.csv"))
    if not files:
        raise FileNotFoundError(f"No merged_*.csv files found in: {cfg.input_dir}")

    chunks = []
    for fp in files:
        df = _read_csv_robust(fp)

        if cfg.station_col not in df.columns:
            # Skip silently: this keeps the function robust to stray files.
            continue

        time_col = _find_time_col(df, cfg.time_col_candidates)

        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        df = df.dropna(subset=[time_col])

        # Local time for hour-of-day
        dt_local = df[time_col].dt.tz_convert("Europe/Paris")
        df["date"] = dt_local.dt.date
        df["hour"] = dt_local.dt.hour

        cols = [cfg.station_col, "date", "hour"]
        cols += [c for c in cfg.metrics if c in df.columns]
        df = df[cols].copy()

        chunks.append(df)

    if not chunks:
        raise ValueError("No valid merged files were loaded (missing station_code/time columns).")

    raw = pd.concat(chunks, ignore_index=True)

    # Aggregate: station x date x hour
    group_keys = [cfg.station_col, "date", "hour"]
    agg_dict = {m: "mean" for m in cfg.metrics if m in raw.columns}
    # count polls per hour
    agg_dict["_polls"] = (cfg.station_col, "count")

    panel = (
        raw.groupby(group_keys, as_index=False)
           .agg(**{m: (m, "mean") for m in agg_dict.keys() if m != "_polls"},
                obs_count=(cfg.station_col, "count"))
    )

    # Filter by minimum number of polls per hour
    panel = panel.loc[panel["obs_count"] >= int(cfg.min_polls_per_hour)].copy()

    # Optional cleanup
    if not cfg.keep_date_column:
        panel = panel.drop(columns=["date"])

    # Tidy ordering
    panel = panel.sort_values([cfg.station_col, "hour"]).reset_index(drop=True)

    # Save
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(cfg.output_csv, index=False)
    return panel


def main() -> None:
    # Defaults designed to be repo-friendly: you can change paths from CLI later if you want.
    cfg = HourlyPanelConfig(
        input_dir=Path("data/derived/merged"),
        output_csv=Path("data/derived/hourly_panel.csv"),
        min_polls_per_hour=1,
        keep_date_column=False,
    )
    build_hourly_panel(cfg)


if __name__ == "__main__":
    main()
