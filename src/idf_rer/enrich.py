from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from .gtfs_stop_index import parse_numeric_stop_code


@dataclass(frozen=True)
class EnrichmentPaths:
    stop_index_csv: Path
    stations_csv: Path  # your stations.csv with monomodal_stop_id + station_code


def enrich_raw_with_station_code(raw_csv: Path, out_csv: Path, paths: EnrichmentPaths) -> Path:
    """
    Add station metadata to a rer_raw daily file:
      stop_id -> numeric_code -> stop_index -> monomodal_stop_id -> stations -> station_code
    Works for both StopPoint:Q and StopArea:SP stop_id patterns.
    """
    raw = pd.read_csv(raw_csv, dtype=str)
    if "stop_id" not in raw.columns:
        raise ValueError(f"{raw_csv} has no stop_id column")

    stop_index = pd.read_csv(paths.stop_index_csv, dtype=str)
    stations = pd.read_csv(paths.stations_csv, dtype=str)

    raw["numeric_code"] = raw["stop_id"].map(parse_numeric_stop_code)
    raw = raw.dropna(subset=["numeric_code"]).copy()

    # Join to stop index
    tmp = raw.merge(stop_index[["numeric_code", "monomodal_stop_id"]].drop_duplicates(),
                    on="numeric_code", how="left")

    # Join to stations
    tmp = tmp.merge(stations[["monomodal_stop_id", "station_code", "stop_name", "stop_lat", "stop_lon", "zone_id"]].drop_duplicates(),
                    on="monomodal_stop_id", how="left", suffixes=("", "_station"))

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp.to_csv(out_csv, index=False)
    return out_csv
