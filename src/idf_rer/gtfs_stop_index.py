from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import re

import pandas as pd


_STOP_ID_RE = re.compile(r":(Q|SP):(\d+):")


def parse_numeric_stop_code(stop_id: str) -> Optional[str]:
    """
    Extract numeric code from SIRI-like stop ids, e.g.
      STIF:StopPoint:Q:474053: -> 474053
      STIF:StopArea:SP:43044:  -> 43044
    """
    if not isinstance(stop_id, str):
        return None
    m = _STOP_ID_RE.search(stop_id)
    return m.group(2) if m else None


@dataclass(frozen=True)
class GtfsStopIndexPaths:
    stops_txt: Path
    stop_extensions_txt: Path
    out_csv: Path


def build_stop_index(paths: GtfsStopIndexPaths) -> Path:
    """
    Build a compact stop index from IDFM GTFS tables.

    Expected inputs (as you already have):
      - stops.txt
      - stop_extensions.txt

    Output columns (minimal but sufficient for joins):
      numeric_code, stop_id_idfm, location_type, stop_name, parent_station,
      stop_lat, stop_lon, monomodal_stop_id, monomodal_code
    """
    stops = pd.read_csv(paths.stops_txt, dtype=str)
    ext = pd.read_csv(paths.stop_extensions_txt, dtype=str)

    # Normalize column names (GTFS variants exist)
    stops = stops.rename(
        columns={
            "stop_id": "stop_id_idfm",
            "stop_name": "stop_name",
            "parent_station": "parent_station",
            "stop_lat": "stop_lat",
            "stop_lon": "stop_lon",
            "location_type": "location_type",
        }
    )
    ext = ext.rename(
        columns={
            "stop_id": "stop_id_idfm",
            "monomodalStopPlace": "monomodal_stop_id",
            "monomodalStopPlace_id": "monomodal_stop_id",
            "monomodalStopPlaceName": "monomodal_stop_place_name",
            "monomodal_code": "monomodal_code",
        }
    )

    if "stop_id_idfm" not in stops.columns:
        raise ValueError("stops.txt missing stop_id column")
    if "stop_id_idfm" not in ext.columns:
        raise ValueError("stop_extensions.txt missing stop_id column")

    df = stops.merge(ext[["stop_id_idfm", "monomodal_stop_id"]].drop_duplicates(), on="stop_id_idfm", how="left")

    # numeric code: IDFM:472963 -> 472963
    df["numeric_code"] = df["stop_id_idfm"].astype(str).str.replace("IDFM:", "", regex=False)

    keep = [
        "numeric_code",
        "stop_id_idfm",
        "location_type",
        "stop_name",
        "parent_station",
        "stop_lat",
        "stop_lon",
        "monomodal_stop_id",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].drop_duplicates()

    paths.out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(paths.out_csv, index=False)
    return paths.out_csv
