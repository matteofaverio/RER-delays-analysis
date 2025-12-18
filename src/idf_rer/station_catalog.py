from pathlib import Path
import pandas as pd


def build_station_catalog(stop_index_csv: Path, out_csv: Path) -> pd.DataFrame:
    idx = pd.read_csv(stop_index_csv, dtype=str).fillna("")
    required = {"quay_code","monomodal_stop_id","monomodal_code","stop_name","parent_station","stop_lat","stop_lon","zone_id"}
    missing = required - set(idx.columns)
    if missing:
        raise RuntimeError(f"stop index missing columns: {', '.join(sorted(missing))}")

    g = idx.groupby(["monomodal_stop_id","monomodal_code","stop_name","parent_station","stop_lat","stop_lon","zone_id"], as_index=False).agg(
        n_quays=("quay_code", "nunique"),
    )

    # add placeholders for enrichment steps (population density, line flags, etc.)
    g["pop_density_km2"] = ""
    g["station_code"] = ""
    g["old_station_code"] = ""

    # reorder
    cols = [
        "station_code","old_station_code",
        "monomodal_stop_id","monomodal_code","stop_name","parent_station",
        "stop_lat","stop_lon","zone_id","pop_density_km2","n_quays",
    ]
    g = g[cols]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    g.to_csv(out_csv, index=False)
    return g
