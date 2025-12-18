from pathlib import Path
from idf_rer.station_catalog import build_station_catalog


def main() -> None:
    stop_index = Path("data/static/rer_stop_index.csv")
    out = Path("data/static/stations.csv")

    df = build_station_catalog(stop_index, out)
    print(f"OK: wrote {out} ({len(df)} stations)")


if __name__ == "__main__":
    main()
