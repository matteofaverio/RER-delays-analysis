from pathlib import Path
from idf_rer.gtfs_stop_index import build_stop_index


def main() -> None:
    stops = Path("data/gtfs/stops.txt")
    ext = Path("data/gtfs/stop_extensions.txt")
    out = Path("data/static/rer_stop_index.csv")

    idx = build_stop_index(stops, ext, out)
    print(f"OK: wrote {out} ({len(idx)} rows)")


if __name__ == "__main__":
    main()
