import argparse
from idf_rer.gtfs_stop_index import GtfsStopIndexPaths, build_stop_index


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stops", default="data/static/stops.txt")
    ap.add_argument("--stop-extensions", default="data/static/stop_extensions.txt")
    ap.add_argument("--out", default="data/derived/stop_index.csv")
    args = ap.parse_args()

    out = build_stop_index(GtfsStopIndexPaths(args.stops, args.stop_extensions, args.out))
    print(out)


if __name__ == "__main__":
    main()
