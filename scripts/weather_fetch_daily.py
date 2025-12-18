import argparse
from idf_rer.weather.build_daily_weather import WeatherDailyPaths, build_daily_weather


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--stations-csv", default="stations.csv")
    ap.add_argument("--out-dir", default="data/weather")
    args = ap.parse_args()

    paths = WeatherDailyPaths(stations_csv=args.stations_csv, out_dir=args.out_dir)
    out = build_daily_weather(args.date, paths)
    print(out)


if __name__ == "__main__":
    main()
