import argparse
from dotenv import load_dotenv

from idf_rer.config import Settings
from idf_rer.polling_pipeline import run_one_poll


def main() -> None:
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-sec", type=int, default=300)
    args = ap.parse_args()

    settings = Settings.from_env()
    raw_path, daily_path = run_one_poll(settings, bin_sec=args.bin_sec)
    print(f"raw: {raw_path}")
    print(f"daily: {daily_path}")


if __name__ == "__main__":
    main()
