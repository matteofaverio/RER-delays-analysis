from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    prim_api_key: str
    estimated_timetable_url: str

    tz_local: str = "Europe/Paris"
    lead_time_horizon_s: int = 600  # keep events whose scheduled time is within +10 min
    service_day_cutoff_h: int = 2
    service_day_cutoff_m: int = 30

    data_dir: Path = Path("data")
    rer_raw_dir: Path = Path("data/rer_raw")
    rer_daily_dir: Path = Path("data/rer_daily")

    @staticmethod
    def from_env() -> "Settings":
        api_key = os.getenv("PRIM_API_KEY", "").strip()
        url = os.getenv("IDFM_ESTIMATED_TIMETABLE_URL", "").strip()

        if not api_key:
            raise RuntimeError("Missing PRIM_API_KEY (set it in .env).")
        if not url:
            raise RuntimeError("Missing IDFM_ESTIMATED_TIMETABLE_URL (set it in .env).")

        return Settings(prim_api_key=api_key, estimated_timetable_url=url)
