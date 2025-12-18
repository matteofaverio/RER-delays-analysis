from dataclasses import dataclass
import os


@dataclass(frozen=True)
class PrimConfig:
    api_key: str
    estimated_timetable_url: str


def load_prim_config() -> PrimConfig:
    api_key = os.getenv("PRIM_API_KEY", "").strip()
    url = os.getenv("PRIM_ESTIMATED_TIMETABLE_URL", "").strip()

    if not api_key:
        raise RuntimeError("Missing PRIM_API_KEY (set it in .env).")
    if not url:
        raise RuntimeError("Missing PRIM_ESTIMATED_TIMETABLE_URL (set it in .env).")

    return PrimConfig(api_key=api_key, estimated_timetable_url=url)
