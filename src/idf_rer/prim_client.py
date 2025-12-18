from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx

from .config import Settings


def fetch_estimated_timetable_json(settings: Settings, timeout_s: float = 30.0) -> Dict[str, Any]:
    """
    Fetch a single Estimated Timetable snapshot from IDFM PRIM.

    Authentication: apiKey header.
    """
    headers = {"apiKey": settings.prim_api_key}
    timeout = httpx.Timeout(connect=10.0, read=timeout_s, write=10.0, pool=10.0)

    backoff_s = [0.0, 1.0, 2.0, 4.0]
    last_err: Optional[Exception] = None

    for b in backoff_s:
        if b:
            time.sleep(b)
        try:
            with httpx.Client(timeout=timeout) as client:
                r = client.get(settings.estimated_timetable_url, headers=headers)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_err = e

    raise RuntimeError(f"PRIM request failed after retries: {last_err}")
