from typing import Any, Dict, Optional
import time
import httpx


class PrimClient:
    def __init__(
        self,
        api_key: str,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        backoff_s: Optional[list] = None,
    ) -> None:
        self._api_key = api_key
        self._timeout = httpx.Timeout(timeout_s)
        self._max_retries = max_retries
        self._backoff = backoff_s or [0.0, 1.0, 2.0, 4.0]

    def get_json(self, url: str) -> Dict[str, Any]:
        headers = {"apiKey": self._api_key}

        last_err: Optional[Exception] = None
        for k in range(min(self._max_retries, len(self._backoff))):
            delay = self._backoff[k]
            if delay > 0:
                time.sleep(delay)

            try:
                with httpx.Client(timeout=self._timeout) as client:
                    r = client.get(url, headers=headers)
                    r.raise_for_status()
                    return r.json()
            except Exception as e:  # intentionally broad: network + decoding
                last_err = e

        raise RuntimeError(f"PRIM request failed after retries: {last_err}")
