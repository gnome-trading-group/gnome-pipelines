"""CollegeFootballData API client with a permanent on-disk response cache.

The free tier allows 1,000 calls a month, so every response is cached forever (completed seasons
never change) and each fetch checks the remaining quota before spending anything.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import time
from pathlib import Path

import requests

from gnome_pipelines.credentials import secret

BASE_URL = "https://api.collegefootballdata.com"
CACHE_DIR = Path.home() / ".cache" / "gnomepy" / "cfbd"
# A connection reset mid-response or an outage of a minute or two failed scheduled runs on Oct 25 (502s for over the
# 31 s the old retries waited) and Oct 26 (reset while reading the body). Waits of 5, 10, 20, 40, 80 and 160 s ride out
# about five minutes before giving up.
ATTEMPTS = 7
BACKOFF = 5.0
RETRYABLE = (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
             requests.exceptions.Timeout)


class BudgetExceeded(RuntimeError):
    pass


class CfbdClient:
    def __init__(self, api_key: str | None = None, cache_dir: Path = CACHE_DIR, reserve: int = 100) -> None:
        self._api_key = api_key or secret("cfbd", "CFBD_API_KEY")
        self._cache_dir = cache_dir
        # Calls held back so a runaway fetch can't strand the month's quota for live/debug use.
        self._reserve = reserve
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._api_key}"
        self.calls_made = 0

    def _request(self, url: str, params: dict | None = None, timeout: float = 120) -> requests.Response:
        """GET with retries on 429, 5xx and dropped connections; any other error is raised at once."""
        for attempt in range(ATTEMPTS):
            last = attempt == ATTEMPTS - 1
            try:
                resp = self._session.get(url, params=params, timeout=timeout)
            except RETRYABLE:
                if last:
                    raise
                time.sleep(BACKOFF * 2 ** attempt)
                continue
            if (resp.status_code == 429 or resp.status_code >= 500) and not last:
                time.sleep(BACKOFF * 2 ** attempt)
                continue
            resp.raise_for_status()
            return resp
        raise AssertionError("unreachable")

    def info(self) -> dict:
        """Quota status. This endpoint does not count against the quota."""
        return self._request(f"{BASE_URL}/info", timeout=30).json()

    def remaining_calls(self) -> int:
        return int(self.info()["remainingCalls"])

    def ensure_budget(self, needed: int) -> None:
        remaining = self.remaining_calls()
        if needed > remaining - self._reserve:
            raise BudgetExceeded(
                f"need {needed} uncached calls but only {remaining} remain (reserve {self._reserve})"
            )

    def _cache_path(self, endpoint: str, params: dict) -> Path:
        key = json.dumps(params, sort_keys=True)
        digest = hashlib.sha1(key.encode()).hexdigest()[:16]
        readable = "_".join(f"{k}-{v}" for k, v in sorted(params.items()))
        return self._cache_dir / endpoint.strip("/").replace("/", "_") / f"{readable}_{digest}.json.gz"

    def is_cached(self, endpoint: str, params: dict) -> bool:
        return self._cache_path(endpoint, params).exists()

    def get(self, endpoint: str, params: dict, *, refresh: bool = False) -> list | dict:
        path = self._cache_path(endpoint, params)
        if path.exists() and not refresh:
            with gzip.open(path, "rt") as f:
                return json.load(f)

        resp = self._request(f"{BASE_URL}{endpoint}", params=params)
        self.calls_made += 1
        data = resp.json()

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with gzip.open(tmp, "wt") as f:
            json.dump(data, f)
        tmp.rename(path)
        return data
