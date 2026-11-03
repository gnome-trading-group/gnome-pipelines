"""FACEIT Data API client (https://open.faceit.com/data/v4), shared by worker threads.

FACEIT rate-limits per key with 1 s windows and reports what is left in each response. The limiter adapts to it rather
than guessing a fixed rate: it adds to the rate while calls succeed, cuts it and pauses everyone on a 429, and waits
for the next window when the gateway says none is left in this one. It also halves the number of requests in flight
on a network timeout, so a weak connection is not flooded into more timeouts.
"""
from __future__ import annotations

import logging
import threading
import time

import requests

BASE = "https://open.faceit.com/data/v4"
logger = logging.getLogger(__name__)


class AdaptiveLimiter:
    def __init__(self, rate: float = 18.0, floor: float = 2.0, ceiling: float = 60.0, step: float = 1.0,
                 backoff: float = 0.7, inflight: int = 32, max_inflight: int = 128):
        self.rate, self.floor, self.ceiling, self.step, self.backoff = rate, floor, ceiling, step, backoff
        self.cap, self.max_cap = float(inflight), max_inflight
        self._lock = threading.Lock()
        self._room = threading.Condition(self._lock)
        self._inflight = 0
        self._next = 0.0
        self._paused_until = 0.0
        self._last_cut = 0.0
        self.throttles = 0

    def acquire(self) -> None:
        with self._room:
            while self._inflight >= int(self.cap):
                self._room.wait()
            self._inflight += 1
            slot = max(time.monotonic(), self._next, self._paused_until)
            self._next = slot + 1.0 / self.rate
        time.sleep(max(0.0, slot - time.monotonic()))

    def release(self) -> None:
        with self._room:
            self._inflight -= 1
            self._room.notify_all()

    def network_error(self) -> None:
        with self._lock:
            self.cap = max(4.0, self.cap * 0.5)

    def ok(self, headers) -> None:
        with self._lock:
            self.cap = min(self.max_cap, self.cap + 1.0 / self.cap)
            self.rate = min(self.ceiling, self.rate + self.step / self.rate)
            if (headers.get("ratelimit-remaining") or "").strip() == "0":
                # reset is whole seconds; 0 means the window ends within the second
                reset = float(headers.get("ratelimit-reset") or 0) or 0.25
                self._paused_until = max(self._paused_until, time.monotonic() + reset)

    def throttled(self, headers) -> None:
        with self._lock:
            self.throttles += 1
            # The 429s from one burst arrive together; cutting once per window keeps them from compounding.
            now = time.monotonic()
            if now - self._last_cut > 1.0:
                self.rate = max(self.floor, self.rate * self.backoff)
                self._last_cut = now
            pause = float(headers.get("retry-after") or headers.get("ratelimit-reset") or 0) or 1.0
            self._paused_until = max(self._paused_until, time.monotonic() + pause)


class FaceitClient:
    def __init__(self, api_key: str, limiter: AdaptiveLimiter | None = None, max_attempts: int = 30):
        self.api_key = api_key
        self.limiter = limiter or AdaptiveLimiter()
        self.max_attempts = max_attempts
        self.calls = 0
        self._local = threading.local()

    def _http(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
            self._local.session.headers["Authorization"] = f"Bearer {self.api_key}"
        return self._local.session

    def get(self, path: str, **params) -> dict | None:
        """The response, or None for a 400/404. Network errors, 429 and 5xx are retried, never turned into None: a None
        reads as "nothing more" to a history walk and would silently truncate it."""
        for attempt in range(self.max_attempts):
            self.limiter.acquire()
            try:
                r = self._http().get(BASE + path, params=params, timeout=30)
            except requests.RequestException as e:
                self.limiter.release()
                self.limiter.network_error()
                logger.debug("%s attempt %d: %s", path, attempt, type(e).__name__)
                time.sleep(min(60, 2 ** min(attempt, 6)))
                continue
            self.limiter.release()
            self.calls += 1
            if r.status_code == 429:
                self.limiter.throttled(r.headers)
                continue
            if r.status_code >= 500:
                time.sleep(min(60, 2 ** min(attempt, 6)))
                continue
            self.limiter.ok(r.headers)
            if r.status_code in (400, 404):
                return None
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"FACEIT {path} still failing after {self.max_attempts} attempts")

    def player_history(self, faceit_id: str, start: int, end: int) -> list[dict]:
        """Every CS2 game of a player that started in [start, end) (unix seconds), newest first."""
        items, offset = [], 0
        while True:
            page = self.get(f"/players/{faceit_id}/history", game="cs2", offset=offset, limit=100, **{"from": start, "to": end})
            batch = (page or {}).get("items") or []
            items += batch
            if len(batch) < 100:
                return items
            offset += 100

    def championship_matches(self, competition_id: str) -> list[dict]:
        """Every finished match listed for a championship."""
        items, offset = [], 0
        while True:
            page = self.get(f"/championships/{competition_id}/matches", type="past", offset=offset, limit=100)
            batch = (page or {}).get("items") or []
            items += batch
            if len(batch) < 100:
                return items
            offset += 100
