"""HTTP for polling sources: polite, conditional, optionally through ZenRows, with the page cache.

- One request at a time per host, at least HOST_GAP seconds apart, so a source is one considerate client per site.
- Conditional GETs (conditional=True, the default): the ETag and Last-Modified of each URL's last 200 are sent back,
  and a 304 costs almost nothing. A page read once (a review) passes conditional=False, so a retry gets the page.
- Routes, cheapest first (ROUTES): a plain request, ZenRows' basic proxy (1 credit), ZenRows' residential "premium"
  proxy (10 credits; no JS rendering either way). A fetch kind's `via` says which it may use. Per host and page kind
  the fetcher remembers the cheapest route that works: ESCALATE_AFTER refusals in a row (a 401/403/407, ZenRows' own
  4xx, or a 200 the caller's `valid` rejects) move it up one route; a 429 or 5xx is transient and moves nothing;
  after RECHECK it tries one route cheaper again; when even the dearest route refuses, it stops asking for a backoff
  (BACKOFF_START doubling to BACKOFF_MAX) instead of paying every poll. ZenRows is paid only for answers it bills (a
  200), within a daily credit budget of which no host and page kind may take more than HOST_SHARE; the key is
  gnome/pipelines/zenrows (or ZENROWS_API_KEY).
- With archive=True a changed page's body is kept once, gzipped, under s3://<research bucket>/<source>/pages/ by the
  hash of its content (a local folder off AWS), so re-reading it later (a better model) never refetches it.
- Every changed page, refusal or failure is a fetched_pages row (via = the route used), handed to the source's
  TableWriter; 304s are not.
"""
from __future__ import annotations

import gzip
import hashlib
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

import boto3
import requests
from botocore.exceptions import ClientError

from gnome_pipelines import credentials
from gnome_pipelines.datasets.polling import FETCHED_PAGES
from gnome_pipelines.polling.writer import TableWriter

HOST_GAP = 1.0
TIMEOUT = 30.0
ZENROWS_API = "https://api.zenrows.com/v1/"
# Credits per ZenRows request without JS rendering: a datacenter proxy, or the residential ("premium") one.
ZENROWS_CREDITS = {"zenrows": 1, "zenrows_premium": 10}
# The routes a fetch kind may take, cheapest first. "plain" never uses ZenRows; "zenrows" starts on its basic proxy;
# "auto" starts plain and uses ZenRows only when the site refuses.
ROUTES = {"plain": ("plain",), "zenrows": ("zenrows", "zenrows_premium"),
          "auto": ("plain", "zenrows", "zenrows_premium")}
# A site's answers that mean "not you", as opposed to "not there" or "not now".
REFUSED = {401, 403, 407}
NOT_THERE = {404, 410}
# "Not now": a rate limit or an outage, from the site or from ZenRows (402 is ZenRows' account being out of credit).
TRANSIENT = {402, 429, 500, 502, 503, 504}
ESCALATE_AFTER = 3
# How long a host stays on a dearer route before the next fetch tries one cheaper again.
RECHECK = 6 * 3600.0
BACKOFF_START = 300.0
BACKOFF_MAX = 3600.0
# The most of a day's ZenRows credits one host and page kind may spend, so one can't starve the rest.
HOST_SHARE = 0.4
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) " \
             "Chrome/130.0 Safari/537.36"


class OverBudget(RuntimeError):
    """Today's ZenRows credits are spent; the source falls back or waits."""


@dataclass
class Page:
    url: str
    status: int
    text: str | None
    fetched_at: datetime
    changed: bool
    content_hash: str | None = None
    archive_key: str | None = None
    # The body as sent, for files requests doesn't decode itself (a .xml.gz sitemap).
    content: bytes | None = None
    # Which route answered, and whether it was a refusal (a block, or a page `valid` rejected). A fetch skipped
    # because the host is backing off is refused with status 0.
    route: str = "plain"
    refused: bool = False
    transient: bool = False

    @property
    def ok(self) -> bool:
        """A page to read: a 200 that no route refused (a 304 isn't ok either: there is nothing new to read)."""
        return self.status == 200 and not self.refused


class Archive(Protocol):
    def keep(self, key: str, data: bytes) -> None: ...


class S3Archive:
    def __init__(self, bucket: str, client=None) -> None:
        self.bucket, self.client = bucket, client or boto3.client("s3")

    def keep(self, key: str, data: bytes) -> None:
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, IfNoneMatch="*")
        except ClientError as e:
            # Already kept: the same content was fetched before.
            if e.response["Error"]["Code"] not in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise


class LocalArchive:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def keep(self, key: str, data: bytes) -> None:
        path = self.root / key
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)


class ZenRowsBudget:
    """Credits per UTC day. A fetch first checks it can afford the request (OverBudget if not, or if its host and page
    kind already took HOST_SHARE of the day) and pays only once ZenRows bills it. spent seeds today's spend, so a
    restart doesn't start the day afresh."""

    def __init__(self, daily_credits: int, clock: Callable[[], datetime] | None = None, spent: int = 0,
                 share: float = HOST_SHARE) -> None:
        self.daily_credits, self.share = daily_credits, share
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.day, self.spent, self.by_key = self.clock().strftime("%Y-%m-%d"), int(spent), {}
        self._lock = threading.Lock()

    def _roll(self) -> None:
        day = self.clock().strftime("%Y-%m-%d")
        if day != self.day:
            self.day, self.spent, self.by_key = day, 0, {}

    def check(self, credits: int, key: tuple = ()) -> None:
        with self._lock:
            self._roll()
            if self.spent + credits > self.daily_credits:
                raise OverBudget(f"ZenRows: {self.spent} of {self.daily_credits} credits spent today")
            if key and self.by_key.get(key, 0) + credits > self.share * self.daily_credits:
                raise OverBudget(f"ZenRows: {key} has spent its {self.share:.0%} of today's credits")

    def pay(self, credits: int, key: tuple = ()) -> None:
        with self._lock:
            self._roll()
            self.spent += credits
            if key:
                self.by_key[key] = self.by_key.get(key, 0) + credits

    def charge(self, credits: int, key: tuple = ()) -> None:
        self.check(credits, key)
        self.pay(credits, key)

    def spent_today(self) -> float:
        with self._lock:
            self._roll()
            return float(self.spent)


@dataclass
class _Route:
    """What the fetcher has learnt about a host and page kind: the cheapest route that works and since when, the
    refusals in a row on it, and how long to stop asking when every route refuses."""
    level: int = 0
    since: float = 0.0
    strikes: int = 0
    blocked_until: float = 0.0
    backoff: float = 0.0
    last_status: int = 0


def _check_text(response: requests.Response) -> str:
    # A .gz sitemap arrives as gzip bytes, which requests doesn't decode; the caller's check needs the XML.
    body = response.content
    if body[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(body).decode("utf-8", "replace")
        except OSError:
            return ""
    return response.text


class Fetcher:
    def __init__(self, source: str, writer: TableWriter, archive: Archive, zenrows: ZenRowsBudget | None = None,
                 session: requests.Session | None = None, secret_fn: Callable[[str, str], str] = credentials.secret,
                 clock: Callable[[], datetime] | None = None, sleep: Callable[[float], None] = time.sleep,
                 host_gap: float = HOST_GAP, monotonic: Callable[[], float] = time.monotonic,
                 zenrows_api: str = ZENROWS_API) -> None:
        self.source, self.writer, self.archive, self.zenrows = source, writer, archive, zenrows
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.secret_fn, self.sleep, self.host_gap, self.monotonic = secret_fn, sleep, host_gap, monotonic
        self.zenrows_api = zenrows_api
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._zenrows_key: str | None = None
        self._validators: dict[str, tuple[str | None, str | None]] = {}
        self._host_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._last: dict[str, float] = {}
        self._routes: dict[tuple[str, str, str], _Route] = {}
        self._locks_lock = threading.Lock()

    def _host_lock(self, host: str) -> threading.Lock:
        with self._locks_lock:
            return self._host_locks[host]

    def route(self, url: str, via: str, kind: str = "") -> str:
        """The route the next fetch of this host and kind starts from."""
        state = self._routes.get((url.split("/")[2], via, kind))
        return self._routes_for(via)[state.level if state else 0]

    def get(self, url: str, via: str = "plain", archive: bool = False,
            valid: Callable[[str], bool] | None = None, kind: str = "", conditional: bool = True) -> Page:
        """The page now, through the cheapest route `via` allows that works for this host and `kind` (a site may
        answer its feed plainly but not its articles). A refusal comes back refused (and moves the host up a route
        once there have been ESCALATE_AFTER in a row); a 429 or 5xx comes back transient. Raises on a transport
        error, and with OverBudget when a fetch must use ZenRows and can't be paid for."""
        host = url.split("/")[2]
        routes = self._routes_for(via)
        with self._host_lock(host):
            state = self._routes.setdefault((host, via, kind), _Route(since=self.monotonic()))
            now = self.monotonic()
            if state.blocked_until > now:
                return Page(url, state.last_status, None, self.clock(), changed=False, route=routes[state.level],
                            refused=True)
            level = state.level
            probing = level > 0 and now - state.since > RECHECK
            if probing:
                level -= 1
            tried_plain, last = False, None
            while True:
                route = routes[level]
                try:
                    response, elapsed = self._attempt(host, url, route, (host, kind), conditional)
                except OverBudget:
                    if last is not None:
                        # Stepped up within this fetch and the dearer route can't be paid for: the refusal stands.
                        return last
                    if routes[0] == "plain" and not tried_plain and route != "plain":
                        # ZenRows can't be paid for: a plain try costs nothing and may work. The memory stays, so
                        # tomorrow's credits go back to the route that worked.
                        response, elapsed = self._attempt(host, url, "plain", (host, kind), conditional)
                        return self._page(url, "plain", response, elapsed, archive, valid, conditional)
                    raise
                tried_plain = tried_plain or route == "plain"
                page = last = self._page(url, route, response, elapsed, archive, valid, conditional)
                state.last_status = page.status
                if not page.refused:
                    if probing:
                        # The cheaper route works again: settle there.
                        state.level, state.since = level, self.monotonic()
                    if page.ok:
                        state.strikes, state.backoff = 0, 0.0
                    return page
                if probing:
                    # Still refused on the cheaper route: back to the one that worked, and wait another RECHECK.
                    probing, level, state.since = False, level + 1, self.monotonic()
                    continue
                state.strikes += 1
                if state.strikes < ESCALATE_AFTER:
                    return page
                state.strikes = 0
                if level + 1 < len(routes):
                    level += 1
                    state.level, state.since = level, self.monotonic()
                    continue
                state.backoff = min(max(BACKOFF_START, state.backoff * 2), BACKOFF_MAX)
                state.blocked_until = self.monotonic() + state.backoff
                return page

    def _routes_for(self, via: str) -> tuple[str, ...]:
        # Without ZenRows an "auto" fetch has only the plain route, so its refusals back off instead of escalating
        # into a route that can't be used; a "zenrows" fetch keeps its routes and raises.
        if self.zenrows is None and "plain" in ROUTES[via]:
            return ("plain",)
        return ROUTES[via]

    def _attempt(self, host: str, url: str, route: str, key: tuple, conditional: bool) -> tuple[requests.Response, float]:
        credits = ZENROWS_CREDITS.get(route)
        if credits is not None:
            if self.zenrows is None:
                raise OverBudget("ZenRows isn't enabled for this source")
            self.zenrows.check(credits, key)
        wait = self._last.get(host, 0.0) + self.host_gap - time.monotonic()
        if wait > 0:
            self.sleep(wait)
        started = time.monotonic()
        try:
            response = self._request(url, route, conditional)
        except Exception as e:
            self._log(url, route, None, error=f"{type(e).__name__}: {e}", elapsed=time.monotonic() - started)
            raise
        finally:
            self._last[host] = time.monotonic()
        if credits is not None and response.status_code == 200:
            # ZenRows bills the requests it answers; a refusal or an error costs nothing.
            self.zenrows.pay(credits, key)
        return response, time.monotonic() - started

    def _page(self, url: str, route: str, response: requests.Response, elapsed: float, archive: bool,
              valid: Callable[[str], bool] | None, conditional: bool) -> Page:
        at, status = self.clock(), response.status_code
        if status == 304:
            return Page(url, 304, None, at, changed=False, route=route)
        if status != 200:
            transient = status in TRANSIENT
            refused = not transient and (status in REFUSED or (route != "plain" and 400 <= status < 500
                                                                and status not in NOT_THERE))
            self._log(url, route, status, elapsed=elapsed,
                      error="refused" if refused else "transient" if transient else None)
            return Page(url, status, None, at, changed=False, route=route, refused=refused, transient=transient)
        body = response.content
        if valid is not None and not valid(_check_text(response)):
            self._log(url, route, status, size=len(body), elapsed=elapsed, error="refused: not the page asked for")
            return Page(url, status, None, at, changed=False, route=route, refused=True)
        digest = hashlib.sha256(body).hexdigest()
        key = f"{self.source}/pages/{digest[:2]}/{digest}.html.gz" if archive else None
        if key:
            self.archive.keep(key, gzip.compress(body))
        if route == "plain" and conditional:
            self._validators[url] = (response.headers.get("ETag"), response.headers.get("Last-Modified"))
        self._log(url, route, 200, digest=digest, size=len(body), elapsed=elapsed, key=key,
                  etag=response.headers.get("ETag"), modified=response.headers.get("Last-Modified"))
        return Page(url, 200, response.text, at, changed=True, content_hash=digest, archive_key=key, content=body,
                    route=route)

    def _request(self, url: str, route: str, conditional: bool) -> requests.Response:
        if route in ZENROWS_CREDITS:
            if self._zenrows_key is None:
                self._zenrows_key = self.secret_fn("zenrows", "ZENROWS_API_KEY")
            params = {"apikey": self._zenrows_key, "url": url}
            if route == "zenrows_premium":
                params["premium_proxy"] = "true"
            return self.session.get(self.zenrows_api, params=params, timeout=TIMEOUT * 3)
        etag, modified = self._validators.get(url, (None, None)) if conditional else (None, None)
        headers = {k: v for k, v in (("If-None-Match", etag), ("If-Modified-Since", modified)) if v}
        return self.session.get(url, headers=headers, timeout=TIMEOUT)

    def _log(self, url: str, route: str, status: int | None, error: str | None = None, digest: str | None = None,
             size: int | None = None, elapsed: float = 0.0, key: str | None = None, etag: str | None = None,
             modified: str | None = None) -> None:
        self.writer.add(FETCHED_PAGES, [{
            "fetched_at": self.clock(), "url": url, "source": self.source, "via": route, "status": status,
            "error": error, "content_hash": digest, "bytes": size, "etag": etag, "last_modified": modified,
            "archive_key": key, "elapsed_ms": round(elapsed * 1000, 1)}])
