"""Where HLTV pages come from: a page source behind the shared page cache.

A source fetches one page. ZenRowsSource goes through ZenRows' residential proxy and runs anywhere,
including the Fargate task; ChromeSource (chrome.py) drives a visible local Chrome window and is the
only one that can download demos; CacheSource never touches the network. HltvFetcher puts the
HtmlCache in front of whichever is used, so every source reads cached pages first and stores what it
fetches, and collect.py's loops don't know which one they have.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from gnome_pipelines.hltv_cs2.html_cache import CachePolicy, HtmlCache, is_cacheable, is_complete
from gnome_pipelines.hltv_cs2.scraper import _Session, _get_html, _make_session

logger = logging.getLogger(__name__)

# A source that can be blocked may fail in a way that doesn't look like a block (chrome's
# _BLOCK_PHRASES are a guess at HLTV's wording), and every page would then quietly be skipped.
# A run of consecutive failures is the wording-independent signal that something is systematically wrong.
MAX_CONSECUTIVE_FAILURES = 15


class RateLimited(RuntimeError):
    """HLTV is refusing requests. Continuing would escalate the block."""


class PageSource(Protocol):
    # How many pages the loops fetch at once.
    concurrency: int
    # Whether a run of misses means HLTV is refusing us, rather than pages simply not being there.
    can_be_blocked: bool
    # Pages a paid service has charged for so far.
    billed: int

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None: ...

    async def close(self) -> None: ...


class ZenRowsSource:
    """ZenRows' JS-rendering residential proxy; the session (and its API key) is made on the first miss."""

    concurrency = 5
    can_be_blocked = True

    def __init__(self) -> None:
        self._session: _Session | None = None

    @property
    def billed(self) -> int:
        return self._session.billed if self._session is not None else 0

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None:
        if self._session is None:
            self._session = _make_session()
        return await asyncio.to_thread(_get_html, self._session, url)

    async def close(self) -> None:
        pass


class CacheSource:
    """Only what is already cached: re-parsing after a parser change costs minutes, not a re-scrape."""

    concurrency = 32
    can_be_blocked = False
    billed = 0

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None:
        return None

    async def close(self) -> None:
        pass


class HltvFetcher:
    """Cache-first page access over a source; pages it fetches are written through to the cache."""

    def __init__(self, cache: HtmlCache, source: PageSource) -> None:
        self.cache = cache
        self.source = source

    @property
    def concurrency(self) -> int:
        return self.source.concurrency

    async def html(
        self, url: str, *, kind: str, wait_selector: str | None = None, wait_count: int = 1,
    ) -> tuple[str | None, datetime.datetime | None]:
        hit = self.cache.get(url, kind=kind)
        if hit is not None and is_complete(hit.html):
            return hit.html, hit.fetched_at
        if hit is not None:
            logger.info("cached copy of %s is truncated — refetching", url)
        fetched_at = datetime.datetime.now(datetime.timezone.utc)
        html = await self.source.html(url, wait_selector=wait_selector, wait_count=wait_count)
        if html is not None and is_cacheable(kind, html):
            # Write through per page, never batched: a ten-hour run that stalls on
            # Cloudflare must resume against what it already fetched.
            self.cache.put(url, html, kind=kind, fetched_at=fetched_at)
        return html, fetched_at


def run_with_source[T](source: PageSource, work: Callable[[PageSource], Awaitable[T]]) -> T:
    """
    Run work(source) to completion on a new event loop, then close the source on that loop.

    On Ctrl+C the work is cancelled rather than abandoned, so its finally blocks (which publish
    what was collected) still run before the interrupt is re-raised.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(work(source))
    try:
        return loop.run_until_complete(task)
    except KeyboardInterrupt:
        logger.info("Interrupted — publishing what was collected...")
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        raise
    finally:
        # On the loop, not after it: closing a source can need the loop (Chrome's websockets).
        loop.run_until_complete(source.close())
        loop.close()


def run_with_fetcher[T](source: PageSource, policy: CachePolicy, work: Callable[[HltvFetcher], Awaitable[T]]) -> T:
    """run_with_source behind the page cache, then push any pages still only local."""
    cache = HtmlCache(policy=policy)
    try:
        cache.warm_index()
    except Exception as exc:
        logger.warning("could not warm the remote cache index (%s) — falling back to per-key lookups", exc)
    fetcher = HltvFetcher(cache, source)
    try:
        return run_with_source(source, lambda _: work(fetcher))
    finally:
        cache.sync_pending()
