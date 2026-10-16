"""Fetching HLTV with a real Chrome window, for local runs: pages and demo downloads.

Chrome opens visibly. If Cloudflare asks "Verify you are human", click through it in the
window (a macOS notification says so); the session cookies then carry the remaining pages.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import shutil
import subprocess
import time
from pathlib import Path

import nodriver as uc
from nodriver import cdp
from nodriver.core import util as nodriver_util

from gnome_pipelines.hltv_cs2.fetch import RateLimited

logger = logging.getLogger(__name__)

_DEMO_TMP = Path("/tmp/hltv_demo")
# Kept from the backfill days: the profile holds the Cloudflare clearance cookies.
_CHROME_PROFILE = Path.home() / ".cache" / "hltv_backfill_chrome"
_DOWNLOAD_TIMEOUT = 300  # seconds to wait for a demo download
_CF_TIMEOUT = 120        # seconds to wait for CF challenge resolution
_START_ATTEMPTS = 3

_CF_PHRASES = (
    "Just a moment",
    "Verify you are human",
    "Checking your browser",
    "Performing security verification",
)

# HLTV rate-limits on cumulative request volume, and a block does NOT look like a
# Cloudflare challenge. Without these the scraper cannot tell it is being refused:
# _wait_for_page would spin to its deadline, _get_html would retry three times, and
# every page would burn 90s and be silently skipped for as long as the block lasted.
_BLOCK_PHRASES = (
    "Access denied",
    "access to this page has been denied",
    "Too many requests",
    "Rate limited",
    "You have been blocked",
    "Error 1015",
    "Error 429",
)

_SELECTOR_GRACE = 3.0    # seconds to keep looking after the document is complete
_POLL_INTERVAL = 0.15

_download_state: dict = {}


def _notify_cf(label: str) -> None:
    subprocess.Popen([
        "osascript", "-e",
        f'display notification "CF challenge on {label} — solve it in Chrome" with title "HLTV Backfill" sound name "Ping"',
    ])


def _on_download_begin(event: uc.cdp.browser.DownloadWillBegin) -> None:
    _download_state["guid"] = event.guid
    _download_state["filename"] = event.suggested_filename
    _download_state["started"] = True


def _on_download_progress(event: uc.cdp.browser.DownloadProgress) -> None:
    if event.guid != _download_state.get("guid"):
        return
    _download_state["received"] = event.received_bytes
    _download_state["total"] = event.total_bytes
    if event.state == "completed":
        _download_state["done"] = True


_PROBE_JS = """
(() => {
  const t = (document.body ? document.body.textContent : "").slice(0, 6000);
  const has = ps => ps.some(p => t.toLowerCase().includes(p.toLowerCase()));
  return JSON.stringify({
    n: document.querySelectorAll(%s).length,
    cf: has(%s),
    blocked: has(%s),
    ready: document.readyState === "complete",
    footer: !!document.querySelector("footer")
  });
})()
"""


async def _probe(page: uc.Tab, selector: str) -> dict | None:
    """
    Readiness, Cloudflare and block state in a single scalar round-trip.

    The previous implementation called page.get_content() every poll, which is
    DOM.getDocument(depth=-1, pierce=True) — the whole DOM serialised to JSON and
    rebuilt as tens of thousands of Python objects, then discarded — just to run a
    substring count. Runtime.evaluate returns one string instead.

    nodriver's Tab.evaluate wrapper always sends serialization_options, which
    overrides returnByValue and makes it drop falsy results, so the CDP command is
    sent directly and .value read off the RemoteObject.
    """
    expr = _PROBE_JS % (json.dumps(selector), json.dumps(list(_CF_PHRASES)), json.dumps(list(_BLOCK_PHRASES)))
    try:
        obj, _errors = await page.send(cdp.runtime.evaluate(expression=expr, return_by_value=True))
    except Exception:
        return None
    if obj is None or obj.value is None:
        return None
    try:
        return json.loads(obj.value)
    except (TypeError, ValueError):
        return None


async def _wait_for_page(page: uc.Tab, url: str, wait_selector: str | None, base_timeout: float = 30.0, wait_count: int = 1) -> str | None:
    """
    Wait for a page to load, prompting on Cloudflare and aborting on a block.

    Raises RateLimited when HLTV refuses the request — retrying into a block only
    escalates it, and a silently-skipped page is worse than a failed run.
    """
    cf_seen = False
    notified = False
    complete_since: float | None = None
    deadline = time.monotonic() + base_timeout

    while time.monotonic() < deadline:
        state = await _probe(page, wait_selector or "html")
        if state is None:
            await asyncio.sleep(0.3)
            continue

        if state.get("blocked"):
            raise RateLimited(f"HLTV refused {url} — cumulative rate limit reached")

        if state.get("cf"):
            if not cf_seen:
                cf_seen = True
                deadline = time.monotonic() + _CF_TIMEOUT
            if not notified:
                logger.info("CF challenge on %s — solve in Chrome...", url)
                _notify_cf(url)
                notified = True
            complete_since = None
            await asyncio.sleep(1.0)
            continue

        selector_met = not wait_selector or state.get("n", 0) >= wait_count
        # The selector alone is not readiness. Both wait selectors sit near the top
        # of the page, so returning on them captured documents mid-stream: 34
        # results listings and 36 match pages were cached with everything below
        # the cut missing, silently dropping 406 series. Only a fully parsed
        # document that has reached its footer is complete.
        if selector_met and state.get("ready") and state.get("footer"):
            if cf_seen:
                logger.info("CF resolved on %s", url)
            try:
                return await page.get_content()
            except Exception:
                await asyncio.sleep(0.3)
                continue

        # A complete document that still lacks the selector (or a footer) will
        # never grow one. Waiting out the full 30s three times costs 90s per page.
        if state.get("ready"):
            if complete_since is None:
                complete_since = time.monotonic()
            elif time.monotonic() - complete_since > _SELECTOR_GRACE:
                missing = "a footer" if selector_met else f"'{wait_selector}'"
                logger.info("%s loaded without %s — not waiting further", url, missing)
                return None

        await asyncio.sleep(_POLL_INTERVAL)

    logger.warning("Timed out on %s (cf_seen=%s, selector='%s')", url, cf_seen, wait_selector)
    return None


async def _get_html(page: uc.Tab, url: str, wait_selector: str | None = None, retries: int = 3, wait_count: int = 1) -> str | None:
    """
    Navigate and return the page HTML once it is ready.

    RateLimited is deliberately not retried: retrying into a block escalates it,
    and the whole point of detecting it is to stop rather than burn the remaining
    pages against a refusing server.
    """
    for attempt in range(1, retries + 1):
        try:
            await page.get(url)
            html = await _wait_for_page(page, url, wait_selector, wait_count=wait_count)
            if html is not None:
                return html
            if attempt < retries:
                logger.info("Retrying %s (attempt %d/%d)...", url, attempt + 1, retries)
        except RateLimited:
            raise
        except Exception as exc:
            logger.warning("nodriver error for %s (attempt %d/%d): %s", url, attempt, retries, exc)
    return None


async def _download_demo(page: uc.Tab, demo_url: str) -> Path | None:
    """Navigate to demo download URL and wait for Chrome to save the file."""
    for f in _DEMO_TMP.iterdir():
        shutil.rmtree(f) if f.is_dir() else f.unlink()

    _download_state.clear()

    try:
        await page.get(demo_url)
    except Exception as exc:
        logger.warning("Demo download navigation error: %s", exc)
        return None

    cf_seen = False
    notified = False
    deadline = time.monotonic() + _DOWNLOAD_TIMEOUT

    while time.monotonic() < deadline:
        if _download_state.get("started"):
            break
        files = list(_DEMO_TMP.iterdir())
        if any(f.suffix == ".crdownload" for f in files) or any(f.suffix != ".crdownload" for f in files):
            break
        try:
            content = await page.get_content()
            if any(p in content for p in _CF_PHRASES):
                if not cf_seen:
                    cf_seen = True
                if not notified:
                    logger.info("CF challenge on demo download — solve in Chrome...")
                    _notify_cf("demo download")
                    notified = True
            elif cf_seen:
                logger.info("CF resolved on demo download")
                cf_seen = False
        except Exception:
            pass
        await asyncio.sleep(2.0)

    filename = _download_state.get("filename", "")
    if filename:
        logger.info("Download started: %s", filename)
    else:
        logger.info("Download started")

    last_log = -1.0
    while time.monotonic() < deadline:
        if _download_state.get("done"):
            break
        files = list(_DEMO_TMP.iterdir())
        in_progress = [f for f in files if f.suffix == ".crdownload"]
        completed = [f for f in files if f.suffix != ".crdownload"]
        if completed and not in_progress:
            break
        if in_progress:
            total = _download_state.get("total", 0)
            received = _download_state.get("received", 0)
            if total > 0:
                pct = int(received * 100 / total)
                if pct != last_log:
                    logger.info("  %s — %.1f / %.1f MB (%d%%)",
                                filename or in_progress[0].name,
                                received / 1_048_576, total / 1_048_576, pct)
                    last_log = pct
            else:
                size_mb = in_progress[0].stat().st_size / 1_048_576
                if abs(size_mb - last_log) > 1.0:
                    elapsed = _DOWNLOAD_TIMEOUT - (deadline - time.monotonic())
                    logger.info("  %s — %.1f MB (%.0fs elapsed)", in_progress[0].name, size_mb, elapsed)
                    last_log = size_mb
        await asyncio.sleep(2.0)

    completed = [f for f in _DEMO_TMP.iterdir() if f.suffix != ".crdownload"]
    if completed:
        logger.info("Demo download complete: %s (%.1f MB)",
                    completed[0].name, completed[0].stat().st_size / 1_048_576)
        return completed[0]

    logger.warning("Demo download timed out after %ds", _DOWNLOAD_TIMEOUT)
    return None


def _extract_dem(archive_path: Path) -> list[Path]:
    """Decompress archive and return all .dem files. Deletes the archive."""
    if archive_path.suffix == ".gz":
        dem_path = archive_path.with_suffix("")
        with gzip.open(archive_path, "rb") as src, open(dem_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        archive_path.unlink()
        return [dem_path]

    if archive_path.suffix == ".dem":
        return [archive_path]

    if archive_path.suffix == ".rar":
        result = subprocess.run(
            ["unar", "-o", str(_DEMO_TMP), "-f", str(archive_path)],
            capture_output=True,
        )
        archive_path.unlink()
        if result.returncode != 0:
            logger.warning("unar failed: %s", result.stderr.decode())
            return []
        dem_files = list(_DEMO_TMP.glob("**/*.dem"))
        if not dem_files:
            logger.warning("No .dem found after extracting %s", archive_path.name)
        return dem_files

    logger.warning("Unsupported demo archive format: %s", archive_path.suffix)
    return []


async def _stop_browser(browser: uc.Browser) -> None:
    # Every tab holds its own websocket with a reader task. Browser.stop() only schedules
    # the browser's own close and then kills Chrome, leaving those readers on dead sockets,
    # and nodriver's reader retries any error forever without yielding.
    for target in browser.targets:
        await target.aclose()
    await browser.aclose()
    # stop() forgets the process it terminates; Chrome's exit has to be reaped on this
    # loop, or asyncio reports it against a closed one.
    process = browser._process
    browser.stop()
    nodriver_util.get_registered_instances().discard(browser)
    if process is not None:
        await process.wait()
    # stop() also queues another aclose() on this loop; let it finish before the loop closes.
    await asyncio.sleep(0)


async def _start_browser() -> uc.Browser:
    """
    nodriver gives Chrome under 3 s to open its debugging port, and that isn't configurable;
    a cold start on a large profile can take longer. The failed attempt's Chrome is left
    running and holding the profile, which would make the next attempt fail too, so it is
    stopped first.
    """
    for attempt in range(1, _START_ATTEMPTS):
        try:
            return await uc.start(headless=False, user_data_dir=str(_CHROME_PROFILE))
        # nodriver raises a bare Exception when it can't connect.
        except Exception:
            logger.warning("Chrome didn't open its debugging port in time (attempt %d/%d) — retrying",
                           attempt, _START_ATTEMPTS)
            for browser in list(nodriver_util.get_registered_instances()):
                await _stop_browser(browser)
    return await uc.start(headless=False, user_data_dir=str(_CHROME_PROFILE))


class ChromeSource:
    """
    A page source over one Chrome tab, started on the first page actually fetched.

    Chrome needs a visible window for the Cloudflare challenge, so it must not start
    at all when every page is already cached: that is where a re-run goes from hours
    to minutes.
    """

    concurrency = 1
    can_be_blocked = True
    billed = 0

    def __init__(self) -> None:
        self._browser = None
        self._page = None

    async def page(self) -> uc.Tab:
        if self._page is None:
            _DEMO_TMP.mkdir(parents=True, exist_ok=True)
            _CHROME_PROFILE.mkdir(parents=True, exist_ok=True)
            logger.info("starting Chrome (first cache miss)")
            self._browser = await _start_browser()
            self._page = await self._browser.get("about:blank")
            await self._page.send(uc.cdp.browser.set_download_behavior(
                behavior="allow", download_path=str(_DEMO_TMP), events_enabled=True,
            ))
            self._browser.add_handler(uc.cdp.browser.DownloadWillBegin, _on_download_begin)
            self._browser.add_handler(uc.cdp.browser.DownloadProgress, _on_download_progress)
        return self._page

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None:
        return await _get_html(await self.page(), url, wait_selector, wait_count=wait_count)

    async def download_demo(self, demo_url: str) -> list[Path]:
        """The demo's .dem files, or none if it couldn't be downloaded or unpacked."""
        archive = await _download_demo(await self.page(), demo_url)
        return _extract_dem(archive) if archive is not None else []

    async def close(self) -> None:
        if self._browser is None:
            return
        await _stop_browser(self._browser)
        self._browser = None
        self._page = None
