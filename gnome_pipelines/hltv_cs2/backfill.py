"""
Local HLTV data backfill — runs on your machine with a real Chrome browser.

Three independent subcommands:
  matches   — scrape match stats + demos, one date at a time
  rankings  — scrape weekly team rankings for a date range
  priors    — rebuild cs2_match_priors from full history (no browser needed)

Usage:
    # Matches — loops one day at a time; stops on any failure
    python -m gnome_pipelines.hltv_cs2.backfill matches \\
        --start-date 2026-09-01 --end-date 2026-09-07 \\
        --min-stars 0 --min-stars-demo 2

    # Rankings
    python -m gnome_pipelines.hltv_cs2.backfill rankings \\
        --start-date 2026-09-01 --end-date 2026-09-28

    # Priors (no browser)
    python -m gnome_pipelines.hltv_cs2.backfill priors

Cloudflare: Chrome opens in visible mode. If prompted with "Verify you are human",
click through manually — the session cookies persist for subsequent pages.
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import datetime
import gzip
import json
import logging
import re
import shutil
import subprocess
import time
from pathlib import Path

import nodriver as uc
from nodriver import cdp
import pandas as pd
from bs4 import BeautifulSoup

from demoparser2 import DemoParser

from gnome_pipelines.hltv_cs2.demo_parser import parse_demo
from gnome_pipelines.hltv_cs2.scraper import (
    _BASE_URL,
    _all_mondays,
    parse_match_detail_html,
    parse_match_ids_from_html,
    parse_team_ranking_html,
)
from gnome_pipelines.hltv_cs2.html_cache import CachePolicy, HtmlCache, is_cacheable, is_complete
from gnome_pipelines.datasets import cs2
from gnome_pipelines.hltv_cs2.publish import publish, publish_match_data, publish_priors

logger = logging.getLogger(__name__)

_DEMO_TMP = Path("/tmp/hltv_demo")
_CHROME_PROFILE = Path.home() / ".cache" / "hltv_backfill_chrome"
_DOWNLOAD_TIMEOUT = 300  # seconds to wait for a demo download
_CF_TIMEOUT = 120        # seconds to wait for CF challenge resolution

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
# _BLOCK_PHRASES is a best guess at HLTV's wording. If it is wrong, a block looks
# exactly like a page that loaded without the selector — complete, no .teamRanking —
# and fail-fast would quietly skip every page. A run of consecutive failures is the
# wording-independent signal that something is systematically wrong.
_MAX_CONSECUTIVE_FAILURES = 15
_POLL_INTERVAL = 0.15


class RateLimited(RuntimeError):
    """HLTV is refusing requests. Continuing would escalate the block."""

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


async def backfill_matches(
    fetcher: "HltvFetcher",
    start_date: datetime.date,
    end_date: datetime.date,
    min_stars_match: int = 0,
    skip_demos: bool = False,
    min_stars_demo: int = 2,
    match_ids: set[int] | None = None,
) -> bool:
    """
    Scrape all matches for every date in [start_date, end_date].
    Accumulates data across all dates and publishes once in the finally block.
    Pages are served from the cache where possible, so a re-run only fetches what is
    still missing. Unreachable dates and failed demos are skipped and reported rather
    than aborting the pass.
    """
    match_rows: list[dict] = []
    player_rows: list[dict] = []
    veto_rows: list[dict] = []
    h2h_rows: list[dict] = []
    form_rows: list[dict] = []
    failed_dates: list[datetime.date] = []
    n_demos_failed = 0
    consecutive_failures = 0
    demo_rows: list[dict] = []
    stars_param = f"&stars={min_stars_match}" if min_stars_match >= 2 else ""

    try:
        current = start_date
        while current <= end_date:
            # --- Fetch match listing for this date ---
            matches: list[tuple[int, str]] = []
            offset = 0
            logger.info("Scraping matches for %s (min_stars=%d)", current.isoformat(), min_stars_match)

            while True:
                url = f"{_BASE_URL}/results?startDate={current.isoformat()}&endDate={current.isoformat()}&offset={offset}{stars_param}"
                html, _ = await fetcher.html(url, kind="results", wait_selector=".contentCol")
                if html is None:
                    # One unreachable day must not end an unattended multi-hour pass.
                    # Per-page cache write-through means a later re-run only refetches
                    # what is still missing, so skipping the date is recoverable.
                    logger.warning("Match listing failed for %s at offset %d — skipping this date", current, offset)
                    failed_dates.append(current)
                    break
                if "results-all" not in html:
                    logger.info("No matches for %s at offset %d", current, offset)
                    break
                page_matches, has_more = parse_match_ids_from_html(html)
                matches.extend(page_matches)
                if not has_more:
                    break
                offset += 100

            seen: set[int] = set()
            unique: list[tuple[int, str, int]] = []
            for mid, href, stars in matches:
                if mid not in seen:
                    seen.add(mid)
                    unique.append((mid, href, stars))
            matches = unique
            if match_ids is not None:
                matches = [(mid, href, stars) for mid, href, stars in matches if mid in match_ids]
                logger.info("Filtered to %d matches for %s (--match-ids)", len(matches), current.isoformat())
            else:
                logger.info("Found %d matches for %s", len(matches), current.isoformat())

            # --- Process each match ---
            n_demos_parsed = 0
            n_demos_skipped = 0

            for i, (match_id, match_href, match_stars) in enumerate(matches):
                n = i + 1
                logger.info("[%d/%d] match %d — fetching page", n, len(matches), match_id)
                html, page_fetched_at = await fetcher.html(
                    f"{_BASE_URL}{match_href}", kind="match",
                    wait_selector=".teamRanking", wait_count=2,
                )
                if html is None:
                    consecutive_failures += 1
                    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                        raise RateLimited(
                            f"{consecutive_failures} consecutive match pages failed — "
                            "almost certainly blocked or rate-limited, stopping before it escalates"
                        )
                    logger.warning("[%d/%d] match %d — page timed out (cancelled/no rankings?) — skipping", n, len(matches), match_id)
                    continue
                consecutive_failures = 0

                soup = BeautifulSoup(html, "lxml")
                result = parse_match_detail_html(soup, match_id, stars=match_stars)
                if result is None:
                    logger.warning("[%d/%d] match %d — parse returned None — skipping", n, len(matches), match_id)
                    continue

                if result["rows"]:
                    match_rows.extend(result["rows"])
                    player_rows.extend(result.get("players", []))
                    veto_rows.extend(result.get("veto", []))
                    h2h_rows.extend(result.get("h2h", []))
                    form_rows.extend(result.get("recent_form", []))
                    logger.info("[%d/%d] match %d — %d maps parsed (stars=%d)", n, len(matches), match_id, len(result["rows"]), match_stars)
                else:
                    logger.warning("[%d/%d] match %d — no map rows", n, len(matches), match_id)

                demo_url = result.get("demo_url")
                if skip_demos or not demo_url:
                    if not skip_demos and not demo_url:
                        logger.info("[%d/%d] match %d — no demo URL", n, len(matches), match_id)
                    continue
                if match_stars < min_stars_demo:
                    logger.info("[%d/%d] match %d — demo skipped (stars=%d < min=%d)", n, len(matches), match_id, match_stars, min_stars_demo)
                    n_demos_skipped += 1
                    continue

                logger.info("[%d/%d] match %d — downloading demo (stars=%d)", n, len(matches), match_id, match_stars)

                archive = await _download_demo(await fetcher.page(), demo_url)
                if archive is None:
                    logger.warning("[%d/%d] match %d — demo download failed, skipping", n, len(matches), match_id)
                    n_demos_failed += 1
                    continue

                dem_paths = _extract_dem(archive)
                if not dem_paths:
                    logger.warning("[%d/%d] match %d — demo extraction failed, skipping", n, len(matches), match_id)
                    n_demos_failed += 1
                    continue

                match_date = result["rows"][0]["match_date"] if result["rows"] else None
                score_by_map = {
                    r["map_name"]: r["team_a_score"] + r["team_b_score"]
                    for r in result["rows"]
                }

                # Group dem files by map (handles multi-part demos)
                map_groups: dict[str, list[Path]] = {}
                for dem_path in dem_paths:
                    try:
                        map_name = DemoParser(str(dem_path)).parse_header().get("map_name", "unknown")
                    except Exception:
                        map_name = "unknown"
                    map_groups.setdefault(map_name, []).append(dem_path)

                for demo_map, group_paths in map_groups.items():
                    df = parse_demo([str(p) for p in group_paths])
                    if df is None:
                        n_demos_skipped += 1
                        logger.info("[%d/%d] match %d — demo parse returned no rows: %s", n, len(matches), match_id, demo_map)
                        for p in group_paths:
                            p.unlink(missing_ok=True)
                        continue

                    actual_rounds = df["round_number"].nunique()
                    expected_rounds = score_by_map.get(demo_map)
                    if expected_rounds is not None and actual_rounds != expected_rounds:
                        logger.warning(
                            "[%d/%d] match %d — demo round count mismatch on %s: got %d, expected %d — skipping",
                            n, len(matches), match_id, demo_map, actual_rounds, expected_rounds,
                        )
                        n_demos_skipped += 1
                        for p in group_paths:
                            p.unlink(missing_ok=True)
                        continue

                    df["match_id"] = match_id
                    df["match_date"] = match_date
                    demo_rows.extend(df.to_dict("records"))
                    n_demos_parsed += 1
                    logger.info("[%d/%d] match %d — demo parsed %s (%d parts, %d/%s rounds, %d snapshots)",
                                n, len(matches), match_id, demo_map, len(group_paths), actual_rounds,
                                str(expected_rounds) if expected_rounds else "?", len(df))
                    for p in group_paths:
                        p.unlink(missing_ok=True)

            logger.info(
                "Done %s: %d maps from %d matches | %d demos parsed | %d skipped",
                current.isoformat(), len(match_rows), len(matches), n_demos_parsed, n_demos_skipped,
            )
            current += datetime.timedelta(days=1)

        return True

    finally:
        if failed_dates:
            logger.warning("%d date(s) could not be listed and were skipped: %s",
                           len(failed_dates), ", ".join(d.isoformat() for d in failed_dates[:10]))
        if n_demos_failed:
            logger.warning("%d demo(s) failed and were skipped", n_demos_failed)
        publish_match_data(match_rows, demo_rows, player_rows, veto_rows, h2h_rows, form_rows)


async def backfill_rankings(
    fetcher: "HltvFetcher",
    start_date: datetime.date,
    end_date: datetime.date,
) -> None:
    """Scrape weekly HLTV team rankings for all Mondays in [start_date-1week, end_date]."""
    ranking_start = start_date - datetime.timedelta(weeks=1)
    mondays = _all_mondays(ranking_start, end_date)
    logger.info("Scraping %d weekly rankings (%s to %s)", len(mondays), ranking_start, end_date)

    ranking_rows: list[dict] = []
    for j, date in enumerate(mondays):
        month_name = calendar.month_name[date.month].lower()
        url = f"{_BASE_URL}/ranking/teams/{date.year}/{month_name}/{date.day}"
        html, _ = await fetcher.html(url, kind="ranking", wait_selector=".ranked-team")
        if html:
            soup = BeautifulSoup(html, "lxml")
            rows = parse_team_ranking_html(soup, date)
            if rows:
                ranking_rows.extend(rows)
                logger.info("Rankings [%d/%d] %s — %d teams", j + 1, len(mondays), date, len(rows))
            else:
                logger.warning("Rankings [%d/%d] %s — no rows", j + 1, len(mondays), date)
        else:
            logger.warning("Rankings [%d/%d] %s — no HTML", j + 1, len(mondays), date)

    publish(cs2.TEAM_RANKINGS, ranking_rows)


def backfill_priors() -> None:
    logger.info("Priors backfill complete: %d rows.", publish_priors())


class HltvFetcher:
    """
    Cache-first page access with a lazily started browser.

    Chrome is the expensive part — it needs a visible window for the Cloudflare
    challenge — so it must not start at all when every page is already cached.
    That is where a re-parse goes from hours to minutes, and it only works if the
    browser is created on the first genuine miss rather than up front.
    """

    def __init__(self, cache: HtmlCache):
        self._cache = cache
        self._browser = None
        self._page = None

    async def page(self) -> uc.Tab:
        if self._page is None:
            _DEMO_TMP.mkdir(parents=True, exist_ok=True)
            _CHROME_PROFILE.mkdir(parents=True, exist_ok=True)
            logger.info("starting Chrome (first cache miss)")
            self._browser = await uc.start(headless=False, user_data_dir=str(_CHROME_PROFILE))
            self._page = await self._browser.get("about:blank")
            await self._page.send(uc.cdp.browser.set_download_behavior(
                behavior="allow", download_path=str(_DEMO_TMP), events_enabled=True,
            ))
            self._browser.add_handler(uc.cdp.browser.DownloadWillBegin, _on_download_begin)
            self._browser.add_handler(uc.cdp.browser.DownloadProgress, _on_download_progress)
        return self._page

    async def html(
        self, url: str, *, kind: str, wait_selector: str | None = None, wait_count: int = 1,
    ) -> tuple[str | None, datetime.datetime | None]:
        hit = self._cache.get(url, kind=kind)
        if hit is not None and is_complete(hit.html):
            return hit.html, hit.fetched_at
        if hit is not None:
            logger.info("cached copy of %s is truncated — refetching", url)
        page = await self.page()
        fetched_at = datetime.datetime.now(datetime.timezone.utc)
        html = await _get_html(page, url, wait_selector, wait_count=wait_count)
        if html is not None and is_cacheable(kind, html):
            # Write through per page, never batched: a ten-hour run that stalls on
            # Cloudflare must resume against what it already fetched.
            self._cache.put(url, html, kind=kind, fetched_at=fetched_at)
        return html, fetched_at

    def stop(self) -> None:
        if self._browser is not None:
            self._browser.stop()
            self._browser = None
            self._page = None


async def _run_with_fetcher(coro, policy: CachePolicy | None = None) -> None:
    """Run coro(fetcher); Chrome starts only if something is not cached."""
    cache = HtmlCache(policy=policy or CachePolicy())
    try:
        cache.warm_index()
    except Exception as exc:
        logger.warning("could not warm the remote cache index (%s) — falling back to per-key lookups", exc)
    fetcher = HltvFetcher(cache)
    try:
        await coro(fetcher)
    finally:
        fetcher.stop()
        cache.sync_pending()


def parse_cache_to_frames(
    start: datetime.date | None = None, end: datetime.date | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Parse every cached page into DataFrames without publishing anything.

    Separate from reparse_from_cache so feature work can read a partially filled
    cache while a scrape is still running, without upserting a half month into
    the shared datasets.
    """
    cache = HtmlCache()
    stars_by_id: dict[int, int] = {}
    listing_keys = cache.local_keys(kind="results")
    for key in listing_keys:
        entry = cache.read_key(key)
        if entry is None:
            continue
        listed, _more = parse_match_ids_from_html(entry.html)
        for match_id, _href, stars in listed:
            stars_by_id[int(match_id)] = int(stars)
    logger.info("event tiers recovered from %d cached listing page(s): %d matches",
                len(listing_keys), len(stars_by_id))

    match_keys = cache.local_keys(kind="match")
    logger.info("re-parsing %d cached match page(s)...", len(match_keys))

    rows: list[dict] = []
    players: list[dict] = []
    vetos: list[dict] = []
    h2hs: list[dict] = []
    forms: list[dict] = []
    skipped = 0
    truncated = 0
    for i, key in enumerate(match_keys):
        entry = cache.read_key(key)
        if entry is None:
            continue
        if not is_complete(entry.html):
            truncated += 1
            continue
        m = re.search(r"matches_(\d+)", key)
        if m is None:
            continue
        match_id = int(m.group(1))
        result = parse_match_detail_html(
            BeautifulSoup(entry.html, "lxml"), match_id, stars=stars_by_id.get(match_id, 0),
        )
        if result is None:
            skipped += 1
            continue
        players.extend(result.get("players", []))
        vetos.extend(result.get("veto", []))
        h2hs.extend(result.get("h2h", []))
        forms.extend(result.get("recent_form", []))
        for row in result["rows"]:
            if start and row["match_date"] < start:
                continue
            if end and row["match_date"] > end:
                continue
            rows.append(row)
        if (i + 1) % 500 == 0:
            logger.info("  %d / %d parsed, %d rows", i + 1, len(match_keys), len(rows))

    logger.info("re-parse complete: %d rows from %d pages (%d skipped)", len(rows), len(match_keys), skipped)
    if truncated:
        logger.warning("%d cached match page(s) are truncated and were skipped — re-run the backfill "
                       "over their dates to refetch them", truncated)
    return {
        "cs2_match_history": pd.DataFrame(rows),
        "cs2_player_map_stats": pd.DataFrame(players),
        "cs2_match_veto": pd.DataFrame(vetos),
        "cs2_h2h_history": pd.DataFrame(h2hs),
        "cs2_team_recent_form": pd.DataFrame(forms),
    }


def reparse_from_cache(start: datetime.date | None = None, end: datetime.date | None = None) -> None:
    """
    Rebuild the published datasets from cached HTML — no network, no Chrome.

    This is what the cache is for: a parser change costs minutes here instead of
    a multi-hour re-scrape behind a manual Cloudflare challenge.
    """
    frames = parse_cache_to_frames(start, end)
    if frames["cs2_match_history"].empty:
        logger.warning("no rows parsed — nothing published")
        return
    publish_match_data(
        frames["cs2_match_history"].to_dict("records"),
        [],
        frames["cs2_player_map_stats"].to_dict("records"),
        frames["cs2_match_veto"].to_dict("records"),
        frames["cs2_h2h_history"].to_dict("records"),
        frames["cs2_team_recent_form"].to_dict("records"),
    )


def _run_cancellable(coro) -> None:
    """Run a coroutine, ensuring finally blocks execute on Ctrl+C."""
    loop = uc.loop()
    task = loop.create_task(coro)
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        logger.info("Interrupted — flushing collected data...")
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    rp_ = sub.add_parser("reparse", help="Rebuild datasets from cached HTML (no network, no browser)")
    rp_.add_argument("--start-date", default=None)
    rp_.add_argument("--end-date", default=None)

    mp = sub.add_parser("matches", help="Scrape match stats and demos")
    mp.add_argument("--start-date", required=True)
    mp.add_argument("--end-date", required=True)
    mp.add_argument("--min-stars", type=int, default=0, help="Min stars to scrape a match (0=all)")
    mp.add_argument("--skip-demos", action="store_true")
    mp.add_argument("--min-stars-demo", type=int, default=2, help="Min stars to download a demo")
    mp.add_argument("--match-ids", type=int, nargs="+", default=None, help="Only process these match IDs")
    mp.add_argument("--refresh", action="store_true", help="Bypass the page cache and re-fetch")
    mp.add_argument("--no-cache-write", action="store_true", help="Fetch live without storing pages")

    rp = sub.add_parser("rankings", help="Scrape weekly team rankings")
    rp.add_argument("--start-date", required=True)
    rp.add_argument("--end-date", required=True)

    sub.add_parser("priors", help="Rebuild match priors from full history (no browser)")

    args = ap.parse_args()

    if args.cmd == "priors":
        backfill_priors()

    elif args.cmd == "rankings":
        start = datetime.date.fromisoformat(args.start_date)
        end = datetime.date.fromisoformat(args.end_date)

        async def _rankings(fetcher: HltvFetcher) -> None:
            await backfill_rankings(fetcher, start, end)

        _run_cancellable(_run_with_fetcher(_rankings))

    elif args.cmd == "reparse":
        reparse_from_cache(
            datetime.date.fromisoformat(args.start_date) if args.start_date else None,
            datetime.date.fromisoformat(args.end_date) if args.end_date else None,
        )

    elif args.cmd == "matches":
        start = datetime.date.fromisoformat(args.start_date)
        end = datetime.date.fromisoformat(args.end_date)
        min_stars = args.min_stars
        skip_demos = args.skip_demos
        min_stars_demo = args.min_stars_demo
        match_ids = set(args.match_ids) if args.match_ids else None

        async def _matches(fetcher: HltvFetcher) -> None:
            await backfill_matches(
                fetcher, start, end,
                min_stars_match=min_stars,
                skip_demos=skip_demos,
                min_stars_demo=min_stars_demo,
                match_ids=match_ids,
            )

        _run_cancellable(_run_with_fetcher(
            _matches,
            CachePolicy(refresh_all=args.refresh, write=not args.no_cache_write),
        ))
