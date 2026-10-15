"""The HLTV collection loops: match pages (and, through Chrome, demos) and weekly rankings.

Both take an HltvFetcher, so the same loop serves the scheduled ZenRows run, a local Chrome
run and a cache-only re-parse; fetch.py has the sources.
"""
from __future__ import annotations

import asyncio
import calendar
import datetime
import logging
from pathlib import Path

from bs4 import BeautifulSoup
from demoparser2 import DemoParser

from gnome_pipelines.datasets import cs2
from gnome_pipelines.hltv_cs2.chrome import ChromeSource
from gnome_pipelines.hltv_cs2.demo_parser import parse_demo
from gnome_pipelines.hltv_cs2.fetch import MAX_CONSECUTIVE_FAILURES, HltvFetcher, RateLimited
from gnome_pipelines.hltv_cs2.publish import publish, publish_match_data
from gnome_pipelines.hltv_cs2.scraper import (
    _BASE_URL,
    _all_mondays,
    parse_match_detail_html,
    parse_match_ids_from_html,
    parse_team_ranking_html,
)

logger = logging.getLogger(__name__)


async def collect_matches(
    fetcher: HltvFetcher,
    start_date: datetime.date,
    end_date: datetime.date,
    min_stars_match: int = 0,
    demos: bool = False,
    min_stars_demo: int = 2,
    match_ids: set[int] | None = None,
) -> dict:
    """
    Scrape all matches for every date in [start_date, end_date] and publish them.

    Accumulates data across all dates and publishes once in the finally block.
    Pages are served from the cache where possible, so a re-run only fetches what is
    still missing. Unreachable dates and failed demos are skipped and reported rather
    than aborting the pass. Demos need ChromeSource.
    """
    if demos and not isinstance(fetcher.source, ChromeSource):
        raise ValueError("demos are downloaded through Chrome; use the chrome fetcher")
    match_rows: list[dict] = []
    player_rows: list[dict] = []
    veto_rows: list[dict] = []
    h2h_rows: list[dict] = []
    form_rows: list[dict] = []
    failed_dates: list[datetime.date] = []
    n_demos_failed = 0
    n_matches = 0
    n_demos_total = 0
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

            for chunk_start in range(0, len(matches), fetcher.concurrency):
                chunk = matches[chunk_start:chunk_start + fetcher.concurrency]
                logger.info("[%d-%d/%d] fetching match pages", chunk_start + 1, chunk_start + len(chunk), len(matches))
                pages = await asyncio.gather(*(
                    fetcher.html(f"{_BASE_URL}{href}", kind="match", wait_selector=".teamRanking", wait_count=2)
                    for _mid, href, _stars in chunk
                ))
                for offset, ((match_id, _href, match_stars), (html, _fetched_at)) in enumerate(zip(chunk, pages)):
                    n = chunk_start + offset + 1
                    if html is None:
                        consecutive_failures += 1
                        if fetcher.source.can_be_blocked and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                            raise RateLimited(
                                f"{consecutive_failures} consecutive match pages failed — "
                                "almost certainly blocked or rate-limited, stopping before it escalates"
                            )
                        logger.warning("[%d/%d] match %d — no page (timed out, no rankings, or not cached) — skipping", n, len(matches), match_id)
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
                    if not demos or not demo_url:
                        if demos and not demo_url:
                            logger.info("[%d/%d] match %d — no demo URL", n, len(matches), match_id)
                        continue
                    if match_stars < min_stars_demo:
                        logger.info("[%d/%d] match %d — demo skipped (stars=%d < min=%d)", n, len(matches), match_id, match_stars, min_stars_demo)
                        n_demos_skipped += 1
                        continue

                    logger.info("[%d/%d] match %d — downloading demo (stars=%d)", n, len(matches), match_id, match_stars)

                    dem_paths = await fetcher.source.download_demo(demo_url)
                    if not dem_paths:
                        logger.warning("[%d/%d] match %d — demo download or extraction failed, skipping", n, len(matches), match_id)
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
            n_matches += len(matches)
            n_demos_total += n_demos_parsed
            current += datetime.timedelta(days=1)

        return {"matches": n_matches, "maps": len(match_rows), "demos": n_demos_total,
                "failed_dates": [d.isoformat() for d in failed_dates], "demos_failed": n_demos_failed}

    finally:
        if failed_dates:
            logger.warning("%d date(s) could not be listed and were skipped: %s",
                           len(failed_dates), ", ".join(d.isoformat() for d in failed_dates[:10]))
        if n_demos_failed:
            logger.warning("%d demo(s) failed and were skipped", n_demos_failed)
        publish_match_data(match_rows, demo_rows, player_rows, veto_rows, h2h_rows, form_rows)


async def collect_rankings(
    fetcher: HltvFetcher,
    start_date: datetime.date,
    end_date: datetime.date,
) -> int:
    """
    Scrape weekly HLTV team rankings for all Mondays in [start_date - 1 week, end_date] and publish them.

    The extra week is the ranking in force for matches early in the range, which the priors need.
    """
    ranking_start = start_date - datetime.timedelta(weeks=1)
    mondays = _all_mondays(ranking_start, end_date)
    logger.info("Scraping %d weekly rankings (%s to %s)", len(mondays), ranking_start, end_date)

    pages = []
    for chunk_start in range(0, len(mondays), fetcher.concurrency):
        chunk = mondays[chunk_start:chunk_start + fetcher.concurrency]
        pages.extend(await asyncio.gather(*(
            fetcher.html(_ranking_url(date), kind="ranking", wait_selector=".ranked-team") for date in chunk
        )))

    ranking_rows: list[dict] = []
    for j, (date, (html, _fetched_at)) in enumerate(zip(mondays, pages)):
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
    return len(ranking_rows)


def _ranking_url(date: datetime.date) -> str:
    return f"{_BASE_URL}/ranking/teams/{date.year}/{calendar.month_name[date.month].lower()}/{date.day}"
