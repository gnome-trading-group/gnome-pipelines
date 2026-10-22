"""HLTV rosters: every team's roster moves (joins, departures, benchings, loans, substitutes, coaches) and its
current roster, from team pages and the site-wide transfers feed.

backfill fetches one team page per team (each holds the team's all-time transfer list); daily reads the transfers
feed back to the moves already stored and refetches only the team pages it touches, plus those of teams with
upcoming matches, so their current-roster snapshots stay fresh. Every page goes through the HLTV page cache, team
pages and feed pages one copy per day (html_cache._SNAPSHOT_KINDS), so fetcher=cache re-parses without the network.

The scheduled run fetches through ZenRows; a backfill runs locally with fetcher=chrome.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Literal

import pandas as pd
from bs4 import BeautifulSoup
from pydantic import Field, model_validator

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.datasets import cs2
from gnome_pipelines.environment import environment
from gnome_pipelines.hltv_cs2.chrome import ChromeSource
from gnome_pipelines.hltv_cs2.fetch import (
    MAX_CONSECUTIVE_FAILURES, CacheSource, HltvFetcher, PageSource, RateLimited, ZenRowsSource, run_with_fetcher,
)
from gnome_pipelines.hltv_cs2.html_cache import CachePolicy
from gnome_pipelines.hltv_cs2.publish import publish
from gnome_pipelines.hltv_cs2.roster import NO_TEAM, parse_team_page_html, parse_transfers_page_html
from gnome_pipelines.hltv_cs2.scraper import _BASE_URL
from gnome_pipelines.stores import dataset_store

logger = logging.getLogger(__name__)

MOVE_KEY = list(cs2.ROSTER_MOVES.key)
# Teams whose current roster is refreshed on a daily run: those playing within this long.
UPCOMING_HORIZON = pd.Timedelta(days=2)


class HltvRostersParams(PipelineParams):
    mode: Literal["backfill", "daily"] = Field("daily", description=(
        "backfill: one team page per team; daily: the transfers feed back to the stored moves, then the team "
        "pages it touches and those of teams with upcoming matches"))
    team_ids: list[int] | None = Field(None, description="backfill these teams instead of every team in match history")
    min_matches: int = Field(3, ge=1, description="backfill from match history: teams with at least this many matches")
    max_feed_pages: int = Field(20, ge=1, description="daily: the most transfers-feed pages to read back")
    fetcher: Literal["zenrows", "chrome", "cache"] = Field("zenrows", description=(
        "zenrows (anywhere), chrome (a visible local window; click through Cloudflare when asked) "
        "or cache (only pages already cached, no network)"))
    refresh: bool = Field(False, description="ignore today's cached copies and fetch again")

    @model_validator(mode="after")
    def _consistent(self) -> HltvRostersParams:
        if self.team_ids is not None and self.mode != "backfill":
            raise ValueError("team_ids is for mode=backfill")
        if self.refresh and self.fetcher == "cache":
            raise ValueError("refresh has nothing to fetch with fetcher=cache")
        return self


class HltvRostersPipeline(Pipeline[HltvRostersParams]):
    name = "hltv_rosters"
    description = "HLTV roster moves and current rosters, from team pages and the transfers feed"
    resources = Resources(cpu=1024, memory=3072)
    schedules = (
        Schedule("daily", "cron(30 5 * * ? *)", {"mode": "daily"},
                 "05:30 UTC: new transfers, and fresh rosters for teams playing in the next two days"),
    )

    def run(self, params: HltvRostersParams) -> PipelineResult:
        if params.fetcher == "chrome" and environment().in_task:
            raise ValueError("fetcher=chrome needs a screen for the Cloudflare challenge; run it locally")
        ds = dataset_store()

        async def collect(fetcher: HltvFetcher) -> dict:
            if params.mode == "backfill":
                team_ids = params.team_ids or history_teams(ds.load(cs2.MATCH_HISTORY.name), params.min_matches)
                return await collect_teams(fetcher, team_ids, feed_moves=[])
            known = ds.load(cs2.ROSTER_MOVES.name)
            feed_moves, pages = await read_feed(fetcher, known, params.max_feed_pages)
            team_ids = sorted(moved_teams(feed_moves) | upcoming_teams(ds.load(cs2.UPCOMING_MATCHES.name)))
            return {**await collect_teams(fetcher, team_ids, feed_moves), "feed_pages": pages, "feed_moves": len(feed_moves)}

        outputs = run_with_fetcher(_source(params.fetcher), _policy(params), collect)
        return PipelineResult(status="succeeded", outputs=outputs)


def history_teams(history: pd.DataFrame, min_matches: int) -> list[int]:
    series = history.drop_duplicates("match_id")
    counts = pd.concat([series.team_a_id, series.team_b_id]).dropna().astype(int).value_counts()
    return sorted(int(t) for t in counts[counts >= min_matches].index)


def upcoming_teams(matches: pd.DataFrame, now: pd.Timestamp | None = None) -> set[int]:
    if matches.empty:
        return set()
    now = now or pd.Timestamp.now(tz="UTC")
    latest = matches.sort_values("generated_at").drop_duplicates("match_id", keep="last")
    soon = latest[(latest.status == "upcoming") & (latest.kickoff <= now + UPCOMING_HORIZON)]
    return {int(t) for t in pd.concat([soon.team_a_id, soon.team_b_id]).dropna()}


def moved_teams(moves: list[dict]) -> set[int]:
    return {t for m in moves for t in (m["from_team_id"], m["to_team_id"]) if t != NO_TEAM}


def new_moves(moves: list[dict], known: pd.DataFrame) -> list[dict]:
    if known.empty or not moves:
        return moves
    seen = set(map(tuple, known[MOVE_KEY].assign(event_date=pd.to_datetime(known.event_date).dt.date).itertuples(index=False)))
    return [m for m in moves if tuple(m[k] for k in MOVE_KEY) not in seen]


async def read_feed(fetcher: HltvFetcher, known: pd.DataFrame, max_pages: int) -> tuple[list[dict], int]:
    """The feed's moves, newest first, back to the first page with nothing new on it."""
    moves: list[dict] = []
    for page in range(max_pages):
        url = f"{_BASE_URL}/transfers" + (f"?page={page}" if page else "")
        html, _ = await fetcher.html(url, kind="transfers", wait_selector=".transfer-row")
        if html is None:
            logger.warning("transfers feed page %d: no page — stopping", page)
            return moves, page
        parsed = parse_transfers_page_html(BeautifulSoup(html, "lxml"), page)
        fresh = new_moves(parsed["moves"], known)
        moves.extend(fresh)
        logger.info("transfers feed page %d: %d moves, %d new", page, len(parsed["moves"]), len(fresh))
        if not fresh or not parsed["has_next"]:
            return moves, page + 1
    logger.warning("read %d feed pages and still finding new moves; a backfill may be due", max_pages)
    return moves, max_pages


async def collect_teams(fetcher: HltvFetcher, team_ids: list[int], feed_moves: list[dict]) -> dict:
    """Fetch and parse each team page, then publish its moves (with the feed's) and its roster snapshot."""
    moves, roster, failed = list(feed_moves), [], []
    consecutive_failures = 0
    try:
        for start in range(0, len(team_ids), fetcher.concurrency):
            chunk = team_ids[start:start + fetcher.concurrency]
            pages = await asyncio.gather(*(
                fetcher.html(f"{_BASE_URL}/team/{t}/x", kind="team", wait_selector=".contentCol") for t in chunk))
            for team_id, (html, fetched_at) in zip(chunk, pages):
                if html is None:
                    failed.append(team_id)
                    consecutive_failures += 1
                    if fetcher.source.can_be_blocked and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        raise RateLimited(f"{consecutive_failures} consecutive team pages failed — stopping before it escalates")
                    logger.warning("team %d: no page — skipping", team_id)
                    continue
                consecutive_failures = 0
                parsed = parse_team_page_html(BeautifulSoup(html, "lxml"), team_id, fetched_at.date())
                moves.extend(parsed["moves"])
                roster.extend(parsed["roster"])
                logger.info("[%d/%d] team %d: %d moves, %d on the roster", start + len(chunk), len(team_ids), team_id,
                            len(parsed["moves"]), len(parsed["roster"]))
        return {"teams": len(team_ids), "teams_failed": len(failed), "moves": len(moves), "roster_rows": len(roster)}
    finally:
        publish_rosters(moves, roster)


def publish_rosters(moves: list[dict], roster: list[dict]) -> None:
    # The same move appears on both teams' pages and in the feed; the key keeps one.
    unique = {tuple(m[k] for k in MOVE_KEY): m for m in moves}
    if unique:
        publish(cs2.ROSTER_MOVES, pd.DataFrame(list(unique.values())).assign(
            event_date=lambda d: pd.to_datetime(d.event_date)))
    if roster:
        publish(cs2.TEAM_ROSTER_SNAPSHOTS, pd.DataFrame(roster).assign(
            snapshot_date=lambda d: pd.to_datetime(d.snapshot_date)))


def _source(fetcher: str) -> PageSource:
    return {"zenrows": ZenRowsSource, "chrome": ChromeSource, "cache": CacheSource}[fetcher]()


def _policy(params: HltvRostersParams) -> CachePolicy:
    if params.fetcher == "cache":
        return CachePolicy(ttl=datetime.timedelta.max, write=False)
    return CachePolicy(refresh_all=params.refresh)
