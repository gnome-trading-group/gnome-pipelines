"""
Every 15 min: HLTV's view of the BO3s about to start, for gnome-launcher and the CS2 strategies.

1. HLTV's /matches listing, every run: kickoff and live status for every listed match, for
   one page. A match is tracked from window_hours before kickoff until it leaves the listing.
2. Each tracked match's own page on a schedule that tightens toward kickoff (refetch_bands),
   for what only the page has: announced lineups, head-to-head, ranks. Features are built
   with build_priors_for, the code that built the training data, and appended to
   cs2_upcoming_features, which the strategies price from as of each event.
3. Each match's markets on every venue (venues.py: Polymarket, Kalshi, Polymarket US), matched
   by HLTV team id + kickoff (identity.py, with aliases kept per venue). A venue's legs are
   fixed once its series market is registered, so they never change under a running session.
4. The top of book of each upcoming match's series and map-1 market on every venue, every run,
   whether or not the market is registered: the spreads and depth the strategies would really
   trade against.
5. cs2_upcoming_matches gets a row per tracked match per run; the newest is its state.

HLTV is the only source of match facts. The venues are read only to find markets and books.
Every page costs ZenRows credits, so a day's spend is capped: past daily_credit_budget,
every match falls back to the slowest refetch interval. Locally, fetcher=chrome reads HLTV
through a visible Chrome window instead, at no cost.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Literal

import pandas as pd
import requests
from bs4 import BeautifulSoup
from gnomepy.data import DatasetStore, TableSpec
from gnomepy.registry import RegistryClient
from pydantic import Field, field_validator
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import GreaterThanOrEqual

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule, register_pipeline
from gnome_pipelines.cs2_upcoming.venues import VENUES, Ladder, Venue
from gnome_pipelines.datasets import cs2
from gnome_pipelines.environment import environment
from gnome_pipelines.hltv_cs2.chrome import ChromeSource
from gnome_pipelines.hltv_cs2.fetch import PageSource, ZenRowsSource, run_with_source
from gnome_pipelines.hltv_cs2.publish import publish
from gnome_pipelines.hltv_cs2.scraper import (
    CREDITS_PER_PAGE, _BASE_URL, _match_url, parse_upcoming_listing_html, parse_upcoming_match_html,
)
from gnome_pipelines.stores import dataset_store
from gnomepy_research.models.cs2.hltv.build_priors import build_priors_for
from gnomepy_research.models.cs2.prematch import identity

logger = logging.getLogger(__name__)

UPCOMING, LIVE, MISSING = "upcoming", "live", "missing"
ACTIVE = (UPCOMING, LIVE)
# One bad parse of the listing must not end a match: its session would be shut down.
MISSING_AFTER = 2
# Runs land a little early or late; without slack a 15 min interval polled every 15 min skips every other run.
SLACK = pd.Timedelta(minutes=3)
# Shares within this many dollars of the touch: what a taker order a few cents deep could fill.
DEPTH_WINDOW = 0.05

MATCH_COLUMNS = cs2.UPCOMING_MATCHES.schema.names


class CS2UpcomingParams(PipelineParams):
    window_hours: float = Field(12, gt=0, description="track a match from this many hours before kickoff")
    refetch_bands: list[tuple[float, float]] = Field(
        [(12, 120), (2, 20), (1, 15)],
        description=("[hours before kickoff, minutes between match-page fetches]: within each band's hours, "
                     "a match page is refetched this often; the tightest band that applies wins"))
    daily_credit_budget: int = Field(15000, ge=0, description=(
        "ZenRows credits this pipeline may spend per UTC day; past it, every match uses the slowest interval"))
    lookback_days: int = Field(3, gt=0, description="how far back to read state, for matches still settling aliases")
    dry_run: bool = Field(False, description="fetch and compute everything, publish nothing")
    fetcher: Literal["zenrows", "chrome"] = Field("zenrows", description=(
        "zenrows (anywhere, costs credits) or chrome (a visible local window, free; "
        "click through Cloudflare when asked)"))

    @field_validator("refetch_bands")
    @classmethod
    def _bands(cls, bands: list[tuple[float, float]]) -> list[tuple[float, float]]:
        if not bands or any(h <= 0 or m <= 0 for h, m in bands):
            raise ValueError("refetch_bands needs at least one band, every hours and minutes positive")
        return sorted(bands, reverse=True)


def refetch_interval(to_kickoff: pd.Timedelta, bands: list[tuple[float, float]], over_budget: bool) -> pd.Timedelta:
    """Time between match-page fetches for a match this far from kickoff (bands sorted widest first)."""
    if over_budget:
        return pd.Timedelta(minutes=max(m for _, m in bands))
    minutes = bands[0][1]
    for hours, m in bands:
        if to_kickoff <= pd.Timedelta(hours=hours):
            minutes = m
    return pd.Timedelta(minutes=minutes)


def page_due(row: dict, now: pd.Timestamp, bands: list[tuple[float, float]], over_budget: bool) -> bool:
    if row["status"] != UPCOMING:
        return False
    if pd.isna(row["features_at"]):
        return True
    interval = refetch_interval(pd.Timestamp(row["kickoff"]) - now, bands, over_budget)
    return now - pd.Timestamp(row["features_at"]) >= interval - SLACK


def new_row(listed: dict) -> dict:
    return {"match_id": int(listed["match_id"]), "team_a_id": None, "team_b_id": None,
            "team_a_name": listed["team_a_name"], "team_b_name": listed["team_b_name"],
            "kickoff": listed["match_time"], "stars": listed["stars"], "status": UPCOMING, "missed_listings": 0,
            "features_at": pd.NaT, "markets": "[]", "venues": "{}"}


def track(prev: dict[int, dict], listing: list[dict], now: pd.Timestamp,
          window: pd.Timedelta) -> dict[int, dict]:
    """
    This run's state for every tracked match. A listed BO3 with both teams named starts being
    tracked inside the window, before kickoff. A tracked match missing from the listing turns
    missing after MISSING_AFTER listings in a row, and then stops being tracked.
    """
    listed = {int(m["match_id"]): m for m in listing
              if m["match_time"] is not None and m["bo_type"] == 3 and m["team_a_name"] and m["team_b_name"]}
    out = {}
    for match_id, m in listed.items():
        kickoff = pd.Timestamp(m["match_time"])
        if match_id in prev and prev[match_id]["status"] in ACTIVE:
            row = dict(prev[match_id])
        elif not m["is_live"] and now < kickoff <= now + window:
            row = new_row(m)
        else:
            continue
        row.update(kickoff=kickoff, stars=m["stars"], missed_listings=0, status=LIVE if m["is_live"] else UPCOMING)
        out[match_id] = row
    for match_id, p in prev.items():
        if match_id in listed or p["status"] not in ACTIVE:
            continue
        row = dict(p)
        row["missed_listings"] = int(p["missed_listings"]) + 1
        if row["missed_listings"] >= MISSING_AFTER:
            row["status"] = MISSING
        out[match_id] = row
    return out


def team_a_markets(r: identity.Resolution, frame: pd.DataFrame) -> list[dict]:
    """
    Team A's instrument, and both teams' registry ids, for the matched series market and the
    same meeting's map 1. Map 1's market may list its outcomes in a different order from the
    series market, so each market is oriented by the name team A has in the series market,
    never by position.
    """
    series = frame[frame.market_slug == r.market_slug].iloc[0]
    a_name = series.o0 if r.team_a_outcome == 0 else series.o1
    meeting = frame[frame.meeting == series.meeting].drop_duplicates("market")
    out = []
    for mk in sorted(meeting.itertuples(), key=lambda m: m.market != "series"):
        side = 0 if mk.o0 == a_name else 1 if mk.o1 == a_name else None
        if side is None:
            continue
        out.append({"market": mk.market, "instrument": mk.instruments[side],
                    "esid_a": mk.esids[side], "esid_b": mk.esids[1 - side]})
    return out


def match_venue(match: dict, frame: pd.DataFrame, aliases: pd.DataFrame,
                venue: str) -> tuple[identity.Resolution, dict | None]:
    """The match's resolution on one venue, and its state there if a series market was found."""
    r = identity.resolve_match(match, frame, aliases, source=venue)
    if r.market_slug is None:
        return r, None
    return r, {"series": r.market_slug, "team_a_outcome": r.team_a_outcome, "markets": team_a_markets(r, frame),
               "learned": [list(x) for x in r.learned], "alias_settled": False}


def venue_legs(venue: str, state: dict, listing) -> list[dict]:
    """A venue's tradable legs: each market with both outcomes registered, and none unless the series is."""
    legs = []
    for m in state["markets"]:
        listing_a, listing_b = listing(m["esid_a"]), listing(m["esid_b"])
        if listing_a is not None and listing_b is not None:
            legs.append({"venue": venue, "market": m["market"],
                         "listing_team_a": int(listing_a), "listing_team_b": int(listing_b)})
    return legs if any(leg["market"] == "series" for leg in legs) else []


def book_row(match_id: int, venue: str, market: str, instrument: str, ladder: Ladder) -> dict:
    """One market's top of book in team A's terms, plus the shares within DEPTH_WINDOW of each touch."""
    bids, asks, book_at = ladder
    best_bid, best_ask = (max(bids) if bids else None), (min(asks) if asks else None)
    return {
        "match_id": match_id, "venue": venue, "market": market, "instrument": instrument,
        "best_bid": best_bid, "best_ask": best_ask,
        "bid_size": bids.get(best_bid), "ask_size": asks.get(best_ask),
        "bid_size_5c": sum(v for k, v in bids.items() if k >= best_bid - DEPTH_WINDOW - 1e-9) if bids else 0.0,
        "ask_size_5c": sum(v for k, v in asks.items() if k <= best_ask + DEPTH_WINDOW + 1e-9) if asks else 0.0,
        "book_at": book_at,
    }


def awaiting_settlement(rows: dict[int, dict], venue: str, series_won: pd.Series) -> dict[int, dict]:
    """Rows whose aliases learned on this venue a finished match can now judge."""
    out = {}
    for mid, row in rows.items():
        state = json.loads(row["venues"]).get(venue)
        if state and state["learned"] and not state["alias_settled"] and mid in series_won.index:
            out[mid] = state
    return out


def settle_aliases(rows: dict[int, dict], venue: str, finals: dict[str, list[float]], series_won: pd.Series,
                   aliases: pd.DataFrame) -> tuple[dict[int, dict], pd.DataFrame]:
    """Fold a venue's settled series markets into the aliases its matches taught; returns the rows that changed."""
    changed = {}
    for mid, state in awaiting_settlement(rows, venue, series_won).items():
        final = finals.get(state["series"])
        if not final:
            continue
        agrees = int(final[int(state["team_a_outcome"])] > 0.5) == int(series_won[mid])
        for name, team_id in state["learned"]:
            aliases = identity.settle(aliases, venue, name, int(team_id), agrees)
        venues = json.loads(rows[mid]["venues"])
        venues[venue] = {**state, "alias_settled": True}
        changed[mid] = {**rows[mid], "venues": json.dumps(venues)}
    return changed, aliases


def _listing_id(registry: RegistryClient, exchange_id: int, esid: str, retries: int = 3) -> int | None:
    for attempt in range(retries):
        try:
            hits = registry.get_listing(exchange_id=exchange_id, exchange_security_id=esid)
            return int(hits[0].listing_id) if hits else None
        except requests.exceptions.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1 + attempt)
    return None


def feature_rows(parsed: list[dict], ds: DatasetStore) -> pd.DataFrame:
    """One unknown-map feature row per parsed page, with the lineups it was priced on."""
    targets = pd.DataFrame([{**p["match"], "map_name": "", "map_position_in_series": 1, "team_a_series_score": 0,
                             "team_b_series_score": 0, "is_decider": 0.0} for p in parsed])
    targets["match_date"] = pd.to_datetime(targets.match_date)
    page_h2h = pd.DataFrame([r for p in parsed for r in p["h2h"]])
    priors = build_priors_for(
        ds.load(cs2.MATCH_HISTORY.name), ds.load(cs2.TEAM_RANKINGS.name), targets,
        player_stats=ds.load(cs2.PLAYER_MAP_STATS.name), veto=ds.load(cs2.MATCH_VETO.name),
        h2h=pd.concat([ds.load(cs2.H2H_HISTORY.name), page_h2h], ignore_index=True),
    )
    if priors.empty:
        return priors
    lineups = targets.set_index("match_id")[["team_a_lineup_ids", "team_b_lineup_ids"]]
    return priors.join(lineups, on="match_id")


async def fetch_due(source: PageSource, prev: dict[int, dict], now: pd.Timestamp,
                    params: CS2UpcomingParams, over_budget: bool) -> tuple[dict[int, dict], dict[int, str | None]] | None:
    """
    This run's tracked matches and the pages of those due a refetch (None where a fetch
    failed), or None if the listing couldn't be fetched.
    """
    html = await source.html(f"{_BASE_URL}/matches")
    if html is None:
        return None
    rows = track(prev, parse_upcoming_listing_html(html), now, pd.Timedelta(hours=params.window_hours))
    due = [mid for mid, r in rows.items() if page_due(r, now, params.refetch_bands, over_budget)]
    pages: dict[int, str | None] = {}
    for start in range(0, len(due), source.concurrency):
        batch = due[start:start + source.concurrency]
        pages.update(zip(batch, await asyncio.gather(*(source.html(_match_url(mid)) for mid in batch))))
    return rows, pages


def _aliases(ds: DatasetStore) -> pd.DataFrame:
    try:
        return ds.load(cs2.TEAM_ALIASES.name)
    except NoSuchTableError:
        return identity.empty_aliases()


def _match_on(venue: str, row: dict, frame: pd.DataFrame, aliases: pd.DataFrame, listing,
              now: pd.Timestamp) -> tuple[pd.DataFrame, str | None]:
    """
    Match one row to one venue and look up its legs, updating the row in place. A venue's
    match is kept once found, so later runs only retry the registry. Returns the aliases and
    why the row has no legs on the venue, if it hasn't.
    """
    venues = json.loads(row["venues"])
    state = venues.get(venue)
    if state is None:
        resolution, state = match_venue({**row, "match_time": row["kickoff"]}, frame, aliases, venue)
        aliases = identity.learn(aliases, resolution.learned, now, source=venue)
        if state is None:
            return aliases, resolution.reason
        venues[venue] = state
        row["venues"] = json.dumps(venues)
    legs = venue_legs(venue, state, listing)
    if not legs:
        return aliases, "series market not in registry"
    row["markets"] = json.dumps(json.loads(row["markets"]) + legs)
    return aliases, None


def _series_won(history: pd.DataFrame) -> pd.Series:
    decided = history[history.team_a_score != history.team_b_score]
    return decided.groupby("match_id").team_a_won.agg(lambda w: int(w.sum() * 2 > len(w)))


def _load_since(ds: DatasetStore, spec: TableSpec, since: pd.Timestamp) -> pd.DataFrame:
    try:
        return ds.load(spec.name, row_filter=GreaterThanOrEqual("generated_at", since.isoformat()))
    except NoSuchTableError:
        return pd.DataFrame(columns=spec.schema.names)


def latest_state(rows: pd.DataFrame) -> dict[int, dict]:
    if rows.empty:
        return {}
    newest = rows.sort_values("generated_at").groupby("match_id").tail(1)
    return {int(r["match_id"]): {c: r[c] for c in MATCH_COLUMNS if c != "generated_at"}
            for r in newest.to_dict("records")}


@register_pipeline
class CS2UpcomingPipeline(Pipeline[CS2UpcomingParams]):
    name = "cs2_upcoming"
    description = "Track upcoming CS2 BO3s on HLTV: kickoff, status, lineups and features; their markets and books on every venue"
    # Measured 2026-10-15: 1.1 GB peak, single-threaded; the spare memory is for history growth and busy days.
    resources = Resources(cpu=1024, memory=3072)
    schedules = (Schedule("quarter_hourly", "rate(15 minutes)", {}, "every 15 minutes"),)

    def run(self, params: CS2UpcomingParams) -> PipelineResult:
        if params.fetcher == "chrome" and environment().in_task:
            raise ValueError("fetcher=chrome needs a screen for the Cloudflare challenge; run it locally")
        now = pd.Timestamp.now(tz="UTC").floor("s")
        ds = dataset_store()
        prev = latest_state(_load_since(ds, cs2.UPCOMING_MATCHES, now - pd.Timedelta(days=params.lookback_days)))
        runs = _load_since(ds, cs2.UPCOMING_RUNS, now.normalize())
        spent = int(runs.credits.sum()) if len(runs) else 0
        over_budget = spent >= params.daily_credit_budget
        if over_budget:
            logger.warning("%d credits spent today, budget %d: slowest refetch interval for every match",
                           spent, params.daily_credit_budget)

        source = {"zenrows": ZenRowsSource, "chrome": ChromeSource}[params.fetcher]()
        fetched = run_with_source(source, lambda s: fetch_due(s, prev, now, params, over_budget))
        credits = source.billed * CREDITS_PER_PAGE
        if fetched is None:
            logger.warning("could not fetch the HLTV listing; no match changes state this run")
            self._publish_run(params, now, False, 0, credits, over_budget, len(prev))
            return PipelineResult(status="succeeded", outputs={"listing_ok": False, "credits": credits})

        rows, pages = fetched
        parsed = []
        for mid, page in pages.items():
            p = parse_upcoming_match_html(BeautifulSoup(page, "lxml"), mid, rows[mid]["stars"]) if page else None
            if p is None:
                logger.warning("match %d: page not fetched or teams not known yet", mid)
                continue
            parsed.append(p)
            rows[mid].update(team_a_id=int(p["match"]["team_a_id"]), team_b_id=int(p["match"]["team_b_id"]))

        features = feature_rows(parsed, ds) if parsed else pd.DataFrame()
        for mid in (features.match_id.astype(int).tolist() if len(features) else []):
            rows[mid]["features_at"] = now

        venues = [v() for v in VENUES]
        summary = {v.name: {"matched": 0, "tradable": 0, "skipped": {}, "books": 0} for v in venues}
        aliases, settled = self._markets(ds, venues, rows, prev, now, summary)
        books = self._books(venues, rows, summary)
        for v in venues:
            states = [json.loads(r["venues"]).get(v.name) for r in rows.values()]
            summary[v.name]["matched"] = sum(s is not None for s in states)
            summary[v.name]["tradable"] = sum(any(leg["venue"] == v.name for leg in json.loads(r["markets"]))
                                              for r in rows.values())

        changed = {**rows, **settled}
        if not params.dry_run:
            if len(features):
                publish(cs2.UPCOMING_FEATURES, features.assign(generated_at=now))
            if changed:
                publish(cs2.UPCOMING_MATCHES, pd.DataFrame(
                    [{**r, "generated_at": now} for r in changed.values()])[MATCH_COLUMNS])
            if len(books):
                publish(cs2.UPCOMING_BOOKS, books.assign(generated_at=now))
            if aliases is not None:
                publish(cs2.TEAM_ALIASES, aliases)
        self._publish_run(params, now, True, sum(p is not None for p in pages.values()), credits, over_budget, len(rows))

        statuses = pd.Series([r["status"] for r in rows.values()]).value_counts().to_dict() if rows else {}
        return PipelineResult(status="succeeded", outputs={
            "listing_ok": True, "tracked": statuses, "pages_due": len(pages), "features": len(features),
            "with_markets": sum(r["markets"] != "[]" for r in rows.values()), "venues": summary,
            "books": len(books), "aliases_settled": len(settled), "credits": credits,
            "credits_today": spent + credits, "over_budget": over_budget,
            "dry_run": params.dry_run,
        })

    @staticmethod
    def _markets(ds: DatasetStore, venues: list[Venue], rows: dict[int, dict], prev: dict[int, dict],
                 now: pd.Timestamp, summary: dict[str, dict]) -> tuple[pd.DataFrame | None, dict[int, dict]]:
        """
        Match each upcoming match to every venue it isn't tradable on yet, then settle the aliases
        finished matches taught. A venue that fails is skipped for this run, the others carry on.
        Returns the aliases (None if untouched) and the rows settlement changed.
        """
        registry = RegistryClient()
        exchange_ids = {e.exchange_name: int(e.exchange_id) for e in registry.get_exchange()}
        aliases: pd.DataFrame | None = None
        settled: dict[int, dict] = {}
        series_won: pd.Series | None = None
        for venue in venues:
            stats = summary[venue.name]
            try:
                pending = [r for r in rows.values() if r["status"] == UPCOMING and not pd.isna(r["team_a_id"])
                           and not any(leg["venue"] == venue.name for leg in json.loads(r["markets"]))]
                if pending:
                    frame = venue.markets(now)
                    aliases = aliases if aliases is not None else _aliases(ds)
                    exchange_id = exchange_ids[venue.exchange_name]
                    listing = lambda esid: _listing_id(registry, exchange_id, esid)
                    for r in pending:
                        aliases, reason = _match_on(venue.name, r, frame, aliases, listing, now)
                        if reason:
                            stats["skipped"][reason] = stats["skipped"].get(reason, 0) + 1

                current = {**prev, **rows, **settled}
                if any(json.loads(r["venues"]).get(venue.name, {}).get("learned") for r in current.values()):
                    series_won = series_won if series_won is not None else _series_won(ds.load(cs2.MATCH_HISTORY.name))
                    waiting = awaiting_settlement(current, venue.name, series_won)
                    if waiting:
                        aliases = aliases if aliases is not None else _aliases(ds)
                        finals = venue.finals(sorted({s["series"] for s in waiting.values()}))
                        changed, aliases = settle_aliases(current, venue.name, finals, series_won, aliases)
                        settled.update(changed)
                        rows.update({mid: r for mid, r in changed.items() if mid in rows})
            except Exception as exc:
                logger.exception("%s: markets skipped this run", venue.name)
                stats["error"] = str(exc)
        return aliases, settled

    @staticmethod
    def _books(venues: list[Venue], rows: dict[int, dict], summary: dict[str, dict]) -> pd.DataFrame:
        """This run's books of every upcoming match's known markets, on each venue that answers."""
        out = []
        for venue in venues:
            wanted = {m["instrument"]: (mid, m["market"]) for mid, r in rows.items() if r["status"] == UPCOMING
                      for m in json.loads(r["venues"]).get(venue.name, {}).get("markets", [])}
            if not wanted:
                continue
            try:
                ladders = venue.books(list(wanted))
            except Exception as exc:
                logger.exception("%s: books skipped this run", venue.name)
                summary[venue.name]["error"] = str(exc)
                continue
            got = [book_row(mid, venue.name, market, inst, ladders[inst])
                   for inst, (mid, market) in wanted.items() if inst in ladders]
            summary[venue.name]["books"] = len(got)
            out += got
        return pd.DataFrame(out)

    @staticmethod
    def _publish_run(params: CS2UpcomingParams, now: pd.Timestamp, listing_ok: bool, pages: int, credits: int,
                     over_budget: bool, tracked: int) -> None:
        if params.dry_run:
            return
        publish(cs2.UPCOMING_RUNS, [{"generated_at": now, "listing_ok": listing_ok, "pages_fetched": pages,
                                     "credits": credits, "over_budget": over_budget,
                                     "tracked": tracked}])
