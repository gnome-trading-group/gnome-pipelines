"""
Every 15 min: HLTV's view of the BO3s about to start, for gnome-launcher and the CS2 strategies.

1. HLTV's /matches listing, every run: kickoff and live status for every listed match, for
   one page. A match is tracked from window_hours before kickoff until it leaves the listing.
2. Each tracked match's own page on a schedule that tightens toward kickoff (refetch_bands),
   for what only the page has: announced lineups, head-to-head, ranks. Features are built
   with build_priors_for, the code that built the training data, and appended to
   cs2_upcoming_features, which the strategies price from as of each event.
3. Each match's Polymarket markets, resolved once by HLTV team id + kickoff (identity.py) and
   then fixed, so the launcher's trigger for the match never changes.
4. cs2_upcoming_matches gets a row per tracked match per run; the newest is its state.

HLTV is the only source of match facts. Polymarket is read only to find the markets.
Every page costs ZenRows credits, so a day's spend is capped: past daily_credit_budget,
every match falls back to the slowest refetch interval.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from bs4 import BeautifulSoup
from gnomepy.data import DatasetStore, TableSpec
from gnomepy.registry import RegistryClient
from pydantic import Field, field_validator
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import GreaterThanOrEqual

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule, register_pipeline
from gnome_pipelines.datasets import cs2
from gnome_pipelines.hltv_cs2.publish import publish
from gnome_pipelines.hltv_cs2.scraper import (
    CREDITS_PER_PAGE, _BASE_URL, _get_html, _make_session, _match_url, parse_upcoming_listing_html,
    parse_upcoming_match_html,
)
from gnome_pipelines.stores import dataset_store
from gnomepy_research.models.cs2.hltv.build_priors import build_priors_for
from gnomepy_research.models.cs2.prematch import identity, polymarket

logger = logging.getLogger(__name__)

UPCOMING, LIVE, MISSING = "upcoming", "live", "missing"
ACTIVE = (UPCOMING, LIVE)
# One bad parse of the listing must not end a match: its session would be shut down.
MISSING_AFTER = 2
# Runs land a little early or late; without slack a 15 min interval polled every 15 min skips every other run.
SLACK = pd.Timedelta(minutes=3)
MARKET_LOOKBACK = pd.Timedelta(days=21)
PAGE_CONCURRENCY = 5

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
            "features_at": pd.NaT, "series_slug": None, "team_a_outcome": None, "markets": "[]", "learned": "[]",
            "alias_settled": False}


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


def _orientation(market_row, team_a_name_norm: str) -> int | None:
    if market_row.o0 == team_a_name_norm:
        return 0
    if market_row.o1 == team_a_name_norm:
        return 1
    return None


def resolve_legs(match: dict, pm: pd.DataFrame, aliases: pd.DataFrame,
                 resolve) -> tuple[identity.Resolution, list[dict], str | None]:
    """
    (series resolution, legs, reason if no legs). Map 1's market may list its outcomes in
    a different order from the series market, so each market is oriented by the
    name team A has in the series market, never by position.
    """
    r = identity.resolve_match(match, pm, aliases)
    if r.market_slug is None:
        return r, [], r.reason
    by_slug = pm.set_index("market_slug")
    series = by_slug.loc[r.market_slug]
    a_name = series.o0 if r.team_a_outcome == 0 else series.o1
    legs = []
    for market, slug in (("series", r.market_slug), ("game1", identity.sibling_market(pm, r.market_slug, "game1"))):
        if slug is None:
            continue
        mk = by_slug.loc[slug]
        side = _orientation(mk, a_name)
        if side is None:
            continue
        tok_a, tok_b = (mk.tokens[0], mk.tokens[1]) if side == 0 else (mk.tokens[1], mk.tokens[0])
        listing_a, listing_b = resolve(mk.condition_id, str(tok_a)), resolve(mk.condition_id, str(tok_b))
        if listing_a is not None and listing_b is not None:
            legs.append({"market": market, "listing_team_a": int(listing_a), "listing_team_b": int(listing_b)})
    if not any(leg["market"] == "series" for leg in legs):
        return r, [], "series market not in registry"
    return r, legs, None


def settle_aliases(rows: dict[int, dict], pm: pd.DataFrame, series_won: pd.Series,
                   aliases: pd.DataFrame) -> tuple[list[int], pd.DataFrame]:
    """Fold settled series markets into the aliases each match taught us; returns the matches settled."""
    by_slug = pm.set_index("market_slug")
    settled = []
    for match_id, r in rows.items():
        learned = json.loads(r["learned"])
        if r["alias_settled"] or not learned or r["series_slug"] not in by_slug.index or match_id not in series_won.index:
            continue
        mk = by_slug.loc[r["series_slug"]]
        final = [float(x) for x in mk.final] if len(mk.final) == 2 else []
        if not final or max(final) < 0.9:
            continue
        agrees = int(final[int(r["team_a_outcome"])] > 0.5) == int(series_won[match_id])
        for name, team_id in learned:
            aliases = identity.settle(aliases, "polymarket", name, int(team_id), agrees)
        settled.append(match_id)
    return settled, aliases


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
    description = "Track upcoming CS2 BO3s on HLTV: kickoff, status, lineups and features, and their Polymarket markets"
    resources = Resources(cpu=2048, memory=8192)
    schedules = (Schedule("quarter_hourly", "rate(15 minutes)", {}, "every 15 minutes"),)

    def run(self, params: CS2UpcomingParams) -> PipelineResult:
        now = pd.Timestamp.now(tz="UTC").floor("s")
        ds = dataset_store()
        prev = latest_state(_load_since(ds, cs2.UPCOMING_MATCHES, now - pd.Timedelta(days=params.lookback_days)))
        runs = _load_since(ds, cs2.UPCOMING_RUNS, now.normalize())
        spent = int(runs.credits.sum()) if len(runs) else 0
        over_budget = spent >= params.daily_credit_budget
        if over_budget:
            logger.warning("%d credits spent today, budget %d: slowest refetch interval for every match",
                           spent, params.daily_credit_budget)

        session = _make_session()
        html = _get_html(session, f"{_BASE_URL}/matches")
        if html is None:
            logger.warning("could not fetch the HLTV listing; no match changes state this run")
            self._publish_run(params, now, False, 0, session.billed, over_budget, len(prev))
            return PipelineResult(status="succeeded", outputs={"listing_ok": False, "credits": session.billed * CREDITS_PER_PAGE})

        rows = track(prev, parse_upcoming_listing_html(html), now, pd.Timedelta(hours=params.window_hours))
        due = [mid for mid, r in rows.items() if page_due(r, now, params.refetch_bands, over_budget)]
        pages_before = session.billed
        with ThreadPoolExecutor(PAGE_CONCURRENCY) as pool:
            pages = list(pool.map(lambda mid: _get_html(session, _match_url(mid)), due))
        parsed = []
        for mid, page in zip(due, pages):
            p = parse_upcoming_match_html(BeautifulSoup(page, "lxml"), mid, rows[mid]["stars"]) if page else None
            if p is None:
                logger.warning("match %d: page not fetched or teams not known yet", mid)
                continue
            parsed.append(p)
            rows[mid].update(team_a_id=int(p["match"]["team_a_id"]), team_b_id=int(p["match"]["team_b_id"]))

        features = feature_rows(parsed, ds) if parsed else pd.DataFrame()
        for mid in (features.match_id.astype(int).tolist() if len(features) else []):
            rows[mid]["features_at"] = now

        aliases, skipped, settled = self._markets(ds, rows, prev, now)

        changed = dict(rows)
        for mid in settled:
            changed[mid] = {**(rows.get(mid) or prev[mid]), "alias_settled": True}
        if not params.dry_run:
            if len(features):
                publish(cs2.UPCOMING_FEATURES, features.assign(generated_at=now))
            if changed:
                publish(cs2.UPCOMING_MATCHES, pd.DataFrame(
                    [{**r, "generated_at": now} for r in changed.values()])[MATCH_COLUMNS])
            if aliases is not None:
                publish(cs2.TEAM_ALIASES, aliases)
        self._publish_run(params, now, True, session.billed - pages_before, session.billed, over_budget, len(rows))

        statuses = pd.Series([r["status"] for r in rows.values()]).value_counts().to_dict() if rows else {}
        return PipelineResult(status="succeeded", outputs={
            "listing_ok": True, "tracked": statuses, "pages_due": len(due), "features": len(features),
            "with_markets": sum(r["markets"] != "[]" for r in rows.values()), "skipped": skipped,
            "aliases_settled": len(settled), "credits": session.billed * CREDITS_PER_PAGE,
            "credits_today": spent + session.billed * CREDITS_PER_PAGE, "over_budget": over_budget,
            "dry_run": params.dry_run,
        })

    @staticmethod
    def _markets(ds: DatasetStore, rows: dict[int, dict], prev: dict[int, dict],
                 now: pd.Timestamp) -> tuple[pd.DataFrame | None, dict[str, int], list[int]]:
        """Resolve markets for upcoming matches that have none, then settle aliases. Aliases are None if untouched."""
        unresolved = [r for r in rows.values()
                      if r["status"] == UPCOMING and r["markets"] == "[]" and not pd.isna(r["team_a_id"])]
        unsettled = {mid: r for mid, r in {**prev, **rows}.items()
                     if not r["alias_settled"] and r["learned"] != "[]"}
        if not unresolved and not unsettled:
            return None, {}, []

        naive = now.tz_localize(None)
        pm = polymarket.fetch_markets(naive - MARKET_LOOKBACK, naive + pd.Timedelta(days=2))
        try:
            aliases = ds.load(cs2.TEAM_ALIASES.name)
        except NoSuchTableError:
            aliases = identity.empty_aliases()

        registry = RegistryClient()
        skipped: dict[str, int] = {}
        for r in unresolved:
            match = {**r, "match_time": r["kickoff"]}
            resolution, legs, reason = resolve_legs(
                match, pm, aliases, lambda cond, tok: polymarket.resolve_listing(cond, tok, registry))
            aliases = identity.learn(aliases, resolution.learned, now)
            if reason:
                skipped[reason] = skipped.get(reason, 0) + 1
                continue
            r.update(series_slug=resolution.market_slug, team_a_outcome=resolution.team_a_outcome,
                     markets=json.dumps(legs), learned=json.dumps([list(x) for x in resolution.learned]))

        settled: list[int] = []
        if unsettled:
            settled, aliases = settle_aliases(unsettled, pm, _series_won(ds.load(cs2.MATCH_HISTORY.name)), aliases)
        return aliases, skipped, settled

    @staticmethod
    def _publish_run(params: CS2UpcomingParams, now: pd.Timestamp, listing_ok: bool, pages: int, billed: int,
                     over_budget: bool, tracked: int) -> None:
        if params.dry_run:
            return
        publish(cs2.UPCOMING_RUNS, [{"generated_at": now, "listing_ok": listing_ok, "pages_fetched": pages,
                                     "credits": billed * CREDITS_PER_PAGE, "over_budget": over_budget,
                                     "tracked": tracked}])
