"""
The cs2_upcoming pipeline's decisions: which matches are tracked, when a page is refetched,
which markets a match trades on each venue, and when an alias is settled. The I/O (DatasetStore, registry)
lives in the pipeline class; HLTV pages come through a page source.
"""
import asyncio
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from gnome_pipelines.cs2_upcoming import pipeline as up
from gnome_pipelines.cs2_upcoming.venues import PolymarketIntl
from gnome_pipelines.hltv_cs2.fetch import run_with_source
from gnomepy_research.models.cs2.prematch import identity
from gnomepy_research.models.cs2.prematch.polymarket import index_markets

NOW = pd.Timestamp("2026-10-05 06:00", tz="UTC")
KICK = pd.Timestamp("2026-10-05 16:30", tz="UTC")
WINDOW = pd.Timedelta(hours=12)
BANDS = [(12, 120), (2, 20), (1, 15)]
MATCH = {"match_id": 2398739, "team_a_id": 9565, "team_a_name": "Vitality", "team_b_id": 11283,
         "team_b_name": "Falcons", "match_time": KICK}


def listed(match_id=2398739, kickoff=KICK, live=False, bo_type=3, team_b="Falcons"):
    return {"match_id": match_id, "match_time": kickoff, "stars": 1, "event_id": 1, "bo_type": bo_type,
            "is_live": live, "team_a_name": "Vitality", "team_b_name": team_b}


def minutes(m):
    return pd.Timedelta(minutes=m)


@pytest.mark.parametrize("to_kickoff,expected", [(11, 120), (2.5, 120), (2, 20), (1.5, 20), (1, 15), (0.2, 15), (30, 120)])
def test_the_tightest_band_that_applies_sets_the_interval(to_kickoff, expected):
    assert up.refetch_interval(pd.Timedelta(hours=to_kickoff), BANDS, over_budget=False) == minutes(expected)


def test_over_budget_every_match_uses_the_slowest_interval():
    assert up.refetch_interval(minutes(10), BANDS, over_budget=True) == minutes(120)


def _row(**kw):
    return {**up.new_row(listed()), **kw}


def test_a_page_is_due_on_first_sight_then_on_its_band():
    assert up.page_due(_row(), NOW, BANDS, False)
    fetched = NOW - minutes(100)
    assert not up.page_due(_row(features_at=fetched), NOW, BANDS, False)
    near = KICK - minutes(30)
    assert up.page_due(_row(features_at=near - minutes(13)), near, BANDS, False), "15 min band, with run slack"
    assert not up.page_due(_row(features_at=near - minutes(10)), near, BANDS, False)


def test_a_live_match_page_is_never_refetched():
    assert not up.page_due(_row(status=up.LIVE), NOW, BANDS, False)


def test_tracking_starts_inside_the_window_for_named_bo3s_only():
    listing = [listed(1, KICK), listed(2, NOW + pd.Timedelta(hours=13)), listed(3, KICK, bo_type=1),
               listed(4, KICK, team_b=None), listed(5, KICK, live=True), listed(6, NOW - minutes(5))]
    assert set(up.track({}, listing, NOW, WINDOW)) == {1}


def test_kickoff_and_status_follow_the_listing():
    first = up.track({}, [listed()], NOW, WINDOW)
    moved = up.track(first, [listed(kickoff=KICK + pd.Timedelta(hours=2))], NOW + minutes(15), WINDOW)
    assert moved[2398739]["kickoff"] == KICK + pd.Timedelta(hours=2)
    live = up.track(moved, [listed(live=True)], KICK, WINDOW)
    assert live[2398739]["status"] == up.LIVE


def test_a_match_turns_missing_only_after_consecutive_absences_then_drops_out():
    state = up.track({}, [listed()], NOW, WINDOW)
    once = up.track(state, [], NOW + minutes(15), WINDOW)
    assert once[2398739]["status"] == up.UPCOMING and once[2398739]["missed_listings"] == 1
    back = up.track(once, [listed()], NOW + minutes(30), WINDOW)
    assert back[2398739]["missed_listings"] == 0, "one bad parse must not end a match"
    gone = up.track(up.track(back, [], NOW + minutes(45), WINDOW), [], NOW + minutes(60), WINDOW)
    assert gone[2398739]["status"] == up.MISSING
    assert up.track(gone, [], NOW + minutes(75), WINDOW) == {}


def test_tracked_state_survives_a_run():
    state = up.track({}, [listed()], NOW, WINDOW)
    state[2398739].update(team_a_id=9565, markets='[{"venue": "kalshi", "market": "series"}]', features_at=NOW)
    nxt = up.track(state, [listed()], NOW + minutes(15), WINDOW)
    assert nxt[2398739]["team_a_id"] == 9565 and nxt[2398739]["features_at"] == NOW
    assert nxt[2398739]["markets"] == '[{"venue": "kalshi", "market": "series"}]', "markets are fixed once found"


def _frame(o1="Falcons"):
    rows = [("cs2-vit-fal-2026-10-05", ["Vitality", o1], ["s_vit", "s_fal"]),
            ("cs2-vit-fal-2026-10-05-game1", [o1, "Vitality"], ["g_fal", "g_vit"])]
    return PolymarketIntl.to_frame(index_markets(pd.DataFrame([
        {"market_slug": s, "condition_id": "0x", "outcomes": o, "tokens": t, "final": [], "closed": False, "kickoff": KICK}
        for s, o, t in rows])))


def test_map1_is_oriented_by_name_not_position():
    res, state = up.match_venue(MATCH, _frame(), identity.empty_aliases(), "polymarket")
    assert res.market_slug == "cs2-vit-fal-2026-10-05" and state["team_a_outcome"] == 0
    assert state["markets"] == [
        {"market": "series", "instrument": "s_vit", "esid_a": "0x:s_vit", "esid_b": "0x:s_fal"},
        {"market": "game1", "instrument": "g_vit", "esid_a": "0x:g_vit", "esid_b": "0x:g_fal"}]


def test_legs_need_both_outcomes_of_the_series_registered():
    _, state = up.match_venue(MATCH, _frame(), identity.empty_aliases(), "kalshi")
    ids = {"0x:s_vit": 1, "0x:s_fal": 2, "0x:g_vit": 3, "0x:g_fal": 4}
    assert up.venue_legs("kalshi", state, ids.get) == [
        {"venue": "kalshi", "market": "series", "listing_team_a": 1, "listing_team_b": 2},
        {"venue": "kalshi", "market": "game1", "listing_team_a": 3, "listing_team_b": 4}]
    only_map = {"0x:g_vit": 3, "0x:g_fal": 4}
    assert up.venue_legs("kalshi", state, only_map.get) == []
    half_series = {"0x:s_vit": 1, **only_map}
    assert up.venue_legs("kalshi", state, half_series.get) == []


def _upcoming_row():
    return {**up.new_row(listed()), "team_a_id": 9565, "team_b_id": 11283}


def test_a_venue_match_is_kept_while_its_registry_lookup_is_retried():
    row, aliases = _upcoming_row(), identity.empty_aliases()
    aliases, reason = up._match_on("kalshi", row, _frame(), aliases, lambda esid: None, NOW)
    assert reason == "series market not in registry" and row["markets"] == "[]"
    assert json.loads(row["venues"])["kalshi"]["series"] == "cs2-vit-fal-2026-10-05", "books start before registration"
    nothing = PolymarketIntl.to_frame(index_markets(pd.DataFrame(
        columns=["market_slug", "condition_id", "outcomes", "tokens", "final", "closed", "kickoff"])))
    aliases, reason = up._match_on("kalshi", row, nothing, aliases, lambda esid: 7, NOW)
    assert reason is None, "the stored match is used, not re-resolved against this run's listing"
    assert {leg["venue"] for leg in json.loads(row["markets"])} == {"kalshi"}


def test_each_venue_is_matched_and_legged_on_its_own():
    row, aliases = _upcoming_row(), identity.empty_aliases()
    aliases, _ = up._match_on("kalshi", row, _frame(), aliases, lambda esid: 1, NOW)
    aliases, reason = up._match_on("polymarket_us", row, _frame(), aliases, lambda esid: None, NOW)
    assert reason == "series market not in registry"
    assert set(json.loads(row["venues"])) == {"kalshi", "polymarket_us"}
    assert {leg["venue"] for leg in json.loads(row["markets"])} == {"kalshi"}


def test_aliases_are_learned_per_venue():
    row = _upcoming_row()
    aliases, _ = up._match_on("polymarket_us", row, _frame(o1="Falkons"), identity.empty_aliases(),
                              lambda esid: None, NOW)
    assert aliases[["source", "hltv_team_id"]].values.tolist() == [["polymarket_us", 11283]]


def test_book_row_reads_the_touch_and_depth_near_it():
    ladder = ({0.01: 500, 0.45: 7, 0.48: 3000, 0.5: 10}, {0.99: 900, 0.81: 5, 0.56: 4, 0.55: 20}, KICK)
    row = up.book_row(2398739, "kalshi", "game1", "g_vit", ladder)
    assert (row["venue"], row["instrument"]) == ("kalshi", "g_vit")
    assert (row["best_bid"], row["bid_size"], row["best_ask"], row["ask_size"]) == (0.5, 10, 0.55, 20)
    assert row["bid_size_5c"] == 3017 and row["ask_size_5c"] == 24 and row["book_at"] == KICK


def test_an_empty_side_has_no_touch_and_no_depth():
    row = up.book_row(1, "polymarket", "series", "t", ({}, {0.6: 5}, pd.NaT))
    assert row["best_bid"] is None and row["bid_size"] is None and row["bid_size_5c"] == 0.0


def _learned_row(venue, team_a_outcome, settled=False):
    state = {"series": "S", "team_a_outcome": team_a_outcome, "markets": [],
             "learned": [["vitalityx", 9565]], "alias_settled": settled}
    return {**up.new_row(listed()), "venues": json.dumps({venue: state})}


def _learned(venue):
    return identity.learn(identity.empty_aliases(), (("vitalityx", 9565),), NOW, source=venue)


WON = pd.Series({2398739: 1})


def test_settlement_confirms_learned_aliases_on_that_venue():
    changed, aliases = up.settle_aliases({2398739: _learned_row("kalshi", 1)}, "kalshi", {"S": [0.0, 1.0]}, WON,
                                         _learned("kalshi"))
    assert aliases.evidence_count.iloc[0] == 1
    assert json.loads(changed[2398739]["venues"])["kalshi"]["alias_settled"]


def test_a_settled_match_counts_once():
    changed, aliases = up.settle_aliases({2398739: _learned_row("kalshi", 1, settled=True)}, "kalshi",
                                         {"S": [0.0, 1.0]}, WON, _learned("kalshi"))
    assert changed == {} and aliases.evidence_count.iloc[0] == 0


def test_settlement_disagreement_rejects_the_alias():
    _, aliases = up.settle_aliases({2398739: _learned_row("kalshi", 0)}, "kalshi", {"S": [0.0, 1.0]}, WON,
                                   _learned("kalshi"))
    assert aliases.status.tolist() == [identity.REJECTED]


def test_an_undecided_market_settles_nothing():
    changed, aliases = up.settle_aliases({2398739: _learned_row("kalshi", 1)}, "kalshi", {}, WON, _learned("kalshi"))
    assert changed == {} and aliases.evidence_count.iloc[0] == 0


def test_latest_state_is_the_newest_row_per_match():
    rows = pd.DataFrame([{**up.new_row(listed()), "status": s, "generated_at": NOW + minutes(i)}
                         for i, s in enumerate([up.UPCOMING, up.LIVE])])
    assert up.latest_state(rows)[2398739]["status"] == up.LIVE


FIXTURES = Path(__file__).parent / "hltv_cs2" / "fixtures"


class _Source:
    """The 2026-10-05 listing and one BO3 page for every match, counting fetches in flight."""

    concurrency = 2
    can_be_blocked = True
    billed = 0

    def __init__(self, listing=True):
        self.listing = gzip.open(FIXTURES / "hltv_upcoming_listing.html.gz", "rt").read() if listing else None
        self.page = gzip.open(FIXTURES / "hltv_upcoming_bo3.html.gz", "rt").read()
        self.in_flight = self.most_in_flight = 0
        self.closed_on_loop = False

    async def html(self, url, *, wait_selector=None, wait_count=1):
        if url.endswith("/matches"):
            return self.listing
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.in_flight -= 1
        return self.page

    async def close(self):
        self.closed_on_loop = asyncio.get_running_loop().is_running()


def test_due_pages_are_fetched_in_batches_of_the_source_concurrency():
    source = _Source()
    params = up.CS2UpcomingParams(window_hours=24)
    rows, pages = run_with_source(source, lambda s: up.fetch_due(s, {}, NOW, params, over_budget=False))
    assert 2398739 in rows and set(pages) == set(rows), "every newly tracked match is due"
    assert len(pages) > source.concurrency and source.most_in_flight == source.concurrency
    assert source.closed_on_loop


def test_no_listing_means_no_state_change():
    source = _Source(listing=False)
    params = up.CS2UpcomingParams()
    assert run_with_source(source, lambda s: up.fetch_due(s, {}, NOW, params, over_budget=False)) is None
    assert source.closed_on_loop


def test_chrome_is_refused_in_the_task(monkeypatch):
    monkeypatch.setattr(up, "environment", lambda: type("E", (), {"in_task": True})())
    with pytest.raises(ValueError, match="run it locally"):
        up.CS2UpcomingPipeline().run(up.CS2UpcomingParams(fetcher="chrome"))
