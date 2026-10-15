"""
The cs2_upcoming pipeline's decisions: which matches are tracked, when a page is refetched,
which markets a match trades, and when an alias is settled. The I/O (HLTV, DatasetStore,
registry) lives in the pipeline class.
"""
import json

import pandas as pd
import pytest

from gnome_pipelines.cs2_upcoming import pipeline as up
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
    state[2398739].update(team_a_id=9565, markets='[{"market": "series"}]', features_at=NOW)
    nxt = up.track(state, [listed()], NOW + minutes(15), WINDOW)
    assert nxt[2398739]["team_a_id"] == 9565 and nxt[2398739]["features_at"] == NOW
    assert nxt[2398739]["markets"] == '[{"market": "series"}]', "markets are fixed once found"


def _pm():
    rows = [("cs2-vit-fal-2026-10-05", ["Vitality", "Falcons"], ["s_vit", "s_fal"]),
            ("cs2-vit-fal-2026-10-05-game1", ["Falcons", "Vitality"], ["g_fal", "g_vit"])]
    return index_markets(pd.DataFrame([{"market_slug": s, "condition_id": "0x" + s, "outcomes": o, "tokens": t,
                                        "final": [], "closed": False, "kickoff": KICK} for s, o, t in rows]))


def test_map1_tokens_are_oriented_by_name_not_position():
    ids = {"s_vit": 1, "s_fal": 2, "g_vit": 3, "g_fal": 4}
    res, legs, reason = up.resolve_legs(MATCH, _pm(), identity.empty_aliases(), lambda c, t: ids[t])
    assert reason is None and res.market_slug == "cs2-vit-fal-2026-10-05"
    assert legs == [{"market": "series", "listing_team_a": 1, "listing_team_b": 2},
                    {"market": "game1", "listing_team_a": 3, "listing_team_b": 4}]


def test_no_legs_without_a_registered_series_market():
    res, legs, reason = up.resolve_legs(MATCH, _pm(), identity.empty_aliases(), lambda c, t: None)
    assert legs == [] and reason == "series market not in registry"
    only_map = {"g_vit": 3, "g_fal": 4}
    _, legs, reason = up.resolve_legs(MATCH, _pm(), identity.empty_aliases(), lambda c, t: only_map.get(t))
    assert legs == [] and reason == "series market not in registry"


def _settling_row(team_a_outcome):
    return {**up.new_row(listed()), "series_slug": "cs2-vit-fal-2026-10-05", "team_a_outcome": team_a_outcome,
            "learned": json.dumps([["vitalityx", 9565]])}


def _settled_pm():
    pm = _pm()
    pm.loc[pm.market_slug == "cs2-vit-fal-2026-10-05", "final"] = pd.Series([["0", "1"]], index=pm.index[:1])
    return pm


def test_settlement_confirms_learned_aliases():
    aliases = identity.learn(identity.empty_aliases(), (("vitalityx", 9565),), NOW)
    settled, aliases = up.settle_aliases({2398739: _settling_row(1)}, _settled_pm(), pd.Series({2398739: 1}), aliases)
    assert settled == [2398739] and aliases.evidence_count.iloc[0] == 1


def test_a_settled_match_counts_once():
    aliases = identity.learn(identity.empty_aliases(), (("vitalityx", 9565),), NOW)
    row = {**_settling_row(1), "alias_settled": True}
    settled, aliases = up.settle_aliases({2398739: row}, _settled_pm(), pd.Series({2398739: 1}), aliases)
    assert settled == [] and aliases.evidence_count.iloc[0] == 0


def test_settlement_disagreement_rejects_the_alias():
    aliases = identity.learn(identity.empty_aliases(), (("vitalityx", 9565),), NOW)
    _, aliases = up.settle_aliases({2398739: _settling_row(0)}, _settled_pm(), pd.Series({2398739: 1}), aliases)
    assert aliases.status.tolist() == [identity.REJECTED]


def test_latest_state_is_the_newest_row_per_match():
    rows = pd.DataFrame([{**up.new_row(listed()), "status": s, "generated_at": NOW + minutes(i)}
                         for i, s in enumerate([up.UPCOMING, up.LIVE])])
    assert up.latest_state(rows)[2398739]["status"] == up.LIVE
