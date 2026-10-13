"""
Golden tests for the data the parser used to discard.

Written against cached fixtures so they run with no network and no browser —
which is the whole point of the page cache: parser changes are verified offline
and applied later with a local reparse.
"""
import gzip
import pathlib

import pytest
from bs4 import BeautifulSoup

from gnome_pipelines.hltv_cs2.scraper import (
    _STAT_SIDES,
    _parse_player_stats,
    parse_match_detail_html,
    _parse_veto,
    _parse_lineups,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def load(name):
    return BeautifulSoup(gzip.open(FIXTURES / f"{name}.html.gz", "rt", errors="ignore").read(), "lxml")


def parsed(name, match_id=1):
    return parse_match_detail_html(load(name), match_id, stars=0)


# --- VRS rank: fills rank_diff where HLTV rank is absent ---

def test_vrs_rank_is_captured():
    row = parsed("hltv_january")["rows"][0]
    assert row["team_a_vrs_rank"] == 11 and row["team_b_vrs_rank"] == 10


def test_vrs_present_where_hltv_rank_is_missing():
    """The exact case that made rank_diff 19.4% NaN, concentrated on lower-tier teams."""
    div = load("hltv_forfeit").select(".teamRanking")[0]
    assert div.select_one("a.hltv-ranking") is None, "fixture should have no HLTV rank here"
    assert div.select_one("a.vrs-ranking") is not None, "but it does have VRS"


# --- match_time: was truncated to a date ---

def test_match_time_keeps_the_clock():
    row = parsed("hltv_mid")["rows"][0]
    assert row["match_time"].hour == 9 and row["match_time"].minute == 0
    assert row["match_date"] == row["match_time"].date()


def test_match_times_differ_across_matches():
    a = parsed("hltv_january")["rows"][0]["match_time"]
    b = parsed("hltv_recent")["rows"][0]["match_time"]
    assert a.hour != b.hour, "a date-truncated field would make these equal"


# --- per-player stats, all three sides ---

def test_all_three_sides_are_parsed():
    players = parsed("hltv_mid")["players"]
    by_side = {s: [p for p in players if p["side"] == s] for s in _STAT_SIDES}
    assert set(by_side) == {"all", "ct", "t"}
    assert all(len(v) == 30 for v in by_side.values()), {k: len(v) for k, v in by_side.items()}


def test_every_discarded_column_is_now_present():
    players = parsed("hltv_mid")["players"]
    for field in ("kills", "deaths", "ek", "ed", "round_swing_pct", "adr", "eadr", "kast", "ekast", "rating"):
        missing = [p for p in players if p[field] is None]
        assert not missing, f"{field} missing on {len(missing)} rows"


def test_round_swing_is_the_signed_win_probability_delta():
    """roundSwing is HLTV's own per-round win-probability change — it must keep its sign."""
    players = [p for p in parsed("hltv_mid")["players"] if p["side"] == "all"]
    assert any(p["round_swing_pct"] > 0 for p in players)
    assert any(p["round_swing_pct"] < 0 for p in players)


def test_ct_and_t_split_differ_from_overall():
    players = parsed("hltv_mid")["players"]
    pid = players[0]["player_id"]
    by_side = {p["side"]: p for p in players if p["player_id"] == pid and p["mapstatsid"] == players[0]["mapstatsid"]}
    assert by_side["ct"]["kills"] + by_side["t"]["kills"] == by_side["all"]["kills"], \
        "CT and T kills must reconcile with the overall figure"


def test_players_are_keyed_on_team_id_not_name():
    players = parsed("hltv_mid")["players"]
    assert all(p["team_id"] is not None for p in players)
    assert len({p["team_id"] for p in players}) == 2


def test_is_team_a_is_assigned():
    players = [p for p in parsed("hltv_mid")["players"] if p["side"] == "all"]
    assert {p["is_team_a"] for p in players} == {True, False}


# --- the parser must stay well-behaved on the awkward pages ---

def test_substitute_page_yields_an_extra_player():
    players = [p for p in parsed("hltv_sub")["players"] if p["side"] == "all"]
    assert len(players) == 31, "a six-man roster on one map should show up, not be dropped"


def test_forfeit_page_still_returns_none():
    assert parsed("hltv_forfeit") is None


def test_bo1_page_has_one_map_and_one_stat_block():
    result = parsed("hltv_match")
    assert len(result["rows"]) == 1
    assert len([p for p in result["players"] if p["side"] == "all"]) == 10


def test_result_shape_is_additive():
    """Existing consumers read result['rows'] and result['demo_url'] — both must survive."""
    result = parsed("hltv_mid")
    assert set(result) >= {"rows", "players", "demo_url"}


# --- veto: order, bans, and exact team attribution ---

def test_full_veto_sequence_with_bans_and_order():
    """Only 'picked' lines used to survive; bans and ordering were discarded."""
    veto = parsed("hltv_january")["veto"]
    assert [v["order"] for v in veto] == [1, 2, 3, 4, 5, 6, 7]
    assert {v["action"] for v in veto} == {"removed", "picked", "left_over"}
    assert sum(1 for v in veto if v["action"] == "removed") == 4


def test_leftover_map_has_no_picker():
    left = [v for v in parsed("hltv_january")["veto"] if v["action"] == "left_over"]
    assert len(left) == 1
    assert left[0]["team_name"] is None and left[0]["map_name"] == "de_mirage"


def test_veto_steps_carry_team_id():
    veto = [v for v in parsed("hltv_january")["veto"] if v["action"] != "left_over"]
    assert all(v["team_id"] is not None for v in veto), "picker must resolve to a stable id"


def test_picked_map_is_attributed_to_the_right_side():
    rows = {r["map_name"]: r for r in parsed("hltv_january")["rows"]}
    # G2 picked dust2, The MongolZ (team_a) picked ancient
    assert rows["de_dust2"]["team_a_picked_map"] == 0.0
    assert rows["de_ancient"]["team_a_picked_map"] == 1.0


def test_exact_team_match_not_substring():
    """
    The old code tested `team_name.lower() in text`, so a team called "G2" matched
    inside "G2 Ares". Attribution must be exact.
    """
    soup = load("hltv_january")
    steps = _parse_veto(soup, ["G2 Ares", "Nobody"], [1, 2])
    assert all(v["team_id"] is None for v in steps if v["action"] != "left_over"), \
        "a merely-overlapping name must not be credited with the pick"


# --- lineups: announced pre-match, with roles ---

def test_lineups_are_five_per_team():
    row = parsed("hltv_mid")["rows"][0]
    assert len(row["team_a_lineup_ids"]) == 5
    assert len(row["team_b_lineup_ids"]) == 5


def test_awp_role_is_detected():
    row = parsed("hltv_mid")["rows"][0]
    assert row["team_a_awp_id"] is not None and row["team_b_awp_id"] is not None


def test_lineup_names_and_roles_resolve():
    teams = _parse_lineups(load("hltv_mid"))
    names = {p["player_name"] for t in teams for p in t}
    assert "FalleN" in names and "torzsi" in names, names
    igls = {p["player_name"] for t in teams for p in t if p["is_igl"]}
    assert "FalleN" in igls, igls


# --- head-to-head: our own h2h_win_rate is 72% NaN on a 9-month window ---

def test_h2h_aggregate_is_captured():
    row = parsed("hltv_january")["rows"][0]
    assert (row["h2h_team_a_wins"], row["h2h_overtimes"], row["h2h_team_b_wins"]) == (10, 2, 4)


def test_h2h_listing_reaches_outside_our_history_window():
    """The whole point: HLTV remembers meetings our 2026-only history cannot."""
    meetings = parsed("hltv_january")["h2h"]
    assert len(meetings) == 14
    assert min(m["h2h_date"] for m in meetings).year < 2026


def test_h2h_meetings_carry_real_timestamps_not_relative_labels():
    meetings = parsed("hltv_january")["h2h"]
    assert all(m["h2h_date"] is not None for m in meetings)


def test_h2h_winner_flag_agrees_with_the_score():
    for m in parsed("hltv_january")["h2h"]:
        if m["team1_score"] is None or m["team1_score"] == m["team2_score"]:
            continue
        assert m["team1_won"] == (m["team1_score"] > m["team2_score"]), m


def test_never_met_is_zeros_not_missing():
    """'These teams have never played' is a regime signal the NaN used to destroy."""
    result = parsed("hltv_match")
    assert result["rows"][0]["h2h_team_a_wins"] == 0
    assert result["h2h"] == []


# --- recent form + event context ---

def test_recent_form_skips_the_duplicate_tables():
    """HLTV renders four tables; 2-3 are byte-identical copies of 0-1."""
    form = parsed("hltv_mid")["recent_form"]
    assert set(f["team_index"] for f in form) == {0, 1}
    assert len(form) < 50, "reading all four tables would double-count"


def test_recent_form_prefers_a_real_match_id_over_weeks_ago():
    """'16 weeks ago' is relative to fetch time, so it cannot anchor a cached page."""
    form = parsed("hltv_mid")["recent_form"]
    assert sum(1 for f in form if f["ref_match_id"] is not None) > len(form) * 0.8


def test_recent_form_carries_opponent_and_score():
    f = parsed("hltv_mid")["recent_form"][0]
    assert f["opponent_name"] and "weeks ago" not in f["opponent_name"]
    assert f["score_for"] is not None and f["score_against"] is not None


def test_event_context_reads_the_stage():
    assert parsed("hltv_recent")["rows"][0]["is_elimination"] == 1.0
    assert parsed("hltv_mid")["rows"][0]["is_group"] == 1.0
    assert parsed("hltv_january")["rows"][0]["is_playoff"] == 1.0


def test_stage_text_excludes_the_substitution_footnote():
    """The blurb can carry a '** X substitutes Y' line; the stage is the single-star one."""
    stage = parsed("hltv_sub")["rows"][0]["stage_text"]
    assert "substitut" not in stage.lower(), stage
