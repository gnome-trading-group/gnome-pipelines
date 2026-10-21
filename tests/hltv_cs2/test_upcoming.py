"""
Upcoming-match parsing, on pages captured 2026-10-05.

The live predictor reads these pages, so they must produce the same series-level
columns as completed pages - otherwise live features silently differ from the
ones the model was trained on.
"""
import datetime
import gzip
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from gnome_pipelines.hltv_cs2.scraper import (
    _parse_lineups,
    parse_match_detail_html,
    parse_upcoming_listing_html,
    parse_upcoming_match_html,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _soup(name):
    return BeautifulSoup(gzip.open(FIXTURES / f"{name}.html.gz", "rt").read(), "lxml")


@pytest.fixture(scope="module")
def listing():
    return {m["match_id"]: m for m in parse_upcoming_listing_html(
        gzip.open(FIXTURES / "hltv_upcoming_listing.html.gz", "rt").read())}


def test_listing_reads_time_format_stars_and_teams(listing):
    m = listing[2398739]
    assert m["match_time"] == datetime.datetime(2026, 10, 5, 16, 30, tzinfo=datetime.timezone.utc)
    assert (m["bo_type"], m["stars"], m["is_live"]) == (3, 4, False)
    assert (m["team_a_name"], m["team_b_name"]) == ("Vitality", "Falcons")
    assert listing[2399010]["bo_type"] == 1
    assert listing[2398737]["is_live"]


def test_listing_marks_tbd_teams(listing):
    assert listing[2398396]["team_a_name"] is None and listing[2398396]["team_b_name"] is None


def test_upcoming_lineups_are_parsed():
    """Upcoming pages render players as compare widgets, not links; this once yielded no lineup."""
    a, b = _parse_lineups(_soup("hltv_upcoming_bo3"))
    assert [p["player_name"] for p in a] == ["mezii", "apEX", "ropz", "ZywOo", "flameZ"]
    assert [p["player_id"] for p in a if p["is_awp"]] == [11893]
    assert [p["player_name"] for p in b if p["is_igl"]] == ["karrigan"]
    assert len(b) == 5


def test_upcoming_match_has_the_history_series_columns():
    up = parse_upcoming_match_html(_soup("hltv_upcoming_bo3"), 2398739, stars=4)["match"]
    done = parse_match_detail_html(_soup("hltv_match"), 2398816, stars=4)["rows"][0]
    map_level = {"mapstatsid", "map_name", "map_position_in_series", "is_decider", "team_a_series_score",
                 "team_b_series_score", "team_a_score", "team_b_score", "team_a_won", "team_a_picked_map",
                 "team_a_h1_score", "team_b_h1_score", "team_a_started_ct", "team_a_player_ids",
                 "team_a_player_names", "team_b_player_ids", "team_b_player_names", "team_a_avg_rating",
                 "team_b_avg_rating"}
    assert set(up) == set(done) - map_level
    assert (up["team_a_id"], up["team_b_id"], up["bo_type"], up["is_lan"], up["event_tier"]) == (9565, 11283, 3, 1, 4)
    assert (up["team_a_rank"], up["team_b_rank"]) == (2, 5)
    assert up["team_a_awp_id"] == 11893 and len(up["team_b_lineup_ids"]) == 5


def test_veto_is_unknown_before_the_match_and_known_once_live():
    pre = parse_upcoming_match_html(_soup("hltv_upcoming_bo3"), 2398739)
    live = parse_upcoming_match_html(_soup("hltv_upcoming_live_veto"), 2398737)
    assert not pre["veto_known"] and pre["veto"] == []
    assert live["veto_known"] and len(live["veto"]) == 7
    assert pre["h2h"] and pre["recent_form"]


def test_upcoming_match_carries_its_lineup_rows():
    """Stand-in flags feed player skill before kickoff, so the page's lineup box must come through as rows."""
    up = parse_upcoming_match_html(_soup("hltv_upcoming_bo3"), 2398739)
    rows = up["lineups"]
    assert len(rows) == 10 and {r["match_id"] for r in rows} == {2398739}
    assert [r["player_id"] for r in rows[:5]] == up["match"]["team_a_lineup_ids"]
    assert all(isinstance(r["is_standin"], bool) for r in rows)
    assert [r["country"] for r in rows[:3]] == ["United Kingdom", "France", "Estonia"]
    assert (up["match"]["team_a_country"], up["match"]["team_b_country"]) == ("Europe", "Europe")


def test_tbd_match_is_not_priced():
    assert parse_upcoming_match_html(_soup("hltv_upcoming_tbd"), 2398396) is None


def test_unranked_team_keeps_its_id():
    m = parse_upcoming_match_html(_soup("hltv_upcoming_bo1"), 2399010)["match"]
    assert m["team_b_id"] is not None and m["team_b_rank"] is None and m["bo_type"] == 1
