from datetime import date, datetime, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.availability import FeedError, fetch_conference, find_game, report_evidence
from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex, TeamIndex

SEEN = datetime(2026, 10, 21, 22, 13, tzinfo=timezone.utc)
LATER = datetime(2026, 10, 22, 1, 0, tzinfo=timezone.utc)

GAMES = pd.DataFrame([
    {"game_id": 7, "season": 2026, "week": 7, "kickoff": pd.Timestamp("2026-10-17T23:00Z"),
     "home_team": "LSU", "home_id": 99, "away_team": "Mississippi State", "away_id": 344},
    {"game_id": 8, "season": 2026, "week": 8, "kickoff": pd.Timestamp("2026-10-24T19:30Z"),
     "home_team": "Mississippi State", "home_id": 344, "away_team": "Oklahoma", "away_id": 201},
])
ROSTERS = pd.DataFrame([
    {"season": 2026, "player_id": "9000001", "first_name": "Avery", "last_name": "Stone", "team": "Mississippi State",
     "position": "QB", "jersey": 1.0, "team_id": 344},
    {"season": 2026, "player_id": "777", "first_name": "Wide", "last_name": "Out", "team": "Mississippi State",
     "position": "WR", "jersey": 9.0, "team_id": 344},
])
PASSERS = pd.DataFrame(columns=["season", "game_id", "offense", "passer", "passer_key", "play_seq"])


def row(**over):
    base = {"Game Day": "Out", "Initial": "Questionable", "Number": "#1", "Opponent": "at LSU",
            "OpponentDisplay": "at LSU", "Player": "Avery Stone", "Team": "Mississippi St.",
            "TeamDisplay": "Mississippi State", "Update 1": "Questionable", "Update 2": "Doubtful",
            "Week": "Sat, 17 Oct 2026 00:00:00 GMT", "WeekNum": 7}
    base.update(over)
    return base


def payload(*rows):
    return {"data": list(rows), "report_days": ["Initial", "Update 1", "Update 2", "Game Day"]}


def run(p, seen=SEEN):
    return report_evidence("SEC", p, seen, TeamIndex(GAMES), {2026: PlayerIndex(ROSTERS, PASSERS, 2026)}, GAMES)


def test_each_filled_slot_becomes_one_evidence():
    ev = run(payload(row()))
    assert [e.report_slot for e in ev] == ["Initial", "Update 1", "Update 2", "Game Day"]
    assert [e.status for e in ev] == ["questionable", "questionable", "doubtful", "out"]
    assert all(e.game_id == 7 and e.player_id == "9000001" and e.review_state == "auto" for e in ev)
    assert all(e.first_seen_at == SEEN and e.source_type == "official_report" for e in ev)


def test_same_payload_twice_gives_identical_ids():
    assert [e.evidence_id for e in run(payload(row()))] == [e.evidence_id for e in run(payload(row()), LATER)]


def test_changed_status_gives_new_id():
    a = {e.report_slot: e.evidence_id for e in run(payload(row()))}
    b = {e.report_slot: e.evidence_id for e in run(payload(row(**{"Update 2": "Questionable"})))}
    assert a["Update 2"] != b["Update 2"] and a["Initial"] == b["Initial"]


def test_non_qb_rows_are_skipped():
    assert run(payload(row(Player="Wide Out", Number="#9"))) == []


def test_unmatched_team_is_kept_pending():
    ev = run(payload(row(Team="Northern Nowhere", TeamDisplay="Northern Nowhere")))
    assert ev and all(e.team_id is None and e.review_state == "pending" for e in ev)


def test_game_found_across_midnight_utc():
    teams = TeamIndex(GAMES)
    assert find_game(GAMES, 344, "at LSU", date(2026, 10, 17), teams) == 7


def test_missing_game_is_null_and_pending():
    ev = run(payload(row(Week="Sat, 31 Oct 2026 00:00:00 GMT", WeekNum=9)))
    assert ev and all(e.game_id is None and e.review_state == "pending" for e in ev)


def test_empty_slots_and_unknown_words_give_no_evidence():
    ev = run(payload(row(**{"Initial": "", "Update 1": "TBD", "Update 2": "", "Game Day": "Out"})))
    assert [e.report_slot for e in ev] == ["Game Day"]


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, response):
        self.response, self.calls = response, []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append((url, json))
        return self.response


def test_malformed_payload_raises_feed_error():
    with pytest.raises(FeedError):
        fetch_conference(FakeSession(FakeResponse(200, {"error": "nope"})), "SEC")
    with pytest.raises(FeedError):
        fetch_conference(FakeSession(FakeResponse(500, {})), "SEC")


def test_fetch_sends_the_documented_body():
    s = FakeSession(FakeResponse(200, payload(row())))
    assert fetch_conference(s, "B10")["data"]
    assert s.calls[0][1] == {"sport": "Football", "organization": "B10", "conference": "B10"}
