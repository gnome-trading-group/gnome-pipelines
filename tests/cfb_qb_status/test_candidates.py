from datetime import datetime, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.candidates import (HOLD_RATE, KEEP_RATE, RETURN_RATE, candidates, game_starters,
                                                      next_game, with_roster_ids)

UTC = timezone.utc
GAMES = pd.DataFrame([
    {"game_id": 1, "season": 2026, "kickoff": pd.Timestamp("2026-10-03T19:00Z"), "home_id": 344, "away_id": 5,
     "completed": True},
    {"game_id": 2, "season": 2026, "kickoff": pd.Timestamp("2026-10-10T19:00Z"), "home_id": 6, "away_id": 344,
     "completed": True},
    {"game_id": 3, "season": 2026, "kickoff": pd.Timestamp("2026-10-17T23:00Z"), "home_id": 99, "away_id": 344,
     "completed": True},
    {"game_id": 4, "season": 2026, "kickoff": pd.Timestamp("2026-10-24T19:30Z"), "home_id": 344, "away_id": 201,
     "completed": False},
])


def plays(game_id, passer_key, n, start=1):
    return [{"season": 2026, "game_id": game_id, "offense": "Mississippi State", "passer_key": passer_key,
             "play_seq": start + i} for i in range(n)]


PASSERS = pd.DataFrame(plays(1, "T", 40) + plays(1, "C", 10, start=100) + plays(2, "T", 35)
                       + plays(3, "P", 29) + plays(3, "T", 0))


def test_game_starters_takes_first_passer():
    s = game_starters(PASSERS)
    assert dict(zip(s["game_id"], s["passer_key"])) == {1: "T", 2: "T", 3: "P"}


def test_after_a_change_the_new_starter_and_the_season_starter_split_by_measured_rates():
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), PASSERS, GAMES)
    base = dict(zip(c["player_id"], c["base"]))
    assert base["P"] == pytest.approx(HOLD_RATE[1])
    assert base["T"] == pytest.approx(RETURN_RATE[1])
    assert base["C"] == pytest.approx(1 - HOLD_RATE[1] - RETURN_RATE[1])
    assert c.set_index("player_id").loc["T", "missed"] == 1


def test_an_established_starter_keeps_more_the_longer_his_streak():
    passers = pd.DataFrame(plays(1, "T", 40) + plays(1, "C", 10, start=100) + plays(2, "T", 35) + plays(3, "T", 30))
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), passers, GAMES)
    base = dict(zip(c["player_id"], c["base"]))
    assert base["T"] == pytest.approx(KEEP_RATE[3])
    assert base["C"] == pytest.approx(1 - KEEP_RATE[3])
    assert c.set_index("player_id").loc["T", "streak"] == 3
    assert KEEP_RATE[1] < KEEP_RATE[3] < KEEP_RATE[8]


def test_games_inside_24h_are_ignored():
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 18, 12, tzinfo=UTC), PASSERS, GAMES)
    assert dict(zip(c["player_id"], c["last_starter"]))["T"]


def test_starter_change_flags_last_vs_season():
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), PASSERS, GAMES)
    assert c.loc[c.player_id == "P", "last_starter"].item() and not c.loc[c.player_id == "P", "season_starter"].item()
    assert c.loc[c.player_id == "T", "season_starter"].item()


def test_single_qb_gets_one():
    only = PASSERS[PASSERS.passer_key == "T"]
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), only, GAMES)
    assert c["base"].tolist() == [1.0]


def test_next_game_skips_completed():
    assert next_game(344, datetime(2026, 10, 21, tzinfo=UTC), GAMES) == 4
    assert next_game(344, datetime(2026, 10, 30, tzinfo=UTC), GAMES) is None


def test_with_roster_ids_maps_keys_and_keeps_unlinked():
    class Index:
        def player_for_key(self, team_id, key):
            return {"b.rowe": "9000002"}.get(key)
    cands = pd.DataFrame([{"player_id": "b.rowe", "passer_key": "b.rowe", "base": 0.8, "last_starter": True,
                           "season_starter": False},
                          {"player_id": "x.walkon", "passer_key": "x.walkon", "base": 0.2, "last_starter": False,
                           "season_starter": True}])
    out = with_roster_ids(cands, 344, Index())
    assert out["player_id"].tolist() == ["9000002", "x.walkon"]
    assert out["passer_key"].tolist() == ["b.rowe", "x.walkon"]


def test_non_qb_passers_and_unlinked_junk_are_left_out():
    passers = pd.DataFrame(plays(1, "T", 40) + plays(2, "T", 35) + plays(3, "T", 30)
                           + plays(3, "WR", 1, start=200) + plays(3, "junk", 1, start=300)
                           + plays(2, "new", 12, start=400))
    eligible = {"T": True, "WR": False}.get
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), passers, GAMES,
                   eligible=eligible)
    assert set(c["passer_key"]) == {"T", "new"}
    assert c.loc[c["passer_key"] == "T", "base"].item() == pytest.approx(KEEP_RATE[3])


def test_changed_flags_a_new_starter_in_the_last_game_or_a_season_starter_out_of_the_job():
    at = datetime(2026, 10, 21, tzinfo=UTC)
    assert candidates("Mississippi State", 344, 2026, at, PASSERS, GAMES)["changed"].all()
    steady = pd.DataFrame(plays(1, "T", 40) + plays(2, "T", 35) + plays(3, "T", 30))
    assert not candidates("Mississippi State", 344, 2026, at, steady, GAMES)["changed"].any()
    swap = pd.DataFrame(plays(2, "T", 35) + plays(3, "P", 30))
    assert candidates("Mississippi State", 344, 2026, at, swap, GAMES)["changed"].all()


def test_every_former_starter_carries_his_starts_and_games_since_his_last_start():
    swap = pd.DataFrame(plays(1, "T", 40) + plays(2, "P", 35) + plays(3, "P", 30))
    c = candidates("Mississippi State", 344, 2026, datetime(2026, 10, 21, tzinfo=UTC), swap, GAMES).set_index("player_id")
    assert (c.loc["T", "starts"], c.loc["T", "missed"]) == (1, 2)
    assert (c.loc["P", "starts"], c.loc["P", "missed"]) == (2, 0)
