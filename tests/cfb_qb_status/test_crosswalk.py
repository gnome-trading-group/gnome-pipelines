import pandas as pd

from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex, TeamIndex, norm_team, players_frame
from gnome_pipelines.datasets.qb_status import PLAYERS

GAMES = pd.DataFrame([
    {"season": 2026, "home_team": "Mississippi State", "home_id": 344, "away_team": "Oklahoma", "away_id": 201},
    {"season": 2026, "home_team": "Miami", "home_id": 2390, "away_team": "UConn", "away_id": 41},
    {"season": 2026, "home_team": "Miami (OH)", "home_id": 193, "away_team": "Ohio", "away_id": 195},
])

ROSTERS = pd.DataFrame([
    {"season": 2026, "player_id": "9000001", "first_name": "Avery", "last_name": "Stone", "team": "Mississippi State",
     "position": "QB", "jersey": 1.0, "team_id": 344},
    {"season": 2026, "player_id": "9000002", "first_name": "Blake", "last_name": "Rowe", "team": "Mississippi State",
     "position": "QB", "jersey": 12.0, "team_id": 344},
    {"season": 2026, "player_id": "900", "first_name": "John", "last_name": "Smith", "team": "Oklahoma",
     "position": "QB", "jersey": 5.0, "team_id": 201},
    {"season": 2026, "player_id": "901", "first_name": "Jake", "last_name": "Smith", "team": "Oklahoma",
     "position": "QB", "jersey": 15.0, "team_id": 201},
])

PASSERS = pd.DataFrame([
    {"season": 2026, "game_id": 1, "offense": "Mississippi State", "passer": "A.Stone", "passer_key": "a.stone",
     "play_seq": 1},
])


def test_report_spellings_find_cfbd_teams():
    teams = TeamIndex(GAMES)
    assert teams.team_id("Mississippi St.", 2026) == 344
    assert teams.team_id("Mississippi State", 2026) == 344
    assert teams.team_id("Miami (FL)", 2026) == 2390
    assert teams.team_id("UConn", 2026) == 41
    assert teams.team_id("Miami (OH)", 2026) == 193
    assert norm_team("Mississippi St.") == "mississippi state"


def test_unknown_team_is_none_not_a_guess():
    assert TeamIndex(GAMES).team_id("Northern Nowhere", 2026) is None


def test_name_and_jersey_match_scores():
    players = PlayerIndex(ROSTERS, PASSERS, 2026)
    m = players.match(344, "Avery Stone", "#1")
    assert (m.player_id, m.confidence) == ("9000001", 1.0)
    m = players.match(344, "A. Stone", None)
    assert (m.player_id, m.confidence) == ("9000001", 0.9)
    only_stone = PlayerIndex(ROSTERS[ROSTERS.player_id == "9000001"], PASSERS, 2026)
    m = only_stone.match(344, "Blake Rowe", "#1")
    assert m.player_id is None or m.confidence < 0.8


def test_shared_surname_without_initial_is_ambiguous():
    m = PlayerIndex(ROSTERS, PASSERS, 2026).match(201, "Smith", None)
    assert m.confidence < 0.8


def test_passer_key_joins_roster_player_id():
    index = PlayerIndex(ROSTERS, PASSERS, 2026)
    keys = dict(zip(index.qbs(344)["player_id"], index.qbs(344)["passer_key"]))
    assert keys["9000001"] == "a.stone"
    assert pd.isna(keys["9000002"])
    assert index.player_for_key(344, "a.stone") == "9000001"
    assert index.player_for_key(344, "b.rowe") == "9000002"
    assert index.player_for_key(201, "j.smith") is None


def test_players_frame_matches_spec():
    df = players_frame(PlayerIndex(ROSTERS, PASSERS, 2026))
    assert list(df.columns) == PLAYERS.schema.names
    assert "A. Stone" in df.loc[df.player_id == "9000001", "aliases"].iloc[0]


SUFFIX_ROSTER = pd.DataFrame([
    {"season": 2026, "player_id": "1", "first_name": "Devon", "last_name": "Marsh Jr.", "team": "Washington",
     "position": "QB", "jersey": 2.0, "team_id": 264},
    {"season": 2026, "player_id": "2", "first_name": "Danny", "last_name": "O'Rourke", "team": "Wisconsin",
     "position": "QB", "jersey": 16.0, "team_id": 275},
    {"season": 2026, "player_id": "3", "first_name": "Kody", "last_name": "Lane-Porter", "team": "Army",
     "position": "QB", "jersey": 8.0, "team_id": 349},
])


def test_suffixes_and_punctuation_still_match():
    index = PlayerIndex(SUFFIX_ROSTER, PASSERS, 2026)
    assert index.match(264, "Devon Marsh Jr.", "#2").confidence == 1.0
    assert index.match(264, "Devon Marsh", None).player_id == "1"
    assert index.match(275, "Danny O'Rourke", "#16").player_id == "2"
    assert index.match(349, "Kody Lane-Porter", None).player_id == "3"


def test_players_frame_has_one_row_per_player_even_when_listed_twice():
    twice = pd.concat([ROSTERS, ROSTERS.iloc[[0]].assign(team="Oklahoma", team_id=201)], ignore_index=True)
    df = players_frame(PlayerIndex(twice, PASSERS, 2026))
    assert not df.duplicated(["season", "player_id"]).any()


def _with(*extra, passers=PASSERS):
    rows = pd.concat([ROSTERS, pd.DataFrame([{"season": 2026, "team": "Mississippi State", "team_id": 344, **e}
                                             for e in extra])], ignore_index=True)
    return PlayerIndex(rows, passers, 2026)


def test_key_shared_with_a_non_qb_teammate_links_to_the_qb():
    index = _with({"player_id": "9000003", "first_name": "Aaron", "last_name": "Stone", "position": "LS",
                   "jersey": 47.0})
    assert index.player_for_key(344, "a.stone") == "9000001"


def test_is_qb_by_roster_position_and_none_for_unknown_keys():
    trick = pd.concat([PASSERS, pd.DataFrame([{"season": 2026, "game_id": 1, "offense": "Mississippi State",
                                               "passer": "C.Fenn", "passer_key": "c.fenn", "play_seq": 2}])])
    index = _with({"player_id": "9000004", "first_name": "Cole", "last_name": "Fenn", "position": "WR",
                   "jersey": 81.0}, passers=trick)
    assert index.is_qb(344, "a.stone") is True
    assert index.is_qb(344, "c.fenn") is False
    assert index.is_qb(344, "n.tech") is None


def test_one_trick_play_pass_does_not_make_a_qb():
    trick = pd.concat([PASSERS, pd.DataFrame([{"season": 2026, "game_id": 1, "offense": "Mississippi State",
                                               "passer": "C.Fenn", "passer_key": "c.fenn", "play_seq": 2}])])
    index = _with({"player_id": "9000004", "first_name": "Cole", "last_name": "Fenn", "position": "WR",
                   "jersey": 81.0}, passers=trick)
    assert "9000004" not in set(index.qbs(344)["player_id"])
