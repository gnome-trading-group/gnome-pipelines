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
    {"season": 2026, "game_id": 1, "offense": "Mississippi State", "passer": "A.Stone", "passer_key": "9000001",
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
    qbs = PlayerIndex(ROSTERS, PASSERS, 2026).qbs(344)
    keys = dict(zip(qbs["player_id"], qbs["passer_key"]))
    assert keys["9000001"] == "9000001"
    assert pd.isna(keys["9000002"])


def test_players_frame_matches_spec():
    df = players_frame(PlayerIndex(ROSTERS, PASSERS, 2026))
    assert list(df.columns) == PLAYERS.schema.names
    assert "A. Stone" in df.loc[df.player_id == "9000001", "aliases"].iloc[0]
