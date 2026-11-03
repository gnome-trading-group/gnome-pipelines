import pandas as pd
import pytest

from gnome_pipelines.faceit.client import AdaptiveLimiter, FaceitClient
from gnome_pipelines.faceit.link import exclusions, series_links
from gnome_pipelines.faceit.normalize import history_rows, is_organised, match_rows, organised_championship, team_of_quarter

START = 1_780_000_000  # 2026-05-28


def _player(pid):
    return {"player_id": pid, "nickname": pid, "skill_level": 10, "game_skill_level": 10}


def _details(mid="1-abc", best_of=1):
    teams = {f"faction{i}": {"faction_id": f"team{i}", "name": f"Team {i}",
                            "roster": [_player(f"p{i}{j}") for j in range(5)], "substitutes": [_player(f"p{i}s")]}
             for i in (1, 2)}
    return {"match_id": mid, "status": "FINISHED", "competition_id": "c1", "competition_name": "ESEA S56 EU Main",
            "best_of": best_of, "started_at": START, "finished_at": START + 3600, "results": {"winner": "faction1"},
            "teams": teams}


def _stats(score_a=13, score_b=7):
    return {"rounds": [{"match_round": "1", "round_stats": {"Map": "de_mirage", "Winner": "team1" if score_a > score_b else "team2"},
                        "teams": [{"team_id": f"team{i}",
                                   "team_stats": {"Final Score": str(s), "Overtime score": "0"},
                                   "players": [{"player_id": f"p{i}{j}"} for j in range(5)]}
                                  for i, s in ((1, score_a), (2, score_b))]}]}


def test_history_rows_keep_finished_games_and_every_player():
    items = [{"match_id": "m1", "status": "finished", "competition_type": "matchmaking", "game_mode": "5v5", "teams_size": 5,
              "started_at": START, "finished_at": START + 1800, "results": {"winner": "faction2"},
              "teams": {"faction1": {"players": [{"player_id": "a", "skill_level": 9}]},
                        "faction2": {"players": [{"player_id": "b", "skill_level": 10}]}}},
             {"match_id": "m2", "status": "ongoing", "teams": {}}]
    matches, players = history_rows(items)
    assert [m["match_id"] for m in matches] == ["m1"] and matches[0]["winner"] == "faction2"
    assert {(p["faceit_id"], p["faction"]) for p in players} == {("a", "faction1"), ("b", "faction2")}


@pytest.mark.parametrize("name,expected", [("ESEA S56 EU Main", True), ("CCT Season 3 Qualifier", True),
                                          ("ESEA Retakes", False), ("Friday Night Cup", False), (None, False)])
def test_organised_scope(name, expected):
    assert is_organised(name) is expected


def test_organised_championship_needs_five_a_side_and_finished():
    row = {"competition_type": "championship", "game_mode": "5v5", "teams_size": 5, "status": "finished",
           "competition_name": "ESEA S56 EU Main"}
    assert organised_championship(row)
    assert not organised_championship({**row, "game_mode": "2v2"})
    assert not organised_championship({**row, "competition_type": "matchmaking"})


def test_match_rows_map_sides_to_hltv_teams_through_linked_players():
    f2h = {f"p1{j}": 100 + j for j in range(5)} | {f"p2{j}": 200 + j for j in range(3)}
    q = pd.Timestamp(START, unit="s").to_period("Q")
    team_of = {(100 + j, q): 10 for j in range(5)} | {(200 + j, q): 20 for j in range(3)}
    out = match_rows("1-abc", _details(), _stats(), f2h, team_of)
    m = out["matches"][0]
    assert (m["faction1_hltv_team"], m["faction2_hltv_team"]) == (10, 20) and m["has_stats"]
    h = out["history"][0]
    assert (h["team_a_score"], h["team_b_score"], h["team_a_won"]) == (13, 7, 1)
    assert h["team_a_player_ids"] == [100, 101, 102, 103, 104] and h["team_b_player_ids"][3] is None
    played = {r["faceit_id"]: r["played"] for r in out["rosters"]}
    assert played["p10"] and not played["p1s"]


def test_match_rows_need_three_linked_players_for_an_hltv_team():
    f2h = {"p10": 100, "p11": 101}
    q = pd.Timestamp(START, unit="s").to_period("Q")
    out = match_rows("1-abc", _details(), _stats(), f2h, {(100, q): 10, (101, q): 10})
    assert out["matches"][0]["faction1_hltv_team"] is None


def test_match_rows_skip_byes():
    d = _details()
    d["teams"]["faction2"]["faction_id"] = "bye"
    assert match_rows("1-abc", d, None, {}, {}) is None


def test_team_of_quarter_takes_the_most_played_team():
    lu = pd.DataFrame({"player_id": [1, 1, 1], "team_id": [10, 10, 20],
                       "match_date": pd.to_datetime(["2026-04-02", "2026-04-09", "2026-05-01"])})
    assert team_of_quarter(lu) == {(1, pd.Period("2026Q2")): 10}


def _faceit(mid, a_team, b_team, maps, t=START, a_players=range(100, 105), b_players=range(200, 205)):
    fm = pd.DataFrame([{"match_id": mid, "started_at": t, "finished_at": t + 3600, "faction1_hltv_team": a_team,
                        "faction2_hltv_team": b_team, "has_stats": True}])
    fh = pd.DataFrame([{"match_id": mid, "map_name": m, "team_a_score": a, "team_b_score": b, "team_a_won": int(a > b),
                        "bo_type": len(maps), "team_a_player_ids": list(a_players), "team_b_player_ids": list(b_players)}
                       for m, a, b in maps])
    return fm, fh


def _hltv(mid, a_team, b_team, maps, t=START, a_players=range(100, 105), b_players=range(200, 205)):
    return pd.DataFrame([{"match_id": mid, "match_time": pd.Timestamp(t, unit="s", tz="UTC"), "team_a_id": a_team,
                          "team_b_id": b_team, "map_name": m, "team_a_score": a, "team_b_score": b,
                          "team_a_lineup_ids": list(a_players), "team_b_lineup_ids": list(b_players)} for m, a, b in maps])


def test_series_link_matches_the_same_series_in_either_orientation():
    fm, fh = _faceit("f1", 10, 20, [("mirage", 13, 7)])
    hltv = _hltv(500, 20, 10, [("mirage", 7, 13)], a_players=range(200, 205), b_players=range(100, 105))
    link = series_links(fm, fh, hltv).iloc[0]
    assert (link.faceit_match_id, link.hltv_match_id, link.swapped, link.quality) == ("f1", 500, True, "exact")


def test_series_link_is_one_to_one_and_skips_taken_hltv_series():
    fm1, fh1 = _faceit("f1", 10, 20, [("mirage", 13, 7)])
    fm2, fh2 = _faceit("f2", 10, 20, [("mirage", 13, 7)], t=START + 600)
    fm, fh = pd.concat([fm1, fm2]), pd.concat([fh1, fh2])
    hltv = _hltv(500, 10, 20, [("mirage", 13, 7)])
    assert len(series_links(fm, fh, hltv)) == 1
    assert series_links(fm, fh, hltv, taken_hltv={500}).empty


def test_series_link_drops_a_far_away_series_with_no_agreeing_map():
    fm, fh = _faceit("f1", 10, 20, [("mirage", 13, 7)])
    hltv = _hltv(500, 10, 20, [("nuke", 13, 9)], t=START + 30 * 3600)
    assert series_links(fm, fh, hltv).empty


def test_exclusions_apply_the_model_rules():
    fm, fh = _faceit("f1", 10, 20, [("mirage", 13, 7), ("nuke", 13, 13), ("inferno", 7, 13)])
    fh.loc[0, "team_a_won"] = 0
    fm2, _ = _faceit("f2", 10, 20, [])
    fm2["has_stats"] = False
    ex = exclusions(pd.concat([fm, fm2]), fh, linked={"f1"})
    got = set(map(tuple, ex[["match_id", "map_name", "reason"]].to_numpy()))
    assert ("f1", "nuke", "implausible map score") in got
    assert ("f1", "mirage", "winner disagrees with score") in got
    assert ("f1", "", "duplicate of an HLTV series") in got
    assert ("f2", "", "no stats (forfeit)") in got
    assert not ex.map_name.isna().any()


class _Response:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class _Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return self.responses.pop(0)


def _client(responses):
    c = FaceitClient("k", limiter=AdaptiveLimiter(rate=1000, ceiling=1000), max_attempts=5)
    c._local.session = _Session(responses)
    return c


def test_client_retries_throttling_and_returns_the_body(monkeypatch):
    monkeypatch.setattr("gnome_pipelines.faceit.client.time.sleep", lambda s: None)
    c = _client([_Response(429, headers={"retry-after": "0"}), _Response(503), _Response(200, {"ok": 1})])
    assert c.get("/x") == {"ok": 1}
    assert c.limiter.throttles == 1 and c.calls == 3


def test_client_returns_none_only_for_not_found(monkeypatch):
    monkeypatch.setattr("gnome_pipelines.faceit.client.time.sleep", lambda s: None)
    assert _client([_Response(404)]).get("/x") is None
    with pytest.raises(RuntimeError, match="still failing"):
        _client([_Response(503)] * 5).get("/x")


def test_history_walk_pages_until_a_short_page(monkeypatch):
    monkeypatch.setattr("gnome_pipelines.faceit.client.time.sleep", lambda s: None)
    c = _client([_Response(200, {"items": [{"match_id": str(i)} for i in range(100)]}), _Response(200, {"items": [{"match_id": "x"}]})])
    assert len(c.player_history("p", START, START + 86400)) == 101
