"""FACEIT API responses -> rows of the faceit_* datasets.

Organised competitions are named leagues, tour qualifiers, ESEA seasons (some lack "ESEA" in their name) and a few
national leagues; community cups and retakes are out. A side's HLTV team is the team at least three of its linked
players played for on HLTV that quarter, so FACEIT maps join the HLTV team graph without trusting team names.
"""
from __future__ import annotations

import re
from collections import Counter

import numpy as np
import pandas as pd

ORGANISED = re.compile(
    r"ESEA|ECL|Challenger League|CCT|Qualifier|IEM|ESL|BLAST|PGL|Pro League|Stake|Thunderpick|Elisa|United21|Exort|"
    r"European Pro League|Major|RMR|Fragadelphia|Skyesports|DACH|Winline|Galaxy Battle|YGames|1XBET|Liquid|UKIC|UESF|"
    r"Conquest of Prague|^S\d+ (EU|NA|SA|SEA|OCE|MENA|AS|ASIA)\b.*(Central|Open|Main|Intermediate|Advanced|Entry)", re.I)
FIVE_A_SIDE = ("5v5", "5v5 League")


def is_organised(name: str | None) -> bool:
    name = name or ""
    return bool(ORGANISED.search(name)) and "retake" not in name.lower()


def history_rows(items: list[dict]) -> tuple[list[dict], list[dict]]:
    """Player-history items -> (faceit_history_matches rows, faceit_history_players rows); finished games only."""
    matches, players = [], []
    for item in items:
        if item.get("status") != "finished":
            continue
        res = item.get("results") or {}
        matches.append({"match_id": item["match_id"], "competition_type": item.get("competition_type"),
                        "competition_id": item.get("competition_id"), "competition_name": item.get("competition_name"),
                        "game_mode": item.get("game_mode"), "teams_size": item.get("teams_size"),
                        "started_at": item.get("started_at"), "finished_at": item.get("finished_at"),
                        "status": item.get("status"), "winner": res.get("winner")})
        for faction, team in (item.get("teams") or {}).items():
            for p in team.get("players") or []:
                players.append({"match_id": item["match_id"], "faceit_id": p.get("player_id"), "faction": faction,
                                "skill_level": p.get("skill_level")})
    return matches, players


def organised_championship(row: dict) -> bool:
    return (row.get("competition_type") == "championship" and row.get("game_mode") in FIVE_A_SIDE
            and (row.get("teams_size") or 0) >= 5 and row.get("status") == "finished" and is_organised(row.get("competition_name")))


def team_of_quarter(lineups: pd.DataFrame) -> dict[tuple[int, pd.Period], int]:
    """(HLTV player id, quarter) -> the HLTV team the player played for most that quarter."""
    lu = lineups.dropna(subset=["player_id", "team_id"])
    q = pd.to_datetime(lu.match_date).dt.to_period("Q")
    mode = lu.assign(q=q).groupby(["player_id", "q"]).team_id.agg(lambda t: t.mode().iloc[0])
    return {(int(p), qq): int(t) for (p, qq), t in mode.items()}


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def _hltv_team(faceit_ids, q, f2h: dict, team_of: dict) -> int | None:
    teams = Counter(team_of.get((f2h[f], q)) for f in faceit_ids if f in f2h)
    teams.pop(None, None)
    if not teams:
        return None
    t, n = teams.most_common(1)[0]
    return int(t) if n >= 3 else None


def match_rows(mid: str, details: dict, stats: dict | None, f2h: dict, team_of: dict) -> dict[str, list[dict]] | None:
    """One organised series -> rows for faceit_matches, faceit_rosters and faceit_match_history; None for byes,
    malformed matches and anything that isn't five-a-side."""
    teams = details.get("teams") or {}
    if len(teams) != 2 or any(t.get("faction_id") == "bye" for t in teams.values()):
        return None
    rounds = (stats or {}).get("rounds") or []
    # Championship listings are not filtered by game mode; wingman and 1v1 cups end up here.
    if rounds and max(len(t["players"]) for r in rounds for t in r["teams"]) < 4:
        return None
    start = pd.Timestamp(details.get("started_at") or details.get("finished_at"), unit="s", tz="UTC")
    q = start.tz_convert(None).to_period("Q")
    fac = {k: teams[k] for k in ("faction1", "faction2")}
    fid = {k: fac[k]["faction_id"] for k in fac}
    played: dict[str, set] = {k: set() for k in fac}
    for r in rounds:
        for t in r["teams"]:
            k = "faction1" if t["team_id"] == fid["faction1"] else "faction2" if t["team_id"] == fid["faction2"] else None
            if k:
                played[k] |= {p["player_id"] for p in t["players"]}
    side_team = {k: _hltv_team(played[k] or {p["player_id"] for p in fac[k].get("roster") or []}, q, f2h, team_of)
                 for k in fac}
    out: dict[str, list[dict]] = {"matches": [{
        "match_id": mid, "competition_id": details.get("competition_id"), "competition_name": details.get("competition_name"),
        "organizer_id": details.get("organizer_id"), "region": details.get("region"), "best_of": details.get("best_of"),
        "round": details.get("round"), "group": details.get("group"), "scheduled_at": details.get("scheduled_at"),
        "started_at": details.get("started_at"), "finished_at": details.get("finished_at"),
        "winner": (details.get("results") or {}).get("winner"),
        "faction1_id": fid["faction1"], "faction1_name": fac["faction1"].get("name"),
        "faction2_id": fid["faction2"], "faction2_name": fac["faction2"].get("name"),
        "faction1_hltv_team": side_team["faction1"], "faction2_hltv_team": side_team["faction2"],
        "n_maps_with_stats": len(rounds), "has_stats": bool(rounds)}], "rosters": [], "history": []}
    for k in fac:
        for role, plist in (("roster", fac[k].get("roster") or []), ("substitute", fac[k].get("substitutes") or [])):
            for p in plist:
                out["rosters"].append({"match_id": mid, "faceit_id": p["player_id"], "faction": k, "role": role,
                                       "nickname": p.get("nickname"), "skill_level": p.get("game_skill_level"),
                                       "played": p["player_id"] in played[k], "hltv_id": f2h.get(p["player_id"])})
    for pos, r in enumerate(sorted(rounds, key=lambda r: int(r.get("match_round") or 0)), 1):
        rs = r["round_stats"]
        by = {("faction1" if t["team_id"] == fid["faction1"] else "faction2"): t for t in r["teams"] if t["team_id"] in fid.values()}
        if len(by) != 2:
            continue
        ts = {k: by[k]["team_stats"] for k in by}
        a_won = int(rs.get("Winner") == fid["faction1"])
        ids = {k: [p["player_id"] for p in by[k]["players"]] for k in by}
        out["history"].append({
            "match_id": mid, "map_position_in_series": pos, "map_name": rs.get("Map"),
            "match_date": start.tz_convert(None).normalize(), "match_time": start, "event_name": details.get("competition_name"),
            "bo_type": details.get("best_of"),
            "team_a_id": side_team["faction1"], "team_a_name": fac["faction1"].get("name"), "team_a_faceit_id": fid["faction1"],
            "team_b_id": side_team["faction2"], "team_b_name": fac["faction2"].get("name"), "team_b_faceit_id": fid["faction2"],
            "team_a_score": int(_num(ts["faction1"].get("Final Score"))), "team_b_score": int(_num(ts["faction2"].get("Final Score"))),
            "team_a_won": a_won,
            "team_a_ot_score": _num(ts["faction1"].get("Overtime score")), "team_b_ot_score": _num(ts["faction2"].get("Overtime score")),
            "team_a_player_ids": [f2h.get(x) for x in ids["faction1"]], "team_b_player_ids": [f2h.get(x) for x in ids["faction2"]],
            "team_a_faceit_ids": ids["faction1"], "team_b_faceit_ids": ids["faction2"],
            "team_a_players": len(ids["faction1"]), "team_b_players": len(ids["faction2"])})
    return out
