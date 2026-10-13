"""Season-level context from CFBD: team-game PPA, talent, recruiting, returning production,
transfer portal, head coaches and rosters.

None of these endpoints says when a value was published, so every table carries an
`available_at` set by a documented, conservative policy (AVAILABILITY_POLICY). Point-in-time
feature code must filter on it.
"""
from __future__ import annotations

import pandas as pd

from gnome_pipelines.cfb.cfbd_client import CfbdClient

# Recruiting classes signed before the first modelled season still describe its rosters.
RECRUITING_LOOKBACK = 4
# Coach tenure and career residuals need history well before the first modelled season.
COACH_HISTORY_START = 2000

AVAILABILITY_POLICY = {
    "cfb_team_game_ppa_cfbd": "game kickoff + 1 day (postgame statistic)",
    "cfb_talent": "season start: the 247 composite is computed from the preseason roster. "
                  "Risk: CFBD may revise it in-season; values can't be verified as of preseason",
    "cfb_recruiting_teams": "Feb 15 of the class year (after National Signing Day)",
    "cfb_recruits": "Feb 15 of the class year (after National Signing Day)",
    "cfb_returning": "season start: computed from prior-season usage and the new roster",
    "cfb_portal": "transfer_date for leaving the origin team; season start for joining the "
                  "destination (commit dates aren't published)",
    "cfb_coaches": "hire_date for the assignment; a coach-season's results (wins, SRS, SP+) only "
                   "after that season ends (Feb 1 of the next year)",
    "cfb_rosters": "season start. Risk: CFBD serves the latest roster snapshot, which can include "
                   "mid-season additions",
}


def context_requests(years: list[int]) -> list[tuple[str, dict]]:
    reqs: list[tuple[str, dict]] = [("/coaches", {"minYear": COACH_HISTORY_START, "maxYear": max(years)})]
    for year in years:
        for st in ("regular", "postseason"):
            reqs.append(("/ppa/games", {"year": year, "seasonType": st, "excludeGarbageTime": "true"}))
        reqs.append(("/talent", {"year": year}))
        reqs.append(("/player/returning", {"year": year}))
        reqs.append(("/player/portal", {"year": year}))
        reqs.append(("/roster", {"year": year}))
    for year in range(min(years) - RECRUITING_LOOKBACK, max(years) + 1):
        reqs.append(("/recruiting/teams", {"year": year}))
        reqs.append(("/recruiting/players", {"year": year, "classification": "HighSchool"}))
    return reqs


def fetch_context(client: CfbdClient, years: list[int], refresh_current: int | None = None) -> None:
    reqs = context_requests(years)
    refresh = {i for i, (ep, p) in enumerate(reqs)
               if refresh_current is not None and (p.get("year") == refresh_current or ep == "/coaches")}
    todo = [r for i, r in enumerate(reqs) if i in refresh or not client.is_cached(*r)]
    if todo:
        client.ensure_budget(len(todo))
    for i, (ep, params) in enumerate(reqs):
        client.get(ep, params, refresh=i in refresh)
    print(f"context: {len(reqs)} requests, {len(todo)} fetched")


def season_starts(games: pd.DataFrame) -> pd.Series:
    """First kickoff of each season minus a day: the cutoff for preseason information."""
    return games.groupby("season")["kickoff"].min().dt.normalize() - pd.Timedelta(days=1)


def team_ids(games: pd.DataFrame) -> dict[str, int]:
    names = pd.concat([
        games[["home_team", "home_id"]].set_axis(["team", "team_id"], axis=1),
        games[["away_team", "away_id"]].set_axis(["team", "team_id"], axis=1),
    ])
    return names.drop_duplicates("team").set_index("team")["team_id"].to_dict()


def _signing_day(year: int) -> pd.Timestamp:
    return pd.Timestamp(year=year, month=2, day=15, tz="UTC")


def normalize_team_game_ppa(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year in years:
        for st in ("regular", "postseason"):
            for r in client.get("/ppa/games", {"year": year, "seasonType": st, "excludeGarbageTime": "true"}):
                o, d = r.get("offense") or {}, r.get("defense") or {}
                rows.append({
                    "game_id": r["gameId"], "season": r["season"], "week": r["week"], "team": r["team"],
                    "opponent": r["opponent"],
                    "off_ppa": o.get("overall"), "off_pass_ppa": o.get("passing"), "off_rush_ppa": o.get("rushing"),
                    "def_ppa": d.get("overall"), "def_pass_ppa": d.get("passing"), "def_rush_ppa": d.get("rushing"),
                })
    df = pd.DataFrame(rows).drop_duplicates(["game_id", "team"])
    ids = team_ids(games)
    df["team_id"] = df["team"].map(ids).astype("Int64")
    df["opponent_id"] = df["opponent"].map(ids).astype("Int64")
    kickoff = df["game_id"].map(games.set_index("game_id")["kickoff"])
    df["available_at"] = kickoff + pd.Timedelta(days=1)
    return df.reset_index(drop=True)


def normalize_talent(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame([r for y in years for r in client.get("/talent", {"year": y})])
    df = df.rename(columns={"year": "season"})
    df["talent"] = pd.to_numeric(df["talent"], errors="coerce")
    df["team_id"] = df["team"].map(team_ids(games)).astype("Int64")
    df["available_at"] = df["season"].map(season_starts(games))
    return df


def normalize_recruiting_teams(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    classes = range(min(years) - RECRUITING_LOOKBACK, max(years) + 1)
    df = pd.DataFrame([r for y in classes for r in client.get("/recruiting/teams", {"year": y})])
    df = df.rename(columns={"year": "class_year"})
    df["team_id"] = df["team"].map(team_ids(games)).astype("Int64")
    df["available_at"] = df["class_year"].map(_signing_day)
    return df


def normalize_recruits(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    classes = range(min(years) - RECRUITING_LOOKBACK, max(years) + 1)
    rows = []
    for y in classes:
        for r in client.get("/recruiting/players", {"year": y, "classification": "HighSchool"}):
            rows.append({
                "recruit_id": r["id"], "athlete_id": r.get("athleteId"), "class_year": r["year"],
                "recruit_type": r.get("recruitType"), "name": r["name"], "committed_to": r.get("committedTo"),
                "position": r.get("position"), "stars": r.get("stars"), "rating": r.get("rating"),
                "ranking": r.get("ranking"),
            })
    df = pd.DataFrame(rows).drop_duplicates("recruit_id")
    df["team_id"] = df["committed_to"].map(team_ids(games)).astype("Int64")
    df["available_at"] = df["class_year"].map(_signing_day)
    return df


def normalize_returning(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame([r for y in years for r in client.get("/player/returning", {"year": y})])
    df = df.rename(columns={
        "totalPPA": "total_ppa", "totalPassingPPA": "passing_ppa", "totalReceivingPPA": "receiving_ppa",
        "totalRushingPPA": "rushing_ppa", "percentPPA": "pct_ppa", "percentPassingPPA": "pct_passing_ppa",
        "percentReceivingPPA": "pct_receiving_ppa", "percentRushingPPA": "pct_rushing_ppa",
        "passingUsage": "passing_usage", "receivingUsage": "receiving_usage", "rushingUsage": "rushing_usage",
    })
    df["team_id"] = df["team"].map(team_ids(games)).astype("Int64")
    df["available_at"] = df["season"].map(season_starts(games))
    return df


def normalize_portal(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame([r for y in years for r in client.get("/player/portal", {"year": y})])
    if df.empty:
        return df
    df = df.rename(columns={"firstName": "first_name", "lastName": "last_name", "transferDate": "transfer_date"})
    df["transfer_date"] = pd.to_datetime(df["transfer_date"], utc=True, errors="coerce")
    ids = team_ids(games)
    df["origin_id"] = df["origin"].map(ids).astype("Int64")
    df["destination_id"] = df["destination"].map(ids).astype("Int64")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce")
    starts = season_starts(games)
    df["origin_available_at"] = df["transfer_date"]
    df["destination_available_at"] = df["season"].map(starts)
    return df


def normalize_coaches(client: CfbdClient, years: list[int]) -> pd.DataFrame:
    rows = []
    for c in client.get("/coaches", {"minYear": COACH_HISTORY_START, "maxYear": max(years)}):
        for s in c.get("seasons") or []:
            rows.append({
                "coach_id": c["id"], "first_name": c["firstName"], "last_name": c["lastName"],
                "hire_date": c.get("hireDate"), "season": s["year"], "team_id": s.get("teamId"),
                "school": s["school"], "games": s.get("games"), "wins": s.get("wins"), "losses": s.get("losses"),
                "srs": s.get("srs"), "sp_overall": s.get("spOverall"), "sp_offense": s.get("spOffense"),
                "sp_defense": s.get("spDefense"), "preseason_rank": s.get("preseasonRank"),
                "postseason_rank": s.get("postseasonRank"),
            })
    df = pd.DataFrame(rows)
    df["hire_date"] = pd.to_datetime(df["hire_date"], utc=True, errors="coerce")
    df["results_available_at"] = pd.to_datetime(
        (df["season"] + 1).astype(str) + "-02-01", utc=True)
    for col in ("srs", "sp_overall", "sp_offense", "sp_defense"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["team_id"] = df["team_id"].astype("Int64")
    return df.sort_values(["team_id", "season", "hire_date"]).reset_index(drop=True)


def normalize_rosters(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for y in years:
        for r in client.get("/roster", {"year": y}):
            rows.append({
                "season": y, "player_id": r["id"], "first_name": r.get("firstName"),
                "last_name": r.get("lastName"), "team": r["team"], "position": r.get("position"),
                "class_year": r.get("year"), "jersey": r.get("jersey"), "height": r.get("height"),
                "weight": r.get("weight"), "recruit_ids": list(r.get("recruitIds") or []),
            })
    df = pd.DataFrame(rows).drop_duplicates(["season", "player_id", "team"])
    df["team_id"] = df["team"].map(team_ids(games)).astype("Int64")
    df["available_at"] = df["season"].map(season_starts(games))
    return df


def build_context(client: CfbdClient, years: list[int], games: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "cfb_team_game_ppa_cfbd": normalize_team_game_ppa(client, years, games),
        "cfb_talent": normalize_talent(client, years, games),
        "cfb_recruiting_teams": normalize_recruiting_teams(client, years, games),
        "cfb_recruits": normalize_recruits(client, years, games),
        "cfb_returning": normalize_returning(client, years, games),
        "cfb_portal": normalize_portal(client, years, games),
        "cfb_coaches": normalize_coaches(client, years),
        "cfb_rosters": normalize_rosters(client, years, games),
    }
