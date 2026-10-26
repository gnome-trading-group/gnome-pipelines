"""Fetch CFBD seasons into the raw cache and normalize them into the cfb_* datasets.

    poetry run python -m gnome_pipelines.cfb.ingest fetch 2018 2026
    poetry run python -m gnome_pipelines.cfb.ingest publish 2018 2026
"""
from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from gnome_pipelines.cfb.cfbd_client import CfbdClient
from gnome_pipelines.cfb.context import (
    build_context,
    fetch_context,
)
from gnome_pipelines.cfb.efficiency import team_game_efficiency
from gnome_pipelines.datasets import cfb
from gnome_pipelines.stores import dataset_store

SEASON_TYPES = ("regular", "postseason")
# A week counts as played once any of its games is final, but its Saturday games finish days after its midweek ones:
# week 8 of 2026 was cached on Oct 22 with only its Tuesday-Wednesday games and never fetched again. A week's plays are
# refetched while any of its games finished within RECENT, which also picks up CFBD's late corrections.
RECENT = timedelta(days=8)


def _season_requests(client: CfbdClient, year: int) -> list[tuple[str, dict]]:
    """Every request a season needs. Play-by-play is only served per week, so the week list
    comes from the (cheap, cached) games response."""
    reqs: list[tuple[str, dict]] = []
    for st in SEASON_TYPES:
        reqs.append(("/games", {"year": year, "seasonType": st, "classification": "fbs"}))
        reqs.append(("/drives", {"year": year, "seasonType": st, "classification": "fbs"}))
        reqs.append(("/lines", {"year": year, "seasonType": st}))
    for st in SEASON_TYPES:
        games = client.get("/games", {"year": year, "seasonType": st, "classification": "fbs"})
        weeks = sorted({g["week"] for g in games if g["completed"]})
        for w in weeks:
            reqs.append(("/plays", {"year": year, "week": w, "seasonType": st, "classification": "fbs"}))
    return reqs


def _recent_weeks(client: CfbdClient, year: int, now: datetime) -> set[tuple[str, int]]:
    """(season type, week) for weeks with a game that finished within RECENT of now."""
    out = set()
    for st in SEASON_TYPES:
        for g in client.get("/games", {"year": year, "seasonType": st, "classification": "fbs"}):
            if g["completed"] and g.get("startDate"):
                kick = datetime.fromisoformat(g["startDate"].replace("Z", "+00:00"))
                if now - RECENT <= kick <= now:
                    out.add((st, g["week"]))
    return out


def fetch(client: CfbdClient, years: list[int], refresh_current: int | None = None, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    for year in years:
        for st in SEASON_TYPES:
            client.get("/games", {"year": year, "seasonType": st, "classification": "fbs"},
                       refresh=(year == refresh_current))
        reqs = _season_requests(client, year)
        # The in-progress season's season-level endpoints grow every week, so refetch them, and its recent weeks of
        # plays too; older weeks of plays are settled and stay cached.
        recent = _recent_weeks(client, year, now) if year == refresh_current else set()
        refresh = {i for i, (ep, p) in enumerate(reqs) if year == refresh_current
                   and (ep != "/plays" or (p["seasonType"], p["week"]) in recent)}
        todo = [r for i, r in enumerate(reqs) if i in refresh or not client.is_cached(*r)]
        if todo:
            client.ensure_budget(len(todo))
        for i, (ep, params) in enumerate(reqs):
            client.get(ep, params, refresh=i in refresh)
        print(f"{year}: {len(reqs)} requests, {len(todo)} fetched (total calls this run {client.calls_made})")


def _clock_seconds(clock: dict | None) -> float:
    if not clock:
        return np.nan
    return float(clock.get("minutes") or 0) * 60 + float(clock.get("seconds") or 0)


def normalize_games(client: CfbdClient, years: list[int]) -> pd.DataFrame:
    rows = []
    for year in years:
        for st in SEASON_TYPES:
            for g in client.get("/games", {"year": year, "seasonType": st, "classification": "fbs"}):
                rows.append({
                    "game_id": g["id"],
                    "season": g["season"],
                    "week": g["week"],
                    "season_type": g["seasonType"],
                    "kickoff": g["startDate"],
                    "start_time_tbd": g["startTimeTBD"],
                    "completed": g["completed"],
                    "neutral_site": g["neutralSite"],
                    "conference_game": g["conferenceGame"],
                    "venue_id": g["venueId"],
                    "home_id": g["homeId"],
                    "home_team": g["homeTeam"],
                    "home_conference": g["homeConference"],
                    "home_classification": g["homeClassification"],
                    "away_id": g["awayId"],
                    "away_team": g["awayTeam"],
                    "away_conference": g["awayConference"],
                    "away_classification": g["awayClassification"],
                    "home_points": g["homePoints"],
                    "away_points": g["awayPoints"],
                    "home_pregame_elo": g["homePregameElo"],
                    "away_pregame_elo": g["awayPregameElo"],
                    "home_line_scores": g["homeLineScores"],
                    "away_line_scores": g["awayLineScores"],
                })
    df = pd.DataFrame(rows).drop_duplicates("game_id")
    df["kickoff"] = pd.to_datetime(df["kickoff"], utc=True)
    for col in ("home_points", "away_points", "home_pregame_elo", "away_pregame_elo"):
        df[col] = df[col].astype("Int64")
    df["periods"] = df["home_line_scores"].map(lambda x: len(x) if isinstance(x, list) else pd.NA).astype("Int64")
    df = df.drop(columns=["home_line_scores", "away_line_scores"])
    return df.sort_values(["kickoff", "game_id"]).reset_index(drop=True)


# CFBD's explicit driveResult labels are trusted first: drive start/end scores are sometimes
# recorded after the score (a TD drive "starting" at 7-0 with 0 points), so score deltas only
# classify drives whose label is missing or ambiguous ("Uncategorized", "SF", "KICKOFF", ...).
_EXPLICIT_RESULTS = {
    "TD": "TD", "FG": "FG", "PUNT": "PUNT", "DOWNS": "DOWNS", "INT": "TURNOVER", "FUMBLE": "TURNOVER",
    "MISSED FG": "MISSED_FG", "BLOCKED FG": "MISSED_FG", "FG MISSED": "MISSED_FG", "BLOCKED PUNT": "PUNT",
    "INT TD": "DEF_TD", "FUMBLE RETURN TD": "DEF_TD", "FUMBLE TD": "DEF_TD", "PUNT RETURN TD": "DEF_TD",
    "PUNT TD": "DEF_TD", "MISSED FG TD": "DEF_TD", "BLOCKED PUNT TD": "DEF_TD", "BLOCKED FG TD": "DEF_TD",
    "SF": "SAFETY",
    "END OF HALF": "END_HALF", "END OF GAME": "END_HALF", "END OF 4TH QUARTER": "END_HALF",
}


def drive_outcome(result: str, off_pts: int, def_pts: int) -> str:
    result = (result or "").upper()
    if result in _EXPLICIT_RESULTS:
        return _EXPLICIT_RESULTS[result]
    if off_pts >= 6:
        return "TD"
    if def_pts >= 6:
        return "DEF_TD"
    if off_pts == 3:
        return "FG"
    if def_pts == 2:
        return "SAFETY"
    return "OTHER"


def normalize_drives(client: CfbdClient, years: list[int], games: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year in years:
        for st in SEASON_TYPES:
            for d in client.get("/drives", {"year": year, "seasonType": st, "classification": "fbs"}):
                off_pts = (d["endOffenseScore"] or 0) - (d["startOffenseScore"] or 0)
                def_pts = (d["endDefenseScore"] or 0) - (d["startDefenseScore"] or 0)
                rows.append({
                    "drive_id": d["id"],
                    "game_id": d["gameId"],
                    "season": year,
                    "drive_number": d["driveNumber"],
                    "offense": d["offense"],
                    "defense": d["defense"],
                    "is_home_offense": d["isHomeOffense"],
                    "start_period": d["startPeriod"],
                    "start_clock_s": _clock_seconds(d["startTime"]),
                    "start_yards_to_goal": d["startYardsToGoal"],
                    "end_period": d["endPeriod"],
                    "end_clock_s": _clock_seconds(d["endTime"]),
                    "end_yards_to_goal": d["endYardsToGoal"],
                    "elapsed_s": _clock_seconds(d["elapsed"]),
                    "n_plays": d["plays"],
                    "yards": d["yards"],
                    "drive_result": d["driveResult"],
                    "start_offense_score": d["startOffenseScore"],
                    "start_defense_score": d["startDefenseScore"],
                    "off_points": off_pts,
                    "def_points": def_pts,
                    "outcome": drive_outcome(d["driveResult"], off_pts, def_pts),
                })
    df = pd.DataFrame(rows).drop_duplicates("drive_id")
    teams = games[["game_id", "home_id", "away_id"]]
    df = df.merge(teams, on="game_id", how="inner")
    df["offense_id"] = np.where(df["is_home_offense"], df["home_id"], df["away_id"])
    df["defense_id"] = np.where(df["is_home_offense"], df["away_id"], df["home_id"])
    df = df.drop(columns=["home_id", "away_id"])
    return df.sort_values(["game_id", "drive_number"]).reset_index(drop=True)


def normalize_plays(client: CfbdClient, years: list[int]) -> pd.DataFrame:
    frames = []
    for year in years:
        for st in SEASON_TYPES:
            games = client.get("/games", {"year": year, "seasonType": st, "classification": "fbs"})
            for w in sorted({g["week"] for g in games if g["completed"]}):
                plays = client.get("/plays", {"year": year, "week": w, "seasonType": st, "classification": "fbs"})
                if not plays:
                    continue
                df = pd.DataFrame(plays)
                df["clock_s"] = df["clock"].map(_clock_seconds)
                df["season"] = year
                frames.append(df[[
                    "id", "gameId", "driveId", "driveNumber", "playNumber", "season", "offense", "defense",
                    "home", "period", "clock_s", "offenseScore", "defenseScore", "offenseTimeouts",
                    "defenseTimeouts", "yardsToGoal", "down", "distance", "yardsGained", "scoring",
                    "playType", "playText", "ppa", "wallclock",
                ]])
    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={
        "id": "play_id", "gameId": "game_id", "driveId": "drive_id", "driveNumber": "drive_number",
        "playNumber": "play_number", "offenseScore": "offense_score", "defenseScore": "defense_score",
        "offenseTimeouts": "offense_timeouts", "defenseTimeouts": "defense_timeouts",
        "yardsToGoal": "yards_to_goal", "yardsGained": "yards_gained", "playType": "play_type",
        "playText": "play_text",
    })
    df["wallclock"] = pd.to_datetime(df["wallclock"], utc=True, errors="coerce")
    df["ppa"] = pd.to_numeric(df["ppa"], errors="coerce")
    df = df.drop_duplicates("play_id")
    # The API returns plays unordered. A play_id normally embeds the game id followed by an
    # increasing sequence number; ~130 games (no wallclock either) carry synthetic negative ids
    # whose magnitude increases in play order instead.
    ids = df["play_id"].astype(str)
    df["synthetic_id"] = ids.str.startswith("-")
    df["play_seq"] = [
        -int(p) if syn else int(p[len(str(g)):])
        for p, g, syn in zip(ids, df["game_id"], df["synthetic_id"])
    ]
    return df.sort_values(["game_id", "play_seq"]).reset_index(drop=True)


def american_to_prob(ml: float) -> float:
    if ml is None or pd.isna(ml) or ml == 0:
        return np.nan
    return 100.0 / (ml + 100.0) if ml > 0 else -ml / (-ml + 100.0)


def normalize_lines(client: CfbdClient, years: list[int]) -> pd.DataFrame:
    rows = []
    for year in years:
        for st in SEASON_TYPES:
            for g in client.get("/lines", {"year": year, "seasonType": st}):
                for ln in g["lines"]:
                    rows.append({
                        "game_id": g["id"],
                        "season": g["season"],
                        "provider": ln["provider"],
                        "spread": ln["spread"],
                        "spread_open": ln.get("spreadOpen"),
                        "over_under": ln["overUnder"],
                        "over_under_open": ln.get("overUnderOpen"),
                        "home_moneyline": ln["homeMoneyline"],
                        "away_moneyline": ln["awayMoneyline"],
                    })
    df = pd.DataFrame(rows)
    for col in ("spread", "spread_open", "over_under", "over_under_open", "home_moneyline", "away_moneyline"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    ph = df["home_moneyline"].map(american_to_prob)
    pa = df["away_moneyline"].map(american_to_prob)
    df["home_prob_novig"] = ph / (ph + pa)
    df["overround"] = ph + pa - 1.0
    return df.drop_duplicates(["game_id", "provider"]).reset_index(drop=True)


# Three text formats across 2018-2026: "Joe Fagnano pass complete ...", the 2025+ play-by-play
# "(07:39) No Huddle-Shotgun #2 J.Fagnano pass complete ...", and scoring summaries
# "Jasaiah Gathings 21 Yd pass from Joe Fagnano (Chris Freeman Kick)".
_NAME = r"[A-Z][A-Za-z.'\-]*(?: [A-Za-z.'\-]+){0,3}?"
_JERSEY_RE = re.compile(rf"#\d+ (?P<name>{_NAME}) (?:pass\b|sacked\b)")
_LEAD_RE = re.compile(rf"^(?P<name>{_NAME}) (?:pass\b|sacked\b)")
_FROM_RE = re.compile(rf"\bpass from (?P<name>{_NAME})(?: \(|,|$| for\b)")
_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
_PASS_PLAY_TYPES = {
    "Pass Reception", "Pass Incompletion", "Passing Touchdown", "Sack", "Pass Interception Return",
    "Interception", "Interception Return Touchdown", "Pass Interception",
}


def passer_name(play_type: str, text: str) -> str | None:
    if play_type not in _PASS_PLAY_TYPES or not isinstance(text, str):
        return None
    text = text.strip()
    for regex in (_JERSEY_RE, _FROM_RE, _LEAD_RE):
        m = regex.search(text)
        if m:
            return m.group("name").strip()
    return None


def passer_key(name: str | None) -> str | None:
    """"Joe Fagnano", "J.Fagnano" and "Joe Fagnano Jr." all map to "j.fagnano". Collisions are only
    a problem within a team, where two same-initial same-surname QBs are vanishingly rare."""
    if not name:
        return None
    parts = [p for p in name.replace(".", ". ").split() if p.lower() not in _SUFFIXES]
    if len(parts) < 2:
        return None
    return f"{parts[0][0].lower()}.{parts[-1].lower().strip('.')}"


def normalize_passers(plays: pd.DataFrame) -> pd.DataFrame:
    """One row per pass/sack play with the dropback player's name, in game order."""
    mask = plays["play_type"].isin(_PASS_PLAY_TYPES)
    sub = plays.loc[mask, ["play_id", "game_id", "season", "play_seq", "offense", "period", "clock_s",
                           "play_type", "play_text", "ppa", "wallclock"]].copy()
    sub["passer"] = [passer_name(t, x) for t, x in zip(sub["play_type"], sub["play_text"])]
    sub["passer_key"] = sub["passer"].map(passer_key)
    return sub.dropna(subset=["passer_key"]).reset_index(drop=True)


SPECS = {s.name: s for s in cfb.ALL}


def build_all(client: CfbdClient, years: list[int]) -> dict[str, pd.DataFrame]:
    games = normalize_games(client, years)
    drives = normalize_drives(client, years, games)
    plays = normalize_plays(client, years)
    # About half of pre-2025 games have a few plays stamped on the wrong day (up to 24h early);
    # the rest of the game is fine, so drop only the impossible stamps.
    kickoff = plays["game_id"].map(games.set_index("game_id")["kickoff"])
    offset = plays["wallclock"] - kickoff
    plays.loc[(offset < pd.Timedelta(hours=-1)) | (offset > pd.Timedelta(hours=8)), "wallclock"] = pd.NaT
    lines = normalize_lines(client, years)
    passers = normalize_passers(plays)
    return {"cfb_games": games, "cfb_drives": drives, "cfb_plays": plays, "cfb_lines": lines,
            "cfb_passers": passers, "cfb_team_game_efficiency": team_game_efficiency(plays, games),
            **build_context(client, years, games)}

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["fetch", "publish", "build"])
    ap.add_argument("first", type=int)
    ap.add_argument("last", type=int)
    ap.add_argument("--refresh-current", type=int, default=None)
    ap.add_argument("--only", nargs="*", default=None, help="dataset names to publish (default: all)")
    args = ap.parse_args()
    years = list(range(args.first, args.last + 1))
    client = CfbdClient()

    if args.cmd == "fetch":
        fetch(client, years, refresh_current=args.refresh_current)
        fetch_context(client, years, refresh_current=args.refresh_current)
        print("remaining calls:", client.remaining_calls())
        return

    tables = build_all(client, years)
    for name, df in tables.items():
        print(f"{name}: {len(df):,} rows")
    if args.cmd == "publish":
        store = dataset_store()
        for name, df in tables.items():
            if args.only and name not in args.only:
                continue
            print(name, store.publish(SPECS[name], df))


if __name__ == "__main__":
    main()
