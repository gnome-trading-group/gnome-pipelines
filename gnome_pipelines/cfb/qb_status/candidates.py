"""Candidate quarterbacks for a team's next game, and their base weights before any news.

Only games that kicked off at least 24 hours before as_of count, because play-by-play lands a day after kickoff; that
keeps the candidates point-in-time. The player id is the play-by-play passer key, which equals the roster player id.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

LAST_STARTER_BASE = 0.80
PBP_DELAY = timedelta(hours=24)
COLUMNS = ["player_id", "passer_key", "base", "last_starter", "season_starter"]


def game_starters(passers: pd.DataFrame) -> pd.DataFrame:
    first = passers.dropna(subset=["passer_key"]).sort_values("play_seq", kind="stable")
    return first.groupby(["game_id", "offense"], as_index=False).first()[["game_id", "offense", "passer_key"]]


def candidates(team: str, team_id: int, season: int, as_of: datetime, passers: pd.DataFrame,
               games: pd.DataFrame) -> pd.DataFrame:
    kick = pd.to_datetime(games["kickoff"], utc=True)
    played = games[((games["home_id"] == team_id) | (games["away_id"] == team_id)) & (games["season"] == season)
                   & (kick < pd.Timestamp(as_of) - PBP_DELAY)]
    p = passers[passers["game_id"].isin(played["game_id"]) & (passers["offense"] == team)].dropna(subset=["passer_key"])
    if p.empty:
        return pd.DataFrame(columns=COLUMNS)
    dropbacks = p.groupby("passer_key").size()
    starts = game_starters(p).merge(played[["game_id", "kickoff"]], on="game_id")
    starts = starts.assign(kickoff=pd.to_datetime(starts["kickoff"], utc=True)).sort_values("kickoff")
    last = starts["passer_key"].iloc[-1]
    counts = starts.groupby("passer_key").agg(n=("game_id", "size"), latest=("kickoff", "max"))
    season_starter = counts.sort_values(["n", "latest"], ascending=False).index[0]
    others = dropbacks.drop(last, errors="ignore")
    rows = [{"player_id": str(last), "passer_key": str(last), "base": LAST_STARTER_BASE if others.sum() else 1.0}]
    total = others.sum()
    for key, n in others.items():
        rows.append({"player_id": str(key), "passer_key": str(key), "base": (1 - LAST_STARTER_BASE) * n / total})
    out = pd.DataFrame(rows)
    out["last_starter"] = out["player_id"] == str(last)
    out["season_starter"] = out["player_id"] == str(season_starter)
    return out[COLUMNS]


def next_game(team_id: int, as_of: datetime, games: pd.DataFrame) -> int | None:
    kick = pd.to_datetime(games["kickoff"], utc=True)
    upcoming = games[((games["home_id"] == team_id) | (games["away_id"] == team_id)) & (kick >= pd.Timestamp(as_of))
                     & ~games["completed"].fillna(False).astype(bool)]
    if upcoming.empty:
        return None
    return int(upcoming.loc[pd.to_datetime(upcoming["kickoff"], utc=True).idxmin(), "game_id"])
