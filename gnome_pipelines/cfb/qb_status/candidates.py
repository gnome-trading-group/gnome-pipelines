"""Candidate quarterbacks for a team's next game, and their base weights before any news.

Only games that kicked off at least 24 hours before as_of count, because play-by-play lands a day after kickoff; that
keeps the candidates point-in-time. candidates() works in play-by-play passer keys (first initial + "." + surname);
with_roster_ids() then swaps in roster player ids, so candidates line up with report and news evidence.

Base weights are how often each QB started the next game in FBS seasons 2016-24, with the service's own starter
definition (first eligible passer), measured by calibrate.py:
- the last starter is also the season starter: he starts next KEEP_RATE[streak] of the time, by consecutive starts;
- the last starter replaced the season starter `missed` games ago: the season starter comes back RETURN_RATE[missed]
  of the time and the last starter keeps it HOLD_RATE[missed].
Whatever is left goes to the other passers by dropbacks.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

import pandas as pd

KEEP_RATE = {1: 0.845, 2: 0.867, 3: 0.881, 4: 0.892, 5: 0.902, 6: 0.907, 7: 0.908, 8: 0.922}
RETURN_RATE = {1: 0.317, 2: 0.182, 3: 0.124}
HOLD_RATE = {1: 0.614, 2: 0.745, 3: 0.755}
# A passer the roster can't identify still counts with this many dropbacks; below it it's a trick play or a
# play-by-play parsing artifact (a team name read as the passer).
MIN_UNLINKED_DROPBACKS = 10
PBP_DELAY = timedelta(hours=24)
COLUMNS = ["player_id", "passer_key", "base", "last_starter", "season_starter", "streak", "missed", "changed",
           "starts"]


def game_starters(passers: pd.DataFrame) -> pd.DataFrame:
    first = passers.dropna(subset=["passer_key"]).sort_values("play_seq", kind="stable")
    return first.groupby(["game_id", "offense"], as_index=False).first()[["game_id", "offense", "passer_key"]]


def start_state(starters: list[str]) -> tuple[str, int, str, int]:
    """From a team's starters in order: the last starter, his run of consecutive starts, the season starter (most
    starts, the latest on a tie) and how many games in a row the season starter has not started."""
    last = starters[-1]
    streak = len(starters) - next((i + 1 for i in range(len(starters) - 1, -1, -1) if starters[i] != last), 0)
    n = pd.Series(starters).value_counts()
    latest = {k: i for i, k in enumerate(starters)}
    season_starter = max(n.index, key=lambda k: (n[k], latest[k]))
    return last, streak, season_starter, len(starters) - 1 - latest[season_starter]


def candidates(team: str, team_id: int, season: int, as_of: datetime, passers: pd.DataFrame,
               games: pd.DataFrame, eligible: Callable[[str], bool | None] | None = None) -> pd.DataFrame:
    """eligible(passer_key) says whether a passer is a QB: True keeps them, False drops them (a receiver's trick-play
    pass), None (unknown to the roster) keeps them only with MIN_UNLINKED_DROPBACKS dropbacks."""
    kick = pd.to_datetime(games["kickoff"], utc=True)
    played = games[((games["home_id"] == team_id) | (games["away_id"] == team_id)) & (games["season"] == season)
                   & (kick < pd.Timestamp(as_of) - PBP_DELAY)]
    p = passers[passers["game_id"].isin(played["game_id"]) & (passers["offense"] == team)].dropna(subset=["passer_key"])
    if eligible is not None and not p.empty:
        n = p.groupby("passer_key").size()
        ok = {k for k, c in n.items()
              if (e := eligible(str(k))) is True or (e is None and c >= MIN_UNLINKED_DROPBACKS)}
        p = p[p["passer_key"].isin(ok)]
    if p.empty:
        return pd.DataFrame(columns=COLUMNS)
    dropbacks = p.groupby("passer_key").size()
    starts = game_starters(p).merge(played[["game_id", "kickoff"]], on="game_id")
    starts = starts.assign(kickoff=pd.to_datetime(starts["kickoff"], utc=True)).sort_values("kickoff")
    seq = starts["passer_key"].tolist()
    last, streak, season_starter, missed = start_state(seq)
    if missed:
        fixed = {last: HOLD_RATE[min(missed, 3)], season_starter: RETURN_RATE[min(missed, 3)]}
    else:
        fixed = {last: KEEP_RATE[min(streak, 8)]}
    others = dropbacks.drop(list(fixed), errors="ignore")
    if not others.sum():
        total = sum(fixed.values())
        fixed = {k: v / total for k, v in fixed.items()}
    rest = 1 - sum(fixed.values())
    rows = [{"player_id": str(k), "passer_key": str(k), "base": v} for k, v in fixed.items()]
    rows += [{"player_id": str(k), "passer_key": str(k), "base": rest * n / others.sum()} for k, n in others.items()]
    out = pd.DataFrame(rows)
    out["last_starter"] = out["player_id"] == str(last)
    out["season_starter"] = out["player_id"] == str(season_starter)
    out["streak"] = streak
    latest = {str(k): i for i, k in enumerate(seq)}
    out["missed"] = [len(seq) - 1 - latest[k] if k in latest else 0 for k in out["player_id"]]
    out["starts"] = [sum(1 for s in seq if str(s) == k) for k in out["player_id"]]
    out["changed"] = bool(missed) or (streak == 1 and len(seq) > 1)
    return out[COLUMNS]


def with_roster_ids(cands: pd.DataFrame, team_id: int, index) -> pd.DataFrame:
    """Replace passer keys with roster player ids where the roster links them; unlinked keys stay as they are."""
    out = cands.copy()
    out["player_id"] = [index.player_for_key(team_id, k) or k for k in out["passer_key"]]
    return out


def next_game(team_id: int, as_of: datetime, games: pd.DataFrame) -> int | None:
    kick = pd.to_datetime(games["kickoff"], utc=True)
    upcoming = games[((games["home_id"] == team_id) | (games["away_id"] == team_id)) & (kick >= pd.Timestamp(as_of))
                     & ~games["completed"].fillna(False).astype(bool)]
    if upcoming.empty:
        return None
    return int(upcoming.loc[pd.to_datetime(upcoming["kickoff"], utc=True).idxmin(), "game_id"])
