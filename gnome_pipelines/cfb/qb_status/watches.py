"""Watches: teams with an open question about their QB, which get fast X and news polling until it is settled.

Opened by a game-night flag, a market alarm, a manual request, an official Questionable / Doubtful / Out /
Game-Time Decision on a candidate, or an unsettled starter: no QB at UNSETTLED_TOP or better within UNSETTLED_WINDOW
of kickoff, so the team's beat writers are searched for who will start. New evidence for the team extends a watch. It closes at kickoff, when an official
game-day status arrives, or after WATCH_HOURS without new evidence.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id
from gnome_pipelines.datasets.qb_status import WATCHES

WATCH_HOURS = 24
ALWAYS_OPEN = {"pbp_flag": "pbp_flag", "market_alarm": "market_alarm", "manual": "manual"}
WORRYING = {"questionable", "doubtful", "out", "game_time_decision"}
# Between the measured chance a new starter keeps the job (HOLD_RATE, at most 0.755) and an established one's
# (KEEP_RATE, at least 0.845): a team whose last start was a QB change is unsettled until news settles it.
UNSETTLED_TOP = 0.80
UNSETTLED_WINDOW = timedelta(days=3)


def opens_watch(ev: Evidence, candidate_ids: set[str] | None) -> bool:
    if ev.source_type in ALWAYS_OPEN:
        return True
    if ev.source_type == "official_report" and ev.status in WORRYING and ev.report_slot != "Game Day":
        return candidate_ids is None or ev.player_id in candidate_ids
    return False


TIME_COLUMNS = ("opened_at", "expires_at", "closed_at")


def _typed(w: pd.DataFrame) -> pd.DataFrame:
    for c in TIME_COLUMNS:
        w[c] = pd.to_datetime(w[c], utc=True)
    return w


def _open(w: pd.DataFrame, now: datetime) -> pd.Series:
    return w["closed_at"].isna() & (pd.to_datetime(w["expires_at"], utc=True) > pd.Timestamp(now))


def update_watches(watches: pd.DataFrame, new: list[Evidence], kickoffs: dict[int, datetime], now: datetime,
                   candidates: dict[int, set[str]] | None = None) -> pd.DataFrame:
    w = _typed(watches.copy() if len(watches) else pd.DataFrame(columns=WATCHES.schema.names))
    window = timedelta(hours=WATCH_HOURS)
    for ev in sorted(new, key=lambda e: e.first_seen_at):
        if ev.team_id is None:
            continue
        live = _open(w, ev.first_seen_at) & (w["team_id"] == ev.team_id) if len(w) else pd.Series(dtype=bool)
        if len(w) and live.any():
            idx = w.index[live]
            new_expiry = pd.Timestamp(ev.first_seen_at + window)
            w.loc[idx, "expires_at"] = [max(pd.Timestamp(x), new_expiry) for x in w.loc[idx, "expires_at"]]
            if ev.source_type == "official_report" and ev.report_slot == "Game Day":
                w.loc[idx, "closed_at"] = pd.Timestamp(ev.first_seen_at)
                w.loc[idx, "close_reason"] = "official_game_day"
        elif opens_watch(ev, (candidates or {}).get(ev.team_id) if candidates is not None else None):
            row = {"watch_id": evidence_id("watch", ev.team_id, ev.evidence_id), "team_id": ev.team_id,
                   "game_id": ev.game_id, "reason": ALWAYS_OPEN.get(ev.source_type, "official_status"),
                   "opened_at": pd.Timestamp(ev.first_seen_at), "expires_at": pd.Timestamp(ev.first_seen_at + window),
                   "closed_at": pd.NaT, "close_reason": None}
            w = _typed(pd.concat([w, pd.DataFrame([row])], ignore_index=True) if len(w) else pd.DataFrame([row]))
    if len(w):
        is_open = w["closed_at"].isna()
        for i in w.index[is_open]:
            game = w.at[i, "game_id"]
            kick = kickoffs.get(int(game)) if pd.notna(game) else None
            if kick is not None and pd.Timestamp(now) >= pd.Timestamp(kick):
                w.at[i, "closed_at"], w.at[i, "close_reason"] = pd.Timestamp(kick), "kickoff"
            elif pd.Timestamp(now) > pd.to_datetime(w.at[i, "expires_at"], utc=True):
                w.at[i, "closed_at"], w.at[i, "close_reason"] = pd.to_datetime(w.at[i, "expires_at"], utc=True), "expired"
    return w[WATCHES.schema.names].reset_index(drop=True)


def watched_teams(watches: pd.DataFrame, now: datetime) -> set[int]:
    if watches.empty:
        return set()
    return {int(t) for t in watches.loc[_open(watches, now), "team_id"]}


def is_unsettled(top_p: float, qb_change: bool) -> bool:
    """A team's starter is up in the air after a QB change (until news settles it) or with no clear favourite."""
    return qb_change or top_p < UNSETTLED_TOP


def unsettled_watches(watches: pd.DataFrame, unsettled: set[tuple[int, int]], kickoffs: dict[int, datetime],
                      now: datetime) -> pd.DataFrame:
    """Open a watch for each unsettled (game_id, team_id) with kickoff inside UNSETTLED_WINDOW, unless the team is
    already watched. It runs until kickoff."""
    w = _typed(watches.copy() if len(watches) else pd.DataFrame(columns=WATCHES.schema.names))
    open_teams = watched_teams(w, now) if len(w) else set()
    rows = []
    for game, team in sorted(unsettled):
        kick = kickoffs.get(int(game))
        if (kick is None or int(team) in open_teams
                or not pd.Timestamp(now) <= pd.Timestamp(kick) <= pd.Timestamp(now) + UNSETTLED_WINDOW):
            continue
        rows.append({"watch_id": evidence_id("watch", int(team), "unsettled", int(game), pd.Timestamp(now).isoformat()),
                     "team_id": int(team), "game_id": int(game), "reason": "unsettled", "opened_at": pd.Timestamp(now),
                     "expires_at": pd.Timestamp(kick), "closed_at": pd.NaT, "close_reason": None})
        open_teams.add(int(team))
    if rows:
        w = _typed(pd.concat([w, pd.DataFrame(rows)], ignore_index=True) if len(w) else pd.DataFrame(rows))
    return w[WATCHES.schema.names].reset_index(drop=True)
