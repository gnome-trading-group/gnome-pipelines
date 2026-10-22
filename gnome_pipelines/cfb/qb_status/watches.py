"""Watches: teams with an open question about their QB, which get fast X and news polling until it is settled.

Opened by a game-night flag, a market alarm, a manual request, or an official Questionable / Doubtful / Out /
Game-Time Decision on a candidate. New evidence for the team extends a watch. It closes at kickoff, when an official
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
