"""Market alarm: Kalshi moved on a game and our QB status didn't, which usually means news we haven't read.

An alarm opens watches on both teams and is logged; if no status change follows within MISSED_AFTER it becomes a
missed-news candidate in the health table.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd

from gnome_pipelines.cfb.qb_status.crosswalk import TeamIndex

EVENTS = "https://api.elections.kalshi.com/trade-api/v2/events"
SERIES = "KXNCAAFGAME"
MOVE, WINDOW, QUIET, MISSED_AFTER = 0.15, timedelta(minutes=60), timedelta(hours=6), timedelta(minutes=30)
MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV",
                                      "DEC"), start=1)}


def _logit(p: float) -> float:
    p = min(max(p, 1e-4), 1 - 1e-4)
    return math.log(p / (1 - p))


def _mid(m: dict) -> float | None:
    try:
        b, a = float(m.get("yes_bid_dollars") or 0), float(m.get("yes_ask_dollars") or 0)
    except ValueError:
        return None
    return (b + a) / 2 if 0 < b < a < 1 else None


def _event_date(ticker: str) -> date | None:
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", ticker or "")
    if not m or m.group(2) not in MONTHS:
        return None
    return date(2000 + int(m.group(1)), MONTHS[m.group(2)], int(m.group(3)))


def kalshi_home_probs(events: list[dict], games: pd.DataFrame, teams: TeamIndex) -> dict[int, float]:
    kick = pd.to_datetime(games["kickoff"], utc=True).dt.date
    out: dict[int, float] = {}
    for ev in events:
        day = _event_date(ev.get("event_ticker", ""))
        if day is None:
            continue
        season = day.year if day.month >= 7 else day.year - 1
        mids = {}
        for m in ev.get("markets", []):
            tid = teams.team_id(m.get("yes_sub_title", ""), season)
            if tid is not None:
                mids[tid] = _mid(m)
        if len(mids) != 2:
            continue
        a, b = list(mids)
        near = games[(kick - day).abs() <= timedelta(days=1)]
        g = near[((near["home_id"] == a) & (near["away_id"] == b)) | ((near["home_id"] == b) & (near["away_id"] == a))]
        if len(g) != 1:
            continue
        home, away = int(g["home_id"].iloc[0]), int(g["away_id"].iloc[0])
        mh, ma = mids[home], mids[away]
        if mh is None and ma is None:
            continue
        out[int(g["game_id"].iloc[0])] = (mh + 1 - ma) / 2 if mh is not None and ma is not None else (
            mh if mh is not None else 1 - ma)
    return out


@dataclass
class Alarm:
    game_id: int
    at: datetime
    move: float


def alarms(history: pd.DataFrame, last_change: dict[int, datetime], now: datetime,
           kickoffs: dict[int, datetime] | None = None) -> list[Alarm]:
    """Pre-kickoff moves only: once a game starts, its price moves on every play, which is not QB news."""
    if history.empty:
        return []
    h = history.assign(at=pd.to_datetime(history["at"], utc=True))
    h = h[h["at"] <= pd.Timestamp(now)]
    out = []
    for game_id, g in h.groupby("game_id"):
        kick = (kickoffs or {}).get(int(game_id))
        if kick is not None and pd.Timestamp(now) >= pd.Timestamp(kick):
            continue
        g = g.sort_values("at")
        changed = last_change.get(int(game_id))
        if changed is not None and pd.Timestamp(now) - pd.Timestamp(changed) < QUIET:
            continue
        now_logit = _logit(float(g["p_home"].iloc[-1]))
        earlier = g[(g["at"] >= pd.Timestamp(now) - WINDOW) & (g["at"] < g["at"].iloc[-1])]
        if earlier.empty:
            continue
        move = max(abs(now_logit - _logit(float(p))) for p in earlier["p_home"])
        if move >= MOVE:
            out.append(Alarm(int(game_id), now, move))
    return out


def missed_news(alarm_list: list[Alarm], changes: dict[int, datetime], now: datetime) -> list[Alarm]:
    out = []
    for a in alarm_list:
        if pd.Timestamp(now) < pd.Timestamp(a.at) + MISSED_AFTER:
            continue
        c = changes.get(a.game_id)
        if c is not None and pd.Timestamp(a.at) <= pd.Timestamp(c) <= pd.Timestamp(a.at) + MISSED_AFTER:
            continue
        out.append(a)
    return out
