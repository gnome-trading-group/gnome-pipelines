"""When each conference's availability report is expected, and how often to poll the feed around it.

Published rules: SEC initial report three nights before at 8pm ET, daily updates, game day at least 90 minutes before
kickoff; Big Ten the same evenings with game day at least 2 hours before; Big 12 game day 90 minutes before (its
evening times aren't published, so they use the unknown-conference window). ACC, American and MAC times are learned
from what the service observes.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from gnome_pipelines.datasets.qb_status import REPORT_SCHEDULE

ET = ZoneInfo("America/New_York")
SLOTS = ("Initial", "Update 1", "Update 2", "Game Day")
EVENING = time(20, 0)
EVENING_RULES = {"SEC", "B10"}
GAME_DAY_LEAD = {"SEC": timedelta(minutes=90), "B10": timedelta(minutes=120), "B12": timedelta(minutes=90)}

WINDOW_BEFORE, WINDOW_AFTER, OVERDUE_FOR = timedelta(minutes=5), timedelta(minutes=20), timedelta(hours=2)
FAST, OVERDUE, UNKNOWN, BASELINE = 15.0, 60.0, 300.0, 1800.0


def expected_releases(conference: str, kickoff: datetime) -> dict[str, datetime | None]:
    kick = kickoff.astimezone(ET)
    out: dict[str, datetime | None] = {s: None for s in SLOTS}
    if conference in EVENING_RULES:
        for days, slot in ((3, "Initial"), (2, "Update 1"), (1, "Update 2")):
            out[slot] = datetime.combine(kick.date() - timedelta(days=days), EVENING, tzinfo=ET)
    if conference in GAME_DAY_LEAD:
        out["Game Day"] = kick - GAME_DAY_LEAD[conference]
    return out


def estimated_release_at(conference: str, kickoff: datetime, slot: str) -> datetime | None:
    return expected_releases(conference, kickoff).get(slot)


def _in_unknown_window(now: datetime, kickoff: datetime) -> bool:
    n, k = now.astimezone(ET), kickoff.astimezone(ET)
    if k - timedelta(hours=3) <= n <= k:
        return True
    days_before = (k.date() - n.date()).days
    return 1 <= days_before <= 3 and time(17, 0) <= n.time() <= time(23, 0)


def report_poll_interval(now: datetime, expectations: list[datetime], unknown_games: list[datetime],
                         overdue: list[datetime]) -> float:
    if any(e - WINDOW_BEFORE <= now <= e + WINDOW_AFTER for e in expectations):
        return FAST
    if any(o + WINDOW_AFTER < now <= o + OVERDUE_FOR for o in overdue):
        return OVERDUE
    if any(_in_unknown_window(now, k) for k in unknown_games):
        return UNKNOWN
    return BASELINE


def schedule_rows(conference: str, game_id: int, kickoff: datetime, observed: dict[str, datetime],
                  now: datetime) -> pd.DataFrame:
    rows = []
    for slot, expected in expected_releases(conference, kickoff).items():
        seen = observed.get(slot)
        if seen is not None:
            state = "arrived"
        elif expected is not None and now > expected + WINDOW_AFTER:
            state = "overdue"
        else:
            state = "expected"
        rows.append({"conference": conference, "game_id": game_id, "report_slot": slot, "expected_at": expected,
                     "observed_at": seen, "state": state})
    return pd.DataFrame(rows, columns=REPORT_SCHEDULE.schema.names)
