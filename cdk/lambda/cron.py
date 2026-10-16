"""When an EventBridge Scheduler expression fires next (the service itself doesn't say).

cron(...): fields are minutes hours day-of-month month day-of-week year, in UTC as the schedules are. Supports
numbers, names (JAN, MON), *, ?, ranges, lists and steps; None for what it doesn't evaluate (L, W, #).

rate(...): a rate counts from when the schedule was last created, enabled or updated, so its next fire is that
moment plus a whole number of intervals.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}
# AWS numbers days of the week 1-7 from Sunday.
WEEKDAYS = {d: i for i, d in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"), 1)}
SEARCH_DAYS = 366 * 2


class Unsupported(ValueError):
    pass


RATE_UNITS = {"minute": 1, "minutes": 1, "hour": 60, "hours": 60, "day": 1440, "days": 1440}


def next_fire(expression: str, after: datetime, started: datetime | None) -> datetime | None:
    """The next fire after `after`; `started` is when the schedule last started counting, for a rate."""
    if expression.strip().startswith("rate("):
        return next_rate(expression, after, started) if started else None
    return next_run(expression, after)


def next_rate(expression: str, after: datetime, started: datetime) -> datetime | None:
    match = re.fullmatch(r"rate\((\d+) (\w+)\)", expression.strip())
    if not match or match.group(2) not in RATE_UNITS or int(match.group(1)) == 0:
        return None
    interval = timedelta(minutes=int(match.group(1)) * RATE_UNITS[match.group(2)])
    after, started = after.astimezone(timezone.utc), started.astimezone(timezone.utc)
    if after < started:
        return started
    return started + interval * ((after - started) // interval + 1)


def next_run(expression: str, after: datetime) -> datetime | None:
    match = re.fullmatch(r"cron\((.+)\)", expression.strip())
    if not match:
        return None
    fields = match.group(1).split()
    if len(fields) != 6:
        return None
    try:
        minutes = _values(fields[0], 0, 59)
        hours = _values(fields[1], 0, 23)
        days = _values(fields[2], 1, 31)
        months = _values(fields[3], 1, 12, MONTHS)
        weekdays = _values(fields[4], 1, 7, WEEKDAYS)
        years = _values(fields[5], 1970, 2199)
    except Unsupported:
        return None

    start = after.astimezone(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    day = start.replace(hour=0, minute=0)
    for _ in range(SEARCH_DAYS):
        aws_weekday = (day.weekday() + 1) % 7 + 1
        if day.year in years and day.month in months and day.day in days and aws_weekday in weekdays:
            for hour in sorted(hours):
                for minute in sorted(minutes):
                    candidate = day.replace(hour=hour, minute=minute)
                    if candidate >= start:
                        return candidate
        day += timedelta(days=1)
    return None


def _values(field: str, low: int, high: int, names: dict[str, int] | None = None) -> set[int]:
    if field in ("*", "?"):
        return set(range(low, high + 1))
    values: set[int] = set()
    for part in field.split(","):
        base, _, step = part.partition("/")
        if base == "*":
            first, last = low, high
        elif "-" in base:
            first, last = (_number(b, names) for b in base.split("-", 1))
        else:
            first = _number(base, names)
            last = high if step else first
        values.update(range(first, last + 1, int(step) if step else 1))
    return values


def _number(token: str, names: dict[str, int] | None) -> int:
    if names and token.upper() in names:
        return names[token.upper()]
    if not token.isdigit():
        raise Unsupported(token)
    return int(token)
