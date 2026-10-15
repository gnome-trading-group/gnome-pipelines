from datetime import datetime, timezone

import pytest

from cron import next_run


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.parametrize("expression, after, expected", [
    # hltv_cs2.weekly from a Thursday: the coming Monday at 06:00.
    ("cron(0 6 ? * MON *)", utc(2026, 10, 15, 13, 0), utc(2026, 10, 19, 6, 0)),
    # On the Monday itself, before and exactly at the time.
    ("cron(0 6 ? * MON *)", utc(2026, 10, 19, 5, 59), utc(2026, 10, 19, 6, 0)),
    ("cron(0 6 ? * MON *)", utc(2026, 10, 19, 6, 0), utc(2026, 10, 26, 6, 0)),
    # cs2_prematch_train.monthly across a year end.
    ("cron(0 7 1 * ? *)", utc(2026, 12, 15), utc(2027, 1, 1, 7, 0)),
    ("cron(0/30 * * * ? *)", utc(2026, 10, 15, 13, 10), utc(2026, 10, 15, 13, 30)),
    ("cron(15 9-17 ? * MON-FRI *)", utc(2026, 10, 16, 17, 30), utc(2026, 10, 19, 9, 15)),
    ("cron(0 0 1 JAN,JUL ? 2027)", utc(2026, 10, 15), utc(2027, 1, 1)),
    # AWS numbers weekdays from Sunday = 1.
    ("cron(0 12 ? * 1 *)", utc(2026, 10, 15), utc(2026, 10, 18, 12, 0)),
])
def test_next_run(expression, after, expected):
    assert next_run(expression, after) == expected


@pytest.mark.parametrize("expression", ["rate(1 day)", "cron(0 12 L * ? *)", "cron(0 12 ? * 6#3 *)", "cron(bad)"])
def test_what_it_cant_evaluate_is_none(expression):
    assert next_run(expression, utc(2026, 10, 15)) is None
