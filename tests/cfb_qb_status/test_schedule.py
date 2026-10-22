from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from gnome_pipelines.cfb.qb_status.schedule import (estimated_release_at, expected_releases, report_poll_interval,
                                                     schedule_rows)
from gnome_pipelines.datasets.qb_status import REPORT_SCHEDULE

ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def et(*args):
    return datetime(*args, tzinfo=ET)


def test_sec_saturday_initial_is_wednesday_8pm_et():
    r = expected_releases("SEC", et(2026, 10, 24, 19, 30))
    assert r["Initial"] == et(2026, 10, 21, 20, 0)
    assert r["Update 1"] == et(2026, 10, 22, 20, 0)
    assert r["Update 2"] == et(2026, 10, 23, 20, 0)
    assert r["Game Day"] == et(2026, 10, 24, 18, 0)


def test_b10_game_day_is_two_hours_before():
    assert expected_releases("B10", et(2026, 10, 24, 12, 0))["Game Day"] == et(2026, 10, 24, 10, 0)


def test_unknown_conference_has_no_expected_times():
    assert set(expected_releases("MAC", et(2026, 11, 4, 19, 0)).values()) == {None}
    assert estimated_release_at("ACC", et(2026, 10, 24, 12, 0), "Initial") is None


def test_interval_is_15s_inside_window_and_1800_outside():
    e = et(2026, 10, 21, 20, 0)
    assert report_poll_interval(e - timedelta(minutes=4), [e], [], []) == 15
    assert report_poll_interval(e + timedelta(minutes=19), [e], [], []) == 15
    assert report_poll_interval(e - timedelta(hours=3), [e], [], []) == 1800


def test_interval_is_60s_when_overdue():
    e = et(2026, 10, 21, 20, 0)
    assert report_poll_interval(e + timedelta(minutes=40), [e], [], [e]) == 60
    assert report_poll_interval(e + timedelta(hours=3), [e], [], [e]) == 1800


def test_unknown_conference_evening_window_is_5min():
    kick = et(2026, 11, 7, 12, 0)
    assert report_poll_interval(et(2026, 11, 5, 18, 0), [], [kick], []) == 300
    assert report_poll_interval(et(2026, 11, 5, 9, 0), [], [kick], []) == 1800
    assert report_poll_interval(et(2026, 11, 7, 10, 0), [], [kick], []) == 300


def test_dst_boundary():
    r = expected_releases("SEC", et(2026, 11, 3, 19, 0))
    assert r["Initial"].astimezone(ET).hour == 20
    assert r["Initial"] == datetime(2026, 11, 1, 0, 0, tzinfo=UTC)


def test_schedule_rows_states():
    kick = et(2026, 10, 24, 19, 30)
    now = et(2026, 10, 22, 21, 0)
    df = schedule_rows("SEC", 8, kick, {"Initial": et(2026, 10, 21, 20, 3)}, now)
    assert list(df.columns) == REPORT_SCHEDULE.schema.names
    states = dict(zip(df["report_slot"], df["state"]))
    assert states == {"Initial": "arrived", "Update 1": "overdue", "Update 2": "expected", "Game Day": "expected"}
