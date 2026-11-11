import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.datasets.polling import POLL_HEALTH
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Meter, Service, Task

T0 = datetime(2026, 10, 22, 0, 0, tzinfo=timezone.utc)


@dataclass
class Item:
    key: str
    first_seen_at: datetime


class Health(PollHealth):
    def __init__(self):
        self.rows = []
        super().__init__("test", publish=lambda df: self.rows.extend(df.to_dict("records")))

    def kinds(self, *ignore):
        return [r["kind"] for r in self.rows if r["kind"] not in ignore]


def quiet(rows):
    return [r for r in rows if r["kind"] != "heartbeat"]


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_a_failing_task_records_an_error_and_the_others_still_run():
    h, ran = Health(), []

    def boom(now):
        raise ValueError("feed down")

    def ok(now):
        ran.append(now)
        return []
    Service([Task("bad", lambda now: 60, boom), Task("good", lambda now: 60, ok)], h).step(T0)
    assert ran == [T0]
    (err,) = quiet(h.rows)
    assert err["kind"] == "task_error" and err["subject"] == "bad" and "bad: ValueError: feed down" in err["detail"]


def test_after_a_sleep_gap_every_task_runs_and_records_are_stamped_with_the_wake_time():
    h, seen, kept = Health(), [], []

    def report(now):
        seen.append(now)
        return [Item(f"r{len(seen)}", now - timedelta(minutes=5))]
    svc = Service([Task("reports", lambda now: 1800, report), Task("fast", lambda now: 30, lambda now: [])], h,
                  store_new=lambda found: kept.extend(found) or found)
    svc.step(T0)
    wake = T0 + timedelta(hours=5)
    svc.step(wake)
    assert seen == [T0, wake] and kept[-1].first_seen_at == wake
    (gap,) = [r for r in h.rows if r["kind"] == "gap"]
    assert gap["value"] == 5 * 3600


def test_on_new_sees_only_what_the_store_had_not_seen():
    calls = []
    svc = Service([Task("t", lambda now: 60, lambda now: [Item("a", now), Item("b", now)])], Health(),
                  store_new=lambda found: [i for i in found if i.key == "a"],
                  on_new=lambda new, now: calls.append([i.key for i in new]))
    svc.step(T0)
    svc.step(T0 + timedelta(seconds=61))
    assert calls == [["a"], ["a"]]


def test_nothing_new_means_no_on_new_call():
    calls = []
    svc = Service([Task("t", lambda now: 60, lambda now: [])], Health(), on_new=lambda new, now: calls.append(new))
    svc.step(T0)
    assert calls == []


def test_a_clock_jump_inside_a_task_restamps_its_records_and_records_a_gap():
    h, clock, kept = Health(), Clock(T0), []

    def sleepy(now):
        clock.t = now + timedelta(hours=8)
        return [Item("late", now)]
    Service([Task("reports", lambda now: 1800, sleepy)], h, store_new=lambda f: kept.extend(f) or f,
            clock=clock).step(T0)
    assert kept[0].first_seen_at == T0 + timedelta(hours=8)
    (gap,) = [r for r in h.rows if r["kind"] == "gap"]
    assert gap["value"] == 8 * 3600 and gap["subject"] == "reports"


def test_a_failed_task_retries_soon_with_backoff():
    runs = []

    def flaky(now):
        runs.append(now)
        raise ConnectionError("offline")
    svc = Service([Task("reports", lambda now: 1800, flaky)], Health())
    for s in (0, 16, 30, 47):
        svc.step(T0 + timedelta(seconds=s))
    assert runs == [T0, T0 + timedelta(seconds=16), T0 + timedelta(seconds=47)]


def test_an_on_new_error_is_recorded_not_raised():
    h = Health()

    def broken(new, now):
        raise KeyError("missing column")
    Service([Task("t", lambda now: 60, lambda now: [Item("a", now)])], h, on_new=broken).step(T0)
    assert "on_new: KeyError" in quiet(h.rows)[0]["detail"]


def test_heartbeats_count_errors_and_come_every_ten_minutes():
    h = Health()
    fail = [True]

    def task(now):
        if fail[0]:
            raise ValueError("x")
        return []
    svc = Service([Task("t", lambda now: 60, task)], h)
    svc.step(T0)
    fail[0] = False
    svc.step(T0 + timedelta(minutes=5))
    svc.step(T0 + timedelta(minutes=10))
    beats = [r for r in h.rows if r["kind"] == "heartbeat"]
    assert [b["at"] for b in beats] == [pd.Timestamp(T0), pd.Timestamp(T0 + timedelta(minutes=10))]
    assert "1 errors" in beats[0]["detail"] and "0 errors" in beats[1]["detail"]
    assert beats[1]["value"] == 600


def test_meters_report_spend_hourly_and_a_source_without_meters_reports_none():
    h, spent = Health(), [0.5]
    svc = Service([Task("t", lambda now: 60, lambda now: [])], h,
                  meters=[Meter("claude:haiku", lambda: spent[0]), Meter("x", lambda: 2.0, "month to date")])
    svc.step(T0)
    spent[0] = 0.75
    svc.step(T0 + timedelta(minutes=30))
    svc.step(T0 + timedelta(hours=1))
    spend = [(r["subject"], r["value"], r["detail"]) for r in h.rows if r["kind"] == "spend"]
    assert spend == [("claude:haiku", 0.5, "since start"), ("x", 2.0, "month to date"),
                     ("claude:haiku", 0.75, "since start"), ("x", 2.0, "month to date")]
    plain = Health()
    Service([Task("t", lambda now: 60, lambda now: [])], plain).step(T0)
    assert "spend" not in plain.kinds()


def fast(**kw):
    # Real threads and real time, with short intervals; the watcher's tick is skipped.
    return dict(sleep=lambda s: time.sleep(0.01), **kw)


def eventually(condition, within=5.0):
    # Threads on a loaded machine run late; wait for the condition instead of counting in a fixed window.
    end = time.monotonic() + within
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def test_run_until_returns_at_the_deadline_and_runs_each_task_on_its_cadence():
    runs = []
    svc = Service([Task("a", lambda now: 0.05, lambda now: runs.append(now) or [])], Health(), **fast())
    svc.run_until(datetime.now(timezone.utc) + timedelta(seconds=0.5))
    # At most one run per 50 ms interval (plus the first), and more than one: it repeats, on its cadence.
    assert 2 <= len(runs) <= 12
    assert all((b - a).total_seconds() >= 0.045 for a, b in zip(runs, runs[1:]))


def test_a_slow_task_does_not_hold_up_a_fast_one_in_another_lane():
    fast_runs, release = [], threading.Event()

    def slow(now):
        release.wait(5)
        return []
    svc = Service([Task("slow", lambda now: 60, slow), Task("fast", lambda now: 0.02, lambda now: fast_runs.append(now) or [])],
                  Health(), **fast())
    worker = threading.Thread(target=svc.run_until, args=(datetime.now(timezone.utc) + timedelta(seconds=5),))
    worker.start()
    assert eventually(lambda: len(fast_runs) >= 5), "the fast lane kept polling while the slow task was stuck"
    release.set()
    svc.stop()
    worker.join(5)
    assert not worker.is_alive()


def test_tasks_in_one_lane_never_overlap():
    active, overlaps = [0], []
    lock = threading.Lock()

    def task(now):
        with lock:
            active[0] += 1
            overlaps.append(active[0])
        time.sleep(0.01)
        with lock:
            active[0] -= 1
        return []
    tasks = [Task(f"t{i}", lambda now: 0.01, task, lane="shared") for i in range(4)]
    Service(tasks, Health(), **fast()).run_until(datetime.now(timezone.utc) + timedelta(seconds=0.3))
    assert overlaps and max(overlaps) == 1


def test_records_from_concurrent_lanes_reach_the_store_one_lane_at_a_time():
    inside, worst = [0], [0]

    def store_new(found):
        inside[0] += 1
        worst[0] = max(worst[0], inside[0])
        time.sleep(0.005)
        inside[0] -= 1
        return found
    tasks = [Task(f"t{i}", lambda now: 0.01, lambda now, i=i: [Item(str(i), now)]) for i in range(5)]
    Service(tasks, Health(), store_new=store_new, **fast()).run_until(
        datetime.now(timezone.utc) + timedelta(seconds=0.3))
    assert worst[0] == 1


def test_stop_lets_each_lane_finish_its_current_call():
    finished, started = [], threading.Event()

    def busy(now):
        started.set()
        time.sleep(0.2)
        finished.append(now)
        return []
    svc = Service([Task("busy", lambda now: 60, busy)], Health(), **fast())
    worker = threading.Thread(target=svc.run_forever)
    worker.start()
    started.wait(2)
    svc.stop()
    worker.join(5)
    assert not worker.is_alive() and len(finished) == 1


def test_a_lane_survives_an_interval_function_that_raises():
    h, runs, calls = Health(), [], [0]

    def interval(now):
        calls[0] += 1
        if calls[0] == 1:
            raise ValueError("bad interval")
        return 0.02
    svc = Service([Task("t", interval, lambda now: runs.append(now) or [])], h, **fast())
    worker = threading.Thread(target=svc.run_forever, daemon=True)
    worker.start()
    assert eventually(lambda: len(runs) >= 3)
    svc.stop()
    worker.join(5)
    assert any(r["kind"] == "task_error" for r in h.rows)


def test_health_rows_land_in_poll_health_and_a_failed_write_does_not_raise(tmp_path):
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path))
    h = PollHealth("rt_reviews", store)
    h.record("task_error", T0, "variety: HTTPError 503", subject="variety")
    got = store.load(POLL_HEALTH.name)
    assert got[["source", "kind", "subject"]].values.tolist() == [["rt_reviews", "task_error", "variety"]]

    def down(df):
        raise ConnectionError("catalog down")
    PollHealth("rt_reviews", publish=down).record("gap", T0, "x")


def test_a_call_that_hangs_is_reported_as_a_stall_and_then_ends_the_process():
    h, hung, release = Health(), threading.Event(), threading.Event()

    def stuck(now):
        release.wait(5)
        return []
    svc = Service([Task("stuck", lambda now: 60, stuck), Task("ok", lambda now: 0.02, lambda now: [])], h,
                  task_timeout=0.1, on_hang=lambda: (hung.set(), svc.stop()), **fast())
    svc.run_until(datetime.now(timezone.utc) + timedelta(seconds=3))
    release.set()
    kinds = [(r["kind"], r["subject"]) for r in h.rows if r["kind"] in ("stall", "hung")]
    assert kinds == [("stall", "stuck"), ("hung", "stuck")] and hung.is_set()


def test_a_record_without_first_seen_at_fails_its_task_and_none_is_stamped():
    h, kept = Health(), []
    svc = Service([Task("dicts", lambda now: 60, lambda now: [{"url": "x"}]),
                   Task("unset", lambda now: 60, lambda now: [Item("a", None)])], h,
                  store_new=lambda found: kept.extend(found) or found)
    svc.step(T0)
    assert [r["subject"] for r in quiet(h.rows)] == ["dicts"] and "without first_seen_at" in quiet(h.rows)[0]["detail"]
    assert [i.key for i in kept] == ["a"] and kept[0].first_seen_at == T0


def test_a_hang_while_handing_records_to_the_store_is_caught_too():
    hung, release = threading.Event(), threading.Event()

    def stuck_store(found):
        release.wait(5)
        return found
    svc = Service([Task("t", lambda now: 60, lambda now: [Item("a", now)], timeout=0.1)], Health(),
                  store_new=stuck_store, on_hang=lambda: (hung.set(), svc.stop()), **fast())
    svc.run_until(datetime.now(timezone.utc) + timedelta(seconds=3))
    release.set()
    assert hung.is_set()


def test_the_watchdog_acts_even_when_health_writes_are_stuck():
    hung, release = threading.Event(), threading.Event()

    class StuckHealth(Health):
        def __init__(self):
            super().__init__()
            self._publish = lambda df: release.wait(10)
    svc = Service([Task("t", lambda now: 60, lambda now: release.wait(10) or [], timeout=0.1)], StuckHealth(),
                  on_hang=lambda: (hung.set(), svc.stop()), **fast())
    worker = threading.Thread(target=svc.run_until, args=(datetime.now(timezone.utc) + timedelta(seconds=8),),
                              daemon=True)
    worker.start()
    # It gives the "hung" row HUNG_ROW_WAIT to land, then acts regardless.
    assert hung.wait(8), "the watchdog was held up by a health write"
    release.set()


def test_a_pause_of_the_whole_process_restarts_the_clock_on_calls_in_flight():
    offset, hung, release = [timedelta()], threading.Event(), threading.Event()
    started = threading.Event()

    def clock():
        return datetime.now(timezone.utc) + offset[0]

    def call(now):
        started.set()
        release.wait(5)
        return []
    svc = Service([Task("t", lambda now: 60, call, timeout=1.0)], Health(), clock=clock,
                  on_hang=hung.set, **fast())
    worker = threading.Thread(target=svc.run_until, args=(clock() + timedelta(hours=1),), daemon=True)
    worker.start()
    started.wait(2)
    offset[0] = timedelta(minutes=10)
    time.sleep(1.0)
    assert not hung.is_set(), "a laptop sleep is not a hang"
    release.set()
    svc.stop()
    worker.join(5)


def test_on_stop_runs_once_the_lanes_have_finished_and_its_error_is_recorded():
    order = []
    svc = Service([Task("t", lambda now: 60, lambda now: order.append("task") or [])], Health(),
                  on_stop=lambda: order.append("on_stop"), **fast())
    svc.run_until(datetime.now(timezone.utc) + timedelta(seconds=0.2))
    assert order == ["task", "on_stop"]
    h = Health()

    def broken():
        raise ValueError("flush failed")
    Service([Task("t", lambda now: 60, lambda now: [])], h, on_stop=broken, **fast()).run_until(
        datetime.now(timezone.utc) + timedelta(seconds=0.1))
    assert any("on_stop: ValueError: flush failed" in r["detail"] for r in h.rows)
