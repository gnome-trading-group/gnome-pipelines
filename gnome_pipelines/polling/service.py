"""The Service loop: every task of a source on its own cadence, concurrently, stall and sleep aware.

run_until() runs each lane on its own thread: a task's lane is its name unless it names a shared one, and tasks in
a shared lane run one after another (for code that keeps state no other thread may touch). A slow outlet or a long
model call therefore delays only its own lane. Records a task returns are stamped with a clock read taken after the
task returns (the true capture time), then handed, one lane at a time, to the source's store (which returns the ones
it hadn't seen) and to its on_new hook. If a task takes longer than task_timeout, or the process stops being
scheduled for a while (a laptop slept, the network stalled), a gap is recorded and every task runs again; whatever
that finds is stamped with the wake time, never backdated. Failed tasks retry after RETRY_BASE seconds, doubling, up
to their normal interval. step() runs every due task once on the calling thread, for tests and one-off passes.

A call that never returns (a socket read with no timeout, a commit that never completes) would leave its lane silent
while heartbeats still look healthy, so a watchdog thread times every call in flight, handing its records over
included: past its task's timeout (Task.timeout, else the Service's) it records a stall, and past HANG_FACTOR times
that it calls on_hang, which by default exits the process non-zero so the source's run is started again. The
watchdog never waits on a health write (the hung call may be one), and a pause of the whole process (a laptop
sleeping) restarts the clock on the calls in flight rather than counting as a hang. Every record a task returns must have a first_seen_at attribute (None is fine: it
is stamped); anything else fails the task rather than reaching the store unstamped.
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence

from gnome_pipelines.polling.health import PollHealth

HEARTBEAT_EVERY = 600.0
SPEND_EVERY = 3600.0
# How often an idle lane rechecks for a stop or a rerun, and how often the watcher looks for clock jumps.
TICK = 1.0
# How long stop waits, in all, for the lanes to finish the calls they are in. A source's task gets 60 s from SIGTERM
# to exit (cdk/lib/source-task.ts), and serve still pushes its state after this.
JOIN_TIMEOUT = 40.0
# A call running HANG_FACTOR x its timeout is taken to be hung.
HANG_FACTOR = 3
# How long the watchdog lets the "hung" row try to land before exiting.
HUNG_ROW_WAIT = 5.0
# The watcher wakes every TICK; a wait this much longer means the whole process was paused (a laptop slept).
PAUSE_GAP = 60.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Task:
    name: str
    interval: Callable[[datetime], float]
    run: Callable[[datetime], list]
    lane: str | None = None
    # Seconds a call may run before it is a stall (and HANG_FACTOR times that, a hang); None takes the Service's.
    timeout: float | None = None

    @property
    def lane_name(self) -> str:
        return self.lane or self.name


@dataclass(frozen=True)
class Meter:
    """Money a source spends on a paid service, reported to poll_health hourly. basis says what usd() covers."""

    name: str
    usd: Callable[[], float]
    basis: str = "since start"


def _keep_all(found: list) -> list:
    return found


def _exit_hung() -> None:
    # A hung thread can't be stopped from Python; exiting is the only way out, and the status Lambda starts it again.
    os._exit(1)


class Service:
    RETRY_BASE = 15.0

    def __init__(self, tasks: list[Task], health: PollHealth, store_new: Callable[[list], list] = _keep_all,
                 on_new: Callable[[list, datetime], None] | None = None, meters: Sequence[Meter] = (),
                 clock: Callable[[], datetime] | None = None, sleep: Callable[[float], None] = time.sleep,
                 task_timeout: float = 180.0, heartbeat_every: float = HEARTBEAT_EVERY,
                 spend_every: float = SPEND_EVERY, on_hang: Callable[[], None] = _exit_hung,
                 on_stop: Callable[[], object] | None = None) -> None:
        self.tasks, self.health, self.store_new, self.on_new = tasks, health, store_new, on_new
        self.meters, self.clock, self.sleep, self.task_timeout = tuple(meters), clock, sleep, task_timeout
        self.heartbeat_every, self.spend_every, self.on_hang = heartbeat_every, spend_every, on_hang
        # Runs once run_until's lanes have finished (or the stop deadline passed): a source's last writes.
        self.on_stop = on_stop
        # Each lane's call in flight: (task name, started, timeout); and the calls already reported as stalled.
        self.in_flight: dict[str, tuple[str, datetime, float]] = {}
        self._stalled: set[tuple[str, datetime]] = set()
        self.next_due: dict[str, datetime] = {}
        self.failures: dict[str, int] = {}
        self.last_step: datetime | None = None
        self.started: datetime | None = None
        self.errors_since_beat = 0
        self.next_beat: datetime | None = None
        self.next_spend: datetime | None = None
        self.stopping = False
        # Guards next_due, failures and the error count; lanes wait on it for their next task, a stop or a rerun.
        self._cond = threading.Condition()
        # One lane at a time hands records to the store and on_new, so the source's tables see them in order.
        self._deliver = threading.Lock()

    def _time(self, fallback: datetime) -> datetime:
        return self.clock() if self.clock is not None else fallback

    def stop(self) -> None:
        """Ends run_until/run_forever once every lane finishes the call it is in (SIGTERM on a stop or redeploy)."""
        with self._cond:
            self.stopping = True
            self._cond.notify_all()

    def rerun_all(self) -> None:
        with self._cond:
            self.next_due = {}
            self._cond.notify_all()

    def _smallest_interval(self, now: datetime) -> float:
        return min(t.interval(now) for t in self.tasks)

    def _timeout(self, t: Task) -> float:
        return t.timeout if t.timeout is not None else self.task_timeout

    def _run_task(self, t: Task, now: datetime) -> bool:
        """Runs t once; True when it took so long that every task should run again."""
        before = self._time(now)
        with self._cond:
            self.in_flight[t.lane_name] = (t.name, before, self._timeout(t))
        try:
            return self._run_and_hand_over(t, before, now)
        finally:
            with self._cond:
                self.in_flight.pop(t.lane_name, None)

    def _run_and_hand_over(self, t: Task, before: datetime, now: datetime) -> bool:
        try:
            found = t.run(before) or []
            after = self._time(now)
            took = (after - before).total_seconds()
            slow = took > self._timeout(t)
            if slow:
                self.health.record("gap", after, f"{t.name} took {took:.0f}s (sleep or stall)", took, t.name)
            for r in found:
                if not hasattr(r, "first_seen_at"):
                    raise TypeError(f"returned a {type(r).__name__} without first_seen_at; nothing was stored")
                if r.first_seen_at is None or r.first_seen_at < after:
                    r.first_seen_at = after
            if found:
                self._hand_over(found, after)
            with self._cond:
                self.failures[t.name] = 0
                self.next_due[t.name] = after + timedelta(seconds=t.interval(after))
            return slow
        except Exception as e:
            self._error(now, f"{t.name}: {type(e).__name__}: {e}", t.name)
            with self._cond:
                n = self.failures[t.name] = self.failures.get(t.name, 0) + 1
            retry = min(t.interval(now), self.RETRY_BASE * 2 ** (n - 1))
            with self._cond:
                self.next_due[t.name] = self._time(now) + timedelta(seconds=retry)
            return False

    def _hand_over(self, found: list, at: datetime) -> None:
        with self._deliver:
            new = self.store_new(found)
            if new and self.on_new is not None:
                try:
                    self.on_new(new, at)
                except Exception as e:
                    self._error(at, f"on_new: {type(e).__name__}: {e}", "on_new")

    def _error(self, at: datetime, detail: str, subject: str) -> None:
        with self._cond:
            self.errors_since_beat += 1
        self.health.record("task_error", at, detail, subject=subject)

    def _due(self, t: Task, now: datetime) -> bool:
        with self._cond:
            due = self.next_due.get(t.name)
        return due is None or now >= due

    def step(self, now: datetime) -> None:
        if self.started is None:
            self.started = now
        if self.last_step is not None:
            elapsed = (now - self.last_step).total_seconds()
            if elapsed > 3 * self._smallest_interval(now) + 60:
                self.health.record("gap", now, f"no step for {elapsed:.0f}s; catching up", elapsed)
                self.rerun_all()
        rerun = False
        for t in self.tasks:
            if self._due(t, now):
                rerun |= self._run_task(t, now)
        if rerun:
            self.rerun_all()
        self._report(now)
        self.last_step = now

    def _report(self, now: datetime) -> None:
        if self.next_beat is None or now >= self.next_beat:
            up = (now - self.started).total_seconds()
            with self._cond:
                errors, self.errors_since_beat = self.errors_since_beat, 0
            self.health.record("heartbeat", now, f"{len(self.tasks)} tasks, {errors} errors since the last heartbeat",
                               up)
            self.next_beat = now + timedelta(seconds=self.heartbeat_every)
        if self.meters and (self.next_spend is None or now >= self.next_spend):
            for m in self.meters:
                try:
                    self.health.record("spend", now, m.basis, float(m.usd()), m.name)
                except Exception as e:
                    self._error(now, f"meter {m.name}: {type(e).__name__}: {e}", m.name)
            self.next_spend = now + timedelta(seconds=self.spend_every)

    def run_forever(self) -> None:
        self.run_until(datetime.max.replace(tzinfo=timezone.utc))

    def run_until(self, deadline: datetime) -> None:
        clock = self.clock = self.clock or utcnow
        self.started = self.started or clock()
        lanes: dict[str, list[Task]] = {}
        for t in self.tasks:
            lanes.setdefault(t.lane_name, []).append(t)
        threads = [threading.Thread(target=self._lane, args=(tasks, deadline), name=f"lane:{name}", daemon=True)
                   for name, tasks in lanes.items()]
        # Its first reading is taken before any lane starts a call, so a pause straight after is still seen.
        threading.Thread(target=self._watchdog, args=(clock(),), name="watchdog", daemon=True).start()
        for th in threads:
            th.start()
        # The first heartbeat, whether or not the loop below gets a turn: a run stopped in its first second still
        # shows it was alive. After the watchdog starts, so a stuck health write here can't keep it from acting.
        self._report(self.started)
        last = clock()
        while not self.stopping and clock() < deadline:
            now = clock()
            elapsed = (now - last).total_seconds()
            # Never the tasks' own interval functions here: they may read state only their lane may touch.
            if elapsed > PAUSE_GAP:
                self.health.record("gap", now, f"process paused {elapsed:.0f}s; catching up", elapsed)
                self.rerun_all()
            last = now
            self._report(now)
            self.sleep(TICK)
        self.stop()
        deadline_at = time.monotonic() + JOIN_TIMEOUT
        for th in threads:
            th.join(max(0.0, deadline_at - time.monotonic()))
        if self.on_stop is not None:
            try:
                self.on_stop()
            except Exception as e:
                self._error(clock(), f"on_stop: {type(e).__name__}: {e}", "on_stop")

    def _watchdog(self, last: datetime) -> None:
        while not self.stopping:
            self.sleep(TICK)
            now = self.clock()
            if (now - last).total_seconds() > PAUSE_GAP:
                # The whole process was paused: the calls in flight weren't running, so their clocks start again.
                with self._cond:
                    self.in_flight = {lane: (task, now, timeout)
                                      for lane, (task, _, timeout) in self.in_flight.items()}
            else:
                self._watch_calls(now)
            last = now

    def _record_detached(self, *args) -> threading.Thread:
        # The watchdog must never wait on a health write: the call it is watching may be one, holding the lock.
        thread = threading.Thread(target=self.health.record, args=args, daemon=True)
        thread.start()
        return thread

    def _watch_calls(self, now: datetime) -> None:
        with self._cond:
            calls = list(self.in_flight.items())
        self._stalled &= {(lane, started) for lane, (_, started, _) in calls}
        for lane, (task, started, timeout) in calls:
            running = (now - started).total_seconds()
            if running > HANG_FACTOR * timeout:
                self._record_detached("hung", now, f"{task} has run {running:.0f}s; exiting so the run restarts",
                                      running, task).join(HUNG_ROW_WAIT)
                self.on_hang()
                return
            if running > timeout and (lane, started) not in self._stalled:
                self._stalled.add((lane, started))
                self._record_detached("stall", now, f"{task} has run {running:.0f}s and is still running", running,
                                      task)

    def _lane(self, tasks: list[Task], deadline: datetime) -> None:
        while not self.stopping:
            now = self.clock()
            if now >= deadline:
                return
            try:
                ran = False
                for t in tasks:
                    if self.stopping:
                        return
                    if self._due(t, self.clock()):
                        ran = True
                        if self._run_task(t, self.clock()):
                            self.rerun_all()
            except Exception as e:
                # _run_task contains a task's own errors; this is the lane's bookkeeping failing (an interval
                # function raising in the retry path). The lane must outlive it.
                self._error(now, f"lane: {type(e).__name__}: {e}", "lane")
                ran = False
            if ran:
                continue
            with self._cond:
                pending = [self.next_due[t.name] for t in tasks if t.name in self.next_due]
                wait = min([(d - now).total_seconds() for d in pending] + [TICK]) if pending else TICK
                if wait > 0 and not self.stopping:
                    self._cond.wait(timeout=wait)
