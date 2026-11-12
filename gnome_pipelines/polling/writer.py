"""One thread that commits a source's table rows in batches, so lanes never wait on a table commit.

A commit to an Iceberg table takes a second or two and they go one at a time, so a lane that published its own rows
would hold every other lane up while it did. Lanes add() rows instead, and the writer commits each table's rows
together every `every` seconds for that table (DEFAULT_EVERY unless set): short for the tables a strategy waits on,
long for logs, which also keeps the number of commits (snapshots) a day reasonable. A batch holding several rows for
one key (a film's two score changes, a retried row) is committed in successive layers, oldest first, since one
publish takes one row per key. A failed commit keeps its rows for the next round, up to MAX_QUEUED rows a table,
past which the oldest are dropped and logged. flush() commits everything now; close() stops the thread after a last
flush.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import pandas as pd
from gnomepy.data import TableSpec

logger = logging.getLogger(__name__)

DEFAULT_EVERY = 300.0
TICK = 0.5
MAX_QUEUED = 200_000


class TableWriter:
    def __init__(self, publish: Callable[[TableSpec, pd.DataFrame], object], every: dict[str, float] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._publish, self.every, self.clock = publish, dict(every or {}), clock
        self._queued: dict[str, tuple[TableSpec, list[pd.DataFrame]]] = {}
        self._due: dict[str, float] = {}
        self._lock = threading.Lock()
        # One commit at a time: flush() from the shutdown path must not race the thread's own round.
        self._committing = threading.Lock()
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None

    def add(self, spec: TableSpec, rows: pd.DataFrame | list[dict]) -> None:
        frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows, columns=spec.schema.names)
        if frame.empty:
            return
        with self._lock:
            _, frames = self._queued.setdefault(spec.name, (spec, []))
            frames.append(frame)
            self._due.setdefault(spec.name, self.clock() + self.every.get(spec.name, DEFAULT_EVERY))

    def pending(self) -> dict[str, int]:
        with self._lock:
            return {name: sum(len(f) for f in frames) for name, (_, frames) in self._queued.items()}

    def flush(self, only_due: bool = False) -> int:
        """Commit queued rows (only the tables whose interval is up, with only_due); returns the rows committed."""
        with self._committing:
            now = self.clock()
            with self._lock:
                names = [n for n in self._queued if not only_due or self._due.get(n, now) <= now]
                batches = [(n, *self._queued.pop(n)) for n in names]
                for n in names:
                    self._due.pop(n, None)
            committed = 0
            for name, spec, frames in batches:
                frame = pd.concat(frames, ignore_index=True)
                layer = frame.groupby(list(spec.key), sort=False, dropna=False).cumcount()
                for i in sorted(layer.unique()):
                    rows = frame[layer == i]
                    try:
                        self._publish(spec, rows.reset_index(drop=True))
                        committed += len(rows)
                    except Exception:
                        rest = frame[layer >= i]
                        logger.exception("couldn't commit %d %s rows; keeping them for the next round", len(rest), name)
                        self._requeue(name, spec, rest)
                        break
            return committed

    def _requeue(self, name: str, spec: TableSpec, frame: pd.DataFrame) -> None:
        with self._lock:
            _, later = self._queued.setdefault(name, (spec, []))
            later.insert(0, frame.reset_index(drop=True))
            total = sum(len(f) for f in later)
            if total > MAX_QUEUED:
                merged = pd.concat(later, ignore_index=True)
                logger.error("%s: %d rows queued after failed commits; dropping the oldest %d", name, total,
                             total - MAX_QUEUED)
                later[:] = [merged.iloc[total - MAX_QUEUED:].reset_index(drop=True)]
            self._due[name] = self.clock() + self.every.get(name, DEFAULT_EVERY)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="table-writer", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stopping.wait(TICK):
            self.flush(only_due=True)

    def close(self, timeout: float = 5.0) -> None:
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(timeout)
        self.flush()
