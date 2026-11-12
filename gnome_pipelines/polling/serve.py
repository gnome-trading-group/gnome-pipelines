"""Runs one polling source until stopped: what runner.py does with a source's run, and `gnome-pipelines serve`.

Builds the source's Service, with a TableWriter that commits the source's rows (and its health rows) in batches,
and runs it until SIGTERM (a stop from the Pipelines page, or CI restarting it on a new
image). A source keeps whatever must outlive a restart in its own tables or objects, written as it goes; the
Service's on_stop hook is its chance for last writes. Rows it publishes are stamped <source>/<RUN_ID>, like any
run's.
"""
from __future__ import annotations

import signal
import threading

from gnome_pipelines import families
from gnome_pipelines.datasets.polling import POLL_HEALTH
from gnome_pipelines.polling.alerts import alerts_for
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import utcnow
from gnome_pipelines.polling.writer import TableWriter
from gnome_pipelines.stores import code_version, dataset_store

# How long a hung process waits for its last rows to commit before it exits.
HANG_FLUSH_WAIT = 10.0


def serve(name: str, params: dict) -> None:
    # First, before anything slow: in a container this process is PID 1, which ignores SIGTERM until a handler is
    # set, so a stop during startup would otherwise wait out the 60 s grace period and end in SIGKILL.
    stop = {"asked": False, "service": None}

    def on_sigterm(signum, frame) -> None:
        stop["asked"] = True
        if stop["service"] is not None:
            stop["service"].stop()
    signal.signal(signal.SIGTERM, on_sigterm)

    available = families.available_sources()
    cls = available.get(name)
    if cls is None:
        raise ValueError(f"Unknown polling source '{name}'. Available here: {sorted(available)}")
    validated = cls.params_type.model_validate(params)
    writer = TableWriter(dataset_store().publish, every=cls.flush_every)
    health = PollHealth(name, publish=lambda df: writer.add(POLL_HEALTH, df))
    writer.start()
    if stop["asked"]:
        writer.close()
        return
    try:
        service = cls().build(validated, health, writer, alerts_for(name))
    except Exception:
        # What build recorded (a note, a source that was turned off) is worth keeping, even if the run fails.
        writer.close()
        raise
    hang = service.on_hang

    def flush_then_hang() -> None:
        # The process is about to exit hard: commit what the writer holds (the "hung" row with it), within a limit,
        # since the writer itself may be what hung.
        flusher = threading.Thread(target=writer.flush, daemon=True)
        flusher.start()
        flusher.join(HANG_FLUSH_WAIT)
        hang()
    service.on_hang = flush_then_hang
    stop["service"] = service
    if stop["asked"]:
        service.stop()
    health.record("started", utcnow(), f"{code_version()} with {validated.model_dump()}")
    try:
        service.run_forever()
    finally:
        health.record("stopped", utcnow(), "SIGTERM" if stop["asked"] else "loop ended")
        writer.close()
