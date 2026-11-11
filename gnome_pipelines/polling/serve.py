"""Runs one polling source until stopped: what runner.py does with a source's run, and `gnome-pipelines serve`.

Builds the source's Service and runs it until SIGTERM (a stop from the Pipelines page, or CI restarting it on a new
image). A source keeps whatever must outlive a restart in its own tables or objects, written as it goes; the
Service's on_stop hook is its chance for last writes. Rows it publishes are stamped <source>/<RUN_ID>, like any
run's.
"""
from __future__ import annotations

import signal

from gnome_pipelines import families
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import utcnow
from gnome_pipelines.stores import code_version, dataset_store


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
    health = PollHealth(name, dataset_store())
    if stop["asked"]:
        return
    service = cls().build(validated, health)
    stop["service"] = service
    if stop["asked"]:
        service.stop()
    health.record("started", utcnow(), f"{code_version()} with {validated.model_dump()}")
    try:
        service.run_forever()
    finally:
        health.record("stopped", utcnow(), "SIGTERM" if stop["asked"] else "loop ended")
