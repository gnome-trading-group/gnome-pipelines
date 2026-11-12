"""Alerts from a polling source to the shared alerts bus, which gnome-registry posts to Slack and the Alerts page.

A source alerts about one thing under one name (a task's name, or a check such as "zenrows_budget"). fire() opens it
and puts it again every REFIRE while it lasts, or at once when its severity changes: the registry folds repeats of an
open key into the one alert, and closes an alert a day after it opened, so a problem that lasts days is posted again
daily rather than going quiet. resolve() closes it. The names open are kept in a small object beside the source's
other state, so an alert left open by a run that stopped (a deploy, a crash) is still closed by the next run once its
cause is gone. Severity is INFO, WARNING or ERROR, never CRITICAL: a source never pages. Alerting never fails the
source: a failed put is logged, and the next fire tries again.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

import boto3
from botocore.exceptions import ClientError
from gnomepy.data import CatalogConfig

from gnome_pipelines.environment import Stage, environment

logger = logging.getLogger(__name__)

TYPE = "SOURCE_DEGRADED"
SEVERITIES = ("INFO", "WARNING", "ERROR")
REFIRE = 3600.0
LOCAL_ROOT = Path.home() / ".cache" / "gnome-pipelines" / "alerts"


class Sink(Protocol):
    def put(self, alert: dict) -> None: ...


class BusSink:
    def __init__(self, bus: str | None = None, client=None) -> None:
        self.bus = bus or os.environ.get("ALERT_BUS_NAME", "gnome-alerts")
        self.client = client or boto3.client("events")

    def put(self, alert: dict) -> None:
        result = self.client.put_events(Entries=[{
            "EventBusName": self.bus, "Source": "gnome.alerts", "DetailType": "Alert", "Detail": json.dumps(alert)}])
        if result.get("FailedEntryCount"):
            raise RuntimeError(f"the bus refused the alert: {result['Entries']}")


class LogSink:
    """A local run's alerts: logged only, since a laptop isn't a source anyone is watching."""

    def put(self, alert: dict) -> None:
        logger.warning("alert %s %s: %s", alert["action"], alert["key"], alert.get("title", ""))


class OpenNames(Protocol):
    def load(self) -> set[str]: ...
    def save(self, names: set[str]) -> None: ...


class S3OpenNames:
    def __init__(self, bucket: str, key: str, client=None) -> None:
        self.bucket, self.key, self.client = bucket, key, client or boto3.client("s3")

    def load(self) -> set[str]:
        try:
            return set(json.loads(self.client.get_object(Bucket=self.bucket, Key=self.key)["Body"].read()))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return set()
            raise

    def save(self, names: set[str]) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self.key, Body=json.dumps(sorted(names)).encode())


class LocalOpenNames:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self) -> set[str]:
        return set(json.loads(self.path.read_text())) if self.path.exists() else set()

    def save(self, names: set[str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(sorted(names)))


class Alerts:
    def __init__(self, source: str, sink: Sink, opened: OpenNames,
                 clock: Callable[[], datetime] | None = None, monotonic: Callable[[], float] = time.monotonic) -> None:
        self.source, self.sink, self.opened = source, sink, opened
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        # name -> (severity, when it was last put); None when the last put failed, so the next fire tries again.
        self._open: dict[str, tuple[str, float | None]] = {}
        self._lock = threading.Lock()
        try:
            # Left open by an earlier run: closed by this one's first resolve() for the name.
            self._inherited = opened.load()
        except Exception:
            logger.warning("couldn't read the alerts left open for %s", source, exc_info=True)
            self._inherited = set()

    def key(self, name: str) -> str:
        return f"{TYPE}:{self.source}:{name}"

    def is_open(self, name: str) -> bool:
        with self._lock:
            return name in self._open or name in self._inherited

    def fire(self, name: str, title: str, body: str = "", severity: str = "WARNING",
             labels: dict[str, str] | None = None) -> None:
        if severity not in SEVERITIES:
            raise ValueError(f"a source's alert is one of {SEVERITIES}, not {severity}")
        with self._lock:
            was = self._open.get(name)
            due = (was is None or was[0] != severity or was[1] is None
                   or self.monotonic() - was[1] >= REFIRE)
            if not due:
                return
            opened = was is None and name not in self._inherited
            put_at = self._put({"action": "FIRING", "severity": severity, "title": title, "body": body,
                                "labels": {"pipeline": self.source, "kind": "source", "check": name,
                                           **(labels or {})}}, name)
            self._open[name] = (severity, put_at)
            self._inherited.discard(name)
            if opened:
                self._save()

    def resolve(self, name: str) -> None:
        with self._lock:
            if name not in self._open and name not in self._inherited:
                return
            if self._put({"action": "RESOLVED", "title": "", "labels": {"pipeline": self.source, "check": name}},
                         name) is None:
                return
            self._open.pop(name, None)
            self._inherited.discard(name)
            self._save()

    def _put(self, fields: dict, name: str) -> float | None:
        alert = {"key": self.key(name), "type": TYPE, "source": "ingest", "stateAt": self.clock().isoformat(),
                 **fields}
        try:
            self.sink.put(alert)
            return self.monotonic()
        except Exception:
            logger.warning("couldn't put alert %s", alert["key"], exc_info=True)
            return None

    def _save(self) -> None:
        try:
            self.opened.save(set(self._open) | self._inherited)
        except Exception:
            logger.warning("couldn't record the open alerts of %s", self.source, exc_info=True)


def alerts_for(source: str) -> Alerts:
    """A source's alerts: to the stage's alerts bus, or only logged in a local run."""
    if environment().stage is Stage.LOCAL:
        return Alerts(source, LogSink(), LocalOpenNames(LOCAL_ROOT / f"{source}.json"))
    bucket = CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]
    return Alerts(source, BusSink(), S3OpenNames(bucket, f"{source}/alerts/open.json"))
