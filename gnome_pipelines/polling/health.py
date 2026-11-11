"""poll_health rows: what the framework observes about a running source."""
from __future__ import annotations

import hashlib
import logging
import threading
from datetime import datetime
from typing import Callable

import pandas as pd
from gnomepy.data import DatasetStore

from gnome_pipelines.datasets.polling import POLL_HEALTH

logger = logging.getLogger(__name__)

DETAIL_CHARS = 500


def health_row(source: str, kind: str, at: datetime, detail: str, value: float | None = None,
               subject: str | None = None) -> dict:
    key = "|".join(map(str, (source, kind, pd.Timestamp(at).isoformat(), subject, detail)))
    return {"health_id": hashlib.sha256(key.encode()).hexdigest()[:32], "source": source, "kind": kind,
            "at": pd.Timestamp(at), "subject": subject, "value": value, "detail": detail[:DETAIL_CHARS]}


class PollHealth:
    """Writes one source's rows to poll_health. A failed write is logged, never raised: health must not stop polling."""

    def __init__(self, source: str, store: DatasetStore | None = None,
                 publish: Callable[[pd.DataFrame], None] | None = None) -> None:
        self.source = source
        self._publish = publish or (lambda df: store.publish(POLL_HEALTH, df))
        # Lanes record concurrently; one commit at a time keeps them from racing on the same table.
        self._lock = threading.Lock()

    def record(self, kind: str, at: datetime, detail: str, value: float | None = None,
               subject: str | None = None) -> None:
        row = health_row(self.source, kind, at, detail, value, subject)
        try:
            with self._lock:
                self._publish(pd.DataFrame([row], columns=POLL_HEALTH.schema.names))
        except Exception:
            logger.exception("couldn't write %s health row: %s", kind, detail)
