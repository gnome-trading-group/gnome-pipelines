"""Reading and writing the QB status tables through DatasetStore.

The laptop phase writes to a local warehouse (QB_STATUS_WAREHOUSE, default under ~/.cache) unless prod is asked
for explicitly. It is not GNOME_LOCAL_WAREHOUSE, which would also redirect the research reads: these are new shared tables and creating them in the prod catalog waits for the user's approval.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_frame
from gnome_pipelines.datasets.qb_status import EVIDENCE, HEALTH, PLAYERS, REPORT_SCHEDULE, SNAPSHOTS, WATCHES

DEFAULT_WAREHOUSE = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_warehouse"
CODE_VERSION = "gnome-pipelines@qb_status"


def open_store(prod: bool = False) -> DatasetStore:
    if prod:
        return DatasetStore(code_version=CODE_VERSION)
    path = Path(os.environ.get("QB_STATUS_WAREHOUSE") or DEFAULT_WAREHOUSE)
    path.mkdir(parents=True, exist_ok=True)
    return DatasetStore(CatalogConfig(local_warehouse=path), code_version=CODE_VERSION)


def _load(store: DatasetStore, spec, as_of: datetime | None = None) -> pd.DataFrame:
    try:
        return store.load(spec.name, as_of=as_of)
    except Exception:
        return pd.DataFrame(columns=spec.schema.names)


class StatusStore:
    def __init__(self, store: DatasetStore) -> None:
        self.store = store
        self._ids: set[str] | None = None
        self._snapshots: dict[tuple, tuple] = {}

    def add_evidence_new(self, items: list[Evidence]) -> list[Evidence]:
        if self._ids is None:
            self._ids = set(_load(self.store, EVIDENCE)["evidence_id"])
        new, seen = [], set()
        for e in items:
            if e.evidence_id not in self._ids and e.evidence_id not in seen:
                new.append(e)
                seen.add(e.evidence_id)
        if new:
            self.store.publish(EVIDENCE, evidence_frame(new))
            self._ids |= seen
        return new

    def add_evidence(self, items: list[Evidence]) -> int:
        return len(self.add_evidence_new(items))

    def evidence(self, as_of: datetime | None = None) -> pd.DataFrame:
        return _load(self.store, EVIDENCE, as_of)

    def put_snapshots(self, df: pd.DataFrame, now: datetime) -> int:
        if df.empty:
            return 0
        rows = []
        for r in df.to_dict("records"):
            key = (r["game_id"], r["team_id"], r["player_id"])
            value = (round(float(r["p_start"]), 6), r.get("official_status"), r.get("official_slot"), r.get("reason"))
            if self._snapshots.get(key) != value:
                rows.append({**r, "computed_at": pd.Timestamp(now)})
                self._snapshots[key] = value
        if rows:
            self.store.publish(SNAPSHOTS, pd.DataFrame(rows, columns=SNAPSHOTS.schema.names))
        return len(rows)

    def snapshots(self, as_of: datetime | None = None) -> pd.DataFrame:
        return _load(self.store, SNAPSHOTS, as_of)

    def put_watches(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self.store.publish(WATCHES, df[WATCHES.schema.names])

    def watches(self) -> pd.DataFrame:
        return _load(self.store, WATCHES)

    def put_health(self, rows: list[dict]) -> None:
        if rows:
            self.store.publish(HEALTH, pd.DataFrame(rows, columns=HEALTH.schema.names))

    def health(self) -> pd.DataFrame:
        return _load(self.store, HEALTH)

    def put_schedule(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self.store.publish(REPORT_SCHEDULE, df[REPORT_SCHEDULE.schema.names])

    def schedule(self) -> pd.DataFrame:
        return _load(self.store, REPORT_SCHEDULE)

    def put_players(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self.store.publish(PLAYERS, df[PLAYERS.schema.names])
