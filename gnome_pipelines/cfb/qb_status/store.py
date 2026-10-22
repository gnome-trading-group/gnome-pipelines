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
from gnomepy.data.datasets import INGESTED_AT
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo

from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_frame, evidence_id
from gnome_pipelines.datasets.qb_status import (ALL, EVIDENCE, HEALTH, PLAYERS, REPORT_SCHEDULE, SNAPSHOTS, SOURCES,
                                                WATCHES)

DEFAULT_WAREHOUSE = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_warehouse"
CODE_VERSION = "gnome-pipelines@qb_status"


def open_store(prod: bool = False) -> DatasetStore:
    if prod:
        return DatasetStore(code_version=CODE_VERSION)
    path = Path(os.environ.get("QB_STATUS_WAREHOUSE") or DEFAULT_WAREHOUSE)
    path.mkdir(parents=True, exist_ok=True)
    return DatasetStore(CatalogConfig(local_warehouse=path), code_version=CODE_VERSION)


def _load(store: DatasetStore, spec, as_of: datetime | None = None) -> pd.DataFrame:
    """A missing table is empty; any other failure is raised, because treating it as empty would re-publish known
    evidence with a later first_seen_at and lose the true first sighting."""
    try:
        return store.load(spec.name, as_of=as_of)
    except NoSuchTableError:
        return pd.DataFrame(columns=spec.schema.names)


def _versions(store: DatasetStore, spec) -> list[tuple[pd.Timestamp, pd.DataFrame]]:
    """Every batch ever published to a table, oldest first, read back through as_of: the newest batch as of a time is
    the rows ingested at that time, and stepping to just before it reveals the one before."""
    out, as_of = [], None
    while True:
        try:
            rows = store.load(spec.name, as_of=as_of, include_metadata=True)
        except NoSuchTableError:
            return []
        if rows.empty:
            return out[::-1]
        at = rows[INGESTED_AT].max()
        out.append((at, rows[rows[INGESTED_AT] == at][spec.schema.names]))
        as_of = (at - pd.Timedelta(microseconds=1)).to_pydatetime()


def copy_tables(src: DatasetStore, dst: DatasetStore, specs=ALL) -> dict[str, int]:
    """Copy QB status tables, every version at its original time, into a destination that has none of them (the
    laptop phase's history into prod, once), so as_of reads there match the source. specs leaves some out."""
    for spec in specs:
        if not _load(dst, spec).empty:
            raise RuntimeError(f"{spec.name} already has rows in the destination; nothing copied")
    copied = {}
    for spec in specs:
        batches = _versions(src, spec)
        for at, rows in batches:
            dst.publish(spec, rows, ingested_at=at.to_pydatetime())
        if batches:
            copied[spec.name] = sum(len(rows) for _, rows in batches)
    return copied


def _plain(v):
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def _to_evidence(row: dict) -> Evidence:
    values = {k: _plain(row.get(k)) for k in EVIDENCE.schema.names}
    for k in ("game_id", "team_id", "week", "season"):
        if values[k] is not None:
            values[k] = int(values[k])
    values["backfilled"] = bool(values["backfilled"])
    return Evidence(**values)


def _snapshot_value(r: dict) -> tuple:
    """What counts as a change for a snapshot row. The rule version is part of it, so a new rule republishes every
    row once and the stored version always names the rule that produced the number."""
    return (round(float(r["p_start"]), 6), r.get("official_status"), r.get("official_slot"), r.get("reason"),
            r.get("rule_version"))


class StatusStore:
    def __init__(self, store: DatasetStore) -> None:
        self.store = store
        self._evidence: pd.DataFrame | None = None
        self._ids: set[str] = set()
        self._latest_official: dict[str, str] = {}
        self._snapshots: dict[tuple, tuple] | None = None
        self._keys: dict[tuple, set] = {}

    def _ensure(self) -> None:
        if self._evidence is not None:
            return
        ev = _load(self.store, EVIDENCE)
        self._evidence = ev
        self._ids = set(ev["evidence_id"])
        official = ev[ev["source_type"] == "official_report"].sort_values("first_seen_at", kind="stable")
        self._latest_official = dict(zip(official["source_id"], official["status"]))

    def add_evidence_new(self, items: list[Evidence]) -> list[Evidence]:
        """Store what's new. Official report cells are tracked as transitions: a status equal to that cell's latest is
        skipped, and a change back to an earlier status is a new row with a time-qualified id."""
        self._ensure()
        new, seen = [], set()
        for e in items:
            if e.source_type == "official_report":
                previous = self._latest_official.get(e.source_id)
                if previous == e.status or (previous is None and e.evidence_id in self._ids | seen):
                    continue
                if e.evidence_id in self._ids or e.evidence_id in seen:
                    e.evidence_id = evidence_id(e.evidence_id, pd.Timestamp(e.first_seen_at).isoformat())
                self._latest_official[e.source_id] = e.status
            elif e.evidence_id in self._ids or e.evidence_id in seen:
                continue
            new.append(e)
            seen.add(e.evidence_id)
        if new:
            frame = evidence_frame(new)
            self.store.publish(EVIDENCE, frame)
            self._ids |= seen
            self._evidence = frame if self._evidence.empty else pd.concat([self._evidence, frame], ignore_index=True)
        return new

    def add_evidence(self, items: list[Evidence]) -> int:
        return len(self.add_evidence_new(items))

    def pull_new(self, source_type: str) -> list[Evidence]:
        """Rows of one source type written by another process (the review page) that this process hasn't seen yet."""
        self._ensure()
        try:
            rows = self.store.load(EVIDENCE.name, row_filter=EqualTo("source_type", source_type))
        except NoSuchTableError:
            return []
        fresh = rows[~rows["evidence_id"].isin(self._ids)]
        if fresh.empty:
            return []
        self._ids |= set(fresh["evidence_id"])
        self._evidence = pd.concat([self._evidence, fresh[EVIDENCE.schema.names]], ignore_index=True)
        return [_to_evidence(r) for r in fresh.to_dict("records")]

    def evidence(self, as_of: datetime | None = None) -> pd.DataFrame:
        if as_of is not None:
            return _load(self.store, EVIDENCE, as_of)
        self._ensure()
        return self._evidence.copy()

    def _seed_snapshots(self) -> None:
        if self._snapshots is not None:
            return
        self._snapshots = {}
        for r in _load(self.store, SNAPSHOTS).to_dict("records"):
            key = (r["game_id"], r["team_id"], r["player_id"])
            self._snapshots[key] = _snapshot_value(r)
            self._keys.setdefault((r["game_id"], r["team_id"]), set()).add(r["player_id"])

    def put_snapshots(self, df: pd.DataFrame, now: datetime) -> int:
        return len(self._publish_snapshots(df, now))

    def put_snapshots_changed(self, df: pd.DataFrame, now: datetime) -> set[tuple[int, int]]:
        """put_snapshots for any number of team-games in one commit (each costs seconds on S3 Tables); returns the
        (game, team) pairs that changed."""
        return {(int(r["game_id"]), int(r["team_id"])) for r in self._publish_snapshots(df, now)}

    def _publish_snapshots(self, df: pd.DataFrame, now: datetime) -> list[dict]:
        """Publish changed rows. A candidate that dropped out of a team-game gets a p_start 0 row, so the latest row
        per key always sums to 1 for that team-game."""
        if df.empty:
            return []
        self._seed_snapshots()
        records = df.to_dict("records")
        for (game, team), group in df.groupby(["game_id", "team_id"]):
            gone = self._keys.get((game, team), set()) - set(group["player_id"])
            for pid in sorted(gone):
                records.append({"game_id": game, "team_id": team, "player_id": pid, "p_start": 0.0,
                                "official_status": None, "official_slot": None, "evidence_ids": [],
                                "reason": "no longer a candidate", "rule_version": group["rule_version"].iloc[0]})
            self._keys[(game, team)] = set(group["player_id"])
        rows = []
        for r in records:
            key = (r["game_id"], r["team_id"], r["player_id"])
            value = _snapshot_value(r)
            if self._snapshots.get(key) != value:
                rows.append({**r, "computed_at": pd.Timestamp(now)})
                self._snapshots[key] = value
        if rows:
            self.store.publish(SNAPSHOTS, pd.DataFrame(rows, columns=SNAPSHOTS.schema.names))
        return rows

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

    def players(self) -> pd.DataFrame:
        return _load(self.store, PLAYERS)

    def put_sources(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self.store.publish(SOURCES, df[SOURCES.schema.names])

    def sources(self) -> pd.DataFrame:
        return _load(self.store, SOURCES)
