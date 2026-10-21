"""Load the interim report capture (capture_availability.py) as evidence.

Its files are real captures, so first_seen_at is the file's fetch time and backfilled stays False. Rows already present
in a conference's first capture were published before we started watching, so they also get the conference's
schedule-based estimate in estimated_release_at, which is labelled as an estimate and never used as a capture time.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from gnome_pipelines.cfb.qb_status.availability import report_evidence
from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex, TeamIndex
from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.schedule import estimated_release_at


def _fetched(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y-%m-%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def capture_evidence(capture_dir: Path, teams: TeamIndex, players: dict[int, PlayerIndex],
                     games: pd.DataFrame) -> list[Evidence]:
    kickoffs = dict(zip(games["game_id"], pd.to_datetime(games["kickoff"], utc=True)))
    first: dict[str, Evidence] = {}
    for conf_dir in sorted(p for p in Path(capture_dir).iterdir() if p.is_dir()):
        files = sorted(conf_dir.glob("*.json"))
        for i, path in enumerate(files):
            body = json.loads(path.read_text())
            seen = _fetched(body["fetched_at"])
            for e in report_evidence(conf_dir.name, body, seen, teams, players, games):
                if e.evidence_id in first:
                    continue
                if i == 0 and e.game_id in kickoffs:
                    e.estimated_release_at = estimated_release_at(conf_dir.name, kickoffs[e.game_id].to_pydatetime(),
                                                                  e.report_slot)
                first[e.evidence_id] = e
    return sorted(first.values(), key=lambda e: e.first_seen_at)
