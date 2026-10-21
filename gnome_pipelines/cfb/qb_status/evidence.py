"""The evidence model: one claim about a quarterback, from any source, as an append-only row."""
from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime

import pandas as pd

from gnome_pipelines.datasets.qb_status import EVIDENCE

STATUSES = ("out", "doubtful", "questionable", "probable", "available", "game_time_decision", "expected_starter",
            "not_starting", "left_game_injured", "qb_change", "returned", "market_alarm")

# Chance a player is available for the game, by status (the SEC's published definitions; GTD treated as a coin flip)
AVAILABILITY = {"available": 1.0, "probable": 0.75, "questionable": 0.5, "doubtful": 0.25, "out": 0.0,
                "game_time_decision": 0.5}

_REPORT_WORDS = {"available": "available", "probable": "probable", "questionable": "questionable",
                 "doubtful": "doubtful", "out": "out", "game time decision": "game_time_decision"}


def parse_report_status(text) -> str | None:
    if not isinstance(text, str):
        return None
    key = re.sub(r"[\s-]+", " ", text.strip().lower())
    return _REPORT_WORDS.get(key)


def evidence_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:24]


@dataclass
class Evidence:
    evidence_id: str
    source_type: str
    source_id: str
    source_org: str
    first_seen_at: datetime
    team: str
    status: str
    season: int
    published_at: datetime | None = None
    backfilled: bool = False
    estimated_release_at: datetime | None = None
    team_id: int | None = None
    player_raw: str | None = None
    jersey: str | None = None
    player_id: str | None = None
    match_confidence: float = 0.0
    game_id: int | None = None
    week: int | None = None
    report_slot: str | None = None
    hedge: str = "confirmed"
    paraphrase: str | None = None
    url: str | None = None
    extractor: str = "none"
    extraction_confidence: float = 1.0
    review_state: str = "auto"
    reviewer: str | None = None
    reviewed_at: datetime | None = None
    extra: dict = field(default_factory=dict, repr=False)

    def to_row(self) -> dict:
        row = asdict(self)
        row.pop("extra")
        return {name: row[name] for name in EVIDENCE.schema.names}


def evidence_frame(items: list[Evidence]) -> pd.DataFrame:
    return pd.DataFrame([e.to_row() for e in items], columns=EVIDENCE.schema.names)
