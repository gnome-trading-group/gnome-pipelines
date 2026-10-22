"""Which claims from posts and headlines apply without a person looking at them.

A claim applies (review_state "auto") when one of these holds, checked in order; otherwise it waits as "pending",
which the engine ignores:
- official: it agrees with the latest official report for that player and game;
- credible: a fully credible account states it plainly or on its own reporting, and the model reading it is at least
  SINGLE_CONFIDENCE sure (rule-based reads, at 0.6, never are);
- agree: other sources made the same claim about the same player and game within AGREE_WINDOW, and the credibility
  of the distinct sources, this one included, sums to AGREE_WEIGHT. A claim the model read as speculation doesn't
  count; a rule-based read has no hedge reading, so its "unclear" does.

The reviewer column names the rule ("auto:credible"), so the review page doubles as an audit log.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import Evidence

SINGLE_CONFIDENCE = 0.8
SINGLE_CREDIBILITY = 1.0
SINGLE_HEDGES = {"confirmed", "per_sources"}
AGREE_WINDOW = timedelta(hours=48)
AGREE_WEIGHT = 2.0
NEWS_CREDIBILITY = 0.5
CLAIM_SOURCES = {"x_post", "news"}


def credibility_for(registry) -> Callable[[str, str], float]:
    def credibility(source_type: str, source_org: str) -> float:
        if source_type == "x_post":
            return registry.credibility(source_org)
        return NEWS_CREDIBILITY if source_type == "news" else 0.0
    return credibility


def _counts(hedge, extractor) -> bool:
    return hedge != "unclear" or extractor == "rules"


def _official(existing: pd.DataFrame, e: Evidence) -> str | None:
    rows = existing[(existing["source_type"] == "official_report") & (existing["team_id"] == e.team_id)
                    & (existing["player_id"].astype(str) == str(e.player_id)) & (existing["game_id"] == e.game_id)]
    if rows.empty:
        return None
    return rows.assign(_t=pd.to_datetime(rows["first_seen_at"], utc=True)).sort_values("_t", kind="stable")[
        "status"].iloc[-1]


def _same_claim(a: Evidence, b: Evidence) -> bool:
    return (a.team_id, str(a.player_id), a.status, a.game_id) == (b.team_id, str(b.player_id), b.status, b.game_id)


def _agreeing_weight(existing: pd.DataFrame, earlier: list[Evidence], e: Evidence,
                     credibility: Callable[[str, str], float], now: datetime) -> float:
    seen = pd.to_datetime(existing["first_seen_at"], utc=True)
    rows = existing[existing["source_type"].isin(CLAIM_SOURCES) & (existing["team_id"] == e.team_id)
                    & (existing["player_id"].astype(str) == str(e.player_id)) & (existing["status"] == e.status)
                    & (existing["game_id"] == e.game_id) & (seen >= pd.Timestamp(now) - AGREE_WINDOW)]
    sources = {(t, o) for t, o, h, x in zip(rows["source_type"], rows["source_org"], rows["hedge"], rows["extractor"])
               if _counts(h, x)}
    sources |= {(b.source_type, b.source_org) for b in earlier if _same_claim(b, e) and _counts(b.hedge, b.extractor)}
    sources.add((e.source_type, e.source_org))
    return sum(credibility(t, o) for t, o in sources)


def decide(new: list[Evidence], existing: pd.DataFrame, credibility: Callable[[str, str], float],
           now: datetime) -> list[Evidence]:
    """Set review_state on new claims from posts and headlines, in order, so a later claim in the batch can agree
    with an earlier one. Other evidence passes through untouched."""
    for i, e in enumerate(new):
        if e.source_type not in CLAIM_SOURCES or e.review_state != "pending" or e.player_id is None:
            continue
        rule = None
        if _official(existing, e) == e.status:
            rule = "official"
        elif (credibility(e.source_type, e.source_org) >= SINGLE_CREDIBILITY and e.hedge in SINGLE_HEDGES
              and e.extraction_confidence >= SINGLE_CONFIDENCE and e.extractor != "rules"):
            rule = "credible"
        elif _counts(e.hedge, e.extractor) and _agreeing_weight(existing, new[:i], e, credibility, now) >= AGREE_WEIGHT:
            rule = "agree"
        if rule is not None:
            e.review_state, e.reviewer, e.reviewed_at = "auto", f"auto:{rule}", now
    return new
