"""Turning free text (headlines, posts) into QB status claims.

RuleExtractor needs a QB's last name and a status phrase in the same clause and never claims more than 0.6 confidence;
its claims always go to human review. ClaudeExtractor is the planned model-based extractor, enabled once
ANTHROPIC_API_KEY is set.
"""
from __future__ import annotations

import os
import re
from typing import Protocol

import pandas as pd

RULE_CONFIDENCE = 0.6
# Most specific first: the first phrase found in a clause decides its status
PHRASES = (("out for the season", "out"), ("will not play", "out"), ("won't start", "not_starting"),
           ("will not start", "not_starting"), ("expected to start", "expected_starter"),
           ("will start", "expected_starter"), ("questionable", "questionable"), ("doubtful", "doubtful"),
           ("left the game", "left_game_injured"), ("carted off", "left_game_injured"),
           ("injured", "left_game_injured"))
HEDGES = ("expected", "likely", "per ")


class NotConfigured(RuntimeError):
    pass


class Extractor(Protocol):
    def extract(self, text: str, team_id: int, qbs: pd.DataFrame) -> list[tuple[str, str, str, float]]: ...


class RuleExtractor:
    def extract(self, text: str, team_id: int, qbs: pd.DataFrame) -> list[tuple[str, str, str, float]]:
        out: list[tuple[str, str, str, float]] = []
        for clause in re.split(r"[.!?;]+\s*", text or ""):
            low = clause.lower()
            status = next((s for phrase, s in PHRASES if phrase in low), None)
            if status is None:
                continue
            hedge = "expected" if any(h in low for h in HEDGES) else "unclear"
            for q in qbs.itertuples(index=False):
                last = str(q.name).split()[-1].lower() if q.name else ""
                if last and re.search(rf"\b{re.escape(last)}\b", low):
                    claim = (str(q.player_id), status, hedge, RULE_CONFIDENCE)
                    if claim not in out:
                        out.append(claim)
        return out


class ClaudeExtractor:
    def __init__(self) -> None:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise NotConfigured("set ANTHROPIC_API_KEY to enable model-based extraction")

    def extract(self, text: str, team_id: int, qbs: pd.DataFrame) -> list[tuple[str, str, str, float]]:
        raise NotImplementedError("model-based extraction is built once the Anthropic key is available")
