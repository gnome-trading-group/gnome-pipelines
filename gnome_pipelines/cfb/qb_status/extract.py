"""Turning free text (headlines, posts) into QB status claims.

RuleExtractor needs a QB's last name and a status phrase in the same clause and never claims more than 0.6 confidence.
ClaudeExtractor has Claude Haiku 5.5 read each text that names one of the team's QBs next to a status word, and is
enabled once its key is found (ANTHROPIC_API_KEY, else gnome/pipelines/anthropic in Secrets Manager). Haiku is the
spec's "cheapest Claude model"; the model is one constant.
Whether a claim then applies on its own is autoapply's decision.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

import anthropic
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from gnome_pipelines import credentials

RULE_CONFIDENCE = 0.6
# Most specific first: the first phrase found in a clause decides its status
PHRASES = (("out for the season", "out"), ("will not play", "out"), ("won't start", "not_starting"),
           ("will not start", "not_starting"), ("expected to start", "expected_starter"),
           ("will start", "expected_starter"), ("questionable", "questionable"), ("doubtful", "doubtful"),
           ("left the game", "left_game_injured"), ("carted off", "left_game_injured"),
           ("injured", "left_game_injured"))
HEDGES = ("expected", "likely", "per ")

CLAUDE_MODEL = "claude-haiku-5-5"
CLAUDE_EFFORT = "medium"                  # 35/36 on the labelled check vs 32/36 at "low"; ~$0.0002 a read
CLAUDE_USD_PER_TOKEN = {"input": 0.10e-6, "output": 0.50e-6}
CLAUDE_DAILY_CALLS = 3000                 # ~$0.60; past it, texts fall back to rules
CLAUDE_CACHE = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_claude.jsonl"
CLAUDE_STATUSES = ("out", "doubtful", "questionable", "probable", "available", "game_time_decision",
                   "expected_starter", "not_starting", "left_game_injured")
CLAUDE_HEDGES = ("confirmed", "per_sources", "expected", "unclear")
TIMINGS = ("upcoming", "past", "unclear")
UNCLEAR_TIMING_CONFIDENCE = 0.5
SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}
CUES = re.compile(r"\b(injur|hurt|questionable|doubtful|probable|out\b|ruled|play|start|status|game.time|concussion|"
                  r"cleared|available|practic|limited|day.to.day|return|back\b|bench|suspen|left the game|carted|"
                  r"surgery|season.ending|miss|sidelined|health|qb1|depth chart|ankle|knee|shoulder|hand\b|wrist|"
                  r"foot|leg\b|arm\b|illness)", re.I)

SYSTEM = """You read one social media post or news headline about college football and report what it says about \
whether the listed quarterbacks will play or start in their team's next game.

Return a claim only for a listed quarterback the text clearly refers to and makes a statement about. Return no claims \
when the text only mentions a quarterback (stats, highlights, recruiting, the transfer portal, the NFL draft, how he \
played), or when the name probably belongs to someone else, such as a player on another team. Practice reports (did \
not practice, limited) count only when the text draws a conclusion about the game.

status:
- out: will not play, ruled out, out for the season, suspended for the game
- doubtful, questionable, probable: as on an injury report, or said in those words
- game_time_decision
- available: cleared, healthy, will play, back at full practice after an injury
- expected_starter: will start, named the starter, expected to start, takes over as the starter
- not_starting: will not start but may play, benched, lost the starting job
- left_game_injured: got hurt in a game or left one with an injury

hedge (how firmly the text states the status itself, not what it means for the next game; details such as the
kind of injury don't hedge it):
- confirmed: the school, the coach, the player or an official report said it, or the text states it as plain fact
- per_sources: the writer attributes it to sources or to their own reporting
- expected: the text calls it likely or expected without the team saying so
- unclear: speculation, a question, an opinion, a rumor or a hypothetical

timing:
- upcoming: about the team's next game or longer, including season-ending news
- past: only about a game already played
- unclear

confidence: from 0 to 1, how sure you are that you read the text correctly."""


class NotConfigured(RuntimeError):
    pass


class ExtractionUnavailable(RuntimeError):
    """The model can't read right now (API error or daily cap); the caller falls back to rules."""


class Extractor(Protocol):
    name: str

    def extract(self, text: str, team_id: int, qbs: pd.DataFrame,
                team: str | None = None) -> list[tuple[str, str, str, float]]: ...


class RuleExtractor:
    name = "rules"

    def extract(self, text: str, team_id: int, qbs: pd.DataFrame,
                team: str | None = None) -> list[tuple[str, str, str, float]]:
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


def surname(name) -> str:
    parts = [p for p in str(name or "").split() if p.lower() not in SUFFIXES]
    return parts[-1] if parts else ""


def names_a_qb(text: str, qbs: pd.DataFrame) -> bool:
    """The pre-filter in front of the model: a listed QB's surname and a status word."""
    low = (text or "").lower()
    named = any(s and re.search(rf"\b{re.escape(s.lower())}\b", low) for s in map(surname, qbs["name"]))
    return bool(named and CUES.search(low))


def _schema(ids: list[str]) -> dict:
    claim = {"type": "object", "additionalProperties": False,
             "required": ["player_id", "status", "hedge", "timing", "confidence"],
             "properties": {"player_id": {"type": "string", "enum": ids},
                            "status": {"type": "string", "enum": list(CLAUDE_STATUSES)},
                            "hedge": {"type": "string", "enum": list(CLAUDE_HEDGES)},
                            "timing": {"type": "string", "enum": list(TIMINGS)},
                            "confidence": {"type": "number"}}}
    return {"type": "object", "additionalProperties": False, "required": ["claims"],
            "properties": {"claims": {"type": "array", "items": claim}}}


def _prompt(text: str, team: str | None, roster: dict[str, str]) -> str:
    lines = [f"Team: {team}"] if team else []
    lines += ["Quarterbacks:"] + [f"- {pid}: {name}" for pid, name in roster.items()]
    return "\n".join(lines + ["", "Text:", "<text>", text, "</text>"])


def _claims(response, ids: set[str]) -> list[tuple[str, str, str, float]]:
    if response.stop_reason != "end_turn":
        return []
    body = next((b.text for b in response.content if b.type == "text"), None)
    try:
        items = json.loads(body)["claims"] if body else []
    except (ValueError, KeyError, TypeError):
        return []
    out = []
    for c in items:
        pid, status, hedge, timing = str(c.get("player_id")), c.get("status"), c.get("hedge"), c.get("timing")
        if pid not in ids or status not in CLAUDE_STATUSES or hedge not in CLAUDE_HEDGES:
            continue
        if timing == "past" and status != "left_game_injured":
            continue
        confidence = min(max(float(c.get("confidence") or 0.0), 0.0), 1.0)
        if timing not in ("upcoming", "past"):
            confidence = min(confidence, UNCLEAR_TIMING_CONFIDENCE)
        claim = (pid, status, hedge, round(confidence, 3))
        if claim not in out:
            out.append(claim)
    return out


class ClaudeExtractor:
    """Reads each distinct text once: results are kept by a hash of the model, prompt, roster and text (never the
    text itself) in an append-only file, so a restart doesn't pay to read the same headlines again."""

    name = CLAUDE_MODEL

    def __init__(self, client=None, cache_path: Path = CLAUDE_CACHE, daily_calls: int = CLAUDE_DAILY_CALLS,
                 clock: Callable[[], datetime] | None = None,
                 secret_fn: Callable[[str, str], str] = credentials.secret) -> None:
        if client is None:
            try:
                key = secret_fn("anthropic", "ANTHROPIC_API_KEY")
            except (RuntimeError, BotoCoreError, ClientError) as e:
                raise NotConfigured(f"no Anthropic key: {e}") from e
            client = anthropic.Anthropic(api_key=key, timeout=30.0, max_retries=2)
        self.client, self.cache_path, self.daily_calls = client, Path(cache_path), daily_calls
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.cache: dict[str, list[tuple[str, str, str, float]]] = {}
        self.tokens = {"input": 0, "output": 0}
        self.day = self.clock().strftime("%Y-%m-%d")
        self.calls_today = 0
        if self.cache_path.exists():
            for line in self.cache_path.read_text().splitlines():
                rec = json.loads(line)
                self.cache[rec["key"]] = [tuple(c) for c in rec["claims"]]
                self.calls_today += rec["at"][:10] == self.day

    def usd(self) -> float:
        return sum(n * CLAUDE_USD_PER_TOKEN[k] for k, n in self.tokens.items())

    def extract(self, text: str, team_id: int, qbs: pd.DataFrame,
                team: str | None = None) -> list[tuple[str, str, str, float]]:
        if not names_a_qb(text, qbs):
            return []
        roster = dict(zip(qbs["player_id"].astype(str), qbs["name"].astype(str)))
        key = hashlib.sha256(json.dumps([CLAUDE_MODEL, SYSTEM, team, sorted(roster.items()), text]).encode())
        key = key.hexdigest()
        if key in self.cache:
            return list(self.cache[key])
        now = self.clock()
        if now.strftime("%Y-%m-%d") != self.day:
            self.day, self.calls_today = now.strftime("%Y-%m-%d"), 0
        if self.calls_today >= self.daily_calls:
            raise ExtractionUnavailable(f"daily cap of {self.daily_calls} reads reached")
        self.calls_today += 1
        try:
            response = self.client.messages.create(
                model=CLAUDE_MODEL, max_tokens=2048, system=SYSTEM,
                messages=[{"role": "user", "content": _prompt(text, team, roster)}],
                output_config={"effort": CLAUDE_EFFORT,
                               "format": {"type": "json_schema", "schema": _schema(list(roster))}})
        except anthropic.APIError as e:
            raise ExtractionUnavailable(f"{type(e).__name__}: {e}") from e
        claims = _claims(response, set(roster))
        usage = getattr(response, "usage", None)
        used = {"input": getattr(usage, "input_tokens", 0) or 0, "output": getattr(usage, "output_tokens", 0) or 0}
        for k, n in used.items():
            self.tokens[k] += n
        self.cache[key] = claims
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with self.cache_path.open("a") as f:
            f.write(json.dumps({"key": key, "claims": claims, "at": now.isoformat(), "stop": response.stop_reason,
                                **used}) + "\n")
        return list(claims)
