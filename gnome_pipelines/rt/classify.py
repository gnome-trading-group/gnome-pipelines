"""Claude's read of a review page: is it a review of this film, and would Rotten Tomatoes count it fresh.

The prompt and schema are the research run's (~/gnome-research-work/rt/outlets/classify.py), which agreed with the
outlets' own ratings on the pages that had one and gave the +11c/contract signal at +5 minutes.
"""
from __future__ import annotations

from dataclasses import dataclass

from gnome_pipelines.polling.claude import ClaudeReader, cache_key
from gnome_pipelines.rt.page import PageFacts

MODEL = "claude-haiku-5-5"
EFFORT = "low"
# Room for the low-effort thinking: at 200 the research run lost 35 of 1,800 answers to truncation.
MAX_TOKENS = 1500
DAILY_CALLS = 2000
SYSTEM = """You read the opening of a published article and decide two things for a Rotten Tomatoes study.
is_review: true only if the article is a critic's review of the named feature film (not a TV show, game, news story,
interview, box-office report or a review of a different film with a similar name).
fresh: Rotten Tomatoes' binary call for this review: true if the critic's overall verdict is positive or mixed-positive
(would be counted Fresh), false if negative or mixed-negative (Rotten). Judge the critic's overall recommendation, not
the film's subject. confidence: your probability that the fresh call is right (0.5-1)."""
SCHEMA = {"type": "object", "additionalProperties": False, "required": ["is_review", "fresh", "confidence"],
          "properties": {"is_review": {"type": "boolean"}, "fresh": {"type": "boolean"},
                         "confidence": {"type": "number"}}}


@dataclass(frozen=True)
class Verdict:
    is_review: bool
    fresh: bool | None
    confidence: float | None


def prompt(title: str, outlet: str, url: str, page: PageFacts) -> str:
    return (f"Film: {title}\nOutlet: {outlet}\nURL: {url}\nHeadline: {page.headline}\n"
            f"Summary: {page.summary}\n\nArticle opening:\n{page.text}")


def classify(reader: ClaudeReader, title: str, outlet: str, url: str, page: PageFacts) -> Verdict | None:
    """None when Claude gave no usable answer (refused or cut off); raises polling.claude.Unavailable when it can't
    be asked (API error, daily cap)."""
    text = prompt(title, outlet, url, page)
    out = reader.read(cache_key(MODEL, SYSTEM, text), SYSTEM, text, SCHEMA, effort=EFFORT, max_tokens=MAX_TOKENS)
    if not out:
        return None
    is_review = bool(out.get("is_review"))
    confidence = out.get("confidence")
    return Verdict(is_review, bool(out.get("fresh")) if is_review else None,
                   float(confidence) if isinstance(confidence, (int, float)) else None)
