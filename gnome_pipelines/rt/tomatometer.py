"""A film's live Tomatometer, from its public RT page: the critics' liked and not-liked counts and the score.

The page carries them as JSON (`"criticsScore":{..."likedCount":45,"notLikedCount":140,...,"scorePercent":"24%"}`),
the same counts the research run read from Wayback captures. Fetched through ZenRows, by the user's decision; only
the public movie page, never RT's session-gated review API.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

PAGE = "https://www.rottentomatoes.com/m/{slug}"


@dataclass(frozen=True)
class Counts:
    liked: int
    not_liked: int
    score: float | None


def is_rt_page(page: str) -> bool:
    """An RT film page (with or without counts yet), rather than a bot wall served with a 200."""
    return '"criticsScore"' in page or 'content="Rotten Tomatoes"' in page


def counts(page: str) -> Counts | None:
    """The first criticsScore block with counts; None when the page has none (a bot wall, a page without critics)."""
    for block in re.findall(r'"criticsScore"\s*:\s*\{([^{}]*)\}', page):
        liked = re.search(r'"likedCount"\s*:\s*(\d+)', block)
        not_liked = re.search(r'"notLikedCount"\s*:\s*(\d+)', block)
        if liked and not_liked:
            score = re.search(r'"scorePercent"\s*:\s*"(\d+)%"', block)
            return Counts(int(liked[1]), int(not_liked[1]), float(score[1]) if score else None)
    return None
