"""What a review page says about itself: when it was published, its headline and opening, and any rating.

Ported from the research run (~/gnome-research-work/rt/outlets/fetch_reviews.py), whose 1,804 pages it was checked on.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime, timezone

TEXT_CHARS = 2500


@dataclass(frozen=True)
class PageFacts:
    published_at: datetime | None
    headline: str | None
    summary: str | None
    text: str
    rating_kind: str | None = None
    rating_value: float | None = None
    rating_grade: str | None = None


def meta(page: str, prop: str) -> str | None:
    m = re.search(rf'<meta[^>]+(?:property|name)="{re.escape(prop)}"[^>]+content="([^"]*)"', page) or \
        re.search(rf'<meta[^>]+content="([^"]*)"[^>]+(?:property|name)="{re.escape(prop)}"', page)
    return html.unescape(m[1]) if m else None


def published(page: str) -> datetime | None:
    raw = next((v for p in ("article:published_time", "datePublished", "pubdate", "parsely-pub-date")
                if (v := meta(page, p))), None)
    if raw is None:
        m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', page)
        raw = m[1] if m else None
    if raw is None:
        return None
    try:
        t = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def rating(page: str, outlet: str) -> tuple[str, float | None, str | None] | None:
    """(kind, value 0-1, grade): a structured score, a letter grade, or the NYT's Critic's Pick."""
    m = re.search(r'"reviewRating"\s*:\s*\{[^}]*?"ratingValue"\s*:\s*"?([\d.]+)"?[^}]*?(?:"bestRating"\s*:\s*"?([\d.]+))?',
                  page)
    if m:
        v = float(m[1])
        # Without bestRating, star ratings are out of 5 and scores out of 10; IGN and Collider always use 10.
        best = float(m[2]) if m[2] else (10.0 if outlet in ("ign", "collider") or v > 5 else 5.0)
        return "score", min(v / best, 1.0), None
    m = re.search(r"Grade:\s*([A-F][+-]?)", page)
    if m:
        return "grade", None, m[1]
    if outlet == "nyt" and re.search(r"Critic[’']s Pick", page):
        return "critics_pick", 1.0, None
    return None


# Only real <p> tags: "<p[^>]*>" also opens on <path> and <picture>, and on a page with thousands of SVG paths each one
# then scans ahead for a </p>, which took minutes on 3 MB pages.
PARAGRAPH = re.compile(r"<p(?:\s[^>]*)?>(.*?)</p>", re.S)


def body_text(page: str) -> str:
    parts, size = [], 0
    for m in PARAGRAPH.finditer(page):
        text = html.unescape(re.sub(r"<[^>]+>", "", m[1])).strip()
        parts.append(text)
        size += len(text) + 1
        if size > TEXT_CHARS:
            break
    return re.sub(r"\s+", " ", " ".join(parts)).strip()[:TEXT_CHARS]


def is_review_page(page: str) -> bool:
    """An article page rather than a bot wall served with a 200: every real page of the research run carried og:title,
    and every page without one was The Times' "Verifying Device" wall."""
    return "og:title" in page


def facts(page: str, outlet: str) -> PageFacts:
    r = rating(page, outlet)
    return PageFacts(published(page), meta(page, "og:title"), meta(page, "og:description"), body_text(page),
                     *(r if r else (None, None, None)))
