"""Which outlet URLs are reviews of which films, and what a sitemap or RSS feed lists.

The matcher is the research run's (~/gnome-research-work/rt/outlets/harvest.py): a film's title tokens must appear as
a run inside the URL's last path segment, which must also say "review"; a one-word title must sit next to "review"
(or start the slug), since "Michael" or "Buddy" would otherwise match other films' reviews.
"""
from __future__ import annotations

import gzip
import html
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Review-shaped URLs that aren't film reviews.
NOT_FILM = re.compile(r"theater-review|season-\d|episode|-tv-review|album|book-review|game-review|switch-2|ps5|xbox",
                      re.I)


def toks(text: str) -> list[str]:
    s = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower().replace("&", "and")
    s = re.sub(r"[’'`.]", "", s)
    return [t for t in re.split(r"[^a-z0-9]+", s) if t]


def film_id(title: str) -> str:
    """A film's id across venues: its title's tokens, so '"How to Rob a Bank"' and 'How to Rob a Bank' agree."""
    return "-".join(toks(title))


def slug_toks(url: str) -> list[str]:
    path = url.rstrip("/").split("/")[-1]
    path = re.sub(r"\.html?$", "", path)
    return toks(re.sub(r"-\d{6,}$", "", path))


def _contains(hay: list[str], needle: list[str]) -> bool:
    n = len(needle)
    return n > 0 and any(hay[i:i + n] == needle for i in range(len(hay) - n + 1))


def is_review_of(url: str, title: str, pattern: str = "review") -> bool:
    if not re.search(pattern, url, re.I) or NOT_FILM.search(url):
        return False
    st = slug_toks(url)
    if "review" not in st:
        return False
    nt = toks(title)
    alt = nt[1:] if nt[:1] == ["the"] and len(nt) > 2 else nt
    if len(alt) <= 1:
        return (_contains(st, alt + ["review"]) or _contains(st, alt + ["movie", "review"])
                or (st[:len(alt)] == alt and bool(alt)))
    return _contains(st, nt) or _contains(st, alt)


def _stamp(text: str | None) -> datetime | None:
    if not text:
        return None
    text = text.strip()
    try:
        t = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            t = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def decode(body: bytes) -> str:
    while body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body.decode("utf-8", "replace")


def entries(xml: str) -> list[tuple[str, datetime | None]]:
    """(url, its listed time) for each <url> of a sitemap or <item> of an RSS feed."""
    out = []
    for block in re.findall(r"<url>(.*?)</url>", xml, re.S):
        loc = re.search(r"<loc>\s*([^<\s]+)", block)
        when = re.search(r"<news:publication_date>([^<]+)|<lastmod>([^<]+)", block)
        if loc:
            out.append((html.unescape(loc[1]), _stamp(when[1] or when[2]) if when else None))
    for block in re.findall(r"<item>(.*?)</item>", xml, re.S):
        link = re.search(r"<link>\s*(?:<!\[CDATA\[)?\s*([^<\]\s]+)", block)
        when = re.search(r"<pubDate>([^<]+)|<dc:date>([^<]+)", block)
        if link:
            out.append((html.unescape(link[1]), _stamp(when[1] or when[2]) if when else None))
    return out


def is_listing(text: str) -> bool:
    """A sitemap, sitemap index or feed, rather than a bot wall served with a 200."""
    head = text[:5000]
    return any(tag in head for tag in ("<urlset", "<sitemapindex", "<rss", "<feed", "<rdf:RDF"))


def child_sitemaps(xml: str) -> list[tuple[str, datetime | None]]:
    """The children of a sitemap index, with their lastmod; empty when xml isn't an index."""
    if "<sitemapindex" not in xml:
        return []
    return [(html.unescape(u), _stamp(lm)) for u, lm in re.findall(
        r"<sitemap>\s*<loc>\s*([^<\s]+)\s*</loc>(?:\s*<lastmod>([^<]*)</lastmod>)?", xml)]
