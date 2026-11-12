"""The films with an open Rotten Tomatoes market on any venue, merged by title, and each one's RT page.

Kalshi lists them as KXRT events (the film is the event's sub_title, the strikes its markets' floor_strike);
Polymarket and Polymarket US as '"<Film>" Rotten Tomatoes Score?' events. None of the venues links the film's own RT
page (Kalshi's settlement source is RT's home page), so a film's slug comes from RT's search, once: the result whose
title is the film's and whose release year is nearest the market's close.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Callable
from urllib.parse import quote

from gnome_pipelines.polling.fetch import Fetcher
from gnome_pipelines.rt.match import film_id, toks

KALSHI = "https://api.elections.kalshi.com/trade-api/v2/events?series_ticker=KXRT&status=open&with_nested_markets=true&limit=200"
POLYMARKET = ("https://gamma-api.polymarket.com/public-search?q=rotten%20tomatoes&events_status=active"
              "&limit_per_type=50&page={page}")
POLYMARKET_US = "https://gateway.polymarket.us/v1/search?query=rotten%20tomatoes&limit=100"
RT_SEARCH = "https://www.rottentomatoes.com/search?search={query}"
TITLE = re.compile(r'^\s*[“"]?(?P<title>.+?)[”"]?\s+Rotten Tomatoes [Ss]core\?\s*$')
MAX_POLYMARKET_PAGES = 10
MISSES_TO_CLOSE = 3


@dataclass
class FilmState:
    """A film as the venues list it now; a row is written when this changes."""
    film_id: str
    title: str
    close: datetime | None
    open: bool = True
    kalshi_event: str | None = None
    kalshi_strikes: tuple[int, ...] = ()
    polymarket_event: str | None = None
    polymarket_us_event: str | None = None
    rt_slug: str | None = None
    first_seen_at: datetime | None = field(default=None, compare=False)

    def row(self) -> dict:
        return {"film_id": self.film_id, "title": self.title, "rt_slug": self.rt_slug, "close": self.close,
                "open": self.open, "kalshi_event": self.kalshi_event, "kalshi_strikes": list(self.kalshi_strikes),
                "polymarket_event": self.polymarket_event, "polymarket_us_event": self.polymarket_us_event,
                "first_seen_at": self.first_seen_at}


def _when(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def kalshi(events: list[dict]) -> list[FilmState]:
    out = []
    for e in events:
        title = (e.get("sub_title") or "").strip() or e.get("title", "").split("·")[0].strip()
        markets = e.get("markets") or []
        closes = [_when(m.get("close_time")) for m in markets if m.get("close_time")]
        strikes = sorted({int(m["floor_strike"]) for m in markets if m.get("floor_strike") is not None})
        out.append(FilmState(film_id(title), title, max(closes) if closes else None, kalshi_event=e["event_ticker"],
                             kalshi_strikes=tuple(strikes)))
    return out


def polymarket(events: list[dict], venue: str) -> list[FilmState]:
    out = []
    for e in events:
        m = TITLE.match(e.get("title") or "")
        if not m or e.get("closed") or e.get("active") is False:
            continue
        title = m["title"].strip()
        key = "polymarket_event" if venue == "polymarket" else "polymarket_us_event"
        out.append(FilmState(film_id(title), title, _when(e.get("endDate")), **{key: e["slug"]}))
    return out


def merge(*venues: list[FilmState]) -> dict[str, FilmState]:
    """One state per film: each venue's ids, the earliest close, the first title seen (Kalshi's when it lists it)."""
    merged: dict[str, FilmState] = {}
    for listed in venues:
        for f in listed:
            have = merged.get(f.film_id)
            if have is None:
                merged[f.film_id] = f
                continue
            closes = [c for c in (have.close, f.close) if c is not None]
            merged[f.film_id] = replace(
                have, close=min(closes) if closes else None,
                kalshi_event=have.kalshi_event or f.kalshi_event, kalshi_strikes=have.kalshi_strikes or f.kalshi_strikes,
                polymarket_event=have.polymarket_event or f.polymarket_event,
                polymarket_us_event=have.polymarket_us_event or f.polymarket_us_event)
    return merged


def rt_slug_from_search(page: str, title: str, close: datetime | None) -> str | None:
    """The slug of the search result titled exactly like the film, released nearest the market's close."""
    want, best = toks(title), None
    for attrs, body in re.findall(r"<search-page-media-row([^>]*)>(.*?)</search-page-media-row>", page, re.S):
        link = re.search(r'href="https://www\.rottentomatoes\.com/m/([^"/?]+)"', body)
        name = re.search(r'slot="title"[^>]*>\s*([^<]+)', body)
        year = re.search(r'release-year="(\d{4})"', attrs)
        if not link or not name or toks(name[1]) != want:
            continue
        distance = abs(int(year[1]) - close.year) if year and close else 99
        if best is None or distance < best[0]:
            best = (distance, link[1])
    return best[1] if best and best[0] <= 1 else None


class Venues:
    """Reads the venues' listings through the source's Fetcher, keeping each URL's last answer for its 304s."""

    def __init__(self, fetcher: Fetcher) -> None:
        self.fetcher = fetcher
        self._last: dict[str, dict] = {}

    def _json(self, url: str) -> dict:
        page = self.fetcher.get(url)
        if page.status == 304:
            return self._last.get(url, {})
        if not page.ok:
            raise RuntimeError(f"HTTP {page.status} from {url.split('/')[2]}")
        self._last[url] = json.loads(page.text)
        return self._last[url]

    def kalshi(self) -> list[dict]:
        events, url = [], KALSHI
        while True:
            d = self._json(url)
            events += d.get("events", [])
            cursor = d.get("cursor")
            if not cursor or not d.get("events"):
                return events
            url = f"{KALSHI}&cursor={cursor}"

    def polymarket(self) -> list[dict]:
        events = []
        for page in range(1, MAX_POLYMARKET_PAGES + 1):
            d = self._json(POLYMARKET.format(page=page))
            events += d.get("events") or []
            if not (d.get("pagination") or {}).get("hasMore"):
                break
        return events

    def polymarket_us(self) -> list[dict]:
        return self._json(POLYMARKET_US).get("events") or []

    def open_films(self) -> dict[str, FilmState]:
        return merge(kalshi(self.kalshi()), polymarket(self.polymarket(), "polymarket"),
                     polymarket(self.polymarket_us(), "polymarket_us"))

    def rt_slug(self, title: str, close: datetime | None, via: str) -> str | None:
        # A 200 without results rows is a bot wall, not an empty search: it steps up to the premium proxy.
        page = self.fetcher.get(RT_SEARCH.format(query=quote(title)), via=via, kind="search",
                                valid=lambda text: "<search-page-media-row" in text or "No Results" in text)
        return rt_slug_from_search(page.text, title, close) if page.ok else None


def changed(previous: dict[str, FilmState], now_open: dict[str, FilmState],
            slug_of: Callable[[FilmState], str | None],
            misses: dict[str, int]) -> tuple[dict[str, FilmState], list[FilmState]]:
    """Every film known now (open ones with their RT slugs, closed ones as last seen) and the states to record: new
    or changed films, and films no longer listed for MISSES_TO_CLOSE passes in a row, recorded once as closed (a
    venue's search can leave a film out of one answer). misses counts each film's absences and is updated here."""
    current, updates = {}, []
    for fid, f in now_open.items():
        misses.pop(fid, None)
        before = previous.get(fid)
        slug = before.rt_slug if before and before.rt_slug else slug_of(f)
        f = replace(f, rt_slug=slug)
        current[fid] = f
        if before != f:
            updates.append(f)
    for fid, before in previous.items():
        if fid in now_open:
            continue
        if before.open:
            misses[fid] = misses.get(fid, 0) + 1
            if misses[fid] < MISSES_TO_CLOSE:
                current[fid] = before
                continue
            misses.pop(fid)
            current[fid] = replace(before, open=False)
            updates.append(current[fid])
        else:
            current[fid] = before
    return current, updates
