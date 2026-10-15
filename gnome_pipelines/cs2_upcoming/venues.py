"""
The venues CS2 matches trade on, behind one shape so cs2_upcoming treats them alike.

Each venue lists its CS2 series and map-1 markets as rows identity.resolve_match can read:
market_slug (the venue's id for the market), market ("series" or "game1"), meeting (shared by
a series and its map markets), o0/o1 (normalised outcome team names), kickoff (UTC),
instruments (what a book is fetched by, per outcome), esids (the registry's
exchange_security_id, per outcome) and final (settled value per outcome, [] until decided).

Books come back in the terms of the instrument asked for: (bids, asks, book time), each side
price -> size. A venue that quotes one side only (Kalshi's yes/no bids, Polymarket US's long
book) is converted here, so a book row never needs to know the venue.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from gnomepy_research.models.cs2.prematch import polymarket
from gnomepy_research.models.cs2.prematch.polymarket import norm

MARKET_COLUMNS = ["market_slug", "market", "meeting", "o0", "o1", "kickoff", "instruments", "esids", "final"]
Ladder = tuple[dict[float, float], dict[float, float], pd.Timestamp]
# Settled prices sit at 0 and 1; anything short of this is not a decided market.
DECIDED = 0.9
# Polymarket US answered 8 parallel book requests with 429s; 3 at a time stays under its limit.
BOOK_WORKERS = 3
TIMEOUT = 30
RETRIES = 4


class Venue(Protocol):
    # Names the venue on legs and book rows, and is its source in cs2_team_aliases.
    name: str
    # The registry's exchange_name, looked up to an exchange_id per account.
    exchange_name: str

    def markets(self, now: pd.Timestamp) -> pd.DataFrame: ...

    def books(self, instruments: list[str]) -> dict[str, Ladder]: ...

    def finals(self, series_ids: list[str]) -> dict[str, list[float]]: ...


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=MARKET_COLUMNS)


def _decided(values: list[float]) -> list[float]:
    return values if len(values) == 2 and max(values) >= DECIDED else []


def _flip(side: dict[float, float]) -> dict[float, float]:
    return {round(1 - p, 4): q for p, q in side.items()}


def _get(url: str, **kwargs) -> requests.Response:
    """GET, waiting out rate limits and transient server errors; anything else raises at once."""
    for attempt in range(RETRIES):
        resp = requests.get(url, timeout=TIMEOUT, **kwargs)
        if resp.status_code != 429 and resp.status_code < 500:
            break
        if attempt < RETRIES - 1:
            time.sleep(float(resp.headers.get("Retry-After") or 2 ** attempt))
    resp.raise_for_status()
    return resp


def _parallel(fetch, keys: list[str]) -> dict:
    with ThreadPoolExecutor(BOOK_WORKERS) as pool:
        return {k: v for k, v in zip(keys, pool.map(fetch, keys)) if v is not None}


class PolymarketIntl:
    """Polymarket's international CLOB: one market per meeting and kind, a token per outcome."""

    name = "polymarket"
    exchange_name = "Polymarket (International)"
    CLOB = "https://clob.polymarket.com"
    # The CLOB doesn't document a batch limit for /books, so requests stay small.
    BOOKS_PER_REQUEST = 20
    LOOKBACK = pd.Timedelta(days=21)

    def __init__(self) -> None:
        self._markets: pd.DataFrame | None = None

    def markets(self, now: pd.Timestamp) -> pd.DataFrame:
        naive = now.tz_localize(None)
        pm = polymarket.fetch_markets(naive - self.LOOKBACK, naive + pd.Timedelta(days=2))
        self._markets = self.to_frame(pm)
        return self._markets

    @staticmethod
    def to_frame(pm: pd.DataFrame) -> pd.DataFrame:
        """polymarket.index_markets output, as venue rows."""
        rows = []
        for m in pm[pm.market.isin(["series", "game1"])].itertuples():
            tokens = [str(t) for t in m.tokens]
            rows.append({
                "market_slug": m.market_slug, "market": m.market,
                "meeting": m.market_slug.removesuffix("-game1"), "o0": m.o0, "o1": m.o1, "kickoff": m.kickoff,
                "instruments": tokens, "esids": [f"{m.condition_id}:{t}" for t in tokens],
                "final": _decided([float(x) for x in m.final]) if m.closed else [],
            })
        return _frame(rows)

    def books(self, instruments: list[str]) -> dict[str, Ladder]:
        out: dict[str, Ladder] = {}
        for start in range(0, len(instruments), self.BOOKS_PER_REQUEST):
            batch = instruments[start:start + self.BOOKS_PER_REQUEST]
            resp = requests.post(f"{self.CLOB}/books", json=[{"token_id": t} for t in batch], timeout=TIMEOUT)
            resp.raise_for_status()
            out.update({b["asset_id"]: self.ladder(b) for b in resp.json()})
        return out

    @staticmethod
    def ladder(book: dict) -> Ladder:
        side = lambda levels: {float(lv["price"]): float(lv["size"]) for lv in levels}
        at = pd.Timestamp(int(book["timestamp"]), unit="ms", tz="UTC") if book.get("timestamp") else pd.NaT
        return side(book.get("bids", [])), side(book.get("asks", [])), at

    def finals(self, series_ids: list[str]) -> dict[str, list[float]]:
        if self._markets is None:
            self.markets(pd.Timestamp.now(tz="UTC"))
        hits = self._markets[self._markets.market_slug.isin(series_ids)]
        return {r.market_slug: r.final for r in hits.itertuples() if r.final}


class Kalshi:
    """Kalshi: an event per meeting and kind, a yes/no market per team; the registry lists each team's market."""

    name = "kalshi"
    exchange_name = "Kalshi"
    API = "https://external-api.kalshi.com/trade-api/v2"
    SERIES = {"series": "KXCS2GAME", "game1": "KXCS2MAP"}
    # Kalshi encodes the scheduled start in the event ticker, in US Eastern time: 26OCT152000 is 20:00 ET, Oct 15 2026.
    STARTS = re.compile(r"^(\d{2})([A-Z]{3})(\d{2})(\d{2})(\d{2})")
    EASTERN = ZoneInfo("America/New_York")
    MONTHS = {m: i for i, m in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}

    def markets(self, now: pd.Timestamp) -> pd.DataFrame:
        events = [e for series in self.SERIES.values() for e in self._events(series)]
        return self.to_frame(events)

    def _events(self, series: str) -> list[dict]:
        events, cursor = [], None
        while True:
            params = {"series_ticker": series, "status": "open", "with_nested_markets": "true", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = _get(f"{self.API}/events", params=params)
            body = resp.json()
            events += body.get("events", [])
            cursor = body.get("cursor")
            if not cursor or not body.get("events"):
                return events

    @classmethod
    def to_frame(cls, events: list[dict]) -> pd.DataFrame:
        rows = []
        for e in events:
            row = cls._row(e)
            if row is not None:
                rows.append(row)
        return _frame(rows)

    @classmethod
    def _row(cls, event: dict) -> dict | None:
        series, _, rest = event["event_ticker"].partition("-")
        market = next((k for k, v in cls.SERIES.items() if v == series), None)
        if market == "game1":
            meeting, _, game = rest.rpartition("-")
            if game != "1":
                return None
        else:
            meeting = rest
        # Sorted by ticker so the outcome order, which team_a_outcome indexes, is the same every run.
        teams = sorted(event.get("markets", []), key=lambda m: m["ticker"])
        if market is None or len(teams) != 2:
            return None
        return {
            "market_slug": event["event_ticker"], "market": market, "meeting": meeting,
            "o0": norm(teams[0].get("yes_sub_title")), "o1": norm(teams[1].get("yes_sub_title")),
            "kickoff": cls.kickoff(meeting), "instruments": [m["ticker"] for m in teams],
            "esids": [m["ticker"] for m in teams], "final": cls._final(teams),
        }

    @classmethod
    def kickoff(cls, meeting: str) -> pd.Timestamp | None:
        m = cls.STARTS.match(meeting)
        if m is None or m.group(2) not in cls.MONTHS:
            return None
        yy, mon, dd, hh, mm = m.groups()
        local = dt.datetime(2000 + int(yy), cls.MONTHS[mon], int(dd), int(hh), int(mm), tzinfo=cls.EASTERN)
        return pd.Timestamp(local).tz_convert("UTC")

    @staticmethod
    def _final(teams: list[dict]) -> list[float]:
        results = [m.get("result") for m in teams]
        return [1.0 if r == "yes" else 0.0 for r in results] if all(r in ("yes", "no") for r in results) else []

    def books(self, instruments: list[str]) -> dict[str, Ladder]:
        return _parallel(self._book, instruments)

    def _book(self, ticker: str) -> Ladder:
        resp = _get(f"{self.API}/markets/{ticker}/orderbook")
        return self.ladder(resp.json())

    @staticmethod
    def ladder(body: dict) -> Ladder:
        """Kalshi quotes bids only: a no bid at p is a yes offer at 1 - p."""
        book = body.get("orderbook_fp") or {}
        side = lambda levels: {float(p): float(q) for p, q in levels or []}
        return side(book.get("yes_dollars")), _flip(side(book.get("no_dollars"))), pd.NaT

    def finals(self, series_ids: list[str]) -> dict[str, list[float]]:
        out = {}
        for event_ticker in series_ids:
            resp = _get(f"{self.API}/events/{event_ticker}", params={"with_nested_markets": "true"})
            event = resp.json()["event"]
            final = self._final(sorted(event.get("markets", []), key=lambda m: m["ticker"]))
            if final:
                out[event_ticker] = final
        return out


class PolymarketUs:
    """Polymarket US: a market per meeting and kind with a long and a short side; the long side names its team."""

    name = "polymarket_us"
    exchange_name = "Polymarket (US)"
    GATEWAY = "https://gateway.polymarket.us/v1"
    KINDS = {"esports_match_winner": "series", "esports_map_winner_1": "game1"}
    PAGE = 100

    def markets(self, now: pd.Timestamp) -> pd.DataFrame:
        events, offset = [], 0
        while True:
            resp = _get(f"{self.GATEWAY}/events", params={
                "tagSlug": "cs2", "closed": "false", "limit": self.PAGE, "offset": offset})
            page = resp.json().get("events", [])
            events += page
            if len(page) < self.PAGE:
                return self.to_frame(events)
            offset += self.PAGE

    @classmethod
    def to_frame(cls, events: list[dict]) -> pd.DataFrame:
        rows = []
        for e in events:
            by_kind = {cls.KINDS[m.get("sportsMarketType")]: m for m in e.get("markets", [])
                       if m.get("sportsMarketType") in cls.KINDS}
            series = by_kind.get("series")
            teams = cls._side_teams(series) if series else None
            if teams is None:
                continue
            for kind, m in by_kind.items():
                sides = cls._side_teams(m) if kind == "game1" else teams
                long_team = sides[0] if sides else None
                if long_team not in teams:
                    continue
                short_team = teams[1] if long_team == teams[0] else teams[0]
                start = m.get("gameStartTime") or e.get("startTime")
                rows.append({
                    "market_slug": m["slug"], "market": kind, "meeting": e["slug"],
                    "o0": norm(long_team), "o1": norm(short_team),
                    "kickoff": pd.Timestamp(start).tz_convert("UTC") if start else None,
                    "instruments": [f"{m['slug']}:long", f"{m['slug']}:short"],
                    "esids": [f"{m['slug']}:long", f"{m['slug']}:short"],
                    "final": cls._final(m),
                })
        return _frame(rows)

    @staticmethod
    def _side_teams(market: dict) -> tuple[str | None, str | None] | None:
        """(long side's team, short side's team) by name; a map market names its team on both sides."""
        sides = market.get("marketSides") or []
        long = next((s for s in sides if s.get("long")), None)
        short = next((s for s in sides if not s.get("long")), None)
        if long is None or short is None:
            return None
        name = lambda s: (s.get("team") or {}).get("name") or s.get("description")
        return name(long), name(short)

    @staticmethod
    def _final(market: dict) -> list[float]:
        if not market.get("closed"):
            return []
        try:
            prices = [float(x) for x in json.loads(market.get("outcomePrices") or "[]")]
        except ValueError:
            return []
        return _decided(prices)

    def books(self, instruments: list[str]) -> dict[str, Ladder]:
        return _parallel(self._book, instruments)

    def _book(self, instrument: str) -> Ladder:
        slug, _, side = instrument.rpartition(":")
        resp = _get(f"{self.GATEWAY}/markets/{slug}/book")
        return self.ladder(resp.json(), side)

    @staticmethod
    def ladder(body: dict, side: str) -> Ladder:
        """The book is in long terms; the short side's bids are the long side's offers, flipped."""
        data = body.get("marketData") or {}
        levels = lambda key: {float(lv["px"]["value"]): float(lv["qty"]) for lv in data.get(key) or []}
        bids, asks = levels("bids"), levels("offers")
        at = pd.Timestamp(data["transactTime"]).tz_convert("UTC") if data.get("transactTime") else pd.NaT
        return (bids, asks, at) if side == "long" else (_flip(asks), _flip(bids), at)

    def finals(self, series_ids: list[str]) -> dict[str, list[float]]:
        if not series_ids:
            return {}
        resp = _get(f"{self.GATEWAY}/markets", params={"slug": series_ids, "limit": len(series_ids)})
        return {m["slug"]: f for m in resp.json().get("markets", []) if (f := self._final(m))}


VENUES: tuple[type[Venue], ...] = (PolymarketIntl, Kalshi, PolymarketUs)
