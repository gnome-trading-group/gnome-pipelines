"""
The venue adapters: each venue's markets as identity-ready rows, and its books in the terms of
the outcome asked for. Events are trimmed captures from 2026-10-15 (FlyQuest vs Off Chops).
"""
import json
from pathlib import Path

import pandas as pd
import pytest
import requests

from gnome_pipelines.cs2_upcoming import venues
from gnome_pipelines.cs2_upcoming.venues import Kalshi, PolymarketIntl, PolymarketUs
from gnomepy_research.models.cs2.prematch.polymarket import index_markets

FIXTURES = Path(__file__).parent / "fixtures" / "cs2_venues"
KICK = pd.Timestamp("2026-10-16 00:00", tz="UTC")


def _events(name):
    return json.loads((FIXTURES / name).read_text())["events"]


def test_kalshi_rows_are_one_per_event_with_the_eastern_kickoff_in_utc():
    frame = Kalshi.to_frame(_events("kalshi_events_2026-10-15.json"))
    assert frame.market.tolist() == ["series", "game1"], "map 2 is not tracked"
    series = frame.iloc[0]
    assert series.market_slug == "KXCS2GAME-26OCT152000OFFFLY" and series.meeting == "26OCT152000OFFFLY"
    assert series.kickoff == KICK, "20:00 EDT is midnight UTC"
    assert (series.o0, series.o1) == ("flyquest", "offchops")
    assert series.instruments == series.esids == ["KXCS2GAME-26OCT152000OFFFLY-FLY", "KXCS2GAME-26OCT152000OFFFLY-OFF"]
    assert frame.iloc[1].meeting == series.meeting and frame.iloc[1].final == []


def test_kalshi_kickoff_follows_daylight_saving():
    assert Kalshi.kickoff("26DEC151200ABCXYZ") == pd.Timestamp("2026-12-15 17:00", tz="UTC")
    assert Kalshi.kickoff("NOTADATE") is None


def test_kalshi_settles_only_when_both_markets_have_a_result():
    teams = [{"ticker": "X-A", "result": "no"}, {"ticker": "X-B", "result": "yes"}]
    assert Kalshi._final(teams) == [0.0, 1.0]
    assert Kalshi._final([{"ticker": "X-A", "result": ""}, {"ticker": "X-B", "result": "yes"}]) == []


def test_polymarket_us_map_market_names_its_team_long_and_the_opponent_short():
    frame = PolymarketUs.to_frame(_events("polymarket_us_events_2026-10-15.json"))
    assert sorted(frame.market) == ["game1", "series"], "map 2 is not tracked"
    by = frame.set_index("market")
    assert (by.loc["series"].o0, by.loc["series"].o1) == ("flyquest", "offchops")
    assert (by.loc["game1"].o0, by.loc["game1"].o1) == ("flyquest", "offchops")
    assert by.loc["game1"].instruments == ["astatc-cs2-fly-ocf-2026-10-16-map1:long",
                                           "astatc-cs2-fly-ocf-2026-10-16-map1:short"]
    assert by.loc["series"].kickoff == KICK and by.loc["series"].meeting == by.loc["game1"].meeting


def test_polymarket_us_settles_from_outcome_prices_once_closed():
    assert PolymarketUs._final({"closed": True, "outcomePrices": '["0","1"]'}) == [0.0, 1.0]
    assert PolymarketUs._final({"closed": False, "outcomePrices": '["0","1"]'}) == []
    assert PolymarketUs._final({"closed": True, "outcomePrices": '["0.5","0.5"]'}) == [], "a tie is not decided"


def test_polymarket_intl_rows_keep_token_order_and_registry_ids():
    pm = index_markets(pd.DataFrame([
        {"market_slug": "cs2-fly-offcho-2026-10-15", "condition_id": "0xs", "outcomes": ["FlyQuest", "Off Chops"],
         "tokens": [11, 12], "final": ["1", "0"], "closed": True, "kickoff": KICK},
        {"market_slug": "cs2-fly-offcho-2026-10-15-game1", "condition_id": "0xg", "outcomes": ["Off Chops", "FlyQuest"],
         "tokens": [21, 22], "final": [], "closed": False, "kickoff": KICK}]))
    frame = PolymarketIntl.to_frame(pm).set_index("market")
    assert frame.loc["series"].esids == ["0xs:11", "0xs:12"] and frame.loc["series"].final == [1.0, 0.0]
    assert frame.loc["game1"].meeting == "cs2-fly-offcho-2026-10-15" and frame.loc["game1"].final == []


def test_kalshi_no_bids_are_yes_offers():
    bids, asks, _ = Kalshi.ladder({"orderbook_fp": {"yes_dollars": [["0.40", "10"], ["0.42", "5"]],
                                                    "no_dollars": [["0.50", "7"], ["0.55", "3"]]}})
    assert bids == {0.40: 10, 0.42: 5} and asks == {0.5: 7, 0.45: 3}


def test_polymarket_us_short_side_is_the_long_book_flipped():
    body = {"marketData": {"bids": [{"px": {"value": "0.88"}, "qty": "8"}],
                           "offers": [{"px": {"value": "0.93"}, "qty": "20"}],
                           "transactTime": "2026-10-15T17:26:12.629571687Z"}}
    assert PolymarketUs.ladder(body, "long")[:2] == ({0.88: 8}, {0.93: 20})
    bids, asks, at = PolymarketUs.ladder(body, "short")
    assert bids == {0.07: 20} and asks == {0.12: 8}
    assert at == pd.Timestamp("2026-10-15 17:26:12.629571687", tz="UTC")


def test_polymarket_intl_book_is_read_by_price_not_position():
    bids, asks, at = PolymarketIntl.ladder({"bids": [{"price": "0.01", "size": "5"}, {"price": "0.48", "size": "3"}],
                                            "asks": [{"price": "0.99", "size": "9"}, {"price": "0.81", "size": "2"}],
                                            "timestamp": "1792084326093"})
    assert bids == {0.01: 5, 0.48: 3} and asks == {0.99: 9, 0.81: 2}
    assert at == pd.Timestamp(1792084326093, unit="ms", tz="UTC")


class _Resp:
    def __init__(self, status, retry_after=None):
        self.status_code, self.headers = status, {"Retry-After": retry_after} if retry_after else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def _serve(monkeypatch, statuses):
    calls, sleeps = [], []
    monkeypatch.setattr(venues.requests, "get", lambda url, **kw: calls.append(url) or _Resp(*statuses[len(calls) - 1]))
    monkeypatch.setattr(venues.time, "sleep", sleeps.append)
    return calls, sleeps


def test_rate_limits_are_waited_out_as_the_venue_asks(monkeypatch):
    calls, sleeps = _serve(monkeypatch, [(429, "3"), (503,), (200,)])
    assert venues._get("u").status_code == 200
    assert len(calls) == 3 and sleeps == [3.0, 2.0]


def test_a_client_error_is_not_retried(monkeypatch):
    calls, _ = _serve(monkeypatch, [(404,)])
    with pytest.raises(requests.HTTPError):
        venues._get("u")
    assert len(calls) == 1


def test_a_venue_that_stays_limited_fails(monkeypatch):
    calls, _ = _serve(monkeypatch, [(429,)] * venues.RETRIES)
    with pytest.raises(requests.HTTPError):
        venues._get("u")
    assert len(calls) == venues.RETRIES
