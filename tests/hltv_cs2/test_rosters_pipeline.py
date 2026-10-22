"""hltv_rosters' run logic against saved pages: which teams it fetches, where the daily feed read stops, what it
publishes, and that a same-day re-run is served from the page cache."""
import asyncio
import gzip
import pathlib
import re

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from gnome_pipelines.hltv_cs2.fetch import HltvFetcher
from gnome_pipelines.hltv_cs2.html_cache import HtmlCache
from gnome_pipelines.hltv_cs2.roster import parse_transfers_page_html
from gnome_pipelines.hltv_rosters import pipeline as rosters

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
TEAMS = {9881: "hltv_team_rooster", 4608: "hltv_team_navi"}


def html(name: str) -> str:
    return gzip.open(FIXTURES / f"{name}.html.gz", "rt", errors="ignore").read()


class PageFixtures:
    """Team pages and the first transfers-feed page; None for anything else."""

    concurrency = 2
    can_be_blocked = True
    billed = 0

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def html(self, url: str, *, wait_selector: str | None = None, wait_count: int = 1) -> str | None:
        self.calls.append(url)
        team = re.search(r"/team/(\d+)/", url)
        if team:
            name = TEAMS.get(int(team.group(1)))
            return html(name) if name else None
        if url.endswith("/transfers"):
            return html("hltv_transfers")
        return None

    async def close(self) -> None:
        pass


@pytest.fixture
def cache(tmp_path):
    return HtmlCache(local_root=tmp_path / "local", remote_root=str(tmp_path / "remote"))


@pytest.fixture
def published(monkeypatch):
    captured = {}
    monkeypatch.setattr(rosters, "publish_rosters", lambda moves, roster: captured.update(moves=moves, roster=roster))
    return captured


def feed_moves() -> list[dict]:
    return parse_transfers_page_html(BeautifulSoup(html("hltv_transfers"), "lxml"), 0)["moves"]


def known(moves: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(moves).assign(event_date=lambda d: pd.to_datetime(d.event_date))


def test_the_daily_feed_read_stops_at_the_first_page_with_nothing_new(cache):
    moves = feed_moves()
    fetcher = HltvFetcher(cache, PageFixtures())

    fresh, pages = asyncio.run(rosters.read_feed(fetcher, known(moves), max_pages=5))

    assert fresh == [] and pages == 1
    assert fetcher.source.calls == ["https://www.hltv.org/transfers"]


def test_a_page_with_new_moves_sends_the_read_one_page_further_back(cache):
    moves = feed_moves()
    fetcher = HltvFetcher(cache, PageFixtures())

    fresh, pages = asyncio.run(rosters.read_feed(fetcher, known(moves[3:]), max_pages=5))

    # Page 1 isn't in the fixtures, so the read stops there having read one page.
    assert fresh == moves[:3] and pages == 1
    assert fetcher.source.calls == ["https://www.hltv.org/transfers", "https://www.hltv.org/transfers?page=1"]


def test_with_no_stored_moves_every_feed_move_is_new():
    assert rosters.new_moves(feed_moves(), pd.DataFrame()) == feed_moves()


def test_teams_are_published_with_their_moves_once_each_and_a_rerun_the_same_day_fetches_nothing(cache, published):
    source = PageFixtures()
    summary = asyncio.run(rosters.collect_teams(HltvFetcher(cache, source), [9881, 4608, 1], feed_moves=[]))

    assert summary == {"teams": 3, "teams_failed": 1, "moves": len(published["moves"]), "roster_rows": 10}
    assert {r["team_id"] for r in published["roster"]} == {9881, 4608}

    again = PageFixtures()
    asyncio.run(rosters.collect_teams(HltvFetcher(cache, again), [9881, 4608], feed_moves=[]))
    assert again.calls == []


def test_a_move_seen_on_two_teams_pages_is_stored_once(monkeypatch):
    captured = {}
    monkeypatch.setattr(rosters, "publish", lambda spec, df: captured.update({spec.name: df}))
    move = feed_moves()[0]

    rosters.publish_rosters([move, dict(move)], [])

    assert len(captured["cs2_roster_moves"]) == 1


def test_backfill_teams_are_those_with_enough_matches():
    history = pd.DataFrame({"match_id": [1, 1, 2, 3], "team_a_id": [10, 10, 10, 30], "team_b_id": [20, 20, 30, 10]})
    assert rosters.history_teams(history, min_matches=2) == [10, 30]


def test_daily_refreshes_teams_with_an_upcoming_match_soon():
    now = pd.Timestamp("2026-10-19 12:00", tz="UTC")
    matches = pd.DataFrame({
        "match_id": [1, 1, 2, 3],
        "generated_at": pd.to_datetime(["2026-10-19 10:00", "2026-10-19 11:00", "2026-10-19 11:00", "2026-10-19 11:00"], utc=True),
        "status": ["upcoming", "live", "upcoming", "upcoming"],
        "kickoff": pd.to_datetime(["2026-10-19 13:00", "2026-10-19 13:00", "2026-10-20 13:00", "2026-10-25 13:00"], utc=True),
        "team_a_id": [1, 1, 2, 3], "team_b_id": [11, 11, 12, 13],
    })
    assert rosters.upcoming_teams(matches, now) == {2, 12}
