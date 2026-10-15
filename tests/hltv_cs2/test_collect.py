"""
The collection loop publishes every dataset the features are built from, whichever source fetches the pages.

The production ingest once kept only match rows, so player stats, veto, head-to-head and form went
stale in production and the priors were rebuilt with 61 features blank.
"""
import asyncio
import gzip
from datetime import date
from pathlib import Path

import pytest

from gnome_pipelines.hltv_cs2 import collect, fetch, scraper
from gnome_pipelines.hltv_cs2 import publish as publish_module
from gnome_pipelines.hltv_cs2.fetch import HltvFetcher, ZenRowsSource
from gnome_pipelines.hltv_cs2.html_cache import HtmlCache
from tests.hltv_cs2.sources import FixtureSource

FIXTURES = Path(__file__).parent / "fixtures"
PAGES = {
    int(mid): gzip.open(FIXTURES / name, "rt").read()
    for mid, name in (("2398816", "hltv_match.html.gz"), ("2398778", "hltv_match2.html.gz"),
                      ("2398817", "hltv_match3.html.gz"))
}
DAY = date(2026, 9, 1)


@pytest.fixture
def cache(tmp_path):
    return HtmlCache(local_root=tmp_path / "local", remote_root=str(tmp_path / "remote"))


@pytest.fixture
def published(monkeypatch):
    captured = {}
    monkeypatch.setattr(collect, "publish_match_data", lambda *a: captured.update(args=a))
    return captured


def collect_day(cache, source, **kwargs):
    return asyncio.run(collect.collect_matches(HltvFetcher(cache, source), DAY, DAY, **kwargs))


def test_every_harvested_dataset_is_published(cache, published):
    summary = collect_day(cache, FixtureSource(PAGES))

    rows, demos, players, veto, h2h, form = published["args"]
    assert summary["matches"] == 3 and summary["maps"] == len(rows) > 0
    assert demos == []
    assert {r["match_id"] for r in rows} == set(PAGES)
    for name, block in (("players", players), ("veto", veto), ("h2h", h2h), ("form", form)):
        assert block, f"{name} rows were dropped"
        assert {r["match_id"] for r in block} <= set(PAGES)


def test_fetched_pages_extend_the_cache_and_a_rerun_fetches_nothing(cache, published):
    collect_day(cache, FixtureSource(PAGES))
    again = FixtureSource(PAGES)

    collect_day(cache, again)

    for mid in PAGES:
        assert cache.get(f"https://www.hltv.org/matches/{mid}/x", kind="match") is not None
    assert [url for url in again.calls if "/matches/" in url] == []


def test_match_ids_limit_the_run(cache, published):
    collect_day(cache, FixtureSource(PAGES), match_ids={2398816})

    rows = published["args"][0]
    assert {r["match_id"] for r in rows} == {2398816}


def test_demos_need_chrome(cache, published):
    with pytest.raises(ValueError, match="Chrome"):
        collect_day(cache, FixtureSource(PAGES), demos=True)


def test_zenrows_retries_a_truncated_page_rather_than_return_it(monkeypatch):
    truncated = PAGES[2398816].split("<footer")[0]
    calls = []

    class _Session:
        def get_html(self, url):
            calls.append(url)
            return truncated

    monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
    monkeypatch.setattr(fetch, "_make_session", lambda: _Session())

    assert asyncio.run(ZenRowsSource().html("https://www.hltv.org/matches/2398816/x")) is None
    assert len(calls) == 3


def test_match_url_carries_a_slug():
    """Without one HLTV serves its home page, which parses as a match with no teams."""
    assert scraper._match_url(2398739) == "https://www.hltv.org/matches/2398739/match"


def test_head_to_head_meetings_off_the_map_pool_publish_under_an_empty_map_name(monkeypatch):
    published = {}
    monkeypatch.setattr(publish_module, "publish", lambda spec, rows: published.update({spec.name: rows}))
    meeting = {"match_id": 1, "h2h_date": DAY, "map_name": None, "team1_score": 1, "team2_score": 0}

    publish_module.publish_match_data([], [], h2h_rows=[meeting, {**meeting, "map_name": "de_inferno"}])

    assert [r["map_name"] for r in published["cs2_h2h_history"]] == ["", "de_inferno"]
