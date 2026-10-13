"""
The production ingest publishes every dataset the features are built from.

It once kept only match rows, so player stats, veto, head-to-head and form went
stale in production and the priors were rebuilt with 61 features blank.
"""
import gzip
import re
from pathlib import Path

import pytest

from gnome_pipelines.hltv_cs2 import scraper
from gnome_pipelines.hltv_cs2.html_cache import HtmlCache

FIXTURES = Path(__file__).parent / "fixtures"
PAGES = {
    int(mid): gzip.open(FIXTURES / name, "rt").read()
    for mid, name in (("2398816", "hltv_match.html.gz"), ("2398778", "hltv_match2.html.gz"),
                      ("2398817", "hltv_match3.html.gz"))
}


class _FixtureSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get_html(self, url):
        self.calls.append(url)
        mid = int(re.search(r"/matches/(\d+)/", url).group(1))
        return self.pages.get(mid)


@pytest.fixture
def published(monkeypatch, tmp_path):
    captured = {}
    cache = HtmlCache(local_root=tmp_path / "local", remote_root=str(tmp_path / "remote"))
    monkeypatch.setattr(scraper, "_make_session", lambda: _FixtureSession(PAGES))
    monkeypatch.setattr(scraper, "scrape_match_ids", lambda *a, **k: {mid: 0 for mid in PAGES})
    monkeypatch.setattr(scraper, "HtmlCache", lambda: cache)
    monkeypatch.setattr(scraper, "publish_match_data", lambda *a: captured.update(args=a))
    df = scraper.run_matches(None, None, min_stars=0)
    return df, captured["args"], cache


def test_every_harvested_dataset_is_published(published):
    df, (rows, demos, players, veto, h2h, form), _ = published
    assert len(df) == len(rows) > 0
    assert demos == []
    assert {r["match_id"] for r in rows} == set(PAGES)
    for name, block in (("players", players), ("veto", veto), ("h2h", h2h), ("form", form)):
        assert block, f"{name} rows were dropped"
        assert {r["match_id"] for r in block} <= set(PAGES)


def test_completed_pages_extend_the_point_in_time_cache(published):
    *_, cache = published
    for mid in PAGES:
        assert cache.get(scraper._match_url(mid), kind="match") is not None


def test_truncated_page_is_retried_not_parsed(monkeypatch):
    truncated = PAGES[2398816].split("<footer")[0]
    session = _FixtureSession({2398816: truncated})
    monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
    assert scraper.scrape_match_detail(session, 2398816) is None
    assert len(session.calls) == 3


def test_match_url_carries_a_slug():
    """Without one HLTV serves its home page, which parses as a match with no teams."""
    assert scraper._match_url(2398739) == "https://www.hltv.org/matches/2398739/match"
