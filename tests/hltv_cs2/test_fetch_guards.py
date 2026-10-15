"""
Guards on the fetch loop.

The rate-limit case is the important one: HLTV limits on cumulative request
volume, and a block does not look like a Cloudflare challenge. Before this,
being blocked was invisible — every page would time out, retry three times,
burn 90s and be silently skipped, and the run would still report success.
"""
import asyncio
import gzip
import time
from datetime import date
from pathlib import Path

import pytest

from gnome_pipelines.hltv_cs2 import chrome, collect, fetch
from gnome_pipelines.hltv_cs2.html_cache import CachePolicy, HtmlCache, is_cacheable, is_complete
from tests.hltv_cs2.sources import FixtureSource, listing_html


def _state(n=0, cf=False, blocked=False, ready=True, footer=True):
    return {"n": n, "cf": cf, "blocked": blocked, "ready": ready, "footer": footer}


def _fake_probe(states):
    seq = list(states)

    async def probe(page, selector):
        return seq.pop(0) if seq else seq_last

    seq_last = states[-1]
    return probe


class _Page:
    async def get(self, url):
        return None

    async def get_content(self):
        return "<html>ok</html>"


def test_block_raises_rate_limited(monkeypatch):
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(blocked=True)]))
    with pytest.raises(fetch.RateLimited):
        asyncio.run(chrome._wait_for_page(_Page(), "u", ".teamRanking", wait_count=2))


def test_rate_limited_is_not_retried(monkeypatch):
    """Retrying into a block escalates it — the run must stop instead."""
    calls = {"n": 0}

    async def probe(page, selector):
        calls["n"] += 1
        return _state(blocked=True)

    monkeypatch.setattr(chrome, "_probe", probe)
    with pytest.raises(fetch.RateLimited):
        asyncio.run(chrome._get_html(_Page(), "u", ".teamRanking", retries=3, wait_count=2))
    assert calls["n"] == 1, "must abort on the first block, not retry into it"


def test_selector_present_returns_html(monkeypatch):
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(n=2)]))
    html = asyncio.run(chrome._wait_for_page(_Page(), "u", ".teamRanking", wait_count=2))
    assert html == "<html>ok</html>"


def test_insufficient_count_keeps_waiting_then_succeeds(monkeypatch):
    """wait_count=2 must not be satisfied by a single match."""
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(n=1, ready=False), _state(n=2)]))
    html = asyncio.run(chrome._wait_for_page(_Page(), "u", ".teamRanking", wait_count=2))
    assert html == "<html>ok</html>"


def test_complete_document_without_selector_fails_fast(monkeypatch):
    """
    A finished document that lacks the selector will never grow one. Waiting out
    the full 30s budget three times costs 90s per such page.
    """
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(n=0, ready=True)]))
    monkeypatch.setattr(chrome, "_SELECTOR_GRACE", 0.2)
    t0 = time.monotonic()
    html = asyncio.run(chrome._wait_for_page(_Page(), "u", ".teamRanking", base_timeout=30.0, wait_count=2))
    elapsed = time.monotonic() - t0
    assert html is None
    assert elapsed < 5.0, f"should fail fast, took {elapsed:.1f}s"


def test_incomplete_document_is_given_the_full_budget(monkeypatch):
    """Fail-fast must not drop a page that is merely still loading."""
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(n=0, ready=False)]))
    monkeypatch.setattr(chrome, "_SELECTOR_GRACE", 0.2)
    t0 = time.monotonic()
    html = asyncio.run(chrome._wait_for_page(_Page(), "u", ".teamRanking", base_timeout=1.0, wait_count=2))
    assert html is None
    assert time.monotonic() - t0 >= 0.9, "an unfinished document must get its full budget"


def test_consecutive_failure_breaker_is_wording_independent(tmp_path, monkeypatch):
    """
    _BLOCK_PHRASES is a guess at HLTV's wording. If it is wrong, a block looks
    identical to a selector-less page and fail-fast would skip everything quietly.
    A run of consecutive failures catches it regardless of what the block says.
    """
    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"), policy=CachePolicy(write=False))
    refused = FixtureSource({mid: None for mid in range(1000, 1000 + fetch.MAX_CONSECUTIVE_FAILURES + 5)})
    monkeypatch.setattr(collect, "publish_match_data", lambda *a: None)

    with pytest.raises(fetch.RateLimited, match="consecutive match pages failed"):
        asyncio.run(collect.collect_matches(fetch.HltvFetcher(cache, refused), date(2026, 9, 1), date(2026, 9, 1)))


def test_cache_misses_never_count_as_a_block(tmp_path, monkeypatch):
    """A cache-only re-parse misses pages that were never fetched; that is not HLTV refusing us."""
    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"))
    listing = listing_html(list(range(1000, 1000 + fetch.MAX_CONSECUTIVE_FAILURES + 5)))
    cache.put("https://www.hltv.org/results?startDate=2026-09-01&endDate=2026-09-01&offset=0", listing, kind="results")
    monkeypatch.setattr(collect, "publish_match_data", lambda *a: None)

    summary = asyncio.run(collect.collect_matches(
        fetch.HltvFetcher(cache, fetch.CacheSource()), date(2026, 9, 1), date(2026, 9, 1)))

    assert summary["matches"] == fetch.MAX_CONSECUTIVE_FAILURES + 5 and summary["maps"] == 0


def test_committed_fixtures_are_complete_pages():
    """Every fixture must reach its footer, or it would be a truncated capture."""
    for path in sorted((Path(__file__).parent / "fixtures").glob("*.html.gz")):
        assert is_complete(gzip.open(path, "rt", errors="ignore").read()), path.name


def test_truncated_page_is_never_cached():
    """
    A DOM captured mid-load still ends in </html>, so it looks whole. Cutting a
    real page off above its footer must make it uncacheable for every kind.
    """
    html = gzip.open(Path(__file__).parent / "fixtures" / "hltv_mid.html.gz", "rt", errors="ignore").read()
    truncated = html[: html.index("<footer")] + "</body></html>"
    assert not is_complete(truncated)
    for kind in ("match", "results"):
        assert is_cacheable(kind, html) or kind == "match" and "results-team-score" not in html
        assert not is_cacheable(kind, truncated)


def test_probe_reports_footer_presence():
    assert 'footer: !!document.querySelector("footer")' in chrome._PROBE_JS


def test_selector_without_footer_is_not_ready(monkeypatch):
    """The selector sits near the top of the page; seeing it says nothing about the rest."""
    monkeypatch.setattr(chrome, "_probe", _fake_probe(
        [_state(n=3, ready=False, footer=False)] * 3 + [_state(n=3, ready=True, footer=True)]))
    monkeypatch.setattr(chrome, "_POLL_INTERVAL", 0.0)
    html = asyncio.run(chrome._wait_for_page(_Page(), "u", ".contentCol", base_timeout=5))
    assert html == "<html>ok</html>"


def test_complete_document_without_footer_gives_up(monkeypatch):
    monkeypatch.setattr(chrome, "_probe", _fake_probe([_state(n=3, ready=True, footer=False)]))
    monkeypatch.setattr(chrome, "_POLL_INTERVAL", 0.0)
    monkeypatch.setattr(chrome, "_SELECTOR_GRACE", 0.05)
    assert asyncio.run(chrome._wait_for_page(_Page(), "u", ".contentCol", base_timeout=5)) is None
