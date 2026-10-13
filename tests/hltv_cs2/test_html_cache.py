import asyncio
import gzip
from datetime import date, datetime, timedelta, timezone

import pytest

from gnome_pipelines.hltv_cs2 import backfill, html_cache as hc
from gnome_pipelines.hltv_cs2.backfill import HltvFetcher
from gnome_pipelines.hltv_cs2.html_cache import (
    CachePolicy,
    HtmlCache,
    normalize_url,
    subject_date,
)


@pytest.fixture
def cache(tmp_path):
    return HtmlCache(local_root=tmp_path / "local", remote_root=str(tmp_path / "remote"))


def test_slug_is_not_part_of_identity(cache):
    """HLTV slugs carry team names; a rebrand must not orphan the cached page."""
    a = "https://www.hltv.org/matches/2389638/mongolz-vs-g2-blast"
    b = "https://www.HLTV.org/matches/2389638/the-mongolz-renamed-vs-g2#scoreboard"
    assert normalize_url(a) == normalize_url(b)
    assert cache.key_for(a, kind="match") == cache.key_for(b, kind="match")


def test_query_params_are_order_independent(cache):
    a = "https://www.hltv.org/stats/teams/maps/6248/x?startDate=2026-01-01&endDate=2026-03-31"
    b = "https://www.hltv.org/stats/teams/maps/6248/y?endDate=2026-03-31&startDate=2026-01-01"
    assert cache.key_for(a, kind="team_map_stats") == cache.key_for(b, kind="team_map_stats")


def test_distinct_windows_are_distinct_keys(cache):
    a = "https://www.hltv.org/stats/teams/maps/6248/x?startDate=2026-01-01&endDate=2026-03-31"
    b = "https://www.hltv.org/stats/teams/maps/6248/x?startDate=2026-01-01&endDate=2026-06-30"
    assert cache.key_for(a, kind="team_map_stats") != cache.key_for(b, kind="team_map_stats")


def test_round_trip_preserves_html_and_fetch_time(cache):
    url = "https://www.hltv.org/matches/2389638/x"
    when = datetime(2026, 9, 30, 12, 34, tzinfo=timezone.utc)
    cache.put(url, "<html>sentinel</html>", kind="match", fetched_at=when)
    got = cache.get(url, kind="match")
    assert got.html == "<html>sentinel</html>"
    assert got.fetched_at == when


def test_fetched_at_is_readable_by_plain_gzip(cache):
    """Stored in the gzip MTIME field, so `zcat` still works and there is no sidecar."""
    url = "https://www.hltv.org/matches/2389639/x"
    when = datetime(2026, 8, 1, 6, 0, tzinfo=timezone.utc)
    key = cache.put(url, "<html>x</html>", kind="match", fetched_at=when)
    blob = (cache.local_root / key).read_bytes()
    assert gzip.decompress(blob) == b"<html>x</html>"


def test_match_pages_never_expire(cache):
    """Match ids come from /results, which lists only finished matches."""
    url = "https://www.hltv.org/matches/2389640/x"
    ancient = datetime.now(timezone.utc) - timedelta(days=900)
    cache.put(url, "<html>old</html>", kind="match", fetched_at=ancient)
    assert cache.get(url, kind="match") is not None


def test_recent_results_listing_expires_but_old_one_does_not(tmp_path):
    c = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"),
                  policy=CachePolicy(ttl=timedelta(hours=1)))
    stale = datetime.now(timezone.utc) - timedelta(days=2)

    today = date.today().isoformat()
    fresh_url = f"https://www.hltv.org/results?startDate={today}&endDate={today}"
    c.put(fresh_url, "<html>today</html>", kind="results", fetched_at=stale)
    assert c.get(fresh_url, kind="results") is None, "a recent listing must expire"

    old = (date.today() - timedelta(days=200)).isoformat()
    old_url = f"https://www.hltv.org/results?startDate={old}&endDate={old}"
    c.put(old_url, "<html>old</html>", kind="results", fetched_at=stale)
    assert c.get(old_url, kind="results") is not None, "a closed-book listing must not"
    c.drain()   # background pushes must not outlive the temp dir


def test_refresh_policy_forces_a_miss(cache):
    url = "https://www.hltv.org/matches/2389641/x"
    cache.put(url, "<html>x</html>", kind="match")
    assert cache.get(url, kind="match") is not None
    cache.policy = CachePolicy(refresh_all=True)
    assert cache.get(url, kind="match") is None


def test_no_cache_write_leaves_nothing_behind(cache):
    cache.policy = CachePolicy(write=False)
    url = "https://www.hltv.org/matches/2389642/x"
    assert cache.put(url, "<html>x</html>", kind="match") is None
    cache.policy = CachePolicy()
    assert cache.get(url, kind="match") is None


def test_subject_date_drives_sharding_and_expiry():
    s = normalize_url("https://www.hltv.org/stats/teams/maps/6248/x?startDate=2026-01-01&endDate=2026-03-31")
    assert subject_date(s) == date(2026, 3, 31), "window end, not window start"


def test_local_keys_enumerates_for_reparse(cache):
    for i in range(3):
        cache.put(f"https://www.hltv.org/matches/23896{i}/x", f"<html>{i}</html>", kind="match")
    cache.put("https://www.hltv.org/results?startDate=2026-01-01&endDate=2026-01-01",
              "<html>r</html>", kind="results")
    assert len(cache.local_keys(kind="match")) == 3
    assert len(cache.local_keys()) == 4


# --- the property the whole cache exists for ---

_COMPLETE = "<html><body>cached<footer></footer></body></html>"


def test_cache_hit_never_starts_chrome(tmp_path):
    """
    Chrome needs a visible window for the Cloudflare challenge and is the entire
    cost of a re-parse. On a full cache hit it must not be constructed at all.
    """
    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"))
    url = "https://www.hltv.org/matches/2389638/x"
    cache.put(url, _COMPLETE, kind="match")

    fetcher = HltvFetcher(cache)
    html, fetched_at = asyncio.run(fetcher.html(url, kind="match", wait_selector="teamRanking"))

    assert html == _COMPLETE
    assert fetched_at is not None
    assert fetcher._browser is None, "a cache hit must not construct a browser"
    assert fetcher._page is None


def test_miss_would_need_a_browser(tmp_path, monkeypatch):
    """The complement: a miss does reach for the page, so the hit path is meaningful."""
    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"))
    fetcher = backfill.HltvFetcher(cache)

    reached = {"page": False}

    async def _fake_page():
        reached["page"] = True
        return object()

    async def _fake_get_html(page, url, wait_selector=None, wait_count=1):
        return None

    monkeypatch.setattr(fetcher, "page", _fake_page)
    monkeypatch.setattr(backfill, "_get_html", _fake_get_html)
    asyncio.run(fetcher.html("https://www.hltv.org/matches/999/x", kind="match"))
    assert reached["page"], "a miss must fall through to the browser"


def test_remote_write_failure_does_not_lose_the_page_or_kill_the_run(tmp_path, monkeypatch):
    """
    A ten-hour unattended scrape must survive a transient S3 failure over a page
    it has already fetched. The local write is authoritative.
    """

    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"))

    def _boom(fs, path, data):
        raise OSError("S3 unavailable")

    monkeypatch.setattr(hc, "fs_write_bytes", _boom)
    url = "https://www.hltv.org/matches/2389700/x"
    key = cache.put(url, "<html>kept</html>", kind="match")

    assert key is not None
    assert cache.get(url, kind="match").html == "<html>kept</html>", "must still serve locally"
    assert cache.drain() == 1, "the failed push should be reported, not raised"
    assert key in cache._pending

    monkeypatch.undo()
    assert cache.sync_pending() == 0, "retry should clear the backlog"


def test_remote_read_failure_is_a_miss_not_a_crash(tmp_path, monkeypatch):
    cache = HtmlCache(local_root=tmp_path / "l", remote_root=str(tmp_path / "r"))
    url = "https://www.hltv.org/matches/2389701/x"
    cache.put(url, "<html>x</html>", kind="match")
    (cache.local_root / cache.key_for(url, kind="match")).unlink()  # force the remote path

    def _boom(fs, path):
        raise OSError("S3 unavailable")

    monkeypatch.setattr(hc, "fs_read_bytes", _boom)
    assert cache.get(url, kind="match") is None, "an unreachable remote is a miss, not an exception"
