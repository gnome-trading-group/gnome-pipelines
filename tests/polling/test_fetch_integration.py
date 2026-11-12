"""The Fetcher's routes over real HTTP: a local site with real refusals, and a local stand-in for ZenRows' API that,
like the real one, fetches the target for us through a "basic" or "premium" proxy and answers 422 when the site
blocks that proxy. Nothing leaves the machine: the stand-in maps every target URL to the local site by its path."""
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.datasets.rt import RT_SCORES
from gnome_pipelines.polling.fetch import ESCALATE_AFTER, Fetcher, LocalArchive, ZenRowsBudget
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.writer import TableWriter
from gnome_pipelines.rt.films import FilmState
from gnome_pipelines.rt.source import TOMATOMETER_WEEK_EVERY, RtContext, RtReviewsParams

SCORES = b'<script>"criticsScore":{"likedCount":30,"notLikedCount":3,"scorePercent":"91%"}</script>'
WALL = b"<html>Please verify you are a human</html>"
# An RT film page before its embargo: the page is there, its critics' counts aren't yet.
PRE_EMBARGO = b'<html><meta property="og:site_name" content="Rotten Tomatoes"><script>"criticsScore":{}</script></html>'


class Site(BaseHTTPRequestHandler):
    """Who's asking is in X-Proxy: absent for a plain request, "basic" or "premium" through the ZenRows stand-in."""

    hits: list = []

    def do_GET(self):
        proxy = self.headers.get("X-Proxy", "direct")
        Site.hits.append((self.path, proxy))
        path = urlparse(self.path).path
        if path == "/open":
            self._send(200, b"<html>open page</html>")
        elif path == "/no-direct":
            self._send(403, b"forbidden") if proxy == "direct" else self._send(200, b"<html>proxied page</html>")
        elif path == "/residential-only":
            self._send(200, SCORES) if proxy == "premium" else self._send(403, b"forbidden")
        elif path == "/down":
            self._send(503, b"service unavailable")
        elif path == "/m/pre_embargo":
            self._send(200, PRE_EMBARGO)
        elif path.startswith("/m/"):
            # A bot wall that answers 200: only the residential proxy gets the real page.
            self._send(200, SCORES if proxy == "premium" else WALL)
        else:
            self._send(404, b"not found")

    def _send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def zenrows_handler(site_port):
    class ZenRows(BaseHTTPRequestHandler):
        calls: list = []

        def do_GET(self):
            q = parse_qs(urlparse(self.path).query)
            premium = q.get("premium_proxy") == ["true"]
            ZenRows.calls.append((q["url"][0], "premium" if premium else "basic", q["apikey"][0]))
            target = urlparse(q["url"][0])
            r = requests.get(f"http://127.0.0.1:{site_port}{target.path}",
                             headers={"X-Proxy": "premium" if premium else "basic"}, timeout=5)
            # ZenRows' own answer when the target refused its proxy; a 404 it passes through.
            status = 422 if r.status_code in (401, 403, 429) else r.status_code
            body = r.content if status == 200 else b'{"code":"REQS004","detail":"target blocked"}'
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    return ZenRows


@pytest.fixture
def servers():
    site = ThreadingHTTPServer(("127.0.0.1", 0), Site)
    zr_handler = zenrows_handler(site.server_port)
    zenrows = ThreadingHTTPServer(("127.0.0.1", 0), zr_handler)
    for s in (site, zenrows):
        threading.Thread(target=s.serve_forever, daemon=True).start()
    Site.hits.clear()
    zr_handler.calls.clear()
    yield f"http://127.0.0.1:{site.server_port}", f"http://127.0.0.1:{zenrows.server_port}/v1/", zr_handler
    for s in (site, zenrows):
        s.shutdown()


class Rows:
    def __init__(self):
        self.rows = []

    def add(self, spec, rows):
        self.rows += rows


def fetcher(tmp_path, zenrows_api, credits=100, writer=None):
    # The whole budget open to one host: these tests are about routes, not the per-host share.
    budget = ZenRowsBudget(credits, share=1.0) if credits else None
    return Fetcher("rt_reviews", writer or Rows(), LocalArchive(tmp_path / "pages"), zenrows=budget,
                   secret_fn=lambda name, env: "test-key", host_gap=0.0, zenrows_api=zenrows_api), budget


def test_an_open_site_is_read_plainly_and_zenrows_is_never_called(tmp_path, servers):
    site, api, zr = servers
    f, budget = fetcher(tmp_path, api)
    assert f.get(f"{site}/open", via="auto").text == "<html>open page</html>"
    assert zr.calls == [] and budget.spent_today() == 0


def until_escalated(f, url, via="auto", **kw):
    """The fetches it takes a host to step up a route: refused ESCALATE_AFTER - 1 times, then the next route."""
    for _ in range(ESCALATE_AFTER - 1):
        assert f.get(url, via=via, **kw).refused
    return f.get(url, via=via, **kw)


def test_a_site_refusing_direct_requests_is_read_through_the_basic_proxy_from_then_on(tmp_path, servers):
    site, api, zr = servers
    f, budget = fetcher(tmp_path, api)
    first = until_escalated(f, f"{site}/no-direct", kind="page")
    second = f.get(f"{site}/no-direct", via="auto", kind="page")
    assert (first.route, second.route) == ("zenrows", "zenrows") and second.text == "<html>proxied page</html>"
    assert Site.hits == [("/no-direct", "direct")] * ESCALATE_AFTER + [("/no-direct", "basic")] * 2
    assert budget.spent_today() == 2 and {c[2] for c in zr.calls} == {"test-key"}


def test_a_site_blocking_the_basic_proxy_is_read_through_premium(tmp_path, servers):
    site, api, _ = servers
    f, budget = fetcher(tmp_path, api)
    # The step up to basic tries it within the same fetch: 2 more refusals there, then premium.
    pages = [f.get(f"{site}/residential-only", via="auto") for _ in range(2 * ESCALATE_AFTER - 1)]
    assert all(p.refused for p in pages[:-1])
    page = pages[-1]
    assert page.ok and page.route == "zenrows_premium"
    assert [h[1] for h in Site.hits] == ["direct"] * ESCALATE_AFTER + ["basic"] * ESCALATE_AFTER + ["premium"]
    assert budget.spent_today() == 10, "ZenRows' 422s for the blocked basic proxy cost nothing"


def test_a_200_bot_wall_steps_up_when_the_caller_checks_the_page(tmp_path, servers):
    site, api, _ = servers
    f, _ = fetcher(tmp_path, api)
    unchecked = f.get(f"{site}/m/a_film", via="zenrows", kind="unchecked")
    assert unchecked.ok and unchecked.content == WALL, "without a check a wall looks like a page"
    checked = until_escalated(f, f"{site}/m/a_film", via="zenrows", kind="checked", valid=lambda t: "criticsScore" in t)
    assert checked.ok and checked.route == "zenrows_premium" and checked.content == SCORES


def test_a_real_404_is_returned_without_spending_anything(tmp_path, servers):
    site, api, _ = servers
    f, budget = fetcher(tmp_path, api)
    for _ in range(ESCALATE_AFTER + 1):
        assert f.get(f"{site}/gone", via="auto").status == 404
    assert f.get(f"{site}/gone", via="zenrows").status == 404
    assert [h[1] for h in Site.hits] == ["direct"] * (ESCALATE_AFTER + 1) + ["basic"] and budget.spent_today() == 0


def test_out_of_credits_a_refused_site_comes_back_refused_and_nothing_raises(tmp_path, servers):
    site, api, _ = servers
    f, budget = fetcher(tmp_path, api, credits=5)
    url = f"{site}/residential-only"
    pages = [f.get(url, via="auto") for _ in range(2 * ESCALATE_AFTER - 1)]
    page = pages[-1]
    assert page.status == 422 and page.refused and f.route(url, "auto") == "zenrows_premium"
    # Remembered on premium with too few credits for it: the next fetch falls back to a free plain try.
    again = f.get(url, via="auto")
    assert (again.status, again.route) == (403, "plain") and budget.spent_today() == 0


def test_an_outage_on_the_site_moves_nothing(tmp_path, servers):
    site, api, zr = servers
    f, budget = fetcher(tmp_path, api)
    for _ in range(3 * ESCALATE_AFTER):
        assert f.get(f"{site}/down", via="auto").transient
    assert zr.calls == [] and f.route(f"{site}/down", "auto") == "plain"


def test_the_tomatometer_reads_a_walled_rt_page_through_premium_end_to_end(tmp_path, servers, monkeypatch):
    """The source's own Tomatometer task, its RT page fetched through the ZenRows stand-in: the basic proxy gets the
    wall, the page check rejects it, and after ESCALATE_AFTER walls premium gets the counts into rt_scores."""
    site, api, zr = servers
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh"))
    writer = TableWriter(store.publish)
    f, budget = fetcher(tmp_path, api, writer=writer)
    health = PollHealth("rt_reviews", publish=lambda df: None)
    ctx = RtContext(store, writer, f, None, health, RtReviewsParams(), (), datetime.now(timezone.utc))
    soon = datetime.now(timezone.utc) + timedelta(days=5)
    ctx.open = {"a-film": FilmState("a-film", "A Film", soon, rt_slug="a_film")}
    # Each pass half an hour apart: a film closing in 5 days is read every TOMATOMETER_WEEK_EVERY.
    passes = (datetime.now(timezone.utc) + i * TOMATOMETER_WEEK_EVERY for i in range(ESCALATE_AFTER))
    for _ in range(ESCALATE_AFTER - 1):
        with pytest.raises(RuntimeError, match="no RT film page read"):
            ctx.tomatometer_run(next(passes))
    (score,) = ctx.tomatometer_run(next(passes))
    assert (score.liked, score.not_liked, score.score) == (30, 3, 91.0)
    rt = "https://www.rottentomatoes.com/m/a_film"
    assert [(u, p) for u, p, _ in zr.calls] == [(rt, "basic")] * ESCALATE_AFTER + [(rt, "premium")]
    assert budget.spent_today() == ESCALATE_AFTER + 10 and f.route(rt, "zenrows", "page") == "zenrows_premium"
    ctx.store_new([score])
    writer.flush()
    assert store.load(RT_SCORES.name)["liked"].tolist() == [30]


def test_an_rt_page_without_counts_yet_is_a_real_answer_and_stays_on_the_basic_proxy(tmp_path, servers):
    site, api, zr = servers
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh"))
    writer = TableWriter(store.publish)
    f, budget = fetcher(tmp_path, api, writer=writer)
    ctx = RtContext(store, writer, f, None, PollHealth("rt_reviews", publish=lambda df: None), RtReviewsParams(), (),
                    datetime.now(timezone.utc))
    soon = datetime.now(timezone.utc) + timedelta(days=5)
    ctx.open = {"b-film": FilmState("b-film", "B Film", soon, rt_slug="pre_embargo")}
    for i in range(3 * ESCALATE_AFTER):
        assert ctx.tomatometer_run(datetime.now(timezone.utc) + i * TOMATOMETER_WEEK_EVERY) == []
    assert {p for _, p, _ in zr.calls} == {"basic"} and budget.spent_today() == 3 * ESCALATE_AFTER
