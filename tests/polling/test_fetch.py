import gzip
import json
import threading
from datetime import datetime, timezone

import pytest

from gnome_pipelines.datasets.polling import FETCHED_PAGES
from gnome_pipelines.polling.fetch import (BACKOFF_MAX, BACKOFF_START, CONN_BACKOFF_MAX, CONN_BACKOFF_START,
                                           CONN_FAILURES, ESCALATE_AFTER, RECHECK, ZENROWS_API, Fetcher, LocalArchive,
                                           OverBudget, ZenRowsBudget)

T0 = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
SITE = "https://www.nytimes.com/2026/11/11/movies/x-review.html"


class Response:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.content, self.headers = status, body, headers or {}
        self.text = body.decode("utf-8", "replace")


class Session:
    """Answers by route: what a plain request, ZenRows' basic proxy and its premium proxy get back, in order; once a
    route's list is used up it keeps giving its last answer."""

    def __init__(self, plain=(), basic=(), premium=()):
        self.queues = {"plain": list(plain), "zenrows": list(basic), "zenrows_premium": list(premium)}
        self.calls, self.headers = [], {}

    def get(self, url, headers=None, params=None, timeout=None):
        route = "plain" if url != ZENROWS_API else ("zenrows_premium" if params.get("premium_proxy") else "zenrows")
        self.calls.append((route, params["url"] if params else url, headers, params))
        q = self.queues[route]
        return q.pop(0) if len(q) > 1 else q[0]

    def routes(self):
        return [c[0] for c in self.calls]


class Writer:
    def __init__(self):
        self.rows = []

    def add(self, spec, rows):
        assert spec is FETCHED_PAGES
        self.rows += rows


class Monotonic:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make(tmp_path, session, credits=1000, share=1.0):
    clock, writer = Monotonic(), Writer()
    budget = ZenRowsBudget(credits, clock=lambda: T0, share=share) if credits is not None else None
    f = Fetcher("rt_reviews", writer, LocalArchive(tmp_path), zenrows=budget, session=session,
                secret_fn=lambda name, env: "zr-key", clock=lambda: T0, sleep=lambda s: None, monotonic=clock)
    return f, writer, budget, clock


def ok(body=b"<html>review</html>", headers=None):
    return Response(200, body, headers)


def refused_until_escalated(f, url, via="auto", **kw):
    """Fetch until the route moves up: the first ESCALATE_AFTER - 1 refusals come back refused."""
    pages = [f.get(url, via=via, **kw) for _ in range(ESCALATE_AFTER)]
    assert all(p.refused for p in pages[:-1])
    return pages[-1]


# --- one route -----------------------------------------------------------------------------------------------------

def test_a_changed_page_is_archived_by_content_and_logged_and_its_validators_sent_next_time(tmp_path):
    session = Session(plain=[ok(headers={"ETag": '"e1"'}), Response(304)])
    f, writer, _, _ = make(tmp_path, session)
    page = f.get(SITE, archive=True)
    assert page.ok and page.text == "<html>review</html>" and page.route == "plain"
    assert gzip.decompress((tmp_path / page.archive_key).read_bytes()) == b"<html>review</html>"
    again = f.get(SITE, archive=True)
    assert not again.ok and again.status == 304 and session.calls[1][2] == {"If-None-Match": '"e1"'}
    assert [(r["via"], r["status"]) for r in writer.rows] == [("plain", 200)]


def test_a_page_fetched_unconditionally_is_got_again_in_full_on_a_retry(tmp_path):
    session = Session(plain=[ok(headers={"ETag": '"e1"'}), ok(headers={"ETag": '"e1"'})])
    f, _, _, _ = make(tmp_path, session)
    f.get(SITE, conditional=False)
    assert f.get(SITE, conditional=False).ok and session.calls[1][2] == {}


def test_plain_only_never_uses_zenrows_even_when_refused(tmp_path):
    session = Session(plain=[Response(403)])
    f, writer, budget, _ = make(tmp_path, session)
    for _ in range(5):
        f.get(SITE)
    assert set(session.routes()) == {"plain"} and budget.spent_today() == 0
    assert writer.rows[0]["error"] == "refused"


def test_a_missing_page_is_not_a_refusal_on_any_route(tmp_path):
    session = Session(plain=[Response(404)], basic=[Response(404)])
    f, _, budget, _ = make(tmp_path, session)
    for _ in range(ESCALATE_AFTER + 1):
        assert f.get(SITE, via="auto").status == 404
        assert f.get(SITE, via="zenrows").status == 404
    assert "zenrows_premium" not in session.routes() and f.route(SITE, "auto") == "plain"
    assert budget.spent_today() == 0, "ZenRows bills only what it answers with a 200"


# --- stepping up ---------------------------------------------------------------------------------------------------

def test_refusals_move_a_host_up_only_after_several_in_a_row(tmp_path):
    session = Session(plain=[Response(403)], basic=[ok(), ok(b"<html>second</html>")])
    f, writer, budget, _ = make(tmp_path, session)
    page = refused_until_escalated(f, SITE, kind="page")
    assert page.ok and page.route == "zenrows"
    assert session.routes() == ["plain"] * ESCALATE_AFTER + ["zenrows"]
    assert f.route(SITE, "auto", "page") == "zenrows"
    assert f.get(SITE, via="auto", kind="page").text == "<html>second</html>"
    assert session.routes()[-1] == "zenrows" and budget.spent_today() == 2
    assert session.calls[ESCALATE_AFTER][3] == {"apikey": "zr-key", "url": SITE}


def test_one_success_between_refusals_resets_the_count(tmp_path):
    session = Session(plain=[Response(403), Response(403), ok(), Response(403), Response(403), ok()])
    f, _, _, _ = make(tmp_path, session)
    for _ in range(6):
        f.get(SITE, via="auto")
    assert set(session.routes()) == {"plain"}


def test_rate_limits_and_outages_never_move_a_host_up(tmp_path):
    session = Session(plain=[Response(429), Response(503), Response(500), Response(502), Response(504), Response(429)])
    f, writer, _, _ = make(tmp_path, session)
    pages = [f.get(SITE, via="auto") for _ in range(6)]
    assert all(p.transient and not p.refused for p in pages)
    assert set(session.routes()) == {"plain"} and {r["error"] for r in writer.rows} == {"transient"}


def test_zenrows_own_rate_limit_and_out_of_credit_answers_are_transient(tmp_path):
    session = Session(basic=[Response(429), Response(402), Response(429)])
    f, _, budget, _ = make(tmp_path, session)
    for _ in range(3):
        assert f.get(SITE, via="zenrows").transient
    assert set(session.routes()) == {"zenrows"} and budget.spent_today() == 0


def test_zenrows_steps_up_to_premium_when_its_basic_proxy_keeps_being_blocked(tmp_path):
    # ZenRows answers with its own 422 when the site blocked the datacenter proxy.
    session = Session(basic=[Response(422)], premium=[ok()])
    f, _, budget, _ = make(tmp_path, session)
    page = refused_until_escalated(f, SITE, via="zenrows")
    assert page.ok and page.route == "zenrows_premium" and session.calls[-1][3]["premium_proxy"] == "true"
    assert budget.spent_today() == 10


def test_a_200_that_is_not_the_page_asked_for_counts_as_refused(tmp_path):
    session = Session(basic=[ok(b"<html>Please verify you are human</html>")],
                      premium=[ok(b'"criticsScore":{"likedCount":3}')])
    f, writer, budget, _ = make(tmp_path, session)
    page = refused_until_escalated(f, SITE, via="zenrows", valid=lambda text: "criticsScore" in text)
    assert page.ok and page.route == "zenrows_premium"
    assert writer.rows[0]["error"] == "refused: not the page asked for"
    assert budget.spent_today() == ESCALATE_AFTER * 1 + 10, "a wall ZenRows answered with a 200 is still billed"


def test_a_gzipped_sitemap_is_checked_as_the_xml_inside(tmp_path):
    sitemap = gzip.compress(b"<urlset><url><loc>https://a.com/x</loc></url></urlset>")
    session = Session(plain=[Response(200, sitemap)])
    f, _, _, _ = make(tmp_path, session)
    assert f.get(SITE, via="auto", valid=lambda t: "<urlset" in t).ok


# --- every route refusing ------------------------------------------------------------------------------------------

def test_when_every_route_refuses_the_host_backs_off_instead_of_paying_every_poll(tmp_path):
    session = Session(plain=[Response(403)], basic=[Response(422)], premium=[ok(b"wall")])
    f, _, budget, clock = make(tmp_path, session)
    wall = lambda t: t != "wall"
    for _ in range(3 * ESCALATE_AFTER):
        page = f.get(SITE, via="auto", valid=wall)
    assert page.refused and not page.ok and page.text is None and f.route(SITE, "auto") == "zenrows_premium"
    asked = len(session.calls)
    for _ in range(20):
        skipped = f.get(SITE, via="auto", valid=wall)
        assert skipped.refused and not skipped.ok
    assert len(session.calls) == asked, "no requests while backing off"
    clock.t += BACKOFF_START + 1
    f.get(SITE, via="auto", valid=wall)
    assert len(session.calls) == asked + 1


def test_the_backoff_doubles_up_to_its_maximum(tmp_path):
    session = Session(premium=[Response(422)], basic=[Response(422)])
    f, _, _, clock = make(tmp_path, session)
    gaps = []
    for _ in range(12):
        before = len(session.calls)
        while True:
            f.get(SITE, via="zenrows")
            if len(session.calls) == before:
                break
            before = len(session.calls)
        held = f._held[SITE]
        gaps.append(held.backoff)
        clock.t = held.until + 1
    assert gaps[0] == BACKOFF_START and gaps[1] == 2 * BACKOFF_START and max(gaps) == BACKOFF_MAX



# --- coming back down ---------------------------------------------------------------------------------------------

def test_after_the_recheck_period_the_next_fetch_tries_one_route_cheaper(tmp_path):
    session = Session(plain=[Response(403), Response(403), Response(403), ok(b"plain again")], basic=[ok()])
    f, _, _, clock = make(tmp_path, session)
    refused_until_escalated(f, SITE)
    clock.t += RECHECK + 1
    assert f.get(SITE, via="auto").route == "plain" and f.route(SITE, "auto") == "plain"


def test_a_cheaper_try_that_is_refused_goes_straight_back_and_waits_another_period(tmp_path):
    session = Session(plain=[Response(403)], basic=[ok()])
    f, _, _, clock = make(tmp_path, session)
    refused_until_escalated(f, SITE)
    clock.t += RECHECK + 1
    page = f.get(SITE, via="auto")
    assert page.ok and page.route == "zenrows" and session.routes()[-2:] == ["plain", "zenrows"]
    clock.t += 60
    f.get(SITE, via="auto")
    assert session.routes()[-1] == "zenrows" and session.routes().count("plain") == ESCALATE_AFTER + 1


# --- memories ------------------------------------------------------------------------------------------------------

def test_a_host_s_feed_and_its_pages_keep_separate_routes(tmp_path):
    feed = "https://www.nytimes.com/sitemaps/new/news.xml.gz"

    class BySite(Session):
        def get(self, url, headers=None, params=None, timeout=None):
            route = "plain" if url != ZENROWS_API else "zenrows"
            target = params["url"] if params else url
            self.calls.append((route, target, headers, params))
            return ok(b"<urlset/>") if target == feed or route == "zenrows" else Response(403)
    session = BySite()
    f, _, _, _ = make(tmp_path, session)
    refused_until_escalated(f, SITE, kind="page")
    assert f.get(feed, via="auto", kind="feed").route == "plain"
    assert (f.route(feed, "auto", "feed"), f.route(SITE, "auto", "page")) == ("plain", "zenrows")


def test_hosts_keep_separate_routes(tmp_path):
    other = "https://variety.com/2026/film/reviews/x-review/"
    session = Session(plain=[Response(403), Response(403), Response(403), ok()], basic=[ok()])
    f, _, _, _ = make(tmp_path, session)
    refused_until_escalated(f, SITE)
    assert f.get(other, via="auto").route == "plain"


# --- the budget ----------------------------------------------------------------------------------------------------

def test_without_zenrows_auto_returns_the_refusal_and_keeps_trying_plain(tmp_path):
    session = Session(plain=[Response(403)])
    f, _, _, _ = make(tmp_path, session, credits=None)
    for _ in range(2 * ESCALATE_AFTER):
        page = f.get(SITE, via="auto")
        assert page.status == 403 or page.refused
    assert set(session.routes()) == {"plain"}


def test_a_fetch_that_must_start_on_zenrows_raises_without_a_budget(tmp_path):
    session = Session()
    f, writer, _, _ = make(tmp_path, session, credits=None)
    with pytest.raises(OverBudget):
        f.get(SITE, via="zenrows")
    assert session.calls == [] and writer.rows == []


def test_out_of_credits_a_host_remembered_on_zenrows_falls_back_to_plain_and_keeps_its_memory(tmp_path):
    session = Session(plain=[Response(403), Response(403), Response(403), ok(b"plain worked today")], basic=[ok()])
    f, _, budget, _ = make(tmp_path, session, credits=1)
    assert refused_until_escalated(f, SITE).route == "zenrows" and budget.spent_today() == 1
    page = f.get(SITE, via="auto")
    assert (page.route, page.text) == ("plain", "plain worked today") and f.route(SITE, "auto") == "zenrows"


def test_one_host_can_t_spend_more_than_its_share_of_the_day(tmp_path):
    other = "https://www.rottentomatoes.com/m/a"
    session = Session(basic=[ok()])
    f, _, budget, _ = make(tmp_path, session, credits=10, share=0.2)
    f.get(SITE, via="zenrows")
    f.get(SITE, via="zenrows")
    with pytest.raises(OverBudget, match="of today.s credits"):
        f.get(SITE, via="zenrows")
    assert f.get(other, via="zenrows").ok and budget.spent_today() == 3


def test_a_restart_seeds_today_s_spend_and_credits_reset_each_utc_day():
    day = [T0]
    budget = ZenRowsBudget(10, clock=lambda: day[0], spent=8)
    budget.charge(2)
    with pytest.raises(OverBudget):
        budget.charge(1)
    day[0] = datetime(2026, 11, 12, 0, 0, 1, tzinfo=timezone.utc)
    budget.charge(10)
    assert budget.spent_today() == 10


# --- transport -----------------------------------------------------------------------------------------------------

def test_a_transport_error_is_logged_and_raised_and_costs_nothing(tmp_path):
    class Broken(Session):
        def get(self, *a, **k):
            raise ConnectionError("reset by peer")
    f, writer, budget, _ = make(tmp_path, Broken())
    with pytest.raises(ConnectionError):
        f.get(SITE, via="zenrows")
    assert writer.rows[0]["error"] == "ConnectionError: reset by peer" and writer.rows[0]["status"] is None
    assert budget.spent_today() == 0


def test_requests_to_one_host_go_one_at_a_time(tmp_path):
    inside, worst = [0], [0]
    lock = threading.Lock()

    class Slow(Session):
        def get(self, url, headers=None, params=None, timeout=None):
            with lock:
                inside[0] += 1
                worst[0] = max(worst[0], inside[0])
            threading.Event().wait(0.02)
            with lock:
                inside[0] -= 1
            return ok()
    f, _, _, _ = make(tmp_path, Slow())
    threads = [threading.Thread(target=f.get, args=(SITE,)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert worst[0] == 1


# --- ZenRows' own answers ------------------------------------------------------------------------------------------

def zenrows_error(status, code, title):
    return Response(status, json.dumps({"code": code, "title": title, "status": status}).encode())


def test_zenrows_400_steps_up_at_once_and_backs_off_at_once_on_premium(tmp_path):
    session = Session(basic=[zenrows_error(400, "REQS002", "Invalid request")],
                      premium=[zenrows_error(400, "REQS002", "Invalid request")])
    f, writer, budget, _ = make(tmp_path, session)
    f.get(SITE, via="zenrows")
    assert session.routes() == ["zenrows", "zenrows_premium"]
    assert f.get(SITE, via="zenrows").refused and len(session.calls) == 2, "backing off"
    assert writer.rows[-1]["error"] == "refused: REQS002 Invalid request" and budget.spent_today() == 0


FORBIDDEN = zenrows_error(400, "REQS001", "Requests to this domain are forbidden")


def test_a_host_zenrows_forbids_stays_plain_and_only_its_refused_urls_wait(tmp_path):
    # ew.com in prod: its old sitemaps refuse plain requests, ZenRows won't fetch the domain at all, and its news
    # feed still answers plain requests.
    feed, old = "https://ew.com/google-news-sitemap.xml", "https://ew.com/sitemap_1.xml"

    class Ew(Session):
        def get(self, url, headers=None, params=None, timeout=None):
            route = "plain" if url != ZENROWS_API else ("zenrows_premium" if params.get("premium_proxy") else "zenrows")
            target = params["url"] if params else url
            self.calls.append((route, target, headers, params))
            if route != "plain":
                return FORBIDDEN
            return ok(b"<urlset/>") if target == feed else Response(403)
    session = Ew()
    f, writer, budget, clock = make(tmp_path, session)
    for _ in range(ESCALATE_AFTER):
        f.get(old, via="auto", kind="feed")
    assert session.routes() == ["plain"] * ESCALATE_AFTER + ["zenrows"], "REQS001: no premium try either"
    assert f.route(feed, "auto", "feed") == "plain"
    asked = len(session.calls)
    for _ in range(5):
        assert f.get(feed, via="auto", kind="feed").ok
        assert f.get(old, via="auto", kind="feed").refused
    assert session.routes()[asked:] == ["plain"] * 5, "the refused sitemap waits; the feed carries on"
    assert writer.rows[ESCALATE_AFTER]["error"] == "refused: REQS001 Requests to this domain are forbidden"
    clock.t += RECHECK + 1
    for _ in range(ESCALATE_AFTER):
        f.get(old, via="auto", kind="feed")
    assert session.routes()[-1] == "zenrows", "after RECHECK ZenRows is asked again, in case it lifted the ban"
    assert budget.spent_today() == 0


def test_a_host_on_zenrows_that_zenrows_starts_forbidding_goes_back_to_plain_in_the_same_fetch(tmp_path):
    session = Session(plain=[Response(403), Response(403), Response(403), ok(b"plain again")],
                      basic=[ok(), FORBIDDEN])
    f, _, _, _ = make(tmp_path, session)
    assert refused_until_escalated(f, SITE).route == "zenrows"
    page = f.get(SITE, via="auto")
    assert (page.route, page.text) == ("plain", "plain again") and f.route(SITE, "auto") == "plain"
    assert session.routes()[-2:] == ["zenrows", "plain"]


def test_a_forbidding_zenrows_never_changes_a_fetch_that_must_use_zenrows(tmp_path):
    session = Session(basic=[FORBIDDEN], premium=[FORBIDDEN])
    f, _, _, _ = make(tmp_path, session)
    f.get(SITE, via="zenrows")
    assert session.routes() == ["zenrows", "zenrows_premium"] and f.route(SITE, "zenrows") == "zenrows_premium"


def test_at_the_dearest_route_one_refused_url_waits_while_the_host_s_others_are_fetched(tmp_path):
    other = "https://www.nytimes.com/2026/11/11/movies/y-review.html"

    class OneWalled(Session):
        def get(self, url, headers=None, params=None, timeout=None):
            target = params["url"] if params else url
            self.calls.append(("zenrows_premium" if params.get("premium_proxy") else "zenrows", target, headers,
                               params))
            return ok(b"wall") if target == SITE else ok()
    session = OneWalled()
    f, _, _, clock = make(tmp_path, session)
    wall = lambda t: t != "wall"
    for _ in range(ESCALATE_AFTER):
        f.get(SITE, via="zenrows", valid=wall)
    assert f.route(SITE, "zenrows") == "zenrows_premium"
    asked = len(session.calls)
    for _ in range(3):
        assert f.get(SITE, via="zenrows", valid=wall).refused
        assert f.get(other, via="zenrows", valid=wall).ok
    assert [c[1] for c in session.calls[asked:]] == [other] * 3
    clock.t += BACKOFF_START + 1
    f.get(SITE, via="zenrows", valid=wall)
    assert session.calls[-1][1] == SITE


def test_a_zenrows_refusal_is_logged_with_zenrows_own_code(tmp_path):
    session = Session(basic=[zenrows_error(422, "REQS004", "Target blocked"), zenrows_error(429, "AUTH006",
                                                                                        "Concurrency limit reached")])
    f, writer, _, _ = make(tmp_path, session)
    f.get(SITE, via="zenrows")
    f.get(SITE, via="zenrows")
    assert [r["error"] for r in writer.rows] == ["refused: REQS004 Target blocked",
                                                  "transient: AUTH006 Concurrency limit reached"]


def test_a_plain_refusal_is_logged_without_a_reason(tmp_path):
    f, writer, _, _ = make(tmp_path, Session(plain=[Response(403, b"<html>no</html>")]))
    f.get(SITE, via="auto")
    assert writer.rows[0]["error"] == "refused"


# --- connections to ZenRows failing --------------------------------------------------------------------------------

class Dropping(Session):
    """ZenRows' API closing connections without an answer (prod, 2026-11-12, for ~5 min), until `fixed`."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.fixed = False

    def get(self, url, headers=None, params=None, timeout=None):
        if url == ZENROWS_API and not self.fixed:
            self.calls.append(("dropped", params["url"], headers, params))
            raise ConnectionError("Remote end closed connection without response")
        return super().get(url, headers, params, timeout)


def test_connections_to_zenrows_failing_pause_that_host_s_zenrows_fetches_then_retry(tmp_path):
    session = Dropping(basic=[ok()])
    f, _, budget, clock = make(tmp_path, session)
    for _ in range(CONN_FAILURES):
        with pytest.raises(ConnectionError):
            f.get(SITE, via="zenrows")
    paused = f.get(SITE, via="zenrows")
    assert paused.status == 0 and paused.transient and not paused.ok and len(session.calls) == CONN_FAILURES
    clock.t += CONN_BACKOFF_START + 1
    session.fixed = True
    assert f.get(SITE, via="zenrows").ok and budget.spent_today() == 1


def test_the_connection_pause_doubles_while_zenrows_keeps_dropping_and_resets_once_it_answers(tmp_path):
    session = Dropping(basic=[ok()])
    f, _, _, clock = make(tmp_path, session)
    state = lambda: f._routes[("www.nytimes.com", "zenrows", "")]
    pauses = []
    for _ in range(6):
        for _ in range(CONN_FAILURES):
            with pytest.raises(ConnectionError):
                f.get(SITE, via="zenrows")
        pauses.append(state().conn_backoff)
        clock.t = state().conn_blocked_until + 1
    assert pauses[:2] == [CONN_BACKOFF_START, 2 * CONN_BACKOFF_START] and max(pauses) == CONN_BACKOFF_MAX
    session.fixed = True
    f.get(SITE, via="zenrows")
    assert state().conn_backoff == 0.0 and state().conn_failures == 0


def test_a_zenrows_connection_pause_never_stops_a_plain_fetch(tmp_path):
    other = "https://www.nytimes.com/sitemaps/new/news.xml.gz"
    session = Dropping(plain=[ok(b"<urlset/>")], basic=[ok()])
    f, _, _, _ = make(tmp_path, session)
    for _ in range(CONN_FAILURES):
        with pytest.raises(ConnectionError):
            f.get(SITE, via="zenrows", kind="page")
    assert f.get(other, via="auto", kind="feed").ok
