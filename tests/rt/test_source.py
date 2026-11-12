import gzip
import json
from datetime import datetime, timedelta, timezone

import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.datasets.rt import RT_ARTICLES, RT_FILMS, RT_REVIEWS, RT_SCORES
from gnome_pipelines.polling.alerts import Alerts
from gnome_pipelines.polling.claude import Unavailable
from gnome_pipelines.polling.fetch import Page, ZenRowsBudget
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Service, Task
from gnome_pipelines.polling.writer import TableWriter
from gnome_pipelines.rt import films as rt_films
from gnome_pipelines.rt.outlets import Outlet
from gnome_pipelines.datasets.polling import POLL_HEALTH
from gnome_pipelines.rt.source import (CLAUDE_RETRY, CREDITS_METER, NOTE_AFTER, RETRY_BASE, RETRY_MAX,
                                       SLUG_LOOKUPS_PER_PASS, TOMATOMETER_HOT_FOR, ZENROWS_ARCHIVE_EVERY,
                                       ZENROWS_FEED_EVERY, Article, RtContext, RtReviewsParams, RtReviewsSource,
                                       credits_spent_today)

T0 = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
REVIEW_PAGE = ('<meta property="article:published_time" content="2026-11-11T12:00:00Z"/>'
               '<meta property="og:title" content="How to Rob a Bank review"/><p>A delight.</p>')
RT_PAGE = "https://www.rottentomatoes.com/m/how_to_rob_a_bank_2026"
OUTLET = Outlet("variety", ("https://variety.com/feed/",), pattern=r"/film/reviews/")
REVIEW_URL = "https://variety.com/2026/film/reviews/how-to-rob-a-bank-review-1/"


class Web:
    """A fake Fetcher: URL -> (status, body). Like the real one, a conditional fetch of a page whose body hasn't
    changed since its last 200 gets a 304."""

    zenrows = None

    def __init__(self, pages):
        self.pages, self.calls, self.last = pages, [], {}
        self.routes = {}

    def route(self, url, via, kind=""):
        return self.routes.get(url.split("/")[2], "plain")

    def get(self, url, via="plain", archive=False, valid=None, kind="", conditional=True):
        self.calls.append((url, via, archive))
        status, body = self.pages.get(url, (404, ""))
        if callable(body):
            body = body()
        if isinstance(body, Exception):
            raise body
        if status == 200 and conditional and self.last.get(url) == body:
            return Page(url, 304, None, T0, False)
        data = body.encode() if isinstance(body, str) else body
        text = body if isinstance(body, str) else gzip.decompress(body).decode()
        refused = status == 200 and valid is not None and not valid(text)
        if status == 200 and not refused and conditional:
            self.last[url] = body
        return Page(url, status, body if status == 200 and not refused else None, T0, status == 200 and not refused,
                    archive_key="k" if archive else None, content=data, refused=refused)


def venue_pages(rss_time="Wed, 11 Nov 2026 12:00:30 GMT"):
    kalshi = {"events": [{"event_ticker": "KXRT-HOWT", "sub_title": "How to Rob a Bank",
                          "markets": [{"floor_strike": 50, "close_time": "2026-11-16T15:00:00Z"}]}]}
    rss = (f"<rss><channel><item><link>{REVIEW_URL}</link><pubDate>{rss_time}</pubDate></item>"
           "<item><link>https://variety.com/2026/film/news/other-story/</link></item></channel></rss>")
    search = ('<search-page-media-row release-year="2026"><a href="https://www.rottentomatoes.com/m/how_to_rob_a_bank_2026">'
              '</a><a slot="title">How to Rob a Bank</a></search-page-media-row>')
    page = REVIEW_PAGE
    return {
        rt_films.KALSHI: (200, json.dumps(kalshi)),
        rt_films.POLYMARKET.format(page=1): (200, json.dumps({"events": [], "pagination": {"hasMore": False}})),
        rt_films.POLYMARKET_US: (200, json.dumps({"events": []})),
        rt_films.RT_SEARCH.format(query="How%20to%20Rob%20a%20Bank"): (200, search),
        "https://variety.com/feed/": (200, rss),
        REVIEW_URL: (200, page),
        RT_PAGE: (200, '"criticsScore":{"likedCount":30,"notLikedCount":3,"scorePercent":"91%"}'),
    }


class Recorded(Alerts):
    """Alerts that keep what was put, in memory."""

    def __init__(self):
        class Sink:
            def __init__(self):
                self.alerts = []

            def put(self, alert):
                self.alerts.append(alert)

        class Names:
            def load(self):
                return set()

            def save(self, names):
                pass
        super().__init__("rt_reviews", Sink(), Names(), clock=lambda: T0)

    def actions(self):
        return [(a["action"], a["key"].split(":")[-1], a.get("severity")) for a in self.sink.alerts]


class Reader:
    def __init__(self, fail=False):
        self.fail, self.asked = fail, []

    def read(self, key, system, user, schema, effort="low", max_tokens=2048):
        self.asked.append(user)
        if self.fail:
            raise Unavailable("API down")
        return {"is_review": True, "fresh": True, "confidence": 0.9}


@pytest.fixture
def env(tmp_path):
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh"))
    rows = []
    health = PollHealth("rt_reviews", publish=lambda df: rows.extend(df.to_dict("records")))
    writer = TableWriter(store.publish)
    return store, writer, health, rows


def context(env, web, reader=None, started=T0, outlets=(OUTLET,)):
    store, writer, health, _ = env
    ctx = RtContext(store, writer, None, reader, health, RtReviewsParams(), outlets, started)
    ctx.fetcher = ctx.venues.fetcher = web
    return ctx


def service(ctx, clock):
    return Service([Task("films", lambda now: 600, ctx.films_run, "films"),
                    Task("variety.fast", lambda now: 30, ctx.fast_run(OUTLET), "variety"),
                    Task("variety.read", lambda now: 1, ctx.read_run(OUTLET), "variety"),
                    Task("tomatometer", lambda now: 300, ctx.tomatometer_run, "tomatometer")],
                   ctx.health, store_new=ctx.store_new, clock=clock)


def test_a_new_review_goes_from_feed_to_verdict_with_its_lags_and_the_tomatometer(env):
    store, writer, _, rows = env
    web, reader = Web(venue_pages()), Reader()
    ctx = context(env, web, reader, started=T0 - timedelta(minutes=5))
    now = [T0 + timedelta(seconds=40)]
    svc = service(ctx, lambda: now[0])
    svc.step(now[0])
    now[0] += timedelta(seconds=5)
    svc.step(now[0])
    writer.flush()
    film = store.load(RT_FILMS.name).iloc[0]
    assert (film.film_id, film.rt_slug, list(film.kalshi_strikes)) == ("how-to-rob-a-bank", "how_to_rob_a_bank_2026", [50])
    article = store.load(RT_ARTICLES.name)
    assert article["url"].tolist() == [REVIEW_URL] and article["feed"].tolist() == ["fast"]
    review = store.load(RT_REVIEWS.name).iloc[0]
    assert (review.fresh, review.is_review, review.headline, review.model) == (True, True, "How to Rob a Bank review",
                                                                                "claude-haiku-5-5")
    # The fake clock stands still within a step, so the read lands "at once"; live, read_lag is the page + Claude time.
    assert review.first_seen_at == review.article_seen_at
    score = store.load(RT_SCORES.name).iloc[0]
    assert (score.liked, score.not_liked, score.score) == (30, 3, 91.0)
    lags = {r["kind"]: r["value"] for r in rows if r["kind"] in ("discovery_lag", "read_lag")}
    assert lags["discovery_lag"] == 40.0 and lags["read_lag"] == 0.0
    # The RT search and the RT page go through ZenRows; the outlet's page is kept in the page cache.
    via = {u: (v, a) for u, v, a in web.calls}
    assert via[REVIEW_URL] == ("auto", True)
    assert via["https://www.rottentomatoes.com/m/how_to_rob_a_bank_2026"] == ("zenrows", True)
    assert via[rt_films.RT_SEARCH.format(query="How%20to%20Rob%20a%20Bank")][0] == "zenrows"


def test_an_article_is_recorded_once_and_an_unchanged_tomatometer_writes_nothing(env):
    store, writer, _, _ = env
    ctx = context(env, Web(venue_pages()), Reader())
    svc = service(ctx, lambda: T0)
    svc.step(T0)
    svc.step(T0 + timedelta(seconds=40))
    svc.step(T0 + timedelta(minutes=6))
    writer.flush()
    assert len(store.load(RT_ARTICLES.name)) == 1 and len(store.load(RT_SCORES.name)) == 1


def test_a_restart_queues_articles_it_saw_but_never_read_and_backlog_waits_behind_new_ones(env):
    store, writer, _, _ = env
    unreadable = venue_pages()
    unreadable[REVIEW_URL] = (503, "")
    first = context(env, Web(unreadable), Reader())
    svc = service(first, lambda: T0)
    svc.step(T0)
    writer.flush()
    assert store.load(RT_ARTICLES.name)["url"].tolist() == [REVIEW_URL] and first.pending["variety"]
    again = context(env, Web(venue_pages()), Reader())
    assert [a.url for a in again.pending["variety"]] == [REVIEW_URL] and REVIEW_URL in again.seen
    old = Article("u-old", "f", "F", "variety", "archive", T0 - timedelta(days=3), "plain")
    new = Article("u-new", "f", "F", "variety", "fast", T0, "plain")
    again.pending["variety"].clear()
    again.store_new([old, new])
    assert [a.url for a in again.pending["variety"]] == ["u-new", "u-old"]


def queued(pages, reader=None, env=None):
    ctx = context(env, Web(pages), reader or Reader())
    ctx.films_run(T0)
    ctx.store_new(ctx.films_run(T0))
    ctx.store_new(ctx.fast_run(OUTLET)(T0))
    return ctx


def test_an_unreadable_page_is_tried_again_later_and_never_dropped_while_its_film_is_open(env):
    pages = venue_pages()
    pages[REVIEW_URL] = (503, "")
    ctx = queued(pages, env=env)
    read = ctx.read_run(OUTLET)
    at = T0
    for attempt in range(1, 8):
        with pytest.raises(RuntimeError, match=f"attempt {attempt}; again in"):
            read(at)
        article = ctx.pending["variety"][0]
        assert read(at) == [], "waiting out its delay, nothing is fetched"
        at = article.retry_at
    assert article.retry_at - T0 > RETRY_MAX
    pages[REVIEW_URL] = (200, REVIEW_PAGE)
    (review,) = read(at)
    assert review.verdict.fresh and not ctx.pending["variety"]


def test_a_retry_fetches_the_page_again_rather_than_getting_a_304(env):
    calls = [0]

    def flaky():
        calls[0] += 1
        return ConnectionError("reset") if calls[0] == 1 else REVIEW_PAGE
    pages = venue_pages()
    pages[REVIEW_URL] = (200, flaky)
    ctx = queued(pages, env=env)
    read = ctx.read_run(OUTLET)
    with pytest.raises(RuntimeError, match="ConnectionError"):
        read(T0)
    (review,) = read(T0 + RETRY_BASE)
    assert review.headline == "How to Rob a Bank review"


def test_without_claude_the_review_is_kept_at_once_and_its_verdict_follows(env):
    store, writer, _, rows = env
    reader = Reader(fail=True)
    ctx = queued(venue_pages(), reader, env)
    read = ctx.read_run(OUTLET)
    (first,) = read(T0)
    assert first.verdict is None and first.headline == "How to Rob a Bank review"
    asked = len(reader.asked)
    reader.fail = False
    assert read(T0 + timedelta(minutes=1)) == [] and len(reader.asked) == asked, "Claude waits out CLAUDE_RETRY"
    (later,) = read(T0 + CLAUDE_RETRY)
    assert later.verdict.fresh and not later.first_read and not ctx.unclassified["variety"]
    ctx.store_new([first])
    ctx.store_new([later])
    writer.flush()
    versions = store.load(RT_REVIEWS.name)
    assert versions["fresh"].tolist() == [True], "the newest version of the row carries the verdict"


def test_a_page_whose_film_has_closed_is_given_up(env):
    pages = venue_pages()
    pages[REVIEW_URL] = (503, "")
    ctx = queued(pages, env=env)
    ctx.open = {}
    assert ctx.read_run(OUTLET)(T0) == [] and not ctx.pending["variety"]


def test_a_bot_wall_on_a_feed_or_page_is_refused_not_read(env):
    pages = venue_pages()
    pages["https://variety.com/feed/"] = (200, "<html>Verifying your device</html>")
    ctx = context(env, Web(pages), Reader())
    ctx.store_new(ctx.films_run(T0))
    with pytest.raises(RuntimeError, match="refused"):
        ctx.fast_run(OUTLET)(T0)
    pages = venue_pages()
    pages[REVIEW_URL] = (200, "<html><title>Verifying Device</title></html>")
    ctx = queued(pages, env=env)
    with pytest.raises(RuntimeError, match="refused"):
        ctx.read_run(OUTLET)(T0)


def sitemap(urls, index=False):
    tag, item = ("sitemapindex", "sitemap") if index else ("urlset", "url")
    return f"<{tag}>" + "".join(f"<{item}><loc>{u}</loc><lastmod>2026-11-11</lastmod></{item}>" for u in urls) + f"</{tag}>"


def test_an_index_walked_outlet_reads_each_child_once_a_pass_and_finds_what_is_new(env):
    outlet = Outlet("empire", index="https://empire.com/sitemap.xml", pattern=r"/reviews/")
    first = "https://empire.com/reviews/how-to-rob-a-bank-review/"
    second = "https://empire.com/reviews/how-to-rob-a-bank-imax-review/"
    pages = venue_pages()
    news, nested, leaf = ("https://empire.com/sitemap-news-2026-11.xml", "https://empire.com/sitemap-posts.xml",
                          "https://empire.com/sitemap-posts-3.xml")
    pages[outlet.index] = (200, sitemap([news, nested], index=True))
    pages[nested] = (200, sitemap([leaf], index=True))
    pages[news] = (200, sitemap([first]))
    pages[leaf] = (200, sitemap([]))
    web = Web(pages)
    ctx = context(env, web, Reader(), outlets=(outlet,))
    ctx.store_new(ctx.films_run(T0))
    archive = ctx.archive_run(outlet)
    found = archive(T0)
    assert [a.url for a in found] == [first]
    ctx.store_new(found)
    # The index changes (its lastmod) because one child did; the child is read once and its new entry found.
    pages[outlet.index] = (200, sitemap([news, nested], index=True) + " ")
    pages[news] = (200, sitemap([first, second]))
    web.calls.clear()
    assert [a.url for a in archive(T0)] == [second]
    assert sum(u == news for u, _, _ in web.calls) == 1


def test_a_sequel_s_review_goes_to_the_sequel_when_both_films_are_open(env):
    ctx = context(env, Web({}), Reader())
    ctx.open = {"zootopia": rt_films.FilmState("zootopia", "Zootopia", T0),
                "zootopia-2": rt_films.FilmState("zootopia-2", "Zootopia 2", T0)}
    (article,) = ctx._match(OUTLET, "fast", [("https://variety.com/2026/film/reviews/zootopia-2-review-1/", T0)])
    assert article.film_id == "zootopia-2"


def test_a_films_pass_makes_at_most_a_few_rt_searches(env):
    searches = []

    class Counting(Web):
        def get(self, url, via="plain", archive=False, valid=None, kind="", conditional=True):
            if kind == "search":
                searches.append(url)
            return super().get(url, via, archive, valid, kind, conditional)
    pages = venue_pages()
    many = {"events": [{"event_ticker": f"KXRT-F{i}", "sub_title": f"Film Number {i}",
                        "markets": [{"floor_strike": 50, "close_time": "2026-11-16T15:00:00Z"}]} for i in range(12)]}
    pages[rt_films.KALSHI] = (200, json.dumps(many))
    ctx = context(env, Counting(pages), Reader())
    ctx.films_run(T0)
    assert len(searches) == SLUG_LOOKUPS_PER_PASS
    ctx.films_run(T0 + timedelta(minutes=10))
    assert len(searches) == 2 * SLUG_LOOKUPS_PER_PASS


def test_zenrows_spend_so_far_today_is_read_back_after_a_restart(env):
    store, _, _, _ = env
    assert credits_spent_today(store, T0) == 0
    health = PollHealth("rt_reviews", publish=lambda df: store.publish(POLL_HEALTH, df))
    health.record("credits", T0 - timedelta(hours=2), "ZenRows credits spent today", 900.0, CREDITS_METER)
    health.record("credits", T0 - timedelta(hours=1), "ZenRows credits spent today", 1200.0, CREDITS_METER)
    health.record("credits", T0 - timedelta(hours=13), "ZenRows credits spent today", 7000.0, CREDITS_METER)
    assert credits_spent_today(store, T0) == 1200


def test_the_source_wires_lanes_per_outlet_and_needs_zenrows_for_the_tomatometer(env, monkeypatch):
    store, writer, health, rows = env
    monkeypatch.setattr("gnome_pipelines.rt.source.dataset_store", lambda: store)
    monkeypatch.setattr("gnome_pipelines.rt.source.page_archive", lambda: None)
    params = RtReviewsParams(claude=False, outlets=["variety", "empire"], zenrows_daily_credits=0)
    svc = RtReviewsSource().build(params, health, writer, Recorded())
    lanes = {t.name: t.lane for t in svc.tasks}
    assert lanes == {"films": "films", "checks": "checks", "variety.fast": "outlet:variety",
                     "variety.archive": "outlet:variety", "variety.read": "outlet:variety",
                     "empire.archive": "outlet:empire", "empire.read": "outlet:empire"}
    alerted = {t.name: t.alert.after for t in svc.tasks if t.alert}
    assert alerted == {"films": 1800.0, "variety.fast": 900.0, "variety.archive": 3600.0, "empire.archive": 3600.0}
    assert svc.alerts is not None
    assert any("Tomatometer is off" in r["detail"] for r in rows)


def test_feeds_and_pages_are_fetched_as_separate_kinds_and_rt_through_zenrows_with_a_check(env):
    seen = []

    class Recording(Web):
        def get(self, url, via="plain", archive=False, valid=None, kind="", conditional=True):
            seen.append((url.split("/")[2], via, kind, valid is not None))
            return super().get(url, via, archive, valid, kind, conditional)
    ctx = context(env, Recording(venue_pages()), Reader())
    svc = service(ctx, lambda: T0)
    svc.step(T0)
    kinds = set(seen)
    assert ("variety.com", "auto", "feed", True) in kinds and ("variety.com", "auto", "page", True) in kinds
    assert ("www.rottentomatoes.com", "zenrows", "search", True) in kinds
    assert ("www.rottentomatoes.com", "zenrows", "page", True) in kinds


def test_the_tomatometer_waits_30_s_at_a_time_until_the_films_are_known(env, monkeypatch):
    store, writer, health, _ = env
    monkeypatch.setattr("gnome_pipelines.rt.source.dataset_store", lambda: store)
    monkeypatch.setattr("gnome_pipelines.rt.source.page_archive", lambda: None)
    svc = RtReviewsSource().build(RtReviewsParams(claude=False, outlets=["variety"]), health, writer, Recorded())
    task = next(t for t in svc.tasks if t.name == "tomatometer")
    assert task.interval(T0) == 30.0


# --- alerts --------------------------------------------------------------------------------------------------------

def test_a_review_page_that_keeps_failing_alerts_for_its_outlet_until_it_is_read(env):
    pages = venue_pages()
    pages[REVIEW_URL] = (503, "")
    ctx = queued(pages, env=env)
    alerts = ctx.alerts = Recorded()
    read, at = ctx.read_run(OUTLET), T0
    for _ in range(NOTE_AFTER - 1):
        with pytest.raises(RuntimeError):
            read(at)
        at = ctx.pending["variety"][0].retry_at
    ctx.checks_run(at)
    assert alerts.actions() == []
    with pytest.raises(RuntimeError):
        read(at)
    ctx.checks_run(at)
    (fired,) = alerts.sink.alerts
    assert fired["title"] == "rt_reviews: 1 variety review page still unread after 4+ tries"
    assert REVIEW_URL in fired["body"] and "HTTP 503" in fired["body"] and fired["labels"]["outlet"] == "variety"
    pages[REVIEW_URL] = (200, REVIEW_PAGE)
    read(ctx.pending["variety"][0].retry_at)
    ctx.checks_run(at)
    assert alerts.actions()[-1] == ("RESOLVED", "variety.pages", None)


def test_claude_out_for_15_minutes_alerts_and_resolves_when_it_answers(env):
    reader = Reader(fail=True)
    ctx = queued(venue_pages(), reader, env)
    alerts = ctx.alerts = Recorded()
    ctx.read_run(OUTLET)(T0)
    ctx.checks_run(T0 + timedelta(minutes=14))
    assert alerts.actions() == []
    ctx.checks_run(T0 + timedelta(minutes=15))
    (fired,) = alerts.sink.alerts
    assert fired["title"] == "rt_reviews: Claude has been unavailable for 15 min" and "1 review kept" in fired["body"]
    reader.fail = False
    ctx.read_run(OUTLET)(T0 + timedelta(minutes=20))
    ctx.checks_run(T0 + timedelta(minutes=20))
    assert alerts.actions()[-1] == ("RESOLVED", "claude", None)


def test_the_zenrows_budget_warns_at_80_percent_errs_once_spent_and_clears_the_next_day(env):
    day = [T0]
    ctx = context(env, Web({}), Reader())
    ctx.fetcher.zenrows = ZenRowsBudget(1000, clock=lambda: day[0])
    alerts = ctx.alerts = Recorded()
    ctx.fetcher.zenrows.pay(799)
    ctx.checks_run(T0)
    assert alerts.actions() == []
    ctx.fetcher.zenrows.pay(1)
    ctx.checks_run(T0)
    ctx.fetcher.zenrows.pay(195)
    ctx.checks_run(T0)
    day[0] = T0 + timedelta(days=1)
    ctx.checks_run(day[0])
    assert alerts.actions() == [("FIRING", "zenrows_budget", "WARNING"), ("FIRING", "zenrows_budget", "ERROR"),
                                ("RESOLVED", "zenrows_budget", None)]


def test_an_archive_pass_fails_only_when_every_sitemap_failed(env):
    outlet = Outlet("empire", index="https://empire.com/sitemap.xml", pattern=r"/reviews/")
    pages = venue_pages()
    news = "https://empire.com/sitemap-news-2026-11.xml"
    ctx = context(env, Web(pages), Reader(), outlets=(outlet,))
    ctx.store_new(ctx.films_run(T0))
    pages[outlet.index] = (403, "")
    with pytest.raises(RuntimeError, match="HTTP 403"):
        ctx.archive_run(outlet)(T0)
    pages[outlet.index] = (200, sitemap([news], index=True))
    pages[news] = (503, "")
    with pytest.raises(RuntimeError, match="HTTP 503"):
        ctx.archive_run(outlet)(T0)
    pages[news] = (200, sitemap([]))
    assert ctx.archive_run(outlet)(T0) == []


def test_one_of_several_feeds_failing_keeps_what_the_others_found(env):
    store, writer, health, rows = env
    outlet = Outlet("variety", ("https://variety.com/feed/", "https://variety.com/other-feed/"), pattern=r"/film/reviews/")
    pages = venue_pages()
    pages["https://variety.com/other-feed/"] = (503, "")
    ctx = context(env, Web(pages), Reader(), outlets=(outlet,))
    ctx.store_new(ctx.films_run(T0))
    assert [a.url for a in ctx.fast_run(outlet)(T0)] == [REVIEW_URL]
    assert any(r["kind"] == "task_error" and "other-feed" in r["detail"] for r in rows)
    pages["https://variety.com/feed/"] = (503, "")
    with pytest.raises(RuntimeError):
        ctx.fast_run(outlet)(T0)


def test_the_tomatometer_fails_when_no_film_page_could_be_read(env):
    pages = venue_pages()
    ctx = queued(pages, env=env)
    pages[RT_PAGE] = (503, "")
    with pytest.raises(RuntimeError, match="no RT film page read"):
        ctx.tomatometer_run(T0)


def test_a_feed_that_needs_zenrows_is_polled_every_5_minutes_and_its_archive_every_30(env):
    web = Web({})
    ctx = context(env, web, Reader())
    times = Outlet("thetimes", ("https://www.thetimes.com/sitemaps/news",), index="https://www.thetimes.com/sitemaps")
    fast, archive = ctx.feed_every(times, 30.0, ZENROWS_FEED_EVERY), ctx.feed_every(times, 300.0, ZENROWS_ARCHIVE_EVERY)
    assert (fast(T0), archive(T0)) == (30.0, 300.0)
    web.routes["www.thetimes.com"] = "zenrows_premium"
    assert (fast(T0), archive(T0)) == (300.0, 1800.0)
    dated = Outlet("nyt", archive=lambda now: ["https://www.nytimes.com/sitemaps/new/sitemap-2026-11.xml.gz"])
    web.routes["www.nytimes.com"] = "zenrows"
    assert ctx.feed_every(dated, 600.0, ZENROWS_ARCHIVE_EVERY)(T0) == 1800.0


def test_one_sitemap_timing_out_never_costs_the_pass_what_the_others_found(env):
    _, _, _, rows = env
    index = "https://nerdist.com/sitemap_index.xml"
    outlet = Outlet("nerdist", index=index)
    slow, articles, tags = ("https://nerdist.com/article-sitemap32.xml", "https://nerdist.com/article-sitemap.xml",
                            "https://nerdist.com/post_tag-sitemap11.xml")
    review, imax = ("https://nerdist.com/article/how-to-rob-a-bank-review/",
                    "https://nerdist.com/article/how-to-rob-a-bank-imax-review/")
    pages = venue_pages()
    pages[index] = (200, sitemap([slow, articles, tags], index=True))
    pages[slow] = (200, TimeoutError("read timed out"))
    pages[articles] = (200, sitemap([review]))
    web = Web(pages)
    ctx = context(env, web, Reader(), outlets=(outlet,))
    ctx.store_new(ctx.films_run(T0))
    archive = ctx.archive_run(outlet)
    found = archive(T0)
    assert [a.url for a in found] == [review]
    ctx.store_new(found)
    assert tags not in [u for u, _, _ in web.calls], "a tag sitemap never lists reviews"
    assert any(r["kind"] == "task_error" and "article-sitemap32" in r["detail"] for r in rows)
    # The index itself timing out: the children it listed last time are read.
    pages[index] = (200, TimeoutError("read timed out"))
    pages[articles] = (200, sitemap([review, imax]))
    assert [a.url for a in archive(T0)] == [imax]
    pages[articles] = (200, TimeoutError("read timed out"))
    with pytest.raises(RuntimeError, match="TimeoutError"):
        archive(T0)


def tomatometer_reads(ctx, web, start, minutes, every=5):
    """When each film's RT page was read, over `minutes` of the lane waking every `every` minutes."""
    reads = {}
    for m in range(0, minutes, every):
        web.calls.clear()
        ctx.tomatometer_run(start + timedelta(minutes=m))
        for url, _, _ in web.calls:
            reads.setdefault(url.rsplit("/", 1)[-1], []).append(m)
    return reads


def films_closing(env, pages, closes):
    web = Web(pages)
    ctx = context(env, web, Reader())
    ctx.open = {slug: rt_films.FilmState(slug, slug, T0 + close, rt_slug=slug) for slug, close in closes.items()}
    for slug in closes:
        pages[f"https://www.rottentomatoes.com/m/{slug}"] = (200, '"criticsScore":{"likedCount":0,"notLikedCount":0}')
    return ctx, web


def test_a_film_s_tomatometer_is_read_hourly_far_out_half_hourly_in_its_last_week_and_often_near_its_close(env):
    pages = {}
    ctx, web = films_closing(env, pages, {"far": timedelta(days=15), "week": timedelta(days=5),
                                          "closing": timedelta(hours=20)})
    reads = tomatometer_reads(ctx, web, T0, 120)
    assert reads["far"] == [0, 60]
    assert reads["week"] == [0, 30, 60, 90]
    assert reads["closing"] == list(range(0, 120, 5))


def test_a_film_whose_counts_move_or_whose_review_is_listed_is_read_often_for_a_while(env):
    pages = {}
    ctx, web = films_closing(env, pages, {"moving": timedelta(days=15), "listed": timedelta(days=15),
                                          "quiet": timedelta(days=15)})
    ctx.tomatometer_run(T0)
    pages["https://www.rottentomatoes.com/m/moving"] = (200, '"criticsScore":{"likedCount":47,"notLikedCount":13}')
    ctx.store_new([Article("u", "listed", "listed", "variety", "fast", T0, "auto", first_seen_at=T0)])
    reads = tomatometer_reads(ctx, web, T0 + timedelta(minutes=60), 60)
    assert reads["moving"] == [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55], "hot from its change at +60 min"
    assert reads["listed"][:3] == [0, 5, 10] and reads["quiet"] == [0]
    later = T0 + timedelta(minutes=60) + TOMATOMETER_HOT_FOR + timedelta(minutes=5)
    assert tomatometer_reads(ctx, web, later, 60)["listed"] == [0], "cooled off: read hourly again"


def test_a_film_moving_before_a_restart_is_still_read_often_after_it(env):
    store, writer, _, _ = env
    pages = {}
    ctx, web = films_closing(env, pages, {"moving": timedelta(days=15)})
    ctx.store_new([Article("u", "moving", "moving", "variety", "fast", T0, "auto", first_seen_at=T0)])
    writer.flush()
    again, web = films_closing(env, pages, {"moving": timedelta(days=15)})
    assert tomatometer_reads(again, web, T0 + timedelta(hours=1), 30)["moving"] == [0, 5, 10, 15, 20, 25]
