"""The rt_reviews polling source: outlet reviews of films with open Rotten Tomatoes markets, read within about a
minute of their outlet listing them, and each film's live Tomatometer.

Lanes (run concurrently; a slow outlet or a long Claude call holds up only its own):
- films: every 10 minutes, the films open on Kalshi, Polymarket and Polymarket US (films.py), each new one's RT slug
  looked up once through RT's search.
- one per outlet: its fast feed every 30-60 s and its archive tier every few minutes (outlets.py), each URL matching an
  open film recorded once in rt_articles and queued; then the queue, newest first: the page fetched (and kept in the
  page cache), read (page.py) and classified by Claude (classify.py), into rt_reviews.
- tomatometer: every 5 minutes, each film closing within tomatometer_days, its public RT page through ZenRows; a row
  in rt_scores when its counts change.

first_seen_at is when the source saw a thing: an article when its feed listed it, a review when its read was ready
(when a strategy could first act on it). Discovery lag (article seen minus published) and read lag (review ready minus
article seen) go to poll_health per outlet, for articles published after the run started.
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from gnomepy.data import CatalogConfig, DatasetStore
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual

from gnome_pipelines import PipelineParams, Resources
from gnome_pipelines.datasets.polling import POLL_HEALTH
from gnome_pipelines.datasets.rt import RT_ARTICLES, RT_FILMS, RT_REVIEWS, RT_SCORES
from gnome_pipelines.environment import Stage, environment
from gnome_pipelines.polling.claude import ClaudeReader, NotConfigured, Unavailable
from gnome_pipelines.polling.alerts import Alerts
from gnome_pipelines.polling.fetch import (ZENROWS_CREDITS, Archive, Fetcher, LocalArchive, Page, S3Archive,
                                           ZenRowsBudget)
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Meter, Service, Task, TaskAlert, utcnow
from gnome_pipelines.polling.source import PollingSource
from gnome_pipelines.polling.writer import TableWriter
from gnome_pipelines.rt import classify as claude_read
from gnome_pipelines.rt.films import FilmState, Venues, changed
from gnome_pipelines.rt.match import child_sitemaps, decode, entries, is_listing, is_review_of, toks
from gnome_pipelines.rt.outlets import OUTLETS, Outlet, recent
from gnome_pipelines.rt.page import PageFacts, facts, is_review_page
from gnome_pipelines.rt.tomatometer import PAGE, counts, is_rt_page
from gnome_pipelines.stores import dataset_store

logger = logging.getLogger(__name__)

SOURCE = "rt_reviews"
CREDITS_METER = "zenrows"
FILMS_EVERY = 600.0
READ_EVERY = 1.0
TOMATOMETER_EVERY = 300.0
# A film's RT search is retried this long after it found nothing.
SLUG_RETRY = timedelta(hours=6)
# A page that can't be read is tried again after RETRY_BASE, doubling to RETRY_MAX, while its film is open; a note
# goes to poll_health at the NOTE_AFTERth attempt.
RETRY_BASE = timedelta(seconds=15)
RETRY_MAX = timedelta(minutes=30)
NOTE_AFTER = 4
# After Claude says it can't read (an outage, the daily cap), verdicts wait this long before it's asked again.
CLAUDE_RETRY = timedelta(minutes=5)
# RT searches per films pass, so the first pass after a start can't run for many minutes.
SLUG_LOOKUPS_PER_PASS = 5
# Alerts (to Slack): how long a task may go without a success, and the checks' thresholds.
FILMS_ALERT_AFTER = 1800.0
FEED_ALERT_AFTER = 900.0
ARCHIVE_ALERT_AFTER = 3600.0
TOMATOMETER_ALERT_AFTER = 1200.0
CLAUDE_ALERT_AFTER = timedelta(minutes=15)
ZENROWS_WARN = 0.8
CHECKS_EVERY = 60.0
# A feed that needs ZenRows is polled at most this often: its news feed, and its archive tier.
ZENROWS_FEED_EVERY = 300.0
ZENROWS_ARCHIVE_EVERY = 1800.0
# An article listed this long before the run started is backlog: read after anything new, and left out of lag stats.
BACKLOG = timedelta(hours=1)
LOCAL_PAGES = Path.home() / ".cache" / "gnome-pipelines" / "rt_pages"


class RtReviewsParams(PipelineParams):
    claude: bool = Field(True, description="have Claude read each review (off: rows are kept with no verdict)")
    tomatometer: bool = Field(True, description="read each film's live Tomatometer from its RT page")
    tomatometer_days: float = Field(21.0, gt=0, description="read the Tomatometer of films closing within this many days")
    zenrows_daily_credits: int = Field(25000, ge=0, description=(
        "ZenRows credits this source may spend per UTC day (RT pages, RT search, outlets that refuse plain requests); "
        "0 turns ZenRows off"))
    outlets: list[str] | None = Field(None, description="only these outlets (default: all of rt/outlets.py)")


@dataclass
class Article:
    url: str
    film_id: str
    title: str
    outlet: str
    feed: str
    feed_published_at: datetime | None
    via: str
    attempts: int = 0
    retry_at: datetime | None = None
    last_error: str | None = None
    first_seen_at: datetime | None = None

    def row(self) -> dict:
        return {"url": self.url, "film_id": self.film_id, "outlet": self.outlet, "feed": self.feed,
                "feed_published_at": self.feed_published_at, "first_seen_at": self.first_seen_at}


@dataclass
class Review:
    article: Article
    published_at: datetime | None
    headline: str | None
    rating_kind: str | None
    rating_value: float | None
    rating_grade: str | None
    verdict: claude_read.Verdict | None
    archive_key: str | None
    first_seen_at: datetime | None = None
    # False for a verdict that came after the page's first row: its lags were counted with that row.
    first_read: bool = False

    def row(self) -> dict:
        v = self.verdict
        return {"url": self.article.url, "film_id": self.article.film_id, "outlet": self.article.outlet,
                "published_at": self.published_at, "article_seen_at": self.article.first_seen_at,
                "first_seen_at": self.first_seen_at, "headline": self.headline, "rating_kind": self.rating_kind,
                "rating_value": self.rating_value, "rating_grade": self.rating_grade,
                "is_review": v.is_review if v else None, "fresh": v.fresh if v else None,
                "confidence": v.confidence if v else None, "model": claude_read.MODEL if v else None,
                "archive_key": self.archive_key, "backfill": False}


@dataclass
class Score:
    film_id: str
    rt_slug: str
    liked: int
    not_liked: int
    score: float | None
    archive_key: str | None
    first_seen_at: datetime | None = None

    def row(self) -> dict:
        return {"film_id": self.film_id, "rt_slug": self.rt_slug, "liked": self.liked, "not_liked": self.not_liked,
                "score": self.score, "first_seen_at": self.first_seen_at, "archive_key": self.archive_key}


def _load(store: DatasetStore, name: str) -> pd.DataFrame:
    try:
        return store.load(name)
    except NoSuchTableError:
        return pd.DataFrame()


def _newest(df: pd.DataFrame, key: str) -> pd.DataFrame:
    if df.empty:
        return df
    return df.sort_values("first_seen_at", kind="stable").drop_duplicates(key, keep="last")


def page_archive() -> Archive:
    if environment().stage is Stage.LOCAL:
        return LocalArchive(LOCAL_PAGES)
    bucket = CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]
    return S3Archive(bucket)


class RtContext:
    def __init__(self, store: DatasetStore, writer: TableWriter, fetcher: Fetcher, reader: ClaudeReader | None,
                 health: PollHealth, params: RtReviewsParams, outlets: tuple[Outlet, ...], started: datetime,
                 alerts: Alerts | None = None) -> None:
        self.writer, self.fetcher, self.reader, self.health, self.params = writer, fetcher, reader, health, params
        self.alerts = alerts
        self.outlets, self.started = outlets, started
        self.venues = Venues(fetcher)
        films = _newest(_load(store, RT_FILMS.name), "film_id")
        self.known: dict[str, FilmState] = {
            r.film_id: FilmState(r.film_id, r.title, _ts(r.close), bool(r.open), r.kalshi_event,
                                 tuple(int(s) for s in (r.kalshi_strikes if r.kalshi_strikes is not None else [])),
                                 r.polymarket_event, r.polymarket_us_event, r.rt_slug, _ts(r.first_seen_at))
            for r in films.itertuples(index=False)}
        self.open = {fid: f for fid, f in self.known.items() if f.open}
        self.slug_tried: dict[str, datetime] = {}
        articles, reviews = _load(store, RT_ARTICLES.name), _load(store, RT_REVIEWS.name)
        read = set(reviews["url"]) if len(reviews) else set()
        self.seen: set[str] = (set(articles["url"]) if len(articles) else set()) | read
        self.pending: dict[str, deque[Article]] = {o.name: deque() for o in outlets}
        # Articles seen before a restart but never read go back on their queues, after anything new.
        by_name = {o.name: o for o in outlets}
        for r in articles.itertuples(index=False) if len(articles) else ():
            film = self.open.get(r.film_id)
            if r.url not in read and film and r.outlet in by_name:
                outlet = by_name[r.outlet]
                self.pending[r.outlet].append(Article(r.url, r.film_id, film.title, r.outlet, r.feed,
                                                      _ts(r.feed_published_at), outlet.via,
                                                      first_seen_at=_ts(r.first_seen_at)))
        scores = _newest(_load(store, RT_SCORES.name), "film_id")
        self.scores = {r.film_id: (int(r.liked), int(r.not_liked)) for r in scores.itertuples(index=False)}
        self.index_children: dict[str, list[str]] = {}
        self.unclassified: dict[str, deque[tuple[Article, PageFacts, str | None]]] = {o.name: deque() for o in outlets}
        self.claude_retry_at: datetime | None = None
        self.claude_down_since: datetime | None = None
        self.no_score: set[str] = set()
        self.misses: dict[str, int] = {}
        self.slug_budget = 0

    # --- films --------------------------------------------------------------------------------------------------
    def films_run(self, now: datetime) -> list[FilmState]:
        self.slug_budget = SLUG_LOOKUPS_PER_PASS
        known, updates = changed(self.known, self.venues.open_films(), lambda f: self._slug(f, now), self.misses)
        self.known = known
        # A new dict, never mutated: the outlet lanes read it without a lock.
        self.open = {fid: f for fid, f in known.items() if f.open}
        return updates

    def _slug(self, film: FilmState, now: datetime) -> str | None:
        tried = self.slug_tried.get(film.film_id)
        if (tried is not None and now - tried < SLUG_RETRY) or self.slug_budget <= 0:
            return None
        self.slug_budget -= 1
        self.slug_tried[film.film_id] = now
        try:
            return self.venues.rt_slug(film.title, film.close, via="zenrows")
        except Exception as e:
            self.health.record("note", now, f"RT search for {film.title}: {type(e).__name__}: {e}", subject=film.film_id)
            return None

    # --- outlets ------------------------------------------------------------------------------------------------
    def _match(self, outlet: Outlet, feed: str, listed: list[tuple[str, datetime | None]]) -> list[Article]:
        # Longest titles first, so "zootopia-2-review" goes to "Zootopia 2" when "Zootopia" is open too.
        films = sorted(self.open.values(), key=lambda f: -len(toks(f.title)))
        out = []
        for url, when in listed:
            if url in self.seen:
                continue
            film = next((f for f in films if is_review_of(url, f.title, outlet.pattern)), None)
            if film is not None:
                out.append(Article(url, film.film_id, film.title, outlet.name, feed, when, outlet.via))
        return out

    def feed_every(self, outlet: Outlet, every: float, through_zenrows: float):
        """A feed's interval: `every`, or at least `through_zenrows` while the outlet's feeds need ZenRows (a 30 s
        poll on its premium proxy would be ~29k credits a day)."""
        def interval(now: datetime) -> float:
            url = outlet.fast[0] if outlet.fast else outlet.index or next(iter(outlet.archive(now)), None)
            if url is None or self.fetcher.route(url, outlet.via, "feed") == "plain":
                return every
            return max(every, through_zenrows)
        return interval

    def _feed(self, outlet: Outlet, url: str):
        return self.fetcher.get(url, via=outlet.via, kind="feed", valid=is_listing)

    def fast_run(self, outlet: Outlet):
        def run(now: datetime) -> list[Article]:
            found, failed = [], []
            for url in outlet.fast:
                page = self._feed(outlet, url)
                if page.ok:
                    found += self._match(outlet, "fast", entries(decode(page.content)))
                elif page.status != 304:
                    failed.append(f"{_status(page)} from {url}")
            if failed and len(failed) == len(outlet.fast):
                raise RuntimeError("; ".join(failed))
            for why in failed:
                # One of several feeds: what the others found still counts, and the failure is on record.
                self.health.record("task_error", now, f"{outlet.name}.fast: {why}", subject=f"{outlet.name}.fast")
            return found
        return run

    def _archive_pages(self, outlet: Outlet, now: datetime) -> list[Page]:
        """The archive tier's sitemaps that changed since their last fetch: the outlet's dated sitemaps, or its
        index's recent children (a child that is itself an index stands for its own recent children). Each is
        fetched once a pass; an index that hasn't changed stands for the children it listed last time."""
        if outlet.archive is not None:
            return [self._feed(outlet, url) for url in outlet.archive(now)]
        pages = []
        for url in self._children(outlet, outlet.index, now):
            page = self._feed(outlet, url)
            if page.ok and "<sitemapindex" in (text := decode(page.content)):
                self.index_children[url] = recent(child_sitemaps(text), now)
            if url in self.index_children and url != outlet.index:
                pages += [self._feed(outlet, child) for child in self.index_children[url]]
            else:
                pages.append(page)
        return pages

    def _children(self, outlet: Outlet, index: str, now: datetime) -> list[str]:
        page = self._feed(outlet, index)
        if page.ok:
            self.index_children[index] = recent(child_sitemaps(decode(page.content)), now)
        elif page.status != 304 and index not in self.index_children:
            raise RuntimeError(f"{_status(page)} from {index}")
        return self.index_children.get(index, [])

    def archive_run(self, outlet: Outlet):
        def run(now: datetime) -> list[Article]:
            found, pages = [], self._archive_pages(outlet, now)
            for page in pages:
                if page.ok:
                    found += self._match(outlet, "archive", entries(decode(page.content)))
            failed = [p for p in pages if not p.ok and p.status != 304]
            if failed and len(failed) == len(pages):
                raise RuntimeError("; ".join(f"{_status(p)} from {p.url}" for p in failed[:3]))
            return found
        return run

    def read_run(self, outlet: Outlet):
        def run(now: datetime) -> list[Review]:
            queue = self.pending[outlet.name]
            article = self._next(queue, now)
            if article is None:
                return self._verdict_later(outlet, now)
            try:
                # Not conditional: a retry must get the page again, not a 304.
                page = self.fetcher.get(article.url, via=article.via, archive=True, kind="page", conditional=False,
                                        valid=is_review_page)
                parsed = facts(page.text, outlet.name) if page.ok else None
            except Exception as e:
                return self._retry(article, now, f"{type(e).__name__}: {e}")
            if parsed is None:
                return self._retry(article, now, _status(page))
            verdict = self._classify(article, parsed, now)
            review = Review(article, parsed.published_at, parsed.headline, parsed.rating_kind, parsed.rating_value,
                            parsed.rating_grade, verdict, page.archive_key, first_read=True)
            if verdict is None and self.reader is not None:
                # Kept without a verdict for now; the verdict follows as a new version of the row.
                self.unclassified[outlet.name].append((article, parsed, page.archive_key))
            return [review]
        return run

    def _next(self, queue: deque, now: datetime) -> Article | None:
        """The first article in the queue that isn't waiting out a retry delay (those rotate to the back)."""
        for _ in range(len(queue)):
            article = queue.popleft()
            if article.retry_at is None or article.retry_at <= now:
                return article
            queue.append(article)
        return None

    def _classify(self, article: Article, parsed: PageFacts, now: datetime) -> claude_read.Verdict | None:
        if self.reader is None or (self.claude_retry_at is not None and now < self.claude_retry_at):
            return None
        try:
            verdict = claude_read.classify(self.reader, article.title, article.outlet, article.url, parsed)
            self.claude_down_since = None
            return verdict
        except Unavailable as e:
            if self.claude_down_since is None:
                self.claude_down_since = now
            self.claude_retry_at = now + CLAUDE_RETRY
            self.health.record("note", now, f"Claude unavailable ({e}); verdicts wait {CLAUDE_RETRY}",
                               subject=article.outlet)
            return None

    def _verdict_later(self, outlet: Outlet, now: datetime) -> list[Review]:
        later = self.unclassified[outlet.name]
        if not later:
            return []
        article, parsed, key = later[0]
        verdict = self._classify(article, parsed, now)
        if verdict is None:
            return []
        later.popleft()
        return [Review(article, parsed.published_at, parsed.headline, parsed.rating_kind, parsed.rating_value,
                       parsed.rating_grade, verdict, key)]

    def _retry(self, article: Article, now: datetime, why: str) -> list[Review]:
        """Back on the queue, after a delay that grows with each attempt (to RETRY_MAX): a page is never dropped
        while its film is open, since an outage or a spent budget passes."""
        article.attempts += 1
        article.last_error = why
        if article.film_id not in self.open:
            self.health.record("note", now, f"gave up reading {article.url}: {why}; its film has closed",
                               subject=article.outlet)
            return []
        delay = min(RETRY_BASE * 2 ** (article.attempts - 1), RETRY_MAX)
        article.retry_at = now + delay
        self.pending[article.outlet].append(article)
        if article.attempts == NOTE_AFTER:
            self.health.record("note", now, f"still can't read {article.url}: {why}", subject=article.outlet)
        raise RuntimeError(f"{article.url}: {why} (attempt {article.attempts}; again in {delay})")

    # --- tomatometer --------------------------------------------------------------------------------------------
    def tomatometer_run(self, now: datetime) -> list[Score]:
        found, tried, failed = [], 0, []
        horizon = now + timedelta(days=self.params.tomatometer_days)
        for f in list(self.open.values()):
            if not f.rt_slug or (f.close is not None and f.close > horizon):
                continue
            tried += 1
            # A 200 that isn't an RT film page is a bot wall: refused, so the route steps up. An RT page without
            # counts (no reviews yet) is a real answer and moves nothing.
            page = self.fetcher.get(PAGE.format(slug=f.rt_slug), via="zenrows", archive=True, valid=is_rt_page,
                                    kind="page")
            c = counts(page.text) if page.ok else None
            if not page.ok:
                failed.append(f"{_status(page)} for {f.rt_slug}")
            if c is None:
                if f.film_id not in self.no_score:
                    self.no_score.add(f.film_id)
                    self.health.record("note", now, f"no Tomatometer counts on {f.rt_slug} yet (HTTP {page.status})",
                                       subject=f.film_id)
                continue
            self.no_score.discard(f.film_id)
            if self.scores.get(f.film_id) != (c.liked, c.not_liked):
                self.scores[f.film_id] = (c.liked, c.not_liked)
                found.append(Score(f.film_id, f.rt_slug, c.liked, c.not_liked, c.score, page.archive_key))
        if tried and len(failed) == tried:
            raise RuntimeError(f"no RT film page read: {'; '.join(failed[:3])}")
        return found

    # --- checks -------------------------------------------------------------------------------------------------
    def checks_run(self, now: datetime) -> list:
        """What no single task's failure shows: review pages that keep failing, Claude out for a while, and the
        ZenRows budget running low. Each fires its alert while it lasts and resolves once it's gone."""
        if self.alerts is None:
            return []
        for o in self.outlets:
            # A copy first: the outlet's lane changes its queue as this reads it.
            stuck = [a for a in list(self.pending[o.name]) if a.attempts >= NOTE_AFTER]
            name = f"{o.name}.pages"
            if stuck:
                self.alerts.fire(name, f"{SOURCE}: {len(stuck)} {o.name} review page{'s' if len(stuck) > 1 else ''} "
                                       f"still unread after {NOTE_AFTER}+ tries",
                                 "\n".join(f"{a.url} ({a.attempts} tries; {a.last_error})" for a in stuck[:5]),
                                 "WARNING", {"outlet": o.name})
            else:
                self.alerts.resolve(name)
        down = self.claude_down_since
        if down is not None and now - down >= CLAUDE_ALERT_AFTER:
            waiting = sum(len(q) for q in self.unclassified.values())
            self.alerts.fire("claude", f"{SOURCE}: Claude has been unavailable for {(now - down).seconds // 60} min",
                             f"{waiting} review{'s' if waiting != 1 else ''} kept without a verdict so far", "WARNING")
        elif down is None:
            self.alerts.resolve("claude")
        budget = self.fetcher.zenrows
        if budget is not None and budget.daily_credits:
            spent = budget.spent_today() / budget.daily_credits
            if budget.daily_credits - budget.spent_today() < ZENROWS_CREDITS["zenrows_premium"]:
                self.alerts.fire("zenrows_budget", f"{SOURCE}: today's ZenRows credits are spent",
                                 f"{budget.spent_today():.0f} of {budget.daily_credits}: pages that need ZenRows "
                                 "(RT, blocked outlets) aren't read until 00:00 UTC", "ERROR")
            elif spent >= ZENROWS_WARN:
                self.alerts.fire("zenrows_budget", f"{SOURCE}: {spent:.0%} of today's ZenRows credits spent",
                                 f"{budget.spent_today():.0f} of {budget.daily_credits}", "WARNING")
            else:
                self.alerts.resolve("zenrows_budget")
        return []

    # --- store --------------------------------------------------------------------------------------------------
    def store_new(self, found: list) -> list:
        new = []
        for r in found:
            if isinstance(r, Article):
                if r.url in self.seen:
                    continue
                self.seen.add(r.url)
                self.writer.add(RT_ARTICLES, [r.row()])
                # An archive entry without a date is old more often than not: it waits behind dated new ones.
                backlog = (r.feed_published_at < self.started - BACKLOG if r.feed_published_at is not None
                           else r.feed == "archive")
                (self.pending[r.outlet].append if backlog else self.pending[r.outlet].appendleft)(r)
            elif isinstance(r, Review):
                self.writer.add(RT_REVIEWS, [r.row()])
                self._lags(r)
            elif isinstance(r, Score):
                self.writer.add(RT_SCORES, [r.row()])
            elif isinstance(r, FilmState):
                self.writer.add(RT_FILMS, [r.row()])
            new.append(r)
        return new

    def _lags(self, r: Review) -> None:
        a = r.article
        published = r.published_at or a.feed_published_at
        if not r.first_read or published is None or published < self.started or a.first_seen_at is None:
            return
        self.health.record("discovery_lag", a.first_seen_at, f"{a.outlet} via {a.feed}",
                           (a.first_seen_at - published).total_seconds(), a.outlet)
        if r.first_seen_at is not None:
            self.health.record("read_lag", r.first_seen_at, f"{a.outlet}: page, read and Claude",
                               (r.first_seen_at - a.first_seen_at).total_seconds(), a.outlet)


def _status(page: Page) -> str:
    if page.status == 0:
        return ("skipped: connections to ZenRows keep failing, backing off" if page.transient
                else "skipped: the host refuses every route, backing off")
    return f"HTTP {page.status}{' (refused)' if page.refused else ''}{' (transient)' if page.transient else ''}"


def credits_spent_today(store: DatasetStore, now: datetime) -> int:
    """Today's ZenRows credits as of this source's last hourly report, so a restart doesn't start the day afresh
    (it still forgets up to an hour's spend)."""
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        rows = store.load(POLL_HEALTH.name, row_filter=And(EqualTo("source", SOURCE), EqualTo("kind", "credits"),
                                                           GreaterThanOrEqual("at", day.isoformat())))
    except NoSuchTableError:
        return 0
    rows = rows[rows["subject"] == CREDITS_METER] if len(rows) else rows
    return int(rows["value"].max()) if len(rows) else 0


def _ts(value) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).to_pydatetime()


class RtReviewsSource(PollingSource[RtReviewsParams]):
    name = SOURCE
    description = ("Rotten Tomatoes: outlet reviews of films with open RT markets read within about a minute, "
                   "Claude's fresh/rotten call on each, and the live Tomatometer")
    resources = Resources(cpu=512, memory=2048)
    secrets = ("anthropic", "zenrows")
    flush_every = {RT_ARTICLES.name: 2.0, RT_REVIEWS.name: 2.0, RT_SCORES.name: 10.0, RT_FILMS.name: 30.0}

    def build(self, params: RtReviewsParams, health: PollHealth, writer: TableWriter, alerts: Alerts) -> Service:
        outlets = tuple(o for o in OUTLETS if params.outlets is None or o.name in params.outlets)
        store = dataset_store()
        budget = (ZenRowsBudget(params.zenrows_daily_credits, spent=credits_spent_today(store, utcnow()))
                  if params.zenrows_daily_credits else None)
        fetcher = Fetcher(SOURCE, writer, page_archive(), zenrows=budget)
        reader = None
        if params.claude:
            try:
                reader = ClaudeReader(claude_read.MODEL, claude_read.DAILY_CALLS)
            except NotConfigured as e:
                health.record("note", utcnow(), f"Claude is off ({e}); reviews are kept without a verdict")
        ctx = RtContext(store, writer, fetcher, reader, health, params, outlets, utcnow(), alerts)
        # Its first pass after a start makes up to SLUG_LOOKUPS_PER_PASS RT searches: slow, not hung.
        tasks = [Task("films", lambda now: FILMS_EVERY, ctx.films_run, "films", timeout=1800.0,
                      alert=TaskAlert(FILMS_ALERT_AFTER, "film discovery (Kalshi, Polymarket, Polymarket US)")),
                 Task("checks", lambda now: CHECKS_EVERY, ctx.checks_run, "checks")]
        for o in outlets:
            lane = f"outlet:{o.name}"
            if o.fast:
                tasks.append(Task(f"{o.name}.fast", ctx.feed_every(o, o.fast_every, ZENROWS_FEED_EVERY),
                                  ctx.fast_run(o), lane, alert=TaskAlert(FEED_ALERT_AFTER, f"{o.name}'s news feed")))
            if o.archive is not None or o.index is not None:
                tasks.append(Task(f"{o.name}.archive", ctx.feed_every(o, o.archive_every, ZENROWS_ARCHIVE_EVERY),
                                  ctx.archive_run(o), lane,
                                  alert=TaskAlert(max(ARCHIVE_ALERT_AFTER, 2 * o.archive_every),
                                                  f"{o.name}'s archive sitemaps")))
            tasks.append(Task(f"{o.name}.read", lambda now: READ_EVERY, ctx.read_run(o), lane))
        if params.tomatometer and budget is None:
            health.record("note", utcnow(), "the Tomatometer is off: it reads RT pages through ZenRows, which is off")
        elif params.tomatometer:
            # Every 30 s until the films lane has found the open films (its first pass takes minutes), then every
            # TOMATOMETER_EVERY. A dozen ZenRows pages in a row can take minutes; that's not a hang.
            tasks.append(Task("tomatometer", lambda now: TOMATOMETER_EVERY if ctx.open else 30.0, ctx.tomatometer_run,
                              "tomatometer", timeout=1800.0,
                              alert=TaskAlert(TOMATOMETER_ALERT_AFTER, "Tomatometer reads (RT film pages)")))
        meters = [reader.meter()] if reader is not None else []
        if budget is not None:
            meters.append(Meter(CREDITS_METER, budget.spent_today, "ZenRows credits spent today", kind="credits"))
        return Service(tasks, health, store_new=ctx.store_new, meters=meters, alerts=alerts)
