"""The outlets whose reviews the source reads, and where each lists new articles.

Each outlet has two tiers. The fast feed (a Google News sitemap or RSS, found by ~/gnome-research-work/rt/live/
probe_feeds.py on 2026-11-11) is polled every fast_every seconds; it is what makes a review visible within a minute
or so. The archive tier is the dated sitemap the research run harvested (~/gnome-research-work/rt/outlets/harvest.py),
limited to the current period and polled every few minutes: it catches what a short feed drops or never lists, and
is all there is for outlets without a fast feed. Which tier saw an article first is kept, so their lags compare.
Every outlet is fetched with via="auto": plainly, and through ZenRows only once it refuses (polling/fetch.py), which
the soak showed NYT's article pages and the Chicago Tribune do.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from gnome_pipelines.polling.fetch import BACKOFF_MAX, BACKOFF_START

ARCHIVE_EVERY = 300.0
FAST_EVERY = 30.0
# Children of a sitemap index dated within this many days are the archive tier's current period.
RECENT_DAYS = 3
# Words in a sitemap's file name that say it lists something other than articles (tags, authors, videos, a site's
# database pages): Nerdist's index lists ~30 of them as recently changed, each a fetch, and a timeout, per pass.
NOT_ARTICLES = frozenset({"tag", "category", "author", "authors", "video", "videos", "image", "images", "page",
                          "static", "listingpages", "db", "collection"})


@dataclass(frozen=True)
class Outlet:
    name: str
    fast: tuple[str, ...] = ()
    # The archive tier's URLs for a moment (dated sitemaps), or a sitemap index walked for its recent children.
    archive: Callable[[datetime], list[str]] | None = None
    index: str | None = None
    # URLs that may be film reviews: the research run's per-outlet section, else any URL saying "review".
    pattern: str = "review"
    via: str = "auto"
    fast_every: float = FAST_EVERY
    archive_every: float = ARCHIVE_EVERY
    # The longest a feed every route refuses waits before it's asked again (the wait doubles up to it), and how long
    # its news feed may go without a success before it alerts (None: the source's default).
    backoff_max: float = BACKOFF_MAX
    feed_alert_after: float | None = None


def _months(now: datetime) -> list[datetime]:
    # Around the turn of a month, the previous month's sitemap still gets late additions.
    earlier = (now.replace(day=1) - timedelta(days=1)) if now.day <= 2 else None
    return [m for m in (earlier, now) if m is not None]


def _days(now: datetime) -> list[datetime]:
    return [now - timedelta(days=1), now]


def penske(host: str) -> Callable[[datetime], list[str]]:
    return lambda now: [f"https://{host}/post-sitemap{m:%Y%m}.xml" for m in _months(now)]


def collider(now: datetime) -> list[str]:
    return [f"https://collider.com/sitemap-{m:%Y-%m}-part{p}-articles.xml" for m in _months(now) for p in (1, 2, 3)]


def thewrap(now: datetime) -> list[str]:
    return [f"https://www.thewrap.com/sitemap.xml?yyyy={d:%Y}&mm={d:%m}&dd={d:%d}" for d in _days(now)]


def nyt(now: datetime) -> list[str]:
    return [f"https://www.nytimes.com/sitemaps/new/sitemap-{m:%Y-%m}.xml.gz" for m in _months(now)]


OUTLETS = (
    Outlet("variety", ("https://variety.com/feed/",), penske("variety.com"), pattern=r"/film/reviews/"),
    Outlet("thr", ("https://www.hollywoodreporter.com/news-sitemap.xml",), penske("www.hollywoodreporter.com"),
           pattern=r"/movies/movie-reviews/"),
    Outlet("indiewire", ("https://www.indiewire.com/news-sitemap.xml",), penske("www.indiewire.com"),
           pattern=r"/criticism/movies/"),
    Outlet("deadline", ("https://deadline.com/feed/",), penske("deadline.com")),
    Outlet("rollingstone", ("https://www.rollingstone.com/feed/",), penske("www.rollingstone.com"),
           pattern=r"/tv-movies/tv-movie-reviews/"),
    Outlet("collider", ("https://collider.com/feed/",), collider),
    Outlet("thewrap", ("https://www.thewrap.com/news-sitemap.xml",), thewrap),
    # Vulture's and IGN's archives are one sitemap a year, a few MB: read them less often.
    Outlet("vulture", ("https://www.vulture.com/_news.xml",),
           lambda now: [f"https://www.vulture.com/sitemaps/sitemap-{now:%Y}.xml"], archive_every=1800.0),
    Outlet("ign", ("https://www.ign.com/rss/news/sitemap",),
           lambda now: [f"https://www.ign.com/rss/sitemap-articles-{now:%Y}.xml"], archive_every=1800.0),
    Outlet("nyt", ("https://www.nytimes.com/sitemaps/new/news.xml.gz",), nyt, pattern=r"/movies/.*review"),
    Outlet("empire", index="https://www.empireonline.com/sitemap_index.xml"),
    Outlet("avclub", ("https://www.avclub.com/rss",), index="https://www.avclub.com/wp-content/site-maps/sitemap-index.xml"),
    # Its news feed answers plain requests; its other sitemaps refuse them, and ZenRows won't fetch ew.com at all.
    Outlet("ew", ("https://ew.com/google-news-sitemap.xml",)),
    Outlet("slashfilm", ("https://www.slashfilm.com/feed/",), index="https://www.slashfilm.com/sitemap_index.xml"),
    Outlet("paste", index="https://www.pastemagazine.com/wp-sitemap.xml"),
    Outlet("gamesradar", ("https://www.gamesradar.com/sitemap-news.xml",), index="https://www.gamesradar.com/sitemap.xml"),
    Outlet("screenrant", ("https://screenrant.com/post_google_news.xml",), index="https://screenrant.com/sitemap.xml"),
    Outlet("denofgeek", ("https://www.denofgeek.com/feed/",), index="https://www.denofgeek.com/sitemap_index.xml"),
    Outlet("standard", ("https://www.standard.co.uk/rss",), index="https://www.standard.co.uk/sitemap.xml"),
    # ZenRows' premium proxy gets The Times' bot check about half the time, a page at random: a refusal waits the
    # usual 5 minutes rather than doubling, and a run of them is normal, so it alerts after an hour without a success.
    Outlet("thetimes", ("https://www.thetimes.com/sitemaps/news",),
           lambda now: [f"https://www.thetimes.com/sitemaps/articles/{now:%Y}"], backoff_max=BACKOFF_START,
           feed_alert_after=3600.0),
    Outlet("nypost", ("https://nypost.com/news-sitemap.xml",), lambda now: [f"https://nypost.com/sitemap-{now:%Y}.xml"]),
    Outlet("chicagotribune", index="https://www.chicagotribune.com/sitemap.xml"),
    Outlet("austinchronicle", ("https://www.austinchronicle.com/news-sitemap.xml",),
           index="https://www.austinchronicle.com/sitemap.xml"),
    Outlet("time", ("https://time.com/news-sitemap.xml",), index="https://time.com/sitemap.xml"),
    Outlet("latimes", index="https://www.latimes.com/sitemaps/sitemap.xml"),
    Outlet("npr", ("https://googlecrawl.npr.org/news/sitemap_news.xml",),
           index="https://googlecrawl.npr.org/standard/sitemap_index.xml"),
    Outlet("polygon", ("https://www.polygon.com/post_google_news.xml",), index="https://www.polygon.com/sitemap.xml"),
    Outlet("inverse", ("https://www.inverse.com/sitemaps/news.xml",), index="https://www.inverse.com/sitemaps/pages.xml"),
    Outlet("mashable", ("https://mashable.com/feed/",), index="https://mashable.com/sitemap-index.xml"),
    Outlet("cbr", ("https://www.cbr.com/post_google_news.xml",), index="https://www.cbr.com/sitemap.xml"),
    Outlet("movieweb", ("https://movieweb.com/post_google_news.xml",), index="https://movieweb.com/sitemap.xml"),
    Outlet("cinemablend", ("https://www.cinemablend.com/sitemap-news.xml",), index="https://www.cinemablend.com/sitemap.xml"),
    Outlet("decider", ("https://decider.com/news-sitemap.xml",),
           lambda now: [f"https://decider.com/sitemap-{now:%Y}.xml"]),
    Outlet("nerdist", ("https://nerdist.com/feed/",), index="https://nerdist.com/sitemap_index.xml"),
    Outlet("vanityfair", ("https://www.vanityfair.com/rss",), index="https://www.vanityfair.com/sitemap.xml"),
    Outlet("newyorker", ("https://www.newyorker.com/feed/google-news-sitemap-feed/sitemap-google-news",),
           index="https://www.newyorker.com/sitemap.xml"),
    # Refused plain sitemap requests in the research run (403); their review feeds do answer.
    Outlet("rogerebert", ("https://www.rogerebert.com/feed/",)),
    Outlet("slant", ("https://www.slantmagazine.com/feed/",)),
)

_DATE_PATTERNS = (r"yyyy=(\d{4})&mm=(\d{1,2})(?:&dd=(\d{1,2}))?", r"sitemap-(\d{4})\.xml\?mm=(\d{1,2})(?:&dd=(\d{1,2}))?",
                  r"/(20\d\d)/(\d{1,2})(?:/(\d{1,2}))?/?$", r"(20\d\d)-(\d\d)(?:-(\d\d))?", r"((?:19|20)\d\d)(\d\d)\.xml")


def url_date(url: str) -> tuple[datetime, bool] | None:
    """The date a dated sitemap's URL names, and whether it names only a month."""
    url = url.replace("&amp;", "&")
    for rx in _DATE_PATTERNS:
        m = re.search(rx, url)
        if m:
            day = int(m[3]) if m.lastindex >= 3 and m[3] else None
            if 1 <= int(m[2]) <= 12:
                return datetime(int(m[1]), int(m[2]), day or 1), day is None
    return None


def article_sitemaps(children: list[tuple[str, datetime | None]]) -> list[tuple[str, datetime | None]]:
    """The children of a sitemap index that may list articles: not its tag, author, video or similar sitemaps."""
    def words(url: str) -> set[str]:
        return set(re.split(r"[-_.]", url.split("?")[0].rstrip("/").rsplit("/", 1)[-1].lower()))
    return [(url, lastmod) for url, lastmod in children if not words(url) & NOT_ARTICLES]


def recent(children: list[tuple[str, datetime | None]], now: datetime) -> list[str]:
    """The children of a sitemap index that cover the last RECENT_DAYS, by the date in their URL, else their lastmod."""
    since = (now - timedelta(days=RECENT_DAYS)).replace(tzinfo=None)
    out = []
    for url, lastmod in children:
        named = url_date(url)
        if named:
            when, month_only = named
            if (when.year, when.month) >= (since.year, since.month) if month_only else when >= since.replace(
                    hour=0, minute=0, second=0, microsecond=0):
                out.append(url)
        elif lastmod is not None and lastmod.replace(tzinfo=None) >= since:
            out.append(url)
    return out
