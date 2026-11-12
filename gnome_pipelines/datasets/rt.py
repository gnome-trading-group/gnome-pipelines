"""Declared Iceberg schemas for the rt_reviews polling source: films with RT markets, outlet review articles, their
fresh/rotten reads, and the live Tomatometer.

Every row is as observed: first_seen_at is when the source saw it (rt_reviews: when its read was ready, which is when a
strategy could first act on it), published_at the outlet's own time. A row is added only when something changed, so
rt_films and rt_scores are histories of a film's state.
"""
import pyarrow as pa
from gnomepy.data import TableSpec

TS = pa.timestamp("us", tz="UTC")
INTS = pa.list_(pa.int64())

# A film with an open Rotten Tomatoes market on any venue; a row per change in what the venues list.
RT_FILMS = TableSpec(
    "rt_films",
    pa.schema([
        pa.field("film_id", pa.string(), nullable=False),
        pa.field("title", pa.string()),
        pa.field("rt_slug", pa.string()),
        pa.field("close", TS),
        pa.field("open", pa.bool_()),
        pa.field("kalshi_event", pa.string()),
        pa.field("kalshi_strikes", INTS),
        pa.field("polymarket_event", pa.string()),
        pa.field("polymarket_us_event", pa.string()),
        # JSON: every venue's open strikes, [{venue, strike, yes, no}] with the registry's exchange security ids of
        # each strike's YES and NO sides.
        pa.field("markets", pa.string()),
        pa.field("first_seen_at", TS),
    ]),
    key=("film_id",),
)

# An outlet URL that looks like a review of an open film, once, from whichever feed listed it first.
RT_ARTICLES = TableSpec(
    "rt_articles",
    pa.schema([
        pa.field("url", pa.string(), nullable=False),
        pa.field("film_id", pa.string()),
        pa.field("outlet", pa.string()),
        pa.field("feed", pa.string()),
        pa.field("feed_published_at", TS),
        pa.field("first_seen_at", TS),
    ]),
    key=("url",),
)

# The read of an article: what the page says (time, headline, rating) and Claude's call.
RT_REVIEWS = TableSpec(
    "rt_reviews",
    pa.schema([
        pa.field("url", pa.string(), nullable=False),
        pa.field("film_id", pa.string()),
        pa.field("outlet", pa.string()),
        pa.field("published_at", TS),
        pa.field("article_seen_at", TS),
        pa.field("first_seen_at", TS),
        pa.field("headline", pa.string()),
        pa.field("rating_kind", pa.string()),
        pa.field("rating_value", pa.float64()),
        pa.field("rating_grade", pa.string()),
        pa.field("is_review", pa.bool_()),
        pa.field("fresh", pa.bool_()),
        pa.field("confidence", pa.float64()),
        pa.field("model", pa.string()),
        pa.field("archive_key", pa.string()),
        pa.field("backfill", pa.bool_()),
    ]),
    key=("url",),
)

# The film's public RT page, when its counts changed.
RT_SCORES = TableSpec(
    "rt_scores",
    pa.schema([
        pa.field("film_id", pa.string(), nullable=False),
        pa.field("rt_slug", pa.string()),
        pa.field("liked", pa.int64()),
        pa.field("not_liked", pa.int64()),
        pa.field("score", pa.float64()),
        pa.field("first_seen_at", TS),
        pa.field("archive_key", pa.string()),
    ]),
    key=("film_id",),
)

ALL = (RT_FILMS, RT_ARTICLES, RT_REVIEWS, RT_SCORES)
