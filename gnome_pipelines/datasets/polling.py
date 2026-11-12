"""Declared Iceberg schemas shared by every polling source.

poll_health holds what the polling framework itself observes about a source's service: heartbeats, clock gaps,
task errors and metered spend. fetched_pages logs every page a source fetched (polling/fetch.py) that came back
changed or failed. Both grow quickly, so their keys lead with the time and they are partitioned by day: a publish
compares new rows against stored ones by the key's first column, and new times fall outside every older file.
"""
import pyarrow as pa
from gnomepy.data import TableSpec

TS = pa.timestamp("us", tz="UTC")

POLL_HEALTH = TableSpec(
    "poll_health",
    pa.schema([
        pa.field("at", TS, nullable=False),
        pa.field("health_id", pa.string(), nullable=False),
        pa.field("source", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("subject", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("detail", pa.string()),
    ]),
    key=("at", "health_id"),
    partition_by=("at", "day"),
)

FETCHED_PAGES = TableSpec(
    "fetched_pages",
    pa.schema([
        pa.field("fetched_at", TS, nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("source", pa.string()),
        pa.field("via", pa.string()),
        pa.field("status", pa.int64()),
        pa.field("error", pa.string()),
        pa.field("content_hash", pa.string()),
        pa.field("bytes", pa.int64()),
        pa.field("etag", pa.string()),
        pa.field("last_modified", pa.string()),
        pa.field("archive_key", pa.string()),
        pa.field("elapsed_ms", pa.float64()),
    ]),
    key=("fetched_at", "url"),
    partition_by=("fetched_at", "day"),
)

ALL = (POLL_HEALTH, FETCHED_PAGES)
