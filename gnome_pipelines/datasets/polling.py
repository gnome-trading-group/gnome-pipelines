"""Declared Iceberg schemas shared by every polling source.

poll_health holds what the polling framework itself observes about a source's service: heartbeats, clock gaps,
task errors and metered spend. A source's own measurements (discovery lag, report lag) stay in its own tables.
"""
import pyarrow as pa
from gnomepy.data import TableSpec

TS = pa.timestamp("us", tz="UTC")

POLL_HEALTH = TableSpec(
    "poll_health",
    pa.schema([
        pa.field("health_id", pa.string(), nullable=False),
        pa.field("source", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("at", TS),
        pa.field("subject", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("detail", pa.string()),
    ]),
    key=("health_id",),
)

ALL = (POLL_HEALTH,)
