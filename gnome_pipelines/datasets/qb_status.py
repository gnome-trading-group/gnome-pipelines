"""Declared Iceberg schemas for the QB Status Service.

Append-only like every dataset: evidence rows are never rewritten. first_seen_at is when we captured a claim,
published_at the source's own time when it has one, and estimated_release_at a labelled estimate used only for
backfilled rows.
"""
import pyarrow as pa
from gnomepy.data import TableSpec

TS = pa.timestamp("us", tz="UTC")
STRINGS = pa.list_(pa.string())

# Every claim about a quarterback from any source: official reports, play-by-play flags, market alarms, news, X, manual
EVIDENCE = TableSpec(
    "qb_status_evidence",
    pa.schema([
        pa.field("evidence_id", pa.string(), nullable=False),
        pa.field("source_type", pa.string()),
        pa.field("source_id", pa.string()),
        pa.field("source_org", pa.string()),
        pa.field("published_at", TS),
        pa.field("first_seen_at", TS),
        pa.field("backfilled", pa.bool_()),
        pa.field("estimated_release_at", TS),
        pa.field("team", pa.string()),
        pa.field("team_id", pa.int64()),
        pa.field("player_raw", pa.string()),
        pa.field("jersey", pa.string()),
        pa.field("player_id", pa.string()),
        pa.field("match_confidence", pa.float64()),
        pa.field("game_id", pa.int64()),
        pa.field("season", pa.int64()),
        pa.field("week", pa.int64()),
        pa.field("report_slot", pa.string()),
        pa.field("status", pa.string()),
        pa.field("hedge", pa.string()),
        pa.field("paraphrase", pa.string()),
        pa.field("url", pa.string()),
        pa.field("extractor", pa.string()),
        pa.field("extraction_confidence", pa.float64()),
        pa.field("review_state", pa.string()),
        pa.field("reviewer", pa.string()),
        pa.field("reviewed_at", TS),
    ]),
    key=("evidence_id",),
)

# Start probability per candidate QB for a team's next game, recomputed whenever its evidence changes
SNAPSHOTS = TableSpec(
    "qb_status_snapshots",
    pa.schema([
        pa.field("game_id", pa.int64(), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("player_id", pa.string(), nullable=False),
        pa.field("computed_at", TS),
        pa.field("p_start", pa.float64()),
        pa.field("official_status", pa.string()),
        pa.field("official_slot", pa.string()),
        pa.field("evidence_ids", STRINGS),
        pa.field("reason", pa.string()),
        pa.field("rule_version", pa.string()),
    ]),
    key=("game_id", "team_id", "player_id"),
)

# Teams under close watch (fast X and news polling) after a flag, a worrying official status or a market alarm
WATCHES = TableSpec(
    "qb_status_watches",
    pa.schema([
        pa.field("watch_id", pa.string(), nullable=False),
        pa.field("team_id", pa.int64()),
        pa.field("game_id", pa.int64()),
        pa.field("reason", pa.string()),
        pa.field("opened_at", TS),
        pa.field("expires_at", TS),
        pa.field("closed_at", TS),
        pa.field("close_reason", pa.string()),
    ]),
    key=("watch_id",),
)

# The service's own health: news and report lags, missed news, laptop sleep gaps, source errors, kickoff accuracy
HEALTH = TableSpec(
    "qb_status_health",
    pa.schema([
        pa.field("health_id", pa.string(), nullable=False),
        pa.field("kind", pa.string()),
        pa.field("at", TS),
        pa.field("team_id", pa.int64()),
        pa.field("game_id", pa.int64()),
        pa.field("value", pa.float64()),
        pa.field("detail", pa.string()),
    ]),
    key=("health_id",),
)

# When each conference's availability report is expected for a game, and when we first saw it
REPORT_SCHEDULE = TableSpec(
    "availability_report_schedule",
    pa.schema([
        pa.field("conference", pa.string(), nullable=False),
        pa.field("game_id", pa.int64(), nullable=False),
        pa.field("report_slot", pa.string(), nullable=False),
        pa.field("expected_at", TS),
        pa.field("observed_at", TS),
        pa.field("state", pa.string()),
    ]),
    key=("conference", "game_id", "report_slot"),
)

# Player crosswalk: roster ids, the play-by-play passer key and the names a player goes by in reports and posts
PLAYERS = TableSpec(
    "cfb_players",
    pa.schema([
        pa.field("season", pa.int64(), nullable=False),
        pa.field("player_id", pa.string(), nullable=False),
        pa.field("team_id", pa.int64()),
        pa.field("team", pa.string()),
        pa.field("first_name", pa.string()),
        pa.field("last_name", pa.string()),
        pa.field("jersey", pa.string()),
        pa.field("position", pa.string()),
        pa.field("passer_key", pa.string()),
        pa.field("aliases", STRINGS),
    ]),
    key=("season", "player_id"),
)

# Reporters and outlets the service reads, with the credibility each has earned
SOURCES = TableSpec(
    "qb_status_sources",
    pa.schema([
        pa.field("platform", pa.string(), nullable=False),
        pa.field("handle", pa.string(), nullable=False),
        pa.field("teams", STRINGS),
        pa.field("kind", pa.string()),
        pa.field("added_at", TS),
        pa.field("vetted_by", pa.string()),
        pa.field("credibility", pa.float64()),
        pa.field("n_scored", pa.int64()),
    ]),
    key=("platform", "handle"),
)

ALL = (EVIDENCE, SNAPSHOTS, WATCHES, HEALTH, REPORT_SCHEDULE, PLAYERS, SOURCES)
