"""Declared Iceberg schemas for the FACEIT datasets the CS2 e12 model fits on.

faceit_matches, faceit_match_history and faceit_rosters are organised 5v5 championship series (leagues, tour
qualifiers, ESEA seasons): one row per series, per map, and per rostered player. Match ids are FACEIT's strings
("1-<uuid>"); a side's HLTV team (faction1_hltv_team, team_a_id) is the team at least three of its linked players
played for on HLTV that quarter. faceit_history_matches and faceit_history_players are every FACEIT game (matchmaking,
hubs, championships) of the players in cs2_hltv_faceit_map, results only. faceit_hltv_series_link pairs a FACEIT series
with the HLTV series it duplicates, one to one; faceit_exclusions lists the series and maps the model must drop, with
map_name "" for a whole series (key columns can't be null).
"""
import pyarrow as pa
from gnomepy.data import TableSpec

HLTV_FACEIT_MAP = TableSpec(
    "cs2_hltv_faceit_map",
    pa.schema([
        pa.field("faceit_id", pa.string(), nullable=False),
        pa.field("hltv_id", pa.int64(), nullable=False),
        pa.field("method", pa.string()),
    ]),
    key=("faceit_id",),
)

MATCHES = TableSpec(
    "faceit_matches",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("competition_id", pa.string()),
        pa.field("competition_name", pa.string()),
        pa.field("organizer_id", pa.string()),
        pa.field("region", pa.string()),
        pa.field("best_of", pa.int64()),
        pa.field("round", pa.int64()),
        pa.field("group", pa.int64()),
        pa.field("scheduled_at", pa.int64()),
        pa.field("started_at", pa.int64()),
        pa.field("finished_at", pa.int64()),
        pa.field("winner", pa.string()),
        pa.field("faction1_id", pa.string()),
        pa.field("faction1_name", pa.string()),
        pa.field("faction2_id", pa.string()),
        pa.field("faction2_name", pa.string()),
        pa.field("faction1_hltv_team", pa.int64()),
        pa.field("faction2_hltv_team", pa.int64()),
        pa.field("n_maps_with_stats", pa.int64()),
        pa.field("has_stats", pa.bool_()),
    ]),
    key=("match_id",),
)

MATCH_HISTORY = TableSpec(
    "faceit_match_history",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("map_position_in_series", pa.int64(), nullable=False),
        pa.field("map_name", pa.string()),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("match_time", pa.timestamp("us", tz="UTC")),
        pa.field("event_name", pa.string()),
        pa.field("bo_type", pa.int64()),
        pa.field("team_a_id", pa.int64()),
        pa.field("team_a_name", pa.string()),
        pa.field("team_a_faceit_id", pa.string()),
        pa.field("team_b_id", pa.int64()),
        pa.field("team_b_name", pa.string()),
        pa.field("team_b_faceit_id", pa.string()),
        pa.field("team_a_score", pa.int64()),
        pa.field("team_b_score", pa.int64()),
        pa.field("team_a_won", pa.int64()),
        pa.field("team_a_ot_score", pa.float64()),
        pa.field("team_b_ot_score", pa.float64()),
        pa.field("team_a_player_ids", pa.list_(pa.int64())),
        pa.field("team_b_player_ids", pa.list_(pa.int64())),
        pa.field("team_a_faceit_ids", pa.list_(pa.string())),
        pa.field("team_b_faceit_ids", pa.list_(pa.string())),
        pa.field("team_a_players", pa.int64()),
        pa.field("team_b_players", pa.int64()),
    ]),
    key=("match_id", "map_position_in_series"),
    partition_by=("match_date", "month"),
)

ROSTERS = TableSpec(
    "faceit_rosters",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("faceit_id", pa.string(), nullable=False),
        pa.field("faction", pa.string()),
        pa.field("role", pa.string()),
        pa.field("nickname", pa.string()),
        pa.field("skill_level", pa.int64()),
        pa.field("played", pa.bool_()),
        pa.field("hltv_id", pa.int64()),
    ]),
    key=("match_id", "faceit_id"),
)

SERIES_LINK = TableSpec(
    "faceit_hltv_series_link",
    pa.schema([
        pa.field("faceit_match_id", pa.string(), nullable=False),
        pa.field("hltv_match_id", pa.int64()),
        pa.field("swapped", pa.bool_()),
        pa.field("quality", pa.string()),
        pa.field("maps_same_score", pa.int64()),
        pa.field("lineup_overlap", pa.int64()),
        pa.field("hours_apart", pa.float64()),
    ]),
    key=("faceit_match_id",),
)

EXCLUSIONS = TableSpec(
    "faceit_exclusions",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
    ]),
    key=("match_id", "map_name", "reason"),
)

HISTORY_MATCHES = TableSpec(
    "faceit_history_matches",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("competition_type", pa.string()),
        pa.field("competition_id", pa.string()),
        pa.field("competition_name", pa.string()),
        pa.field("game_mode", pa.string()),
        pa.field("teams_size", pa.int64()),
        pa.field("started_at", pa.int64()),
        pa.field("finished_at", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("winner", pa.string()),
    ]),
    key=("match_id",),
)

HISTORY_PLAYERS = TableSpec(
    "faceit_history_players",
    pa.schema([
        pa.field("match_id", pa.string(), nullable=False),
        pa.field("faceit_id", pa.string(), nullable=False),
        pa.field("faction", pa.string()),
        pa.field("skill_level", pa.int64()),
    ]),
    key=("match_id", "faceit_id"),
)

ALL = (HLTV_FACEIT_MAP, MATCHES, MATCH_HISTORY, ROSTERS, SERIES_LINK, EXCLUSIONS, HISTORY_MATCHES, HISTORY_PLAYERS)
