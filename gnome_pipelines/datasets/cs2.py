"""Declared Iceberg schemas for the CS2 datasets.

Types mirror the frames the pipelines produce today, except id and rank columns,
which are nullable int64: producers still hand over float columns with NaN, and
pyarrow writes NaN as null and rejects any non-integral value.

cs2_round_features rows are game-state snapshots at demo ticks (freeze end, each
kill, the plant). tick restarts per demo file and a plant and a kill can share a
tick, so the key needs source_demo and bomb_planted too. Rows parsed before the
parser kept tick are not migrated; the table starts empty and is re-backfilled.

Key columns can't be null. cs2_h2h_history has series-level results with no map;
the producer writes map_name "" for those.
"""
import pyarrow as pa
from gnomepy.data import TableSpec

MATCH_HISTORY = TableSpec(
    "cs2_match_history",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("event_name", pa.string()),
        pa.field("event_tier", pa.int64()),
        pa.field("format", pa.string()),
        pa.field("bo_type", pa.int64()),
        pa.field("is_lan", pa.int64()),
        pa.field("team_a_name", pa.string()),
        pa.field("team_a_id", pa.int64()),
        pa.field("team_b_name", pa.string()),
        pa.field("team_b_id", pa.int64()),
        pa.field("team_a_rank", pa.int64()),
        pa.field("team_b_rank", pa.int64()),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("map_position_in_series", pa.int64()),
        pa.field("is_decider", pa.float64()),
        pa.field("team_a_series_score", pa.int64()),
        pa.field("team_b_series_score", pa.int64()),
        pa.field("team_a_score", pa.int64()),
        pa.field("team_b_score", pa.int64()),
        pa.field("team_a_won", pa.int64()),
        pa.field("team_a_picked_map", pa.float64()),
        pa.field("team_a_h1_score", pa.float64()),
        pa.field("team_b_h1_score", pa.float64()),
        pa.field("team_a_started_ct", pa.float64()),
        pa.field("team_a_player_ids", pa.list_(pa.int64())),
        pa.field("team_a_player_names", pa.list_(pa.string())),
        pa.field("team_b_player_ids", pa.list_(pa.int64())),
        pa.field("team_b_player_names", pa.list_(pa.string())),
        pa.field("team_a_avg_rating", pa.float64()),
        pa.field("team_b_avg_rating", pa.float64()),
        pa.field("team_a_vrs_rank", pa.int64()),
        pa.field("team_b_vrs_rank", pa.int64()),
        pa.field("match_time", pa.timestamp("us", tz="UTC")),
        pa.field("mapstatsid", pa.string()),
        pa.field("team_a_lineup_ids", pa.list_(pa.int64())),
        pa.field("team_b_lineup_ids", pa.list_(pa.int64())),
        pa.field("h2h_team_a_wins", pa.float64()),
        pa.field("h2h_team_b_wins", pa.float64()),
        pa.field("h2h_overtimes", pa.float64()),
        pa.field("stage_text", pa.string()),
        pa.field("is_elimination", pa.float64()),
        pa.field("is_qualifier", pa.float64()),
        pa.field("is_playoff", pa.float64()),
        pa.field("is_group", pa.float64()),
        pa.field("team_a_awp_id", pa.int64()),
        pa.field("team_b_awp_id", pa.int64()),
    ]),
    key=("match_id", "map_name"),
    partition_by=("match_date", "month"),
)

PLAYER_MAP_STATS = TableSpec(
    "cs2_player_map_stats",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("mapstatsid", pa.string()),
        pa.field("is_team_a", pa.bool_()),
        pa.field("team_name", pa.string()),
        pa.field("team_id", pa.int64()),
        pa.field("player_id", pa.int64(), nullable=False),
        pa.field("player_name", pa.string()),
        pa.field("side", pa.string(), nullable=False),
        pa.field("kills", pa.float64()),
        pa.field("deaths", pa.float64()),
        pa.field("ek", pa.float64()),
        pa.field("ed", pa.float64()),
        pa.field("round_swing_pct", pa.float64()),
        pa.field("adr", pa.float64()),
        pa.field("eadr", pa.float64()),
        pa.field("kast", pa.float64()),
        pa.field("ekast", pa.float64()),
        pa.field("rating", pa.float64()),
    ]),
    key=("match_id", "map_name", "player_id", "side"),
    partition_by=("match_date", "month"),
)

MATCH_VETO = TableSpec(
    "cs2_match_veto",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("order", pa.int64(), nullable=False),
        pa.field("team_name", pa.string()),
        pa.field("team_id", pa.int64()),
        pa.field("action", pa.string()),
        pa.field("map_name", pa.string()),
    ]),
    key=("match_id", "order"),
)

# One row per player in a match's lineup box, which HLTV renders before the match (so it is point-in-time).
# is_standin is HLTV's "Substitute" label: a player not on the team's registered roster.
MATCH_LINEUPS = TableSpec(
    "cs2_match_lineups",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("team_name", pa.string()),
        pa.field("player_id", pa.int64(), nullable=False),
        pa.field("player_name", pa.string()),
        pa.field("slot", pa.int64()),
        pa.field("is_standin", pa.bool_()),
        pa.field("is_awp", pa.bool_()),
        pa.field("is_igl", pa.bool_()),
    ]),
    key=("match_id", "team_id", "player_id"),
    partition_by=("match_date", "month"),
)

H2H_HISTORY = TableSpec(
    "cs2_h2h_history",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("h2h_date", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("team1_name", pa.string()),
        pa.field("team2_name", pa.string()),
        pa.field("team1_won", pa.bool_()),
        pa.field("event_name", pa.string()),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("team1_score", pa.int64()),
        pa.field("team2_score", pa.int64()),
    ]),
    key=("match_id", "h2h_date", "map_name"),
)

TEAM_RECENT_FORM = TableSpec(
    "cs2_team_recent_form",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("team_index", pa.int64(), nullable=False),
        pa.field("position", pa.int64(), nullable=False),
        pa.field("opponent_team_id", pa.int64()),
        pa.field("opponent_name", pa.string()),
        pa.field("ref_match_id", pa.int64()),
        pa.field("weeks_ago", pa.float64()),
        pa.field("score_for", pa.int64()),
        pa.field("score_against", pa.int64()),
    ]),
    key=("match_id", "team_index", "position"),
    partition_by=("match_date", "month"),
)

ROUND_FEATURES = TableSpec(
    "cs2_round_features",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("round_number", pa.int64()),
        pa.field("ct_equip_value", pa.float64()),
        pa.field("t_equip_value", pa.float64()),
        pa.field("ct_money", pa.float64()),
        pa.field("t_money", pa.float64()),
        pa.field("ct_hp", pa.float64()),
        pa.field("t_hp", pa.float64()),
        pa.field("ct_alive", pa.int64()),
        pa.field("t_alive", pa.int64()),
        pa.field("bomb_planted", pa.int64(), nullable=False),
        pa.field("bomb_time_remaining", pa.float64()),
        pa.field("ct_consecutive_losses", pa.int64()),
        pa.field("t_consecutive_losses", pa.int64()),
        pa.field("ct_win_rate_last_5", pa.float64()),
        pa.field("ct_score", pa.int64()),
        pa.field("t_score", pa.int64()),
        pa.field("current_half", pa.int64()),
        pa.field("ct_won", pa.int64()),
        pa.field("source_demo", pa.string(), nullable=False),
        pa.field("tick", pa.int64(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("ct_armor", pa.float64()),
        pa.field("t_armor", pa.float64()),
        pa.field("ct_armored", pa.float64()),
        pa.field("t_armored", pa.float64()),
        pa.field("ct_round_start_equip", pa.float64()),
        pa.field("t_round_start_equip", pa.float64()),
        pa.field("ct_alive_equip", pa.float64()),
        pa.field("t_alive_equip", pa.float64()),
        pa.field("ot_offset", pa.int64()),
        pa.field("team_a_is_ct", pa.bool_()),
        pa.field("team_a_won_round", pa.int64()),
        pa.field("team_a_score_before", pa.int64()),
        pa.field("team_b_score_before", pa.int64()),
    ]),
    key=("match_id", "map_name", "source_demo", "tick", "bomb_planted"),
    partition_by=("match_date", "month"),
)

# HLTV roster moves (hltv_rosters): one row per dated status transition of a player or coach between two sides,
# e.g. Falcons(active) -> Falcons(benched), or Natus Vincere(benched) -> MOUZ(active) for a loan. Team 0 is
# "No team". event_date is HLTV's date; read the table as of a time for the moves we had seen by then.
ROSTER_MOVES = TableSpec(
    "cs2_roster_moves",
    pa.schema([
        pa.field("event_date", pa.timestamp("us"), nullable=False),
        pa.field("person_id", pa.int64(), nullable=False),
        pa.field("person_name", pa.string()),
        pa.field("person_role", pa.string()),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("from_team_id", pa.int64(), nullable=False),
        pa.field("from_team_name", pa.string()),
        pa.field("from_status", pa.string()),
        pa.field("to_team_id", pa.int64(), nullable=False),
        pa.field("to_team_name", pa.string()),
        pa.field("to_status", pa.string()),
        pa.field("text", pa.string()),
    ]),
    key=("person_id", "event_date", "from_team_id", "to_team_id", "kind"),
)

# A team page's players table and coach as they stood on the day it was fetched (the page shows nothing older).
TEAM_ROSTER_SNAPSHOTS = TableSpec(
    "cs2_team_roster_snapshots",
    pa.schema([
        pa.field("snapshot_date", pa.timestamp("us"), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("person_id", pa.int64(), nullable=False),
        pa.field("person_name", pa.string()),
        pa.field("role", pa.string(), nullable=False),
        pa.field("status", pa.string()),
        pa.field("time_on_team", pa.string()),
        pa.field("maps_played", pa.int64()),
    ]),
    key=("team_id", "snapshot_date", "person_id", "role"),
)

TEAM_RANKINGS = TableSpec(
    "cs2_team_rankings",
    pa.schema([
        pa.field("date", pa.timestamp("us"), nullable=False),
        pa.field("team_name", pa.string()),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("rank", pa.int64()),
        pa.field("points", pa.int64()),
        pa.field("change", pa.int64()),
    ]),
    key=("date", "team_id"),
)

MATCH_PRIORS = TableSpec(
    "cs2_match_priors",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("map_name", pa.string(), nullable=False),
        pa.field("match_date", pa.timestamp("us")),
        pa.field("team_a_name", pa.string()),
        pa.field("team_b_name", pa.string()),
        pa.field("team_a_id", pa.int64()),
        pa.field("team_b_id", pa.int64()),
        pa.field("team_a_rating_diff", pa.float64()),
        pa.field("team_a_rank", pa.int64()),
        pa.field("team_b_rank", pa.int64()),
        pa.field("rank_diff", pa.float64()),
        pa.field("ranking_age_days", pa.float64()),
        pa.field("team_a_map_winrate_long", pa.float64()),
        pa.field("team_b_map_winrate_long", pa.float64()),
        pa.field("team_a_map_winrate_short", pa.float64()),
        pa.field("team_b_map_winrate_short", pa.float64()),
        pa.field("team_a_overall_winrate", pa.float64()),
        pa.field("team_b_overall_winrate", pa.float64()),
        pa.field("h2h_win_rate", pa.float64()),
        pa.field("team_a_recent_form", pa.float64()),
        pa.field("team_b_recent_form", pa.float64()),
        pa.field("recent_form_diff", pa.float64()),
        pa.field("team_a_picked_map", pa.float64()),
        pa.field("is_lan", pa.float64()),
        pa.field("is_decider", pa.float64()),
        pa.field("bo_type", pa.float64()),
        pa.field("map_position_in_series", pa.float64()),
        pa.field("event_tier", pa.float64()),
        pa.field("team_a_series_score", pa.float64()),
        pa.field("team_b_series_score", pa.float64()),
        pa.field("elo_a", pa.float64()),
        pa.field("elo_b", pa.float64()),
        pa.field("elo_diff", pa.float64()),
        pa.field("elo_map_diff", pa.float64()),
        pa.field("elo_map_resid_diff", pa.float64()),
        pa.field("elo_side_asym", pa.float64()),
        pa.field("elo_games_min", pa.float64()),
        pa.field("team_a_form_rating", pa.float64()),
        pa.field("team_a_form_worst", pa.float64()),
        pa.field("team_a_form_spread", pa.float64()),
        pa.field("team_a_form_swing", pa.float64()),
        pa.field("team_a_form_adr_resid", pa.float64()),
        pa.field("team_a_form_kast_resid", pa.float64()),
        pa.field("team_a_form_ct", pa.float64()),
        pa.field("team_a_form_t", pa.float64()),
        pa.field("team_a_form_maps_min", pa.float64()),
        pa.field("team_a_form_known", pa.float64()),
        pa.field("team_b_form_rating", pa.float64()),
        pa.field("team_b_form_worst", pa.float64()),
        pa.field("team_b_form_spread", pa.float64()),
        pa.field("team_b_form_swing", pa.float64()),
        pa.field("team_b_form_adr_resid", pa.float64()),
        pa.field("team_b_form_kast_resid", pa.float64()),
        pa.field("team_b_form_ct", pa.float64()),
        pa.field("team_b_form_t", pa.float64()),
        pa.field("team_b_form_maps_min", pa.float64()),
        pa.field("team_b_form_known", pa.float64()),
        pa.field("form_rating_diff", pa.float64()),
        pa.field("form_worst_diff", pa.float64()),
        pa.field("form_swing_diff", pa.float64()),
        pa.field("form_adr_resid_diff", pa.float64()),
        pa.field("form_kast_resid_diff", pa.float64()),
        pa.field("form_ct_diff", pa.float64()),
        pa.field("form_t_diff", pa.float64()),
        pa.field("form_maps_min", pa.float64()),
        pa.field("form_known_min", pa.float64()),
        pa.field("team_a_rest_days", pa.float64()),
        pa.field("team_b_rest_days", pa.float64()),
        pa.field("rest_days_diff", pa.float64()),
        pa.field("team_a_maps_14d", pa.float64()),
        pa.field("team_b_maps_14d", pa.float64()),
        pa.field("maps_14d_diff", pa.float64()),
        pa.field("own_h2h_maps", pa.float64()),
        pa.field("own_h2h_rate", pa.float64()),
        pa.field("own_h2h_never_met", pa.float64()),
        pa.field("team_a_rank_best", pa.float64()),
        pa.field("team_a_log_rank", pa.float64()),
        pa.field("team_b_rank_best", pa.float64()),
        pa.field("team_b_log_rank", pa.float64()),
        pa.field("rank_best_diff", pa.float64()),
        pa.field("log_rank_diff", pa.float64()),
        pa.field("rank_known_both", pa.float64()),
        pa.field("rank_source_disagreement", pa.float64()),
        pa.field("is_elimination", pa.float64()),
        pa.field("is_qualifier", pa.float64()),
        pa.field("is_playoff", pa.float64()),
        pa.field("is_group", pa.float64()),
        pa.field("team_a_picked_map_exact", pa.float64()),
        pa.field("map_pick_index", pa.float64()),
        pa.field("team_a_banned_first", pa.float64()),
        pa.field("team_a_bans_before_pick", pa.float64()),
        pa.field("team_b_bans_before_pick", pa.float64()),
        pa.field("veto_known", pa.float64()),
        pa.field("page_h2h_maps", pa.float64()),
        pa.field("page_h2h_rate", pa.float64()),
        pa.field("page_h2h_days_since", pa.float64()),
        pa.field("page_h2h_recent_rate", pa.float64()),
        pa.field("page_h2h_never_met", pa.float64()),
    ]),
    key=("match_id", "map_name"),
    partition_by=("match_date", "month"),
)


# The cs2_upcoming pipeline's tables. Each run appends; the newest row per match is its state.
# status is upcoming, live, or missing (gone from HLTV's
# listing for MISSING_AFTER consecutive successful listings: postponed, cancelled or finished).
UPCOMING_MATCHES = TableSpec(
    "cs2_upcoming_matches",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("team_a_id", pa.int64()),
        pa.field("team_b_id", pa.int64()),
        pa.field("team_a_name", pa.string()),
        pa.field("team_b_name", pa.string()),
        pa.field("kickoff", pa.timestamp("us", tz="UTC")),
        pa.field("stars", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("missed_listings", pa.int64()),
        pa.field("features_at", pa.timestamp("us", tz="UTC")),
        # JSON: the tradable legs on every venue, [{venue, market, listing_team_a, listing_team_b}].
        pa.field("markets", pa.string()),
        # JSON: per venue, the matched series market, team A's outcome, team A's instrument per
        # market, and the aliases the match taught (with whether settlement has judged them).
        pa.field("venues", pa.string()),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]),
    key=("match_id", "generated_at"),
    partition_by=("generated_at", "day"),
)

# One feature row per match page fetched: what the strategies price from, as of generated_at.
UPCOMING_FEATURES = TableSpec(
    "cs2_upcoming_features",
    pa.schema([
        *MATCH_PRIORS.schema,
        pa.field("team_a_lineup_ids", pa.list_(pa.int64())),
        pa.field("team_b_lineup_ids", pa.list_(pa.int64())),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]),
    key=("match_id", "map_name", "generated_at"),
    partition_by=("generated_at", "day"),
)

# What each run spent, so the daily ZenRows budget can be enforced across runs.
UPCOMING_RUNS = TableSpec(
    "cs2_upcoming_runs",
    pa.schema([
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("listing_ok", pa.bool_()),
        pa.field("pages_fetched", pa.int64()),
        pa.field("credits", pa.int64()),
        pa.field("over_budget", pa.bool_()),
        pa.field("tracked", pa.int64()),
    ]),
    key=("generated_at",),
    partition_by=("generated_at", "day"),
)

# Every venue's top of book per tracked match and market each run, in team A's terms, registered
# or not: what trading actually costs from T-12h to kickoff, which the backtest could only assume.
UPCOMING_BOOKS = TableSpec(
    "cs2_upcoming_books",
    pa.schema([
        pa.field("match_id", pa.int64(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("instrument", pa.string()),
        pa.field("best_bid", pa.float64()),
        pa.field("best_ask", pa.float64()),
        pa.field("bid_size", pa.float64()),
        pa.field("ask_size", pa.float64()),
        pa.field("bid_size_5c", pa.float64()),
        pa.field("ask_size_5c", pa.float64()),
        pa.field("book_at", pa.timestamp("us", tz="UTC")),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]),
    key=("match_id", "venue", "market", "generated_at"),
    partition_by=("generated_at", "day"),
)

TEAM_ALIASES = TableSpec(
    "cs2_team_aliases",
    pa.schema([
        pa.field("source", pa.string(), nullable=False),
        pa.field("source_name_norm", pa.string(), nullable=False),
        pa.field("hltv_team_id", pa.int64(), nullable=False),
        pa.field("status", pa.string()),
        pa.field("evidence_count", pa.int64()),
        pa.field("first_seen", pa.timestamp("us", tz="UTC")),
        pa.field("last_seen", pa.timestamp("us", tz="UTC")),
    ]),
    key=("source", "source_name_norm", "hltv_team_id"),
)

ALL = (MATCH_HISTORY, PLAYER_MAP_STATS, MATCH_VETO, MATCH_LINEUPS, ROSTER_MOVES, TEAM_ROSTER_SNAPSHOTS, H2H_HISTORY, TEAM_RECENT_FORM, ROUND_FEATURES, TEAM_RANKINGS,
       MATCH_PRIORS, UPCOMING_MATCHES, UPCOMING_FEATURES, UPCOMING_RUNS, UPCOMING_BOOKS, TEAM_ALIASES)
