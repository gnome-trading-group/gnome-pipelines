"""Team-game offensive efficiency observations built from play-by-play PPA.

One row per (game, offense). The defense's observation is the opponent's offensive row, so the
model sees each game twice: once per offense, with the opponent's identity attached.

Matches CFBD's /ppa/games (garbage time excluded) at corr 0.993 on 2019 and 2024; CFBD rounds to
2 dp and has no play counts, which the observation model needs for precision.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PASS_TYPES = {
    "Pass Reception", "Pass Incompletion", "Passing Touchdown", "Pass", "Pass Completion", "Sack",
    "Interception", "Pass Interception Return", "Interception Return Touchdown",
}
RUSH_TYPES = {"Rush", "Rushing Touchdown"}
# Below this the play-by-play is incomplete (e.g. 2 plays logged for a whole FCS game), not just a
# short game: even a 1st-quarter blowout keeps every 1st-quarter play.
MIN_PLAYS = 10


def garbage_time(period: pd.Series, lead: pd.Series) -> pd.Series:
    """Connelly's definition, which CFBD also uses: 2nd quarter > 38, 3rd > 28, 4th > 22."""
    abs_lead = lead.abs()
    return ((period == 2) & (abs_lead > 38)) | ((period == 3) & (abs_lead > 28)) | ((period == 4) & (abs_lead > 22))


def successful(down: pd.Series, distance: pd.Series, gained: pd.Series) -> pd.Series:
    """Standard success rate: 50% of the distance on 1st down, 70% on 2nd, all of it on 3rd/4th."""
    need = np.select([down == 1, down == 2], [0.5 * distance, 0.7 * distance], distance)
    return gained >= need


def team_game_efficiency(plays: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    x = plays[plays["ppa"].notna() & (plays["period"] <= 4)].copy()
    x = x[~garbage_time(x["period"], x["offense_score"] - x["defense_score"])]
    x["is_pass"] = x["play_type"].isin(PASS_TYPES)
    x["is_rush"] = x["play_type"].isin(RUSH_TYPES)
    scrimmage = x["is_pass"] | x["is_rush"]
    x["success"] = np.where(scrimmage, successful(x["down"], x["distance"], x["yards_gained"]), np.nan)
    x["pass_ppa"] = np.where(x["is_pass"], x["ppa"], np.nan)
    x["rush_ppa"] = np.where(x["is_rush"], x["ppa"], np.nan)
    x["success_ppa"] = np.where(x["success"] == 1, x["ppa"], np.nan)

    agg = x.groupby(["game_id", "offense"]).agg(
        plays=("ppa", "size"),
        off_ppa=("ppa", "mean"),
        off_ppa_sd=("ppa", "std"),
        pass_plays=("is_pass", "sum"),
        rush_plays=("is_rush", "sum"),
        off_pass_ppa=("pass_ppa", "mean"),
        off_rush_ppa=("rush_ppa", "mean"),
        success_rate=("success", "mean"),
        explosiveness=("success_ppa", "mean"),
    ).reset_index()

    g = games[["game_id", "season", "week", "season_type", "kickoff", "neutral_site", "home_team", "home_id",
               "away_team", "away_id", "home_classification", "away_classification"]]
    agg = agg.merge(g, on="game_id", how="inner")
    is_home = agg["offense"] == agg["home_team"]
    is_away = agg["offense"] == agg["away_team"]
    agg = agg[is_home | is_away].copy()
    is_home = is_home[agg.index]
    agg["team_id"] = np.where(is_home, agg["home_id"], agg["away_id"])
    agg["opponent_id"] = np.where(is_home, agg["away_id"], agg["home_id"])
    agg["team_classification"] = np.where(is_home, agg["home_classification"], agg["away_classification"])
    agg["opponent_classification"] = np.where(is_home, agg["away_classification"], agg["home_classification"])
    agg["venue"] = np.where(agg["neutral_site"], 0, np.where(is_home, 1, -1))
    agg["available_at"] = agg["kickoff"] + pd.Timedelta(days=1)
    agg["usable"] = agg["plays"] >= MIN_PLAYS
    cols = ["game_id", "season", "week", "season_type", "kickoff", "team_id", "opponent_id", "offense",
            "team_classification", "opponent_classification", "venue", "plays", "off_ppa", "off_ppa_sd",
            "pass_plays", "rush_plays", "off_pass_ppa", "off_rush_ppa", "success_rate", "explosiveness",
            "usable", "available_at"]
    return agg[cols].sort_values(["kickoff", "game_id", "team_id"]).reset_index(drop=True)
