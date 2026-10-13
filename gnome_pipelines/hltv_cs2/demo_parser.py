"""
CS2 demo parser — reads local .dem files and emits per-round snapshot DataFrames.

Demo files are downloaded locally via backfill.py using nodriver.

Usage:
    python -m gnome_pipelines.hltv_cs2.demo_parser \\
        --demos-dir /tmp/demos \\
        --out /tmp/cs2_round_features.parquet
"""
from __future__ import annotations

import argparse
import glob
import logging
import os
import re

import pandas as pd
from demoparser2 import DemoParser

from gnomepy_research.hltv_cs2.config import MAP_POOL

logger = logging.getLogger(__name__)

_PLAYER_PROPS = [
    "health",
    "armor_value",
    "team_name",
    "balance",
    "current_equip_value",
    "round_start_equip_value",
]

_BOMB_FUSE_SECONDS = 40.0
_DEMO_TICK_RATE_FALLBACK = 64.0


def _detect_tick_rate(parser: DemoParser, freeze_ticks_df: pd.DataFrame) -> float:
    """Derive tick rate from round_freeze_end timing. Falls back to 64 if computation fails."""
    try:
        ticks = freeze_ticks_df["tick"].tolist()
        t0, t1 = int(ticks[0]), int(ticks[1])
        tdf = parser.parse_ticks(
            wanted_props=["CCSGameRulesProxy.CCSGameRules.m_fRoundStartTime"],
            ticks=[t0, t1],
        )
        rst0 = float(tdf[tdf["tick"] == t0].iloc[0]["CCSGameRulesProxy.CCSGameRules.m_fRoundStartTime"])
        rst1 = float(tdf[tdf["tick"] == t1].iloc[0]["CCSGameRulesProxy.CCSGameRules.m_fRoundStartTime"])
        delta_time = rst1 - rst0
        if delta_time <= 0:
            return _DEMO_TICK_RATE_FALLBACK
        return (t1 - t0) / delta_time
    except Exception:
        return _DEMO_TICK_RATE_FALLBACK


def _extract_match_id(dem_path: str) -> int | None:
    """Try to extract HLTV match ID from demo filename (e.g. '12345-team-vs-team.dem')."""
    name = os.path.basename(dem_path)
    m = re.match(r"^(\d+)", name)
    return int(m.group(1)) if m else None


def _parse_single_demo(dem_path: str) -> list[dict]:
    """
    Parse one .dem file and return raw row dicts without running scores.

    Round mapping: total_rounds_played (trp) on freeze_end/kill/plant events counts rounds
    completed BEFORE the current round. round_end(trp=N) ends round N. So freeze_end(trp=N)
    is the start of round N+1 — we use round_num = trp + 1 throughout.
    """
    try:
        parser = DemoParser(dem_path)

        header = parser.parse_header()
        map_name = header.get("map_name", "unknown")
        if map_name not in MAP_POOL:
            return []

        match_id = _extract_match_id(dem_path)

        rounds_df = parser.parse_event("round_end", other=["total_rounds_played"])
        freeze_ticks_df = parser.parse_event("round_freeze_end", other=["tick", "total_rounds_played"])
        kill_ticks_df = parser.parse_event("player_death", other=["tick", "total_rounds_played", "attacker_side", "user_side"])
        plant_ticks_df = parser.parse_event("bomb_planted", other=["tick", "total_rounds_played"])

        if freeze_ticks_df is None or len(freeze_ticks_df) < 2:
            return []

        tick_rate = _detect_tick_rate(parser, freeze_ticks_df)

        # round_end(trp=N) ends round N → round_winner[N] = winner of round N
        round_winner: dict[int, int] = {}
        for i, r in rounds_df.iterrows():
            winner = r.get("winner")
            if winner is None:
                continue
            rn = int(r.get("total_rounds_played", i + 1))
            round_winner[rn] = 1 if winner == "CT" else 0

        # freeze_end/kill/plant events during round N have trp=N-1 → round_num = trp + 1
        plant_tick_by_round: dict[int, int] = {}
        if plant_ticks_df is not None and len(plant_ticks_df) > 0:
            for _, r in plant_ticks_df.iterrows():
                rn = int(r.get("total_rounds_played", 0)) + 1
                plant_tick_by_round[rn] = int(r["tick"])

        snapshot_specs: list[tuple[int, int, int, float]] = []

        for _, r in freeze_ticks_df.iterrows():
            rn = int(r.get("total_rounds_played", 0)) + 1
            snapshot_specs.append((rn, int(r["tick"]), 0, 0.0))

        if kill_ticks_df is not None:
            for _, r in kill_ticks_df.iterrows():
                rn = int(r.get("total_rounds_played", 0)) + 1
                tick = int(r["tick"])
                plant_tick = plant_tick_by_round.get(rn)
                if plant_tick and tick > plant_tick:
                    elapsed = (tick - plant_tick) / tick_rate
                    bomb_time = max(0.0, _BOMB_FUSE_SECONDS - elapsed)
                    snapshot_specs.append((rn, tick, 1, bomb_time))
                else:
                    snapshot_specs.append((rn, tick, 0, 0.0))

        if plant_ticks_df is not None:
            for _, r in plant_ticks_df.iterrows():
                rn = int(r.get("total_rounds_played", 0)) + 1
                snapshot_specs.append((rn, int(r["tick"]), 1, _BOMB_FUSE_SECONDS))

        if not snapshot_specs:
            return []

        all_ticks = list({tick for _, tick, _, _ in snapshot_specs})
        ticks_df = parser.parse_ticks(wanted_props=_PLAYER_PROPS, ticks=all_ticks)
        if ticks_df is None or len(ticks_df) == 0:
            return []

        rows = []
        for round_num, tick, bomb_planted, bomb_time in snapshot_specs:
            if round_num not in round_winner:
                continue

            round_ticks = ticks_df[ticks_df["tick"] == tick]
            if len(round_ticks) == 0:
                continue

            ct_players = round_ticks[round_ticks["team_name"] == "CT"]
            t_players = round_ticks[round_ticks["team_name"] == "TERRORIST"]
            if len(ct_players) == 0 or len(t_players) == 0:
                continue
            if (ct_players["health"] > 0).sum() == 0 or (t_players["health"] > 0).sum() == 0:
                continue

            half = 1 if round_num <= 12 else (2 if round_num <= 24 else 3)
            rows.append({
                "match_id": match_id,
                "map_name": map_name,
                "round_number": round_num,
                "tick": tick,
                "ct_equip_value": ct_players["current_equip_value"].sum(),
                "t_equip_value": t_players["current_equip_value"].sum(),
                "ct_money": ct_players["balance"].sum(),
                "t_money": t_players["balance"].sum(),
                "ct_hp": ct_players[ct_players["health"] > 0]["health"].sum(),
                "t_hp": t_players[t_players["health"] > 0]["health"].sum(),
                "ct_alive": (ct_players["health"] > 0).sum(),
                "t_alive": (t_players["health"] > 0).sum(),
                "ct_alive_equip": ct_players[ct_players["health"] > 0]["current_equip_value"].sum(),
                "t_alive_equip": t_players[t_players["health"] > 0]["current_equip_value"].sum(),
                "ct_armor": ct_players["armor_value"].sum(),
                "t_armor": t_players["armor_value"].sum(),
                "ct_armored": (ct_players["armor_value"] > 0).sum(),
                "t_armored": (t_players["armor_value"] > 0).sum(),
                "ct_round_start_equip": ct_players["round_start_equip_value"].sum(),
                "t_round_start_equip": t_players["round_start_equip_value"].sum(),
                "bomb_planted": bomb_planted,
                "bomb_time_remaining": bomb_time,
                "ct_consecutive_losses": 0,
                "t_consecutive_losses": 0,
                "ct_win_rate_last_5": 0.5,
                "ct_score": 0,
                "t_score": 0,
                "current_half": half,
                "ct_won": round_winner[round_num],
                "source_demo": os.path.basename(dem_path),
            })

        return rows

    except Exception as exc:
        logger.warning("Failed to parse %s: %s", dem_path, exc)
        return []


def parse_demo(dem_paths: str | list[str]) -> pd.DataFrame | None:
    """
    Parse one or more .dem files (multi-part demos) and return per-round snapshot rows.

    Pass a list of paths for all parts of the same map — running scores are computed
    once across all combined parts so loss streaks and scores carry correctly.
    """
    if isinstance(dem_paths, str):
        dem_paths = [dem_paths]

    all_rows: list[dict] = []
    for path in dem_paths:
        rows = _parse_single_demo(path)
        all_rows.extend(rows)

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    # tick restarts in each part of a multi-part demo, but round_number keeps counting across them.
    df = df.sort_values(["round_number", "tick", "bomb_planted"], kind="stable").reset_index(drop=True)
    df = _propagate_round_start_equip(df)
    df = _compute_running_scores(df)
    return df


def _propagate_round_start_equip(df: pd.DataFrame) -> pd.DataFrame:
    """Fill round_start_equip from the freeze_end snapshot (all players alive) to every row in the round."""
    freeze_idx = df.groupby(["match_id", "map_name", "round_number"])["ct_alive"].idxmax()
    freeze_vals = df.loc[freeze_idx, ["match_id", "map_name", "round_number", "ct_round_start_equip", "t_round_start_equip"]]
    freeze_vals = freeze_vals.rename(columns={"ct_round_start_equip": "_ct_rse", "t_round_start_equip": "_t_rse"})
    df = df.merge(freeze_vals, on=["match_id", "map_name", "round_number"], how="left")
    df["ct_round_start_equip"] = df["_ct_rse"]
    df["t_round_start_equip"] = df["_t_rse"]
    df = df.drop(columns=["_ct_rse", "_t_rse"])
    return df


def _compute_running_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute running scores, loss streaks, and momentum from round outcomes.
    All stats reflect state BEFORE the current round completes — no lookahead.
    """
    ct_score = 0
    t_score = 0
    ct_losses = 0
    t_losses = 0
    round_results: list[str] = []
    last_round = -1

    ct_scores, t_scores, ct_ls, t_ls, ct_wr5 = [], [], [], [], []

    for _, row in df.iterrows():
        round_num = int(row["round_number"])

        if round_num != last_round and last_round >= 0:
            prev_rows = df[df["round_number"] == last_round]
            if len(prev_rows) > 0:
                ct_won = int(prev_rows.iloc[0]["ct_won"])
                if ct_won:
                    ct_score += 1
                    t_losses += 1
                    ct_losses = 0
                    round_results.append("CT")
                else:
                    t_score += 1
                    ct_losses += 1
                    t_losses = 0
                    round_results.append("T")

        last_round = round_num

        recent = round_results[-5:]
        wr5 = sum(r == "CT" for r in recent) / len(recent) if recent else float("nan")

        ct_scores.append(ct_score)
        t_scores.append(t_score)
        ct_ls.append(ct_losses)
        t_ls.append(t_losses)
        ct_wr5.append(wr5)

    df["ct_score"] = ct_scores
    df["t_score"] = t_scores
    df["ct_consecutive_losses"] = ct_ls
    df["t_consecutive_losses"] = t_ls
    df["ct_win_rate_last_5"] = ct_wr5
    return df


def build_dataset(demos_dir: str, out_path: str, max_demos: int | None = None) -> pd.DataFrame:
    """Parse all .dem files in demos_dir and write combined DataFrame to out_path."""
    dem_files = glob.glob(os.path.join(demos_dir, "**/*.dem"), recursive=True)
    if max_demos:
        dem_files = dem_files[:max_demos]

    logger.info("Parsing %d demo files...", len(dem_files))
    frames = []
    for i, dem in enumerate(dem_files):
        df = parse_demo(dem)
        if df is not None:
            frames.append(df)
        if (i + 1) % 50 == 0:
            logger.info("Parsed %d / %d demos (%d rounds so far)", i + 1, len(dem_files), sum(len(f) for f in frames))

    if not frames:
        raise RuntimeError("No usable demos found in %s" % demos_dir)

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Dataset: %d rounds from %d demos", len(combined), len(frames))
    combined.to_parquet(out_path, index=False)
    return combined


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--demos-dir", required=True)
    parser.add_argument("--out", required=True, help="Output parquet path")
    parser.add_argument("--max-demos", type=int, default=None)
    args = parser.parse_args()

    build_dataset(args.demos_dir, args.out, max_demos=args.max_demos)
    print(f"Saved to {args.out}")
