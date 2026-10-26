"""Measure the starter base rates in candidates.py and the return rates in engine.py from past seasons, with the
service's own definitions: a team's starter is its first eligible passer (PlayerIndex.is_qb), in kickoff order.

    AWS_PROFILE=... PIPELINE_FAMILY=cfb poetry run python -m gnome_pipelines.cfb.qb_status.calibrate [--first 2016 --last 2024]

KEEP_RATE: the last starter is the season starter; how often he starts next, by consecutive starts (8 = 8 or more).
RETURN_RATE / HOLD_RATE: the season starter has missed 1, 2, 3+ games in a row; how often he, or the last starter,
starts next. The healthy-return bracket conditions RETURN on the season starter starting again later that season
(low: some were still hurt next game) or throwing a pass in that game (high: a benched QB who doesn't play drops out).
"""
from __future__ import annotations

import argparse

import pandas as pd
from pyiceberg.expressions import EqualTo

from gnome_pipelines.cfb.qb_status.candidates import MIN_UNLINKED_DROPBACKS, game_starters, start_state
from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex
from gnome_pipelines.cfb.qb_status.service import fbs_games
from gnome_pipelines.stores import dataset_store

MAX_STREAK, MAX_MISSED = 8, 3


def transitions(starters: list[str], threw: list[set[str]] | None = None) -> pd.DataFrame:
    """One row per game after a team's first: the start state before it and who started it. threw[i], when given,
    is everyone who passed in game i."""
    rows = []
    for i in range(1, len(starters)):
        last, streak, season, missed = start_state(starters[:i])
        rows.append({"streak": streak, "missed": missed, "next_is_last": starters[i] == last,
                     "next_is_season": starters[i] == season, "season_threw": season in threw[i] if threw else None,
                     "season_starts_later": season in starters[i:]})
    return pd.DataFrame(rows, columns=["streak", "missed", "next_is_last", "next_is_season", "season_threw",
                                       "season_starts_later"])


def rates(t: pd.DataFrame) -> dict[str, dict[int, float]]:
    t = t.reset_index(drop=True)
    keep = t[t["missed"] == 0].groupby(t["streak"].clip(upper=MAX_STREAK))["next_is_last"].mean()
    changed = t[t["missed"] > 0]
    by = changed["missed"].clip(upper=MAX_MISSED)
    return {"KEEP_RATE": keep.round(3).to_dict(),
            "RETURN_RATE": changed.groupby(by)["next_is_season"].mean().round(3).to_dict(),
            "HOLD_RATE": changed.groupby(by)["next_is_last"].mean().round(3).to_dict()}


def season_transitions(ds, season: int) -> pd.DataFrame:
    games = fbs_games(ds.load("cfb_games", row_filter=EqualTo("season", season)))
    games = games[games["completed"].fillna(False).astype(bool)]
    passers = ds.load("cfb_passers", row_filter=EqualTo("season", season)).dropna(subset=["passer_key"])
    index = PlayerIndex(ds.load("cfb_rosters", row_filter=EqualTo("season", season)), passers, season)
    sides = pd.concat([games[["game_id", "kickoff", f"{s}_id", f"{s}_team", f"{s}_classification"]]
                       .set_axis(["game_id", "kickoff", "team_id", "team", "cls"], axis=1) for s in ("home", "away")])
    out = []
    for (tid, team), f in sides[sides["cls"] == "fbs"].dropna(subset=["team_id"]).groupby(["team_id", "team"]):
        p = passers[(passers["offense"] == team) & passers["game_id"].isin(f["game_id"])]
        n = p.groupby("passer_key").size()
        ok = {k for k, c in n.items()
              if (e := index.is_qb(int(tid), str(k))) is True or (e is None and c >= MIN_UNLINKED_DROPBACKS)}
        p = p[p["passer_key"].isin(ok)]
        starts = game_starters(p).merge(f[["game_id", "kickoff"]], on="game_id")
        starts = starts.assign(kickoff=pd.to_datetime(starts["kickoff"], utc=True)).sort_values("kickoff")
        threw = p.groupby("game_id")["passer_key"].agg(set)
        out.append(transitions(starts["passer_key"].tolist(), [threw.get(g, set()) for g in starts["game_id"]])
                   .assign(season=season))
    return pd.concat(out, ignore_index=True)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--first", type=int, default=2016)
    ap.add_argument("--last", type=int, default=2024)
    a = ap.parse_args(argv)
    ds = dataset_store()
    t = pd.concat([season_transitions(ds, s) for s in range(a.first, a.last + 1)], ignore_index=True)
    print(f"team-games {len(t)}, seasons {a.first}-{a.last}")
    for name, table in rates(t).items():
        print(f"{name} = {table}")
    changed = t[t["missed"] > 0]
    by = changed["missed"].clip(upper=MAX_MISSED)
    low = changed[changed["season_starts_later"]].groupby(by[changed["season_starts_later"]])["next_is_season"].mean()
    high = changed[changed["season_threw"] == True].groupby(by[changed["season_threw"] == True])["next_is_season"].mean()  # noqa: E712
    print("healthy return, low:", low.round(3).to_dict(), " high:", high.round(3).to_dict(),
          " midpoint:", ((low + high) / 2).round(2).to_dict())


if __name__ == "__main__":
    main()
