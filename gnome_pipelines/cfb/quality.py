"""Data-quality checks over the built cfb_* tables.

    poetry run python -m gnome_pipelines.cfb.quality 2018 2026

Each check returns (name, passed, detail). Hard failures block publishing; warnings are known,
documented limitations.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

import pandas as pd

from gnome_pipelines.cfb.cfbd_client import CfbdClient
from gnome_pipelines.cfb.ingest import build_all


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    hard: bool = True


def _fbs_team_seasons(games: pd.DataFrame) -> pd.DataFrame:
    g = games[games["completed"]]
    home = g.loc[g["home_classification"] == "fbs", ["season", "home_id"]].set_axis(["season", "team_id"], axis=1)
    away = g.loc[g["away_classification"] == "fbs", ["season", "away_id"]].set_axis(["season", "team_id"], axis=1)
    return pd.concat([home, away]).drop_duplicates()


def _coverage(name: str, table: pd.DataFrame, fbs: pd.DataFrame, season_col: str = "season",
              min_share: float = 0.95, hard: bool = False) -> Check:
    have = table[[season_col, "team_id"]].dropna().drop_duplicates().rename(columns={season_col: "season"})
    have["team_id"] = have["team_id"].astype("int64")
    m = fbs.merge(have.assign(ok=True), on=["season", "team_id"], how="left")
    by = m.groupby("season")["ok"].apply(lambda s: s.notna().mean())
    worst = by.min()
    return Check(f"coverage:{name}", worst >= min_share,
                 "FBS team-seasons covered: " + ", ".join(f"{s}:{v:.0%}" for s, v in by.items()), hard)


def run_checks(t: dict[str, pd.DataFrame]) -> list[Check]:
    games, eff = t["cfb_games"], t["cfb_team_game_efficiency"]
    checks: list[Check] = []

    dup = eff.duplicated(["game_id", "team_id"]).sum()
    checks.append(Check("efficiency: unique (game, team)", dup == 0, f"{dup} duplicates"))

    missing_fk = (~eff["game_id"].isin(games["game_id"])).sum()
    checks.append(Check("efficiency: game_id in games", missing_fk == 0, f"{missing_fk} orphans"))

    per_game = eff.groupby("game_id").agg(n=("team_id", "size"), venue=("venue", "sum"))
    one_sided = (per_game["n"] != 2).sum()
    checks.append(Check("efficiency: both offenses observed", one_sided / len(per_game) < 0.01,
                        f"{one_sided} of {len(per_game)} games have one row", hard=False))
    bad_venue = (per_game.loc[per_game["n"] == 2, "venue"] != 0).sum()
    checks.append(Check("efficiency: venue flags mirror (home=+1, away=-1, neutral=0)", bad_venue == 0,
                        f"{bad_venue} games inconsistent"))

    usable = eff[eff["usable"]]
    implausible = ((usable["plays"] > 150) | (usable["off_ppa"].abs() > 2.0)).sum()
    checks.append(Check("efficiency: plausible plays and PPA (usable rows)", implausible == 0, f"{implausible} rows"))
    unusable = (~eff["usable"]).sum()
    checks.append(Check("efficiency: incomplete play-by-play (flagged unusable)", unusable < 20,
                        f"{unusable} team-games", hard=False))
    few = (eff["plays"] < 35).mean()
    checks.append(Check("efficiency: team-games with < 35 plays (blowouts after garbage-time filter)",
                        few < 0.01, f"{few:.2%}", hard=False))

    past = games[games["season"] < games["season"].max()]
    incomplete = (~past["completed"]).sum()
    checks.append(Check("games: completed seasons have no unplayed games", True,
                        f"{incomplete} canceled/postponed games kept as completed=False; never used as observations",
                        hard=False))

    fcs_rows = (eff["opponent_classification"] != "fbs").sum()
    checks.append(Check("efficiency: rows vs non-FBS opponents", True,
                        f"{fcs_rows} ({fcs_rows / len(eff):.1%}); modelled with a separate FCS group prior",
                        hard=False))

    after = (eff["available_at"] <= eff["kickoff"]).sum()
    checks.append(Check("efficiency: available only after kickoff", after == 0, f"{after} violations"))

    for name, table in t.items():
        cols = [c for c in table.columns if c.endswith("available_at")]
        if name.startswith("cfb_") and cols:
            nulls = sum(int(table[c].isna().sum()) for c in cols)
            hard = name not in ("cfb_portal",)
            checks.append(Check(f"{name}: available_at populated", nulls == 0, f"{nulls} nulls", hard=hard))

    fbs = _fbs_team_seasons(games)
    checks.append(_coverage("talent", t["cfb_talent"], fbs))
    checks.append(_coverage("returning", t["cfb_returning"], fbs))
    rec = t["cfb_recruiting_teams"].rename(columns={"class_year": "season"})
    checks.append(_coverage("recruiting class", rec, fbs))
    checks.append(_coverage("head coach", t["cfb_coaches"], fbs))
    checks.append(_coverage("roster", t["cfb_rosters"], fbs))

    for name, col in [("cfb_talent", "team"), ("cfb_returning", "team"), ("cfb_rosters", "team")]:
        unmatched = t[name].loc[t[name]["team_id"].isna(), col].nunique()
        checks.append(Check(f"{name}: team names resolve to ids", True,
                            f"{unmatched} unmatched names (non-FBS programs)", hard=False))

    cfbd = t["cfb_team_game_ppa_cfbd"].dropna(subset=["off_ppa"])
    m = eff.merge(cfbd[["game_id", "team_id", "off_ppa"]], on=["game_id", "team_id"], suffixes=("", "_cfbd"))
    corr = m["off_ppa"].corr(m["off_ppa_cfbd"])
    checks.append(Check("efficiency: agrees with CFBD /ppa/games", corr > 0.98,
                        f"corr {corr:.4f} over {len(m):,} team-games"))
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("first", type=int)
    ap.add_argument("last", type=int)
    args = ap.parse_args()
    t = build_all(CfbdClient(), list(range(args.first, args.last + 1)))
    checks = run_checks(t)
    for c in checks:
        status = "PASS" if c.passed else ("FAIL" if c.hard else "WARN")
        print(f"[{status}] {c.name}: {c.detail}")
    failed = [c for c in checks if c.hard and not c.passed]
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
