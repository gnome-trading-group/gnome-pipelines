"""Team and player crosswalk: names as reports and posts write them -> CFBD team ids and roster player ids.

A wrong match is the quiet failure mode, so matching is conservative: anything below MATCH_OK goes to review instead
of being applied.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import pandas as pd

from gnome_pipelines.datasets.qb_status import PLAYERS

MATCH_OK = 0.8

TEAM_ALIASES = {"miami fl": "miami", "fiu": "florida international", "umass": "massachusetts",
                "app state": "appalachian state", "ul monroe": "louisiana monroe", "ulm": "louisiana monroe",
                "hawai i": "hawaii", "southern mississippi": "southern miss", "uconn": "uconn",
                "louisiana lafayette": "louisiana", "texas a and m": "texas a and m", "san jose state": "san jose state",
                "nc state": "nc state", "north carolina state": "nc state", "ole miss": "ole miss",
                "mississippi": "ole miss", "sam houston state": "sam houston", "utsa": "utsa",
                "texas san antonio": "utsa", "southern methodist": "smu", "central florida": "ucf",
                "brigham young": "byu", "louisiana state": "lsu", "texas christian": "tcu"}


def _fold(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def norm_team(name: str) -> str:
    s = _fold(str(name)).lower().replace("&", " and ").replace("ʻ", "").replace("'", "")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bst$", "state", s)
    return TEAM_ALIASES.get(s, s)


def _norm_name(text: str) -> str:
    return re.sub(r"[^a-z ]", " ", _fold(str(text)).lower()).strip()


class TeamIndex:
    def __init__(self, games: pd.DataFrame) -> None:
        self._ids: dict[tuple[int, str], int] = {}
        for side in ("home", "away"):
            for season, name, tid in games[["season", f"{side}_team", f"{side}_id"]].itertuples(index=False):
                if pd.notna(tid):
                    self._ids[(int(season), norm_team(name))] = int(tid)

    def team_id(self, name: str, season: int) -> int | None:
        return self._ids.get((int(season), norm_team(name)))


@dataclass
class Match:
    player_id: str | None
    confidence: float


def _jersey(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().lstrip("#")
    s = s[:-2] if s.endswith(".0") else s
    return s or None


def _split_name(name: str) -> tuple[str | None, str]:
    parts = _norm_name(name.replace(".", ". ")).split()
    if not parts:
        return None, ""
    if len(parts) == 1:
        return None, parts[0]
    suffixes = {"jr", "sr", "ii", "iii", "iv"}
    while len(parts) > 2 and parts[-1] in suffixes:
        parts = parts[:-1]
    return parts[0], parts[-1]


class PlayerIndex:
    def __init__(self, rosters: pd.DataFrame, passers: pd.DataFrame, season: int) -> None:
        self.season = int(season)
        r = rosters[rosters["season"] == self.season].copy()
        r["jersey_s"] = r["jersey"].map(_jersey)
        r["first_n"] = r["first_name"].map(lambda s: _norm_name(s) if pd.notna(s) else "")
        r["last_n"] = r["last_name"].map(lambda s: _norm_name(s) if pd.notna(s) else "")
        self.rosters = r
        p = passers[passers["season"] == self.season] if "season" in passers else passers
        self.passer_keys = set(p["passer_key"].dropna().astype(str))

    def match(self, team_id: int, name: str, jersey: str | None) -> Match:
        team = self.rosters[self.rosters["team_id"] == team_id]
        first, last = _split_name(name)
        num = _jersey(jersey)
        scores: dict[str, float] = {}
        for row in team.itertuples(index=False):
            s = 0.0
            if num is not None and row.jersey_s == num and row.last_n == last:
                s = 1.0
            elif first and row.last_n == last and row.first_n[:1] == first[:1]:
                s = 0.9
            elif num is not None and row.jersey_s == num:
                s = 0.6
            elif not first and num is None and row.last_n == last:
                s = 0.7
            if s:
                scores[str(row.player_id)] = max(s, scores.get(str(row.player_id), 0.0))
        if not scores:
            return Match(None, 0.0)
        best = max(scores.values())
        top = [pid for pid, s in scores.items() if s == best]
        if len(top) > 1:
            return Match(None, 0.5)
        return Match(top[0], best)

    def qbs(self, team_id: int) -> pd.DataFrame:
        team = self.rosters[self.rosters["team_id"] == team_id]
        keep = team[(team["position"] == "QB") | team["player_id"].astype(str).isin(self.passer_keys)].copy()
        keep["player_id"] = keep["player_id"].astype(str)
        keep["name"] = (keep["first_name"].fillna("") + " " + keep["last_name"].fillna("")).str.strip()
        keep["passer_key"] = keep["player_id"].where(keep["player_id"].isin(self.passer_keys))
        return keep[["player_id", "name", "jersey_s", "passer_key"]].rename(columns={"jersey_s": "jersey"}).reset_index(
            drop=True)


def _aliases(first, last) -> list[str]:
    first, last = (first or "").strip(), (last or "").strip()
    if not first:
        return [last]
    return [f"{first} {last}", f"{first[0]}. {last}", f"{first[0]}.{last}", last]


def players_frame(index: PlayerIndex) -> pd.DataFrame:
    rows = []
    for tid in index.rosters["team_id"].dropna().unique():
        qbs = index.qbs(int(tid))
        team = index.rosters[index.rosters["team_id"] == tid]
        for q in qbs.itertuples(index=False):
            r = team[team["player_id"].astype(str) == q.player_id].iloc[0]
            rows.append({"season": index.season, "player_id": q.player_id, "team_id": int(tid), "team": r["team"],
                         "first_name": r["first_name"], "last_name": r["last_name"], "jersey": q.jersey,
                         "position": r["position"], "passer_key": q.passer_key,
                         "aliases": _aliases(r["first_name"], r["last_name"])})
    return pd.DataFrame(rows, columns=PLAYERS.schema.names)
