"""Team and player crosswalk: names as reports and posts write them -> CFBD team ids and roster player ids.

A wrong match is the quiet failure mode, so matching is conservative: anything below MATCH_OK goes to review instead
of being applied.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import pandas as pd
from gnomepy_research.models.cfb.features.qb import roster_keys

from gnome_pipelines.datasets.qb_status import PLAYERS

MATCH_OK = 0.8
# A rostered non-QB counts as a QB only with this many dropbacks: trick plays throw 1-3, real QBs far more.
MIN_DROPBACKS = 10
QB_POSITIONS = {"QB"}

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


SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _compact(text: str) -> str:
    return re.sub(r"[^a-z]", "", _fold(str(text)).lower())


def _surname(last: str) -> str:
    tokens = [t for t in _fold(str(last)).lower().replace(",", " ").split()]
    while len(tokens) > 1 and tokens[-1].strip(".") in SUFFIXES:
        tokens = tokens[:-1]
    return _compact("".join(tokens))


def _split_name(name: str) -> tuple[str | None, str]:
    """(first name or initial, surname compacted: lowercase letters only, suffixes dropped)."""
    tokens = _fold(str(name)).lower().replace(".", ". ").replace(",", " ").split()
    while len(tokens) > 1 and tokens[-1].strip(".") in SUFFIXES:
        tokens = tokens[:-1]
    tokens = [t for t in tokens if _compact(t)]
    if not tokens:
        return None, ""
    if len(tokens) == 1:
        return None, _compact(tokens[0])
    return _compact(tokens[0]), _compact("".join(tokens[1:]))


class PlayerIndex:
    def __init__(self, rosters: pd.DataFrame, passers: pd.DataFrame, season: int) -> None:
        self.season = int(season)
        r = rosters[rosters["season"] == self.season].copy()
        r["jersey_s"] = r["jersey"].map(_jersey)
        r["first_n"] = r["first_name"].map(lambda s: _norm_name(s) if pd.notna(s) else "")
        r["last_n"] = r["last_name"].map(lambda s: _surname(s) if pd.notna(s) else "")
        self.rosters = r
        p = passers[passers["season"] == self.season] if "season" in passers else passers
        names = r.dropna(subset=["team_id"]).drop_duplicates("team").set_index("team")["team_id"]
        counts = p.dropna(subset=["passer_key"]).groupby(["offense", "passer_key"]).size()
        self._dropbacks = {(int(names[o]), str(k)): int(n) for (o, k), n in counts.items() if o in names.index}
        keyed = r.dropna(subset=["team_id"])
        # roster_keys leaves a key unlinked when two teammates share it. When exactly one of them is a QB (a QB and a
        # long snapper both "A. Johnson"), the passer is the QB, so QB-only keys are layered on top.
        self._by_key = self._keys(keyed)
        self._by_key.update(self._keys(keyed[keyed["position"].isin(QB_POSITIONS)]))
        self._key_of = {(t, pid): k for (t, k), pid in self._by_key.items()}
        self._position = {(int(t), str(pid)): pos for t, pid, pos in keyed[["team_id", "player_id", "position"]]
                          .itertuples(index=False)}
        self._qbs: dict[int, pd.DataFrame] = {}

    @staticmethod
    def _keys(rows: pd.DataFrame) -> dict[tuple[int, str], str]:
        if not len(rows):
            return {}
        keys = roster_keys(rows)
        return {(int(t), k): str(pid) for t, k, pid in keys[["team_id", "passer_key", "player_id"]].itertuples(
            index=False)}

    def dropbacks(self, team_id: int, passer_key: str | None) -> int:
        return self._dropbacks.get((int(team_id), str(passer_key)), 0) if passer_key else 0

    def is_qb(self, team_id: int, passer_key: str) -> bool | None:
        """True if the passer key is a rostered QB (or a non-QB with real QB volume), False if it is a rostered
        non-QB (a trick-play passer), None if the roster can't tell who it is."""
        pid = self.player_for_key(team_id, passer_key)
        if pid is None:
            return None
        return (self._position.get((int(team_id), pid)) in QB_POSITIONS
                or self.dropbacks(team_id, passer_key) >= MIN_DROPBACKS)

    def player_for_key(self, team_id: int, passer_key: str) -> str | None:
        """The roster player id behind a play-by-play passer key (first initial + "." + surname), or None."""
        return self._by_key.get((int(team_id), str(passer_key)))

    def match(self, team_id: int, name: str, jersey: str | None) -> Match:
        team = self.rosters[self.rosters["team_id"] == team_id]
        first, last = _split_name(name)
        last_token = _compact(name.replace(".", " ").split()[-1]) if name.split() else ""
        num = _jersey(jersey)
        scores: dict[str, float] = {}
        for row in team.itertuples(index=False):
            s = 0.0
            same_last = bool(last) and (row.last_n == last or row.last_n == last_token)
            if num is not None and row.jersey_s == num and same_last:
                s = 1.0
            elif first and same_last and row.first_n[:1] == first[:1]:
                s = 0.9
            elif num is not None and row.jersey_s == num:
                s = 0.6
            elif not first and num is None and same_last:
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
        """The team's QBs (cached: the index doesn't change once built, and report parsing asks once per row)."""
        if int(team_id) not in self._qbs:
            self._qbs[int(team_id)] = self._find_qbs(team_id)
        return self._qbs[int(team_id)].copy()

    def _find_qbs(self, team_id: int) -> pd.DataFrame:
        team = self.rosters[self.rosters["team_id"] == team_id]
        key = team["player_id"].astype(str).map(lambda pid: self._key_of.get((int(team_id), pid)))
        n = key.map(lambda k: self.dropbacks(team_id, k))
        keep = team[team["position"].isin(QB_POSITIONS) | (n >= MIN_DROPBACKS)].copy()
        keep["player_id"] = keep["player_id"].astype(str)
        keep["name"] = (keep["first_name"].fillna("") + " " + keep["last_name"].fillna("")).str.strip()
        keep["passer_key"] = key[keep.index].where(n[keep.index] > 0)
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
    # A transfer can appear on two rosters in one season; keep one row per (season, player): the last listed
    return pd.DataFrame(rows, columns=PLAYERS.schema.names).drop_duplicates(["season", "player_id"], keep="last")
