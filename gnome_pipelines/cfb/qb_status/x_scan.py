"""What the service asks X, and how it pays for it.

X bills per post returned ($0.005, once per post per UTC day), not per request, so the scan polls often with precise
queries and remembers where each query left off. Measured on 2026-10-15..22 over the 257-account registry, a team
sweep (each team's reporters x that team's QB surnames or "QB" x status words) matched ~57 posts a day (112 on a
Saturday), about $9 a month. Everything those accounts post would be ~2,300 a day.

- sweep: every team's reporters, packed several teams to a query, polled round-robin every few minutes.
- national: the national insiders with QB words and strong phrases.
- gap: teams with no reporter in the registry, by QB full name and strong phrases, from anyone.
- watch: a team with an open question, its reporters plus the insiders, with broader words, every minute.
- lookback: after an unexplained market move, one relevancy-sorted search of anyone over the last three days, capped
  at LOOKBACK_POSTS.

Following X's terms, evidence keeps the post id, handle and our claim, never the post's text.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from gnome_pipelines.cfb.qb_status.extract import RuleExtractor
from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.x_source import QUERY_LIMIT, SWEEP_SHARE, x_evidence
from gnome_pipelines.datasets.qb_status import SOURCES

PRICE_PER_POST = 0.005
MONTHLY_USD = 25.0
DAILY_ALERT_USD = 1.5
LOOKBACK_POSTS = 50
CREDIBILITY = {"normal": 1.0, "low": 0.5}
UNLISTED_CREDIBILITY = 0.25            # accounts found by gap and lookback searches of anyone
QB_WORDS = ["QB", "quarterback"]
STATUS = ["injury", "injured", "hurt", "questionable", "doubtful", "ruled out", "won't play", "will not play",
          "will start", "won't start", "expected to start", "game-time", "left the game", "concussion", "cleared",
          "status"]
STRONG = ["ruled out", "out for the season", "will not play", "won't play", "won't start", "will not start",
          "expected to start", "will start", "left the game", "carted off", "game-time decision", "season-ending"]
WATCH_EXTRA = ["did not practice", "limited", "practiced", "day-to-day", "available", "return", "to start", "QB1",
               "named the starter", "gets the start", "get the start", "the nod"]
TAIL = "-is:retweet"


@dataclass(frozen=True)
class Query:
    key: str
    text: str
    kind: str
    teams: tuple[str, ...]


def registry_frame(rows: pd.DataFrame, vetted_by: str, now: datetime) -> pd.DataFrame:
    """The reviewed registry CSV as rows of the qb_status_sources table."""
    out = pd.DataFrame({
        "platform": rows["platform"].fillna("x"),
        "handle": rows["handle"].astype(str),
        "teams": [[t.strip() for t in str(s).split(";") if t.strip()] if pd.notna(s) else [] for s in rows["teams"]],
        "kind": rows["kind"],
        "added_at": pd.Timestamp(now),
        "vetted_by": vetted_by,
        "credibility": rows.get("confidence", pd.Series("normal", index=rows.index)).map(CREDIBILITY).fillna(1.0),
        "n_scored": 0,
    })
    return out[SOURCES.schema.names]


class Registry:
    def __init__(self, sources: pd.DataFrame) -> None:
        x = sources[sources["platform"] == "x"] if len(sources) else sources
        self._teams: dict[str, list[str]] = {}
        self._by_team: dict[str, list[str]] = {}
        self.nationals: list[str] = []
        self._cred: dict[str, float] = {}
        for r in x.itertuples(index=False):
            teams = list(r.teams) if r.teams is not None else []
            self._teams[r.handle] = teams
            self._cred[r.handle] = float(r.credibility) if pd.notna(r.credibility) else 1.0
            if r.kind == "national" or not teams:
                self.nationals.append(r.handle)
            for t in teams:
                self._by_team.setdefault(t, []).append(r.handle)

    def handles_for(self, team: str) -> list[str]:
        return list(self._by_team.get(team, []))

    def teams_of(self, handle: str) -> list[str]:
        return list(self._teams.get(handle, []))

    def credibility(self, handle: str) -> float:
        return self._cred.get(handle, UNLISTED_CREDIBILITY)


def _or(items: list[str]) -> str:
    return items[0] if len(items) == 1 else "(" + " OR ".join(items) + ")"


def _quoted(words: list[str]) -> list[str]:
    return [f'"{w}"' if (" " in w or "'" in w or "-" in w) else w for w in words]


def _key(kind: str, teams) -> str:
    return f"{kind}:" + hashlib.sha1("|".join(sorted(teams)).encode()).hexdigest()[:12]


def _surnames(qbs: list[tuple[str, str]]) -> list[str]:
    return sorted({last for _, last in qbs if last})


def _full(qbs: list[tuple[str, str]]) -> list[str]:
    return sorted({f'"{first} {last}"' for first, last in qbs if first and last})


def _from_groups(handles: list[str], room: int) -> list[str]:
    """(from:a OR from:b ...) groups, each at most room characters long."""
    groups, cur = [], []
    for h in handles:
        if cur and len(_or([f"from:{x}" for x in cur + [h]])) > room:
            groups.append(_or([f"from:{x}" for x in cur]))
            cur = []
        cur.append(h)
    if cur:
        groups.append(_or([f"from:{x}" for x in cur]))
    return groups


def _team_blocks(handles: list[str], words: list[str], room: int) -> list[str]:
    """(from:... (words)) clauses for one team, splitting the handles when they don't fit in room."""
    w = _or(words)
    return [f"({g} {w})" for g in _from_groups(handles, room - len(w) - 3)]


def _pack(blocks: list[tuple[str, str]], tail: str, kind: str) -> list[Query]:
    """blocks are (label, clause); pack as many as fit under QUERY_LIMIT around the shared tail. The key comes from
    the labels only, so it survives a change of QB names."""
    out, cur = [], []

    def emit(items):
        labels = [label for label, _ in items]
        out.append(Query(_key(kind, labels), f"{_or([c for _, c in items])} {tail}", kind,
                         tuple(dict.fromkeys(label.split("#")[0] for label in labels))))

    for block in blocks:
        if cur and len(f"{_or([c for _, c in cur + [block]])} {tail}") > QUERY_LIMIT:
            emit(cur)
            cur = []
        cur.append(block)
    if cur:
        emit(cur)
    return out


def sweep_queries(registry: Registry, team_qbs: dict[str, list[tuple[str, str]]], teams: list[str]) -> list[Query]:
    status = f"{_or(_quoted(STATUS))} {TAIL}"
    room = QUERY_LIMIT - len(status) - 3
    blocks, gaps = [], []
    for team in teams:
        handles, qbs = registry.handles_for(team), team_qbs.get(team, [])
        if handles:
            for i, b in enumerate(_team_blocks(handles, _surnames(qbs) + QB_WORDS, room)):
                blocks.append((team if i == 0 else f"{team}#{i}", b))
        elif qbs:
            gaps.append((team, _or(_full(qbs))))
    queries = _pack(blocks, status, "sweep") + _pack(gaps, f"{_or(_quoted(STRONG))} {TAIL} lang:en", "gap")
    tail = f"{_or(QB_WORDS)} {_or(_quoted(STRONG))} {TAIL}"
    for i, g in enumerate(_from_groups(registry.nationals, QUERY_LIMIT - len(tail) - 1)):
        queries.append(Query(f"national:{i}", f"{g} {tail}", "national", tuple(teams)))
    return queries


def watch_queries(registry: Registry, team: str, qbs: list[tuple[str, str]]) -> list[Query]:
    status = f"{_or(_quoted(STATUS + WATCH_EXTRA))} {TAIL}"
    room = QUERY_LIMIT - len(status) - 3
    handles = registry.handles_for(team) + registry.nationals
    blocks = [(f"{team}#{i}", b) for i, b in enumerate(_team_blocks(handles, _surnames(qbs) + QB_WORDS, room))]
    return [Query(f"watch:{team}:{i}", q.text, "watch", (team,)) for i, q in enumerate(_pack(blocks, status, "watch"))]


def lookback_query(team: str, qbs: list[tuple[str, str]]) -> Query:
    status = f"{_or(_quoted(STATUS))} {TAIL} lang:en"
    return Query(f"lookback:{team}", f"{_or(_full(qbs))} {status}", "lookback", (team,))


X_EPOCH_MS = 1288834974657


def post_time(post_id: str) -> datetime:
    """When a post was created, from its id (X ids carry milliseconds since X_EPOCH_MS above bit 22)."""
    return datetime.fromtimestamp(((int(post_id) >> 22) + X_EPOCH_MS) / 1000, tz=timezone.utc)


class XBudget:
    """Monthly X spend from the posts we were billed for, persisted with each query's since_id. Sweeps stop at
    SWEEP_SHARE of the cap; watches and lookbacks may use the rest."""

    def __init__(self, path: Path, monthly_usd: float = MONTHLY_USD, daily_alert_usd: float = DAILY_ALERT_USD) -> None:
        self.path = Path(path)
        self.cap = int(round(monthly_usd / PRICE_PER_POST))
        self.daily_alert = int(round(daily_alert_usd / PRICE_PER_POST))
        self.state = {"month": "", "posts_month": 0, "day": "", "posts_day": 0, "since": {}}
        if self.path.exists():
            self.state.update(json.loads(self.path.read_text()))

    def _roll(self, now: datetime) -> None:
        month, day = now.strftime("%Y-%m"), now.strftime("%Y-%m-%d")
        if self.state["month"] != month:
            self.state.update(month=month, posts_month=0)
        if self.state["day"] != day:
            self.state.update(day=day, posts_day=0)

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state))

    def allow(self, kind: str, now: datetime) -> bool:
        self._roll(now)
        cap = self.cap * (SWEEP_SHARE if kind in ("sweep", "national", "gap") else 1.0)
        return self.state["posts_month"] < cap

    def charge(self, n: int, now: datetime) -> bool:
        """Record n billed posts; True the first time today's spend crosses the daily alert."""
        self._roll(now)
        before = self.state["posts_day"]
        self.state["posts_month"] += n
        self.state["posts_day"] += n
        self._save()
        return before <= self.daily_alert < self.state["posts_day"]

    def spent_usd(self) -> float:
        return self.state["posts_month"] * PRICE_PER_POST

    def since(self, key: str) -> str | None:
        return self.state["since"].get(key)

    def set_since(self, key: str, post_id: str) -> None:
        self.state["since"][key] = post_id
        self._save()


def _names_in(text: str, qbs: pd.DataFrame, full_only: bool) -> pd.DataFrame:
    low = (text or "").lower()
    keep = []
    for r in qbs.itertuples(index=False):
        name = str(r.name).strip()
        needle = name if full_only else name.split()[-1]
        if needle and re.search(rf"\b{re.escape(needle.lower())}\b", low):
            keep.append(r)
    return pd.DataFrame(keep, columns=qbs.columns)


def posts_to_evidence(posts: list[dict], query: Query, registry: Registry, qb_frames: dict[str, pd.DataFrame],
                      team_ids: dict[str, int], now: datetime, game_for: Callable[[int], int | None],
                      extractor=None) -> list[Evidence]:
    """Claims from posts. A team reporter's post is read against their own team's QBs (surnames are enough);
    everyone else's must name a QB in full, which also decides the team."""
    extractor = extractor or RuleExtractor()
    out: list[Evidence] = []
    for p in posts:
        handle = p.get("handle") or ""
        own = [t for t in registry.teams_of(handle) if t in qb_frames]
        if own:
            pairs = [(t, qb_frames[t]) for t in own]
        else:
            pairs = [(t, _names_in(p.get("text", ""), qb_frames[t], full_only=True)) for t in query.teams
                     if t in qb_frames]
            pairs = [(t, f) for t, f in pairs if len(f)]
        for team, qbs in pairs:
            tid = team_ids.get(team)
            if tid is None:
                continue
            claims = extractor.extract(p.get("text", ""), tid, qbs, team=team)
            if claims:
                names = dict(zip(qbs["player_id"].astype(str), qbs["name"]))
                for e in x_evidence(p, handle, claims, team, tid, now, game_id=game_for(tid),
                                    extractor=extractor.name):
                    e.player_raw = names.get(str(e.player_id))
                    out.append(e)
    return out
