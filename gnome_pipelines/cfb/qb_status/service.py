"""The QB Status Service: every poller on its own cadence in one process, laptop-sleep aware.

    AWS_PROFILE=... AWS_DEFAULT_REGION=us-east-1 poetry run python -m gnome_pipelines.cfb.qb_status.service [--once]

X_BEARER_TOKEN turns on X search and ANTHROPIC_API_KEY has Claude read posts and headlines; without the key they are
read by rules and wait for review. Which claims apply on their own is autoapply's decision.

Reads cfb_games, cfb_rosters and cfb_passers from the research catalog (read-only) and writes the QB status tables to a
local warehouse unless --prod. If the wall clock jumps (the laptop slept), it logs a gap and polls every source at
once; whatever it finds is stamped with the wake time, never backdated.
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
import requests
from gnomepy.data import DatasetStore
from pyiceberg.expressions import EqualTo

from gnome_pipelines.cfb.qb_status.autoapply import credibility_for, decide, redecide
from gnome_pipelines.cfb.qb_status.availability import (CONFERENCES, PAST_GRACE, fetch_conference, report_arrivals,
                                                        report_evidence)
from gnome_pipelines.cfb.qb_status.backfill import capture_evidence
from gnome_pipelines.cfb.qb_status.candidates import candidates, game_starters, next_game, with_roster_ids
from gnome_pipelines.cfb.qb_status.crosswalk import MATCH_OK, PlayerIndex, TeamIndex, _surname, players_frame
from gnome_pipelines.cfb.qb_status.engine import UNKNOWN, team_status
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id
from gnome_pipelines.cfb.qb_status.extract import (ClaudeExtractor, ExtractionUnavailable, Extractor, NotConfigured,
                                                   RuleExtractor)
from gnome_pipelines.cfb.qb_status.health import (health_report, health_row, kickoff_accuracy, news_lag_rows,
                                                   report_lag_rows)
from gnome_pipelines.cfb.qb_status.market_alarm import EVENTS, SERIES, Alarm, alarms, kalshi_home_probs, missed_news
from gnome_pipelines.cfb.qb_status.news import news_evidence, parse_rss, rss_url
from gnome_pipelines.cfb.qb_status.pbp_flags import SCOREBOARD, SUMMARY, game_flags, live_games
from gnome_pipelines.cfb.qb_status.state_sync import StateMirror
from gnome_pipelines.cfb.qb_status.schedule import (WINDOW_AFTER, expected_releases, report_poll_interval,
                                                     schedule_rows)
from gnome_pipelines.cfb.qb_status.store import StatusStore, open_store
from gnome_pipelines.cfb.qb_status.watches import is_unsettled, unsettled_watches, update_watches, watched_teams
from gnome_pipelines.cfb.qb_status.x_scan import (LOOKBACK_POSTS, Query, Registry, XBudget, lookback_query,
                                                   post_time, posts_to_evidence, registry_frame, sweep_queries,
                                                   watch_queries)
from gnome_pipelines.cfb.qb_status.x_source import ReadBudget, XSource

CONF_CODES = {"SEC": "SEC", "Big Ten": "B10", "Big 12": "B12", "ACC": "ACC", "American Athletic": "American",
              "Mid-American": "MAC"}
EVENING_UNKNOWN = {"ACC", "American", "MAC", "B12"}
NEWS_WATCHED_EVERY, NEWS_SWEEP_EVERY, NEWS_MAX_AGE = 600.0, 10800.0, timedelta(days=3)
HOUR = 3600.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Task:
    name: str
    interval: Callable[[datetime], float]
    run: Callable[[datetime], list[Evidence]]


class Service:
    """Runs due tasks each step. Evidence is stamped with a clock read taken after its task returns (the true capture
    time). A task that takes longer than task_timeout, or a step that starts long after the last one, means the
    laptop slept or the network stalled: a gap row is written and every task runs again. Failed tasks retry after
    RETRY_BASE seconds, doubling, up to their normal interval."""

    RETRY_BASE = 15.0

    def __init__(self, tasks: list[Task], store: StatusStore,
                 recompute: Callable[[set[int], list[Evidence], datetime], None] | None = None,
                 clock: Callable[[], datetime] | None = None, sleep: Callable[[float], None] = time.sleep,
                 task_timeout: float = 180.0) -> None:
        self.tasks, self.store, self.recompute, self.clock, self.sleep = tasks, store, recompute, clock, sleep
        self.task_timeout = task_timeout
        self.next_due: dict[str, datetime] = {}
        self.failures: dict[str, int] = {}
        self.last_step: datetime | None = None

    def _time(self, fallback: datetime) -> datetime:
        return self.clock() if self.clock is not None else fallback

    def step(self, now: datetime) -> None:
        smallest = min(t.interval(now) for t in self.tasks)
        if self.last_step is not None:
            elapsed = (now - self.last_step).total_seconds()
            if elapsed > 3 * smallest + 60:
                self.store.put_health([health_row("gap", now, f"no step for {elapsed:.0f}s; catching up", elapsed)])
                self.next_due = {}
        new: list[Evidence] = []
        rerun_all = False
        for t in self.tasks:
            due = self.next_due.get(t.name)
            if due is not None and now < due:
                continue
            before = self._time(now)
            try:
                found = t.run(before) or []
                after = self._time(now)
                took = (after - before).total_seconds()
                if took > self.task_timeout:
                    self.store.put_health([health_row("gap", after, f"{t.name} took {took:.0f}s (sleep or stall)",
                                                      took)])
                    rerun_all = True
                for e in found:
                    if e.first_seen_at < after:
                        e.first_seen_at = after
                new += self.store.add_evidence_new(found)
                self.failures[t.name] = 0
                self.next_due[t.name] = after + timedelta(seconds=t.interval(after))
            except Exception as e:
                self.store.put_health([health_row("source_error", now, f"{t.name}: {type(e).__name__}: {e}")])
                n = self.failures[t.name] = self.failures.get(t.name, 0) + 1
                retry = min(t.interval(now), self.RETRY_BASE * 2 ** (n - 1))
                self.next_due[t.name] = self._time(now) + timedelta(seconds=retry)
        if rerun_all:
            self.next_due = {}
        teams = {e.team_id for e in new if e.team_id is not None}
        if teams and self.recompute is not None:
            try:
                self.recompute(teams, new, self._time(now))
            except Exception as e:
                self.store.put_health([health_row("source_error", now, f"recompute: {type(e).__name__}: {e}")])
        self.last_step = now

    def run_forever(self) -> None:
        self.run_until(datetime.max.replace(tzinfo=timezone.utc))

    def run_until(self, deadline: datetime) -> None:
        clock = self.clock or utcnow
        self.clock = clock
        while clock() < deadline:
            self.step(clock())
            self.last_step = clock()
            self.sleep(1.0)


CACHE_DIR = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_research_cache"
X_STATE = Path.home() / ".cache" / "gnome-pipelines" / "qb_status_x_state.json"
X_SWEEP_EVERY = 180.0       # a full sweep of every team query; empty polls cost nothing
X_WATCH_EVERY = 60.0
X_FIRST_SWEEP = timedelta(hours=24)    # a query we haven't run before looks back this far, not 7 days
X_FIRST_WATCH = timedelta(hours=72)
X_LOOKBACK_GAP = timedelta(hours=6)    # at most one alarm lookback per team in this window
RESEARCH_TABLES = ("cfb_games", "cfb_rosters", "cfb_passers")


def fbs_games(games: pd.DataFrame) -> pd.DataFrame:
    """Games with at least one FBS side: FCS opponents are real games for the FBS team's status and its QB history."""
    keep = (games["home_classification"] == "fbs") | (games["away_classification"] == "fbs")
    return games[keep].reset_index(drop=True)


def _compact(text: str | None) -> str:
    return "".join(c for c in (text or "").lower() if c.isalpha())


def resolve_flags(flags: list[Evidence], team_ids: dict[str, int], index: PlayerIndex) -> list[Evidence]:
    """Attach team and player ids to game-night flags. A QB change counts only if the new passer is one of the team's
    rostered QBs; anything else (a trick-play pass, penalty text) is dropped rather than queued for review."""
    out = []
    for f in flags:
        tid = team_ids.get(f.team)
        if tid is None:
            continue
        qbs = index.qbs(tid)
        f.team_id = tid
        if f.status == "qb_change":
            m = index.match(tid, f.player_raw or "", None)
            if m.player_id is None or m.confidence < MATCH_OK or m.player_id not in set(qbs["player_id"]):
                continue
            f.player_id, f.match_confidence = m.player_id, m.confidence
        else:
            last = _compact((f.player_raw or "").split()[-1] if f.player_raw else "")
            hits = [pid for pid, name in zip(qbs["player_id"], qbs["name"]) if _compact(str(name).split()[-1]) == last]
            if len(hits) != 1:
                f.review_state = "pending"
            f.player_id, f.match_confidence = (hits[0], 0.9) if len(hits) == 1 else (None, 0.0)
        out.append(f)
    return out


def fresh_alarms(found: list[Alarm], alarmed_until: dict[int, datetime], now: datetime) -> list[Alarm]:
    """One alarm per game per move: after an alarm, the same game can't alarm again for the alarm window."""
    out = []
    for a in found:
        if pd.Timestamp(now) >= pd.Timestamp(alarmed_until.get(a.game_id, datetime.min.replace(tzinfo=timezone.utc))):
            out.append(a)
            alarmed_until[a.game_id] = now + timedelta(minutes=60)
    return out


class Context:
    """Everything the real pollers share: the research data, indexes, watches and in-memory market history."""

    def __init__(self, research: DatasetStore, status: StatusStore, season: int, cache_dir: Path | None = None,
                 x: XSource | None = None, x_budget: XBudget | None = None,
                 extractor: Extractor | None = None) -> None:
        self.research, self.status, self.season = research, status, season
        self.cache_dir = Path(cache_dir or CACHE_DIR)
        self.x = x
        self.x_budget = x_budget or (XBudget(X_STATE) if x is not None else None)
        self.x_queries: list[Query] = []
        self.x_cursor, self.x_watch_cursor = 0, 0
        self.x_lookbacks: list[int] = []
        self.x_lookback_at: dict[int, datetime] = {}
        self.x_blocked_day: str | None = None
        self.extractor, self.rules = extractor or RuleExtractor(), RuleExtractor()
        self.fallback_at: datetime | None = None
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        self.live: list[str] = []
        self.market: list[dict] = []
        self.pending_alarms: list[Alarm] = []
        self.alarmed_until: dict[int, datetime] = {}
        self.last_change: dict[int, datetime] = {}
        self.unsettled: dict[int, bool] = {}
        self.accuracy_done: set[int] = set()
        self.news_cursor, self.watch_cursor = 0, 0
        self.refresh()
        self.watches = status.watches()
        ev = status.evidence()
        official = ev[ev["source_type"] == "official_report"].dropna(subset=["game_id"])
        self.observed = {(r.source_org, int(r.game_id), r.report_slot): pd.Timestamp(r.first_seen_at)
                         for r in official.sort_values("first_seen_at", ascending=False).itertuples(index=False)}

    def _load_research(self) -> dict[str, pd.DataFrame]:
        try:
            tables = {name: self.research.load(name, row_filter=EqualTo("season", self.season))
                      for name in RESEARCH_TABLES}
        except Exception as e:
            cached = {name: self.cache_dir / f"{name}_{self.season}.parquet" for name in RESEARCH_TABLES}
            if not all(p.exists() for p in cached.values()):
                raise
            self.status.put_health([health_row("source_error", utcnow(),
                                               f"research reads failed ({type(e).__name__}); using the local cache")])
            return {name: pd.read_parquet(p) for name, p in cached.items()}
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        for name, df in tables.items():
            df.to_parquet(self.cache_dir / f"{name}_{self.season}.parquet")
        return tables

    def refresh(self) -> None:
        t = self._load_research()
        self.games = fbs_games(t["cfb_games"])
        self.rosters, self.passers = t["cfb_rosters"], t["cfb_passers"]
        self.teams = TeamIndex(self.games)
        self.players = {self.season: PlayerIndex(self.rosters, self.passers, self.season)}
        self.kickoffs = dict(zip(self.games["game_id"].astype(int), pd.to_datetime(self.games["kickoff"], utc=True)))
        names = pd.concat([self.games[["home_id", "home_team", "home_classification"]].set_axis(["id", "name", "c"], axis=1),
                           self.games[["away_id", "away_team", "away_classification"]].set_axis(["id", "name", "c"], axis=1)])
        names = names.dropna(subset=["id"]).drop_duplicates("id")
        self.team_names = {int(i): n for i, n, _ in names.itertuples(index=False)}
        self.fbs_ids = sorted(int(i) for i, _, c in names.itertuples(index=False) if c == "fbs")
        try:
            self.status.put_players(players_frame(self.players[self.season]))
        except Exception as e:
            self.status.put_health([health_row("source_error", utcnow(), f"players: {type(e).__name__}: {e}")])
        self.x_registry = Registry(self.status.sources())
        if self.x is not None:
            self._refresh_x(utcnow())

    def _read(self, read: Callable[[Extractor], list[Evidence]], now: datetime) -> list[Evidence]:
        """Read with the model, or with rules when it can't (an API error or its daily cap), saying so at most hourly;
        then decide which claims apply on their own."""
        try:
            found = read(self.extractor)
        except ExtractionUnavailable as e:
            if self.fallback_at is None or now - self.fallback_at >= timedelta(hours=1):
                self.fallback_at = now
                self.status.put_health([health_row("extractor_fallback", now, f"{self.extractor.name}: {e}")])
            found = read(self.rules)
        if not found:
            return found
        return decide(found, self.status.evidence(), credibility_for(self.x_registry), now,
                      incumbent=lambda tid: self.last_starter(tid, now),
                      unsettled=lambda tid: self.unsettled.get(tid, False))

    # --- X ---------------------------------------------------------------------------------------------------------
    def _refresh_x(self, now: datetime) -> None:
        """Rebuild each team's QB names and the sweep queries (hourly, with the research refresh)."""
        index = self.players[self.season]
        names = dict(zip(self.rosters["player_id"].astype(str), zip(self.rosters["first_name"], self.rosters["last_name"])))
        self.x_team_ids, self.x_qbs, self.x_frames = {}, {}, {}
        for tid in self.fbs_ids:
            team = self.team_names[tid]
            self.x_team_ids[team] = tid
            cands = self.team_candidates(tid, now)
            ordered = cands.sort_values("base", ascending=False)["player_id"].astype(str) if len(cands) else []
            self.x_qbs[team] = [(str(f), str(l)) for f, l in (names[p] for p in ordered if p in names)
                                if pd.notna(f) and pd.notna(l)][:4]
            self.x_frames[team] = index.qbs(tid)[["player_id", "name"]]
        self.x_queries = sweep_queries(self.x_registry, self.x_qbs, [self.team_names[t] for t in self.fbs_ids])

    def _x_poll(self, q: Query, now: datetime, first_back: timedelta, max_results: int = 100,
                sort_order: str | None = None, resume: bool = True) -> list[Evidence]:
        if not self.x_budget.allow(q.kind, now):
            day = now.strftime("%Y-%m-%d")
            if self.x_blocked_day != day:
                self.x_blocked_day = day
                self.status.put_health([health_row("x_budget_stop", now, f"X {q.kind} paused: "
                                                   f"${self.x_budget.spent_usd():.2f} spent this month")])
            return []
        since = self.x_budget.since(q.key) if resume else None
        if since is not None and post_time(since) < now - first_back:
            since = None
        posts = self.x.poll(q.text, since_id=since, kind=q.kind, start_time=None if since else now - first_back,
                            max_results=max_results, sort_order=sort_order)
        if self.x_budget.charge(len(posts), now):
            self.status.put_health([health_row("x_spend_alert", now, f"X spend today passed "
                                               f"${self.x_budget.daily_alert * 0.005:.2f}", self.x_budget.spent_usd())])
        if posts and resume:
            self.x_budget.set_since(q.key, max(posts, key=lambda p: int(p["id"]))["id"])
        return self._read(lambda ex: posts_to_evidence(posts, q, self.x_registry, self.x_frames, self.x_team_ids, now,
                                                       game_for=lambda tid: next_game(tid, now, self.games),
                                                       extractor=ex), now)

    def x_sweep_interval(self, now: datetime) -> float:
        return X_SWEEP_EVERY / max(len(self.x_queries), 1)

    def x_sweep_run(self, now: datetime) -> list[Evidence]:
        if not self.x_queries:
            return []
        q = self.x_queries[self.x_cursor % len(self.x_queries)]
        self.x_cursor += 1
        return self._x_poll(q, now, X_FIRST_SWEEP)

    def _x_watch_queries(self, now: datetime) -> list[Query]:
        out = []
        for tid in sorted(watched_teams(self.watches, now)):
            team = self.team_names.get(tid)
            if team in self.x_qbs:
                out += watch_queries(self.x_registry, team, self.x_qbs[team])
        return out

    def x_watch_interval(self, now: datetime) -> float:
        n = len(self._x_watch_queries(now))
        return X_WATCH_EVERY / n if n else X_WATCH_EVERY

    def x_watch_run(self, now: datetime) -> list[Evidence]:
        qs = self._x_watch_queries(now)
        if not qs:
            return []
        q = qs[self.x_watch_cursor % len(qs)]
        self.x_watch_cursor += 1
        return self._x_poll(q, now, X_FIRST_WATCH)

    def queue_lookback(self, tid: int, now: datetime) -> None:
        last = self.x_lookback_at.get(tid)
        if tid in self.x_lookbacks or (last is not None and now - last < X_LOOKBACK_GAP):
            return
        self.x_lookbacks.append(tid)
        self.x_lookback_at[tid] = now

    def x_lookback_run(self, now: datetime) -> list[Evidence]:
        if not self.x_lookbacks:
            return []
        tid = self.x_lookbacks.pop(0)
        team = self.team_names.get(tid)
        if not self.x_qbs.get(team):
            return []
        return self._x_poll(lookback_query(team, self.x_qbs[team]), now, X_FIRST_WATCH, max_results=LOOKBACK_POSTS,
                            sort_order="relevancy", resume=False)

    def upcoming_conference_games(self, now: datetime, days: int) -> pd.DataFrame:
        kick = pd.to_datetime(self.games["kickoff"], utc=True)
        g = self.games[(kick >= pd.Timestamp(now)) & (kick <= pd.Timestamp(now) + timedelta(days=days))
                       & self.games["conference_game"].fillna(False).astype(bool)]
        return g.assign(code=g["home_conference"].map(CONF_CODES)).dropna(subset=["code"])

    def team_candidates(self, team_id: int, now: datetime) -> pd.DataFrame:
        index = self.players[self.season]
        c = candidates(self.team_names.get(team_id, ""), team_id, self.season, now, self.passers, self.games,
                       eligible=lambda key: index.is_qb(team_id, key))
        return with_roster_ids(c, team_id, index)

    def previous_kickoff(self, team_id: int, now: datetime) -> datetime | None:
        g = self.games[(self.games["home_id"] == team_id) | (self.games["away_id"] == team_id)]
        kick = pd.to_datetime(g["kickoff"], utc=True)
        past = kick[kick < pd.Timestamp(now)]
        return past.max().to_pydatetime() if len(past) else None

    def last_starter(self, team_id: int, now: datetime) -> str | None:
        c = self.team_candidates(team_id, now)
        return str(c.loc[c["last_starter"], "player_id"].iloc[0]) if len(c) and c["last_starter"].any() else None

    def starter_surname(self, team_id: int, now: datetime) -> str | None:
        c = self.team_candidates(team_id, now)
        if c.empty:
            return None
        pid = c.loc[c["last_starter"], "player_id"].iloc[0]
        r = self.rosters[self.rosters["player_id"].astype(str) == pid]
        return _surname(r["last_name"].iloc[0]) if len(r) else None

    def watched_count(self, now: datetime) -> int:
        return len(watched_teams(self.watches, now))

    def _team_status(self, tid: int, game: int, cands: pd.DataFrame, evidence: pd.DataFrame,
                     now: datetime) -> pd.DataFrame:
        qb_ids = set(self.players[self.season].qbs(tid)["player_id"]) | set(cands["player_id"].astype(str))
        return team_status(game, tid, cands, evidence, now, since=self.previous_kickoff(tid, now), qb_ids=qb_ids)

    def recompute(self, teams: set[int], new: list[Evidence], now: datetime) -> None:
        evidence = self.status.evidence()
        frames, open_question = [], set()
        for tid in teams:
            name = self.team_names.get(tid)
            game = next_game(tid, now, self.games) if name else None
            if game is None:
                continue
            cands = self.team_candidates(tid, now)
            status = self._team_status(tid, game, cands, evidence, now)
            if status.empty:
                continue
            named = status[status["player_id"] != UNKNOWN]
            qb_change = bool(cands["changed"].any()) if "changed" in cands.columns else False
            self.unsettled[tid] = is_unsettled(float(named["p_start"].max()) if len(named) else 0.0, qb_change)
            if self.unsettled[tid]:
                open_question.add((game, tid))
                late = redecide(evidence, credibility_for(self.x_registry), now, team_id=tid, game_id=game,
                                unsettled=True)
                if self.status.add_evidence_new(late):
                    evidence = self.status.evidence()
                    status = self._team_status(tid, game, cands, evidence, now)
            frames.append(status)
        pairs = self.status.put_snapshots_changed(pd.concat(frames, ignore_index=True), now) if frames else set()
        for game, _ in pairs:
            self.last_change[game] = now
        changed = {tid for _, tid in pairs}
        self.status.put_health(news_lag_rows(new, changed))
        self.watches = update_watches(self.watches, new, self.kickoffs, now)
        self.watches = unsettled_watches(self.watches, open_question, self.kickoffs, now)
        self.status.put_watches(self.watches)

    # --- pollers ---------------------------------------------------------------------------------------------------

    def reports_interval(self, now: datetime) -> float:
        expected, unknown, overdue = [], [], []
        for g in self.upcoming_conference_games(now, 4).itertuples(index=False):
            kick = pd.Timestamp(g.kickoff).to_pydatetime()
            for slot, t in expected_releases(g.code, kick).items():
                if t is None:
                    continue
                expected.append(t)
                if t + WINDOW_AFTER < now and (g.code, int(g.game_id), slot) not in self.observed:
                    overdue.append(t)
            if g.code in EVENING_UNKNOWN:
                unknown.append(kick)
        return report_poll_interval(now, expected, unknown, overdue)

    def reports_run(self, now: datetime) -> list[Evidence]:
        out, health, new_obs = [], [], {}
        since = now.date() - PAST_GRACE
        for conf in CONFERENCES:
            try:
                payload = fetch_conference(self.session, conf)
                errors: list[str] = []
                out += report_evidence(conf, payload, now, self.teams, self.players, self.games, errors=errors,
                                       since=since)
                health += [health_row("source_error", now, e) for e in errors[:5]]
                for game_id, slot in report_arrivals(conf, payload, self.teams, self.games, since=since):
                    if (conf, game_id, slot) not in self.observed:
                        self.observed[(conf, game_id, slot)] = pd.Timestamp(now)
                        new_obs[(conf, game_id, slot)] = now
            except Exception as e:
                health.append(health_row("source_error", now, f"reports {conf}: {type(e).__name__}: {e}"))
        sched = [schedule_rows(g.code, int(g.game_id), pd.Timestamp(g.kickoff).to_pydatetime(),
                               {s: t for (c, gid, s), t in self.observed.items() if c == g.code and gid == g.game_id},
                               now) for g in self.upcoming_conference_games(now, 7).itertuples(index=False)]
        if sched:
            frame = pd.concat(sched, ignore_index=True)
            health += report_lag_rows(frame.assign(state="expected"), new_obs)
            self.status.put_schedule(frame)
        self.status.put_health(health)
        return out

    def espn_interval(self, now: datetime) -> float:
        return 30.0 if self.live else 600.0

    def espn_run(self, now: datetime) -> list[Evidence]:
        board = self.session.get(SCOREBOARD, params={"groups": 80, "limit": 300}, timeout=20).json()
        self.live = live_games(board)
        out, health = [], []
        index = self.players[self.season]
        for gid in self.live:
            try:
                summary = self.session.get(SUMMARY, params={"event": gid}, timeout=20).json()
                comps = summary["header"]["competitions"][0]["competitors"]
                ids = {c["team"]["location"]: int(c["team"]["id"]) for c in comps}
                starters = {loc: s for loc, tid in ids.items() if (s := self.starter_surname(tid, now))}
                qb_names = {loc: {_surname(str(n).split()[-1]) for n in index.qbs(tid)["name"]}
                            for loc, tid in ids.items()}
                out += resolve_flags(game_flags(summary, starters, now, qb_names), ids, index)
            except Exception as e:
                health.append(health_row("source_error", now, f"espn {gid}: {type(e).__name__}: {e}"))
        self.status.put_health(health)
        return out

    def market_run(self, now: datetime) -> list[Evidence]:
        events, cursor, seen = [], "", set()
        while True:
            params = {"series_ticker": SERIES, "status": "open", "with_nested_markets": "true", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            d = self.session.get(EVENTS, params=params, timeout=30).json()
            events += d.get("events", [])
            cursor = d.get("cursor") or ""
            if not cursor or cursor in seen:
                break
            seen.add(cursor)
        for game_id, p in kalshi_home_probs(events, self.games, self.teams).items():
            self.market.append({"game_id": game_id, "at": pd.Timestamp(now), "p_home": p})
        self.market = [m for m in self.market if pd.Timestamp(now) - m["at"] <= timedelta(hours=2)]
        found = alarms(pd.DataFrame(self.market), self.last_change, now, self.kickoffs)
        out = []
        for a in fresh_alarms(found, self.alarmed_until, now):
            g = self.games[self.games["game_id"] == a.game_id].iloc[0]
            hour = pd.Timestamp(now).floor("h").isoformat()
            for tid in (int(g["home_id"]), int(g["away_id"])):
                out.append(Evidence(evidence_id=evidence_id("market_alarm", a.game_id, tid, hour),
                                    source_type="market_alarm", source_id=f"{SERIES}:{a.game_id}", source_org="kalshi",
                                    first_seen_at=now, team=self.team_names.get(tid, ""), status="market_alarm",
                                    season=self.season, team_id=tid, game_id=a.game_id, hedge="unclear",
                                    paraphrase=f"Kalshi moved {a.move:.2f} logit in 60 min with no QB status change",
                                    extractor="market", extraction_confidence=1.0))
            self.pending_alarms.append(a)
            if self.x is not None:
                for tid in (int(g["home_id"]), int(g["away_id"])):
                    self.queue_lookback(tid, now)
        missed = missed_news(self.pending_alarms, self.last_change, now)
        self.status.put_health([health_row("missed_news", a.at, f"game {a.game_id}: market moved {a.move:.2f}",
                                           a.move, game_id=a.game_id) for a in missed])
        self.pending_alarms = [a for a in self.pending_alarms if pd.Timestamp(now) < pd.Timestamp(a.at)
                               + timedelta(minutes=30)]
        return out

    def _news_for(self, tid: int, now: datetime) -> list[Evidence]:
        name = self.team_names[tid]
        xml = self.session.get(rss_url(name), timeout=20).text
        fresh = [i for i in parse_rss(xml) if i["published"] is None or now - i["published"] <= NEWS_MAX_AGE]
        qbs = self.players[self.season].qbs(tid)
        return self._read(lambda ex: news_evidence(fresh, name, tid, qbs, ex, now,
                                                   game_id=next_game(tid, now, self.games)), now)

    def news_watched_interval(self, now: datetime) -> float:
        n = self.watched_count(now)
        return NEWS_WATCHED_EVERY / n if n else NEWS_WATCHED_EVERY

    def news_watched_run(self, now: datetime) -> list[Evidence]:
        watched = sorted(watched_teams(self.watches, now))
        if not watched:
            return []
        tid = watched[self.watch_cursor % len(watched)]
        self.watch_cursor += 1
        return self._news_for(tid, now)

    def news_sweep_interval(self, now: datetime) -> float:
        return NEWS_SWEEP_EVERY / max(len(self.fbs_ids), 1)

    def news_sweep_run(self, now: datetime) -> list[Evidence]:
        if not self.fbs_ids:
            return []
        tid = self.fbs_ids[self.news_cursor % len(self.fbs_ids)]
        self.news_cursor += 1
        return self._news_for(tid, now)

    def manual_run(self, now: datetime) -> list[Evidence]:
        pulled = self.status.pull_new("manual")
        teams = {e.team_id for e in pulled if e.team_id is not None}
        if teams:
            self.recompute(teams, pulled, now)
        return []

    def accuracy_run(self, now: datetime) -> list[Evidence]:
        kick = pd.to_datetime(self.games["kickoff"], utc=True)
        done = self.games[(kick < pd.Timestamp(now) - timedelta(hours=24))
                          & (kick > pd.Timestamp(now) - timedelta(hours=72))
                          & ~self.games["game_id"].isin(self.accuracy_done)]
        if done.empty:
            return []
        starters = game_starters(self.passers[self.passers["game_id"].isin(done["game_id"])])
        name_to_id = {n: i for i, n in self.team_names.items()}
        actual = pd.DataFrame({"game_id": starters["game_id"], "team_id": starters["offense"].map(name_to_id),
                               "passer_key": starters["passer_key"].astype(str)}).dropna(subset=["team_id"])
        index = self.players[self.season]
        actual["player_id"] = [index.player_for_key(int(t), k) or k for t, k in zip(actual["team_id"],
                                                                                      actual["passer_key"])]
        rows = []
        for kickoff, group in done.groupby("kickoff"):
            snaps = self.status.snapshots(as_of=pd.Timestamp(kickoff).to_pydatetime())
            for g in group.itertuples(index=False):
                s = snaps[snaps["game_id"] == g.game_id]
                a = actual[actual["game_id"] == g.game_id]
                if len(s) and len(a):
                    rows += kickoff_accuracy(s, a, pd.Timestamp(g.kickoff).to_pydatetime())
                    self.accuracy_done.add(int(g.game_id))
        self.status.put_health(rows)
        return []

    def refresh_run(self, now: datetime) -> list[Evidence]:
        self.refresh()
        usd = getattr(self.extractor, "usd", None)
        if usd is not None:
            self.status.put_health([health_row("claude_spend", now, f"{self.extractor.name} reads since start", usd())])
        return []

    def recompute_all_run(self, now: datetime) -> list[Evidence]:
        self.recompute(set(self.fbs_ids), [], now)
        return []


STATE_PUSH_EVERY = 60.0


def build_service(ctx: Context, espn: bool = True, market: bool = True, news: bool = True,
                  mirror: StateMirror | None = None) -> Service:
    tasks = [Task("reports", ctx.reports_interval, ctx.reports_run)]
    if espn:
        tasks.append(Task("espn", ctx.espn_interval, ctx.espn_run))
    if market:
        tasks.append(Task("market", lambda now: 120.0, ctx.market_run))
    if news:
        tasks += [Task("news_watched", ctx.news_watched_interval, ctx.news_watched_run),
                  Task("news_sweep", ctx.news_sweep_interval, ctx.news_sweep_run)]
    x = getattr(ctx, "x", None)
    if x is not None and getattr(x, "enabled", False) is True:
        tasks += [Task("x_sweep", ctx.x_sweep_interval, ctx.x_sweep_run),
                  Task("x_watch", ctx.x_watch_interval, ctx.x_watch_run),
                  Task("x_lookback", lambda now: 15.0, ctx.x_lookback_run)]
    tasks += [Task("manual", lambda now: 10.0, ctx.manual_run),
              Task("recompute_all", lambda now: HOUR, ctx.recompute_all_run),
              Task("refresh", lambda now: HOUR, ctx.refresh_run),
              Task("accuracy", lambda now: HOUR, ctx.accuracy_run)]
    if mirror is not None:
        def push_state(now: datetime) -> list[Evidence]:
            mirror.push()
            return []
        tasks.append(Task("state_push", lambda now: STATE_PUSH_EVERY, push_state))
    return Service(tasks, ctx.status, recompute=ctx.recompute)


def service_context(research: DatasetStore, status: StatusStore, season: int, use_x: bool = True,
                    use_claude: bool = True, say: Callable[[str], None] = print,
                    cache_dir: Path | None = None) -> Context:
    """The Context with X and Claude switched on where their keys can be found (environment, else Secrets Manager)."""
    x = XSource(budget=ReadBudget(10**9)) if use_x else None   # XBudget is the real governor
    if x is not None and not x.enabled:
        say("X is off: no X_BEARER_TOKEN in the environment and no gnome/pipelines/x secret")
    extractor = None
    if use_claude:
        try:
            extractor = ClaudeExtractor()
        except NotConfigured as e:
            say(f"Claude is off ({e}); posts and headlines are read by rules and wait for review")
    return Context(research, status, season, cache_dir=cache_dir, x=x if x is not None and x.enabled else None,
                   extractor=extractor)


def _print_team(ctx: Context, team: str, now: datetime) -> None:
    tid = ctx.teams.team_id(team, ctx.season)
    snaps = ctx.status.snapshots()
    s = snaps[snaps["team_id"] == tid].sort_values("p_start", ascending=False) if tid is not None else snaps.iloc[0:0]
    names = dict(zip(ctx.rosters["player_id"].astype(str), ctx.rosters["first_name"] + " " + ctx.rosters["last_name"]))
    print(f"{team} (team {tid}), next game {next_game(tid, now, ctx.games) if tid else None}:")
    for r in s.itertuples(index=False):
        print(f"  {names.get(r.player_id, r.player_id):<22} p_start {r.p_start:.2f}  official {r.official_status} "
              f"({r.official_slot})  {r.reason}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--once", action="store_true", help="run every source once and exit")
    ap.add_argument("--prod", action="store_true", help="write the status tables to the shared prod catalog")
    ap.add_argument("--capture-dir", type=Path, help="load the interim report capture first")
    ap.add_argument("--no-espn", action="store_true")
    ap.add_argument("--no-market", action="store_true")
    ap.add_argument("--no-news", action="store_true")
    ap.add_argument("--no-x", action="store_true", help="don't search X even if a key is configured")
    ap.add_argument("--no-claude", action="store_true", help="read posts and headlines with rules only")
    ap.add_argument("--load-registry", type=Path, metavar="CSV",
                    help="load the reviewed X reporter registry into the sources table and exit")
    ap.add_argument("--health", action="store_true", help="print the last 7 days' health summary and exit")
    ap.add_argument("--team", default="Mississippi State", help="team to print after --once")
    a = ap.parse_args(argv)
    now = utcnow()
    season = now.year if now.month >= 7 else now.year - 1
    status = StatusStore(open_store(a.prod))
    if a.health:
        print(health_report(status.health(), now - timedelta(days=7)))
        return
    if a.load_registry:
        status.put_sources(registry_frame(pd.read_csv(a.load_registry), vetted_by="user", now=now))
        print(f"loaded {len(status.sources())} X sources")
        return
    ctx = service_context(DatasetStore(), status, season, use_x=not a.no_x, use_claude=not a.no_claude)
    if a.capture_dir:
        new = status.add_evidence_new(capture_evidence(a.capture_dir, ctx.teams, ctx.players, ctx.games))
        print(f"loaded {len(new)} new evidence rows from the capture")
    svc = build_service(ctx, espn=not a.no_espn, market=not a.no_market, news=not a.no_news)
    if a.once:
        svc.step(now)
        ctx.recompute_all_run(now)
        _print_team(ctx, a.team, now)
        return
    svc.run_forever()


if __name__ == "__main__":
    main()
