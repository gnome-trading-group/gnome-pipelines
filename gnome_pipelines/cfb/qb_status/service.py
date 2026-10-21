"""The QB Status Service: every poller on its own cadence in one process, laptop-sleep aware.

    AWS_PROFILE=... AWS_DEFAULT_REGION=us-east-1 poetry run python -m gnome_pipelines.cfb.qb_status.service [--once]

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

from gnome_pipelines.cfb.qb_status.availability import CONFERENCES, FeedError, fetch_conference, report_evidence
from gnome_pipelines.cfb.qb_status.backfill import capture_evidence
from gnome_pipelines.cfb.qb_status.candidates import candidates, game_starters, next_game, with_roster_ids
from gnome_pipelines.cfb.qb_status.crosswalk import MATCH_OK, PlayerIndex, TeamIndex
from gnome_pipelines.cfb.qb_status.engine import team_status
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id
from gnome_pipelines.cfb.qb_status.extract import RuleExtractor
from gnome_pipelines.cfb.qb_status.health import health_report, health_row, kickoff_accuracy, report_lag_rows
from gnome_pipelines.cfb.qb_status.market_alarm import EVENTS, SERIES, Alarm, alarms, kalshi_home_probs, missed_news
from gnome_pipelines.cfb.qb_status.news import news_evidence, parse_rss, rss_url
from gnome_pipelines.cfb.qb_status.pbp_flags import SCOREBOARD, SUMMARY, game_flags, live_games
from gnome_pipelines.cfb.qb_status.schedule import (WINDOW_AFTER, expected_releases, report_poll_interval,
                                                     schedule_rows)
from gnome_pipelines.cfb.qb_status.store import StatusStore, open_store
from gnome_pipelines.cfb.qb_status.watches import update_watches, watched_teams

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
    def __init__(self, tasks: list[Task], store: StatusStore,
                 recompute: Callable[[set[int], list[Evidence], datetime], None] | None = None,
                 clock: Callable[[], datetime] = utcnow, sleep: Callable[[float], None] = time.sleep) -> None:
        self.tasks, self.store, self.recompute, self.clock, self.sleep = tasks, store, recompute, clock, sleep
        self.next_due: dict[str, datetime] = {}
        self.last_step: datetime | None = None

    def step(self, now: datetime) -> None:
        smallest = min(t.interval(now) for t in self.tasks)
        if self.last_step is not None:
            elapsed = (now - self.last_step).total_seconds()
            if elapsed > 3 * smallest + 60:
                self.store.put_health([health_row("gap", now, f"no step for {elapsed:.0f}s; catching up", elapsed)])
                self.next_due = {}
        new: list[Evidence] = []
        for t in self.tasks:
            due = self.next_due.get(t.name)
            if due is not None and now < due:
                continue
            try:
                new += self.store.add_evidence_new(t.run(now) or [])
            except Exception as e:
                self.store.put_health([health_row("source_error", now, f"{t.name}: {type(e).__name__}: {e}")])
            finally:
                self.next_due[t.name] = now + timedelta(seconds=t.interval(now))
        teams = {e.team_id for e in new if e.team_id is not None}
        if teams and self.recompute is not None:
            self.recompute(teams, new, now)
        self.last_step = now

    def run_forever(self) -> None:
        while True:
            self.step(self.clock())
            self.last_step = self.clock()
            self.sleep(1.0)


class Context:
    """Everything the real pollers share: the research data, indexes, watches and in-memory market history."""

    def __init__(self, research: DatasetStore, status: StatusStore, season: int) -> None:
        self.research, self.status, self.season = research, status, season
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        self.live: list[str] = []
        self.market: list[dict] = []
        self.pending_alarms: list[Alarm] = []
        self.last_change: dict[int, datetime] = {}
        self.news_cursor = 0
        self.refresh()
        self.watches = status.watches()
        ev = status.evidence()
        official = ev[ev["source_type"] == "official_report"].dropna(subset=["game_id"])
        self.observed = {(r.source_org, int(r.game_id), r.report_slot): pd.Timestamp(r.first_seen_at)
                         for r in official.sort_values("first_seen_at", ascending=False).itertuples(index=False)}

    def refresh(self) -> None:
        games = self.research.load("cfb_games", row_filter=EqualTo("season", self.season))
        fbs = (games["home_classification"] == "fbs") & (games["away_classification"] == "fbs")
        self.games = games[fbs].reset_index(drop=True)
        self.rosters = self.research.load("cfb_rosters", row_filter=EqualTo("season", self.season))
        self.passers = self.research.load("cfb_passers", row_filter=EqualTo("season", self.season))
        self.teams = TeamIndex(self.games)
        self.players = {self.season: PlayerIndex(self.rosters, self.passers, self.season)}
        self.kickoffs = dict(zip(self.games["game_id"].astype(int), pd.to_datetime(self.games["kickoff"], utc=True)))
        names = pd.concat([self.games[["home_id", "home_team"]].set_axis(["id", "name"], axis=1),
                           self.games[["away_id", "away_team"]].set_axis(["id", "name"], axis=1)])
        self.team_names = {int(i): n for i, n in names.drop_duplicates("id").itertuples(index=False)}

    def upcoming_conference_games(self, now: datetime, days: int) -> pd.DataFrame:
        kick = pd.to_datetime(self.games["kickoff"], utc=True)
        g = self.games[(kick >= pd.Timestamp(now)) & (kick <= pd.Timestamp(now) + timedelta(days=days))
                       & self.games["conference_game"].fillna(False).astype(bool)]
        return g.assign(code=g["home_conference"].map(CONF_CODES)).dropna(subset=["code"])

    def team_candidates(self, team_id: int, now: datetime) -> pd.DataFrame:
        c = candidates(self.team_names.get(team_id, ""), team_id, self.season, now, self.passers, self.games)
        return with_roster_ids(c, team_id, self.players[self.season])

    def last_starter_name(self, team_id: int, now: datetime) -> str | None:
        c = self.team_candidates(team_id, now)
        if c.empty:
            return None
        pid = c.loc[c["last_starter"], "player_id"].iloc[0]
        r = self.rosters[self.rosters["player_id"].astype(str) == pid]
        return str(r["last_name"].iloc[0]).lower() if len(r) else None

    def recompute(self, teams: set[int], new: list[Evidence], now: datetime) -> None:
        evidence = self.status.evidence()
        for tid in teams:
            name = self.team_names.get(tid)
            game = next_game(tid, now, self.games) if name else None
            if game is None:
                continue
            cands = self.team_candidates(tid, now)
            if self.status.put_snapshots(team_status(game, tid, cands, evidence, now), now):
                self.last_change[game] = now
        self.watches = update_watches(self.watches, new, self.kickoffs, now)
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
        out, errors = [], []
        for conf in CONFERENCES:
            try:
                payload = fetch_conference(self.session, conf)
            except (FeedError, requests.RequestException) as e:
                errors.append(health_row("source_error", now, f"reports {conf}: {e}"))
                continue
            out += report_evidence(conf, payload, now, self.teams, self.players, self.games)
        new_obs = {}
        for e in out:
            key = (e.source_org, e.game_id, e.report_slot)
            if e.game_id is not None and key not in self.observed:
                self.observed[key] = pd.Timestamp(now)
                new_obs[key] = now
        sched = [schedule_rows(g.code, int(g.game_id), pd.Timestamp(g.kickoff).to_pydatetime(),
                               {s: t for (c, gid, s), t in self.observed.items() if c == g.code and gid == g.game_id},
                               now) for g in self.upcoming_conference_games(now, 7).itertuples(index=False)]
        if sched:
            frame = pd.concat(sched, ignore_index=True)
            self.status.put_health(report_lag_rows(frame.assign(state="expected"), new_obs) + errors)
            self.status.put_schedule(frame)
        elif errors:
            self.status.put_health(errors)
        return out

    def espn_interval(self, now: datetime) -> float:
        return 30.0 if self.live else 600.0

    def espn_run(self, now: datetime) -> list[Evidence]:
        board = self.session.get(SCOREBOARD, params={"groups": 80, "limit": 300}, timeout=20).json()
        self.live = live_games(board)
        out = []
        for gid in self.live:
            summary = self.session.get(SUMMARY, params={"event": gid}, timeout=20).json()
            comps = summary["header"]["competitions"][0]["competitors"]
            ids = {c["team"]["location"]: int(c["team"]["id"]) for c in comps}
            starters = {loc: s for loc, tid in ids.items() if (s := self.last_starter_name(tid, now))}
            for f in game_flags(summary, starters, now):
                f.team_id = ids.get(f.team)
                if f.team_id is None:
                    f.review_state = "pending"
                    continue
                m = self.players[self.season].match(f.team_id, f.player_raw or "", None)
                f.player_id, f.match_confidence = m.player_id, m.confidence
                if m.player_id is None or m.confidence < MATCH_OK:
                    f.review_state = "pending"
                out.append(f)
        return out

    def market_run(self, now: datetime) -> list[Evidence]:
        events, cursor = [], ""
        while True:
            params = {"series_ticker": SERIES, "status": "open", "with_nested_markets": "true", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            d = self.session.get(EVENTS, params=params, timeout=30).json()
            events += d.get("events", [])
            cursor = d.get("cursor") or ""
            if not cursor:
                break
        for game_id, p in kalshi_home_probs(events, self.games, self.teams).items():
            self.market.append({"game_id": game_id, "at": pd.Timestamp(now), "p_home": p})
        self.market = [m for m in self.market if pd.Timestamp(now) - m["at"] <= timedelta(hours=2)]
        out = []
        for a in alarms(pd.DataFrame(self.market), self.last_change, now):
            g = self.games[self.games["game_id"] == a.game_id].iloc[0]
            hour = pd.Timestamp(now).floor("h").isoformat()
            for tid in (int(g["home_id"]), int(g["away_id"])):
                out.append(Evidence(evidence_id=evidence_id("market_alarm", a.game_id, tid, hour),
                                    source_type="market_alarm", source_id=f"{SERIES}:{a.game_id}", source_org="kalshi",
                                    first_seen_at=now, team=self.team_names.get(tid, ""), status="market_alarm",
                                    season=self.season, team_id=tid, game_id=a.game_id, hedge="unclear",
                                    paraphrase=f"Kalshi moved {a.move:.2f} logit in 60 min with no QB status change",
                                    extractor="market", extraction_confidence=1.0))
            if all(p.game_id != a.game_id for p in self.pending_alarms):
                self.pending_alarms.append(a)
        missed = missed_news(self.pending_alarms, self.last_change, now)
        self.status.put_health([health_row("missed_news", a.at, f"game {a.game_id}: market moved {a.move:.2f}",
                                           a.move, game_id=a.game_id) for a in missed])
        self.pending_alarms = [a for a in self.pending_alarms if pd.Timestamp(now) < pd.Timestamp(a.at)
                               + timedelta(minutes=30)]
        return out

    def news_interval(self, now: datetime) -> float:
        watched = watched_teams(self.watches, now)
        n = max(len(self.team_names), 1)
        return min(NEWS_WATCHED_EVERY / max(len(watched), 1), NEWS_SWEEP_EVERY / n) if watched else NEWS_SWEEP_EVERY / n

    def news_run(self, now: datetime) -> list[Evidence]:
        watched = sorted(watched_teams(self.watches, now))
        everyone = sorted(self.team_names)
        queue = watched + everyone
        tid = queue[self.news_cursor % len(queue)]
        self.news_cursor += 1
        name = self.team_names[tid]
        xml = self.session.get(rss_url(name), timeout=20).text
        fresh = [i for i in parse_rss(xml) if i["published"] is None or now - i["published"] <= NEWS_MAX_AGE]
        qbs = self.players[self.season].qbs(tid)
        return news_evidence(fresh, name, tid, qbs, RuleExtractor(), now, game_id=next_game(tid, now, self.games))

    def accuracy_run(self, now: datetime) -> list[Evidence]:
        kick = pd.to_datetime(self.games["kickoff"], utc=True)
        done = self.games[(kick < pd.Timestamp(now) - timedelta(hours=24))
                          & (kick > pd.Timestamp(now) - timedelta(hours=72))]
        starters = game_starters(self.passers[self.passers["game_id"].isin(done["game_id"])])
        name_to_id = {n: i for i, n in self.team_names.items()}
        actual = pd.DataFrame({"game_id": starters["game_id"], "team_id": starters["offense"].map(name_to_id),
                               "passer_key": starters["passer_key"].astype(str)}).dropna(subset=["team_id"])
        index = self.players[self.season]
        actual["player_id"] = [index.player_for_key(int(t), k) or k for t, k in zip(actual["team_id"],
                                                                                      actual["passer_key"])]
        rows = []
        for g in done.itertuples(index=False):
            snaps = self.status.snapshots(as_of=pd.Timestamp(g.kickoff).to_pydatetime())
            snaps = snaps[snaps["game_id"] == g.game_id]
            if len(snaps):
                rows += kickoff_accuracy(snaps, actual[actual["game_id"] == g.game_id],
                                         pd.Timestamp(g.kickoff).to_pydatetime())
        self.status.put_health(rows)
        return []

    def refresh_run(self, now: datetime) -> list[Evidence]:
        self.refresh()
        return []

    def recompute_all_run(self, now: datetime) -> list[Evidence]:
        self.recompute(set(self.team_names), [], now)
        return []


def build_service(ctx: Context, espn: bool = True, market: bool = True, news: bool = True) -> Service:
    tasks = [Task("reports", ctx.reports_interval, ctx.reports_run)]
    if espn:
        tasks.append(Task("espn", ctx.espn_interval, ctx.espn_run))
    if market:
        tasks.append(Task("market", lambda now: 120.0, ctx.market_run))
    if news:
        tasks.append(Task("news", ctx.news_interval, ctx.news_run))
    tasks += [Task("recompute_all", lambda now: HOUR, ctx.recompute_all_run),
              Task("refresh", lambda now: HOUR, ctx.refresh_run),
              Task("accuracy", lambda now: HOUR, ctx.accuracy_run)]
    return Service(tasks, ctx.status, recompute=ctx.recompute)


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
    ap.add_argument("--health", action="store_true", help="print the last 7 days' health summary and exit")
    ap.add_argument("--team", default="Mississippi State", help="team to print after --once")
    a = ap.parse_args(argv)
    now = utcnow()
    season = now.year if now.month >= 7 else now.year - 1
    status = StatusStore(open_store(a.prod))
    if a.health:
        print(health_report(status.health(), now - timedelta(days=7)))
        return
    ctx = Context(DatasetStore(), status, season)
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
