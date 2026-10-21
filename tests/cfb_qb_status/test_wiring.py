from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex
from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.market_alarm import Alarm
from gnome_pipelines.cfb.qb_status.review import decide
from gnome_pipelines.cfb.qb_status.service import (Context, build_service, fbs_games, fresh_alarms, resolve_flags)
from gnome_pipelines.cfb.qb_status.store import StatusStore

T0 = datetime(2026, 10, 24, 20, 0, tzinfo=timezone.utc)
ROSTERS = pd.DataFrame([
    {"season": 2026, "player_id": "9000001", "first_name": "Avery", "last_name": "Stone", "team": "Testville State",
     "position": "QB", "jersey": 1.0, "team_id": 501},
    {"season": 2026, "player_id": "9000002", "first_name": "Blake", "last_name": "Rowe", "team": "Testville State",
     "position": "QB", "jersey": 12.0, "team_id": 501},
    {"season": 2026, "player_id": "9000003", "first_name": "Casey", "last_name": "Vale", "team": "Testville State",
     "position": "WR", "jersey": 4.0, "team_id": 501},
])
PASSERS = pd.DataFrame(columns=["season", "game_id", "offense", "passer", "passer_key", "play_seq"])


def flag(status, raw):
    return Evidence(evidence_id=f"{status}-{raw}", source_type="pbp_flag", source_id="401", source_org="espn",
                    first_seen_at=T0, team="Testville State", status=status, season=2026, player_raw=raw)


def test_fbs_games_keep_fcs_opponents():
    games = pd.DataFrame([{"game_id": 1, "home_classification": "fbs", "away_classification": "fcs"},
                          {"game_id": 2, "home_classification": "fcs", "away_classification": "fcs"},
                          {"game_id": 3, "home_classification": "fbs", "away_classification": "fbs"}])
    assert fbs_games(games)["game_id"].tolist() == [1, 3]


def test_resolve_flags_keeps_rostered_qbs_only():
    index = PlayerIndex(ROSTERS, PASSERS, 2026)
    out = resolve_flags([flag("qb_change", "Blake Rowe"), flag("qb_change", "Casey Vale"),
                         flag("left_game_injured", "Stone")], {"Testville State": 501}, index)
    assert [(f.status, f.player_id, f.team_id) for f in out] == [("qb_change", "9000002", 501),
                                                                 ("left_game_injured", "9000001", 501)]


def test_fresh_alarms_fire_once_per_move():
    until: dict[int, datetime] = {}
    a = fresh_alarms([Alarm(8, T0, 0.2)], until, T0)
    b = fresh_alarms([Alarm(8, T0 + timedelta(minutes=40), 0.2)], until, T0 + timedelta(minutes=40))
    c = fresh_alarms([Alarm(8, T0 + timedelta(minutes=70), 0.2)], until, T0 + timedelta(minutes=70))
    assert (len(a), len(b), len(c)) == (1, 0, 1)


class FakeCtx:
    status = None

    def __init__(self, watched):
        self._watched = watched

    def watched_count(self, now):
        return self._watched

    def __getattr__(self, name):
        return lambda *a, **k: 600.0 if name.endswith("interval") else []


def test_watched_news_rotation_runs_every_ten_minutes_per_team():
    svc = build_service(FakeCtx(2), espn=False, market=False)
    tasks = {t.name: t for t in svc.tasks}
    assert {"news_watched", "news_sweep", "manual"} <= set(tasks)
    assert Context.news_watched_interval(FakeCtx(2), T0) == pytest.approx(300.0)
    assert Context.news_watched_interval(FakeCtx(0), T0) == pytest.approx(600.0)


class Research:
    def __init__(self, fail):
        self.fail = fail

    def load(self, name, **kw):
        if self.fail:
            raise RuntimeError("SSO session expired")
        if name == "cfb_games":
            return pd.DataFrame([{"game_id": 1, "season": 2026, "kickoff": pd.Timestamp("2026-10-24T19:00Z"),
                                  "home_team": "Testville State", "home_id": 501, "away_team": "Example Tech",
                                  "away_id": 502, "home_classification": "fbs", "away_classification": "fbs",
                                  "completed": False, "conference_game": True, "home_conference": "SEC"}])
        if name == "cfb_rosters":
            return ROSTERS
        return PASSERS


def test_context_starts_from_cache_when_research_reads_fail(tmp_path):
    status = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
    Context(Research(fail=False), status, 2026, cache_dir=tmp_path / "cache")
    ctx = Context(Research(fail=True), status, 2026, cache_dir=tmp_path / "cache")
    assert ctx.team_names == {501: "Testville State", 502: "Example Tech"}
    with pytest.raises(RuntimeError):
        Context(Research(fail=True), status, 2026, cache_dir=tmp_path / "empty")


def test_manual_decisions_are_pulled_from_another_process(tmp_path):
    wh = tmp_path / "wh"
    service_side = StatusStore(DatasetStore(CatalogConfig(local_warehouse=wh)))
    service_side.add_evidence([Evidence(evidence_id="n1", source_type="news", source_id="g", source_org="x",
                                        first_seen_at=T0, team="Testville State", status="out", season=2026,
                                        team_id=501, review_state="pending")])
    review_side = StatusStore(DatasetStore(CatalogConfig(local_warehouse=wh)))
    decide(review_side, "n1", "confirm", "user", player_id="9000001", game_id=1, now=T0 + timedelta(minutes=1))
    pulled = service_side.pull_new("manual")
    assert [(e.player_id, e.game_id, e.review_state) for e in pulled] == [("9000001", 1, "confirmed")]
    assert service_side.pull_new("manual") == []
