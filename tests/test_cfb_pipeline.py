"""The CFB refresh job: which season it refreshes, the S3 response mirror, and that a failed quality check publishes
nothing (a bad CFBD response must never overwrite good data)."""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pandas as pd
import pytest

from gnome_pipelines import families
from gnome_pipelines.cfb import cache_sync, pipeline
from gnome_pipelines.cfb.pipeline import CfbUpdateParams
from gnome_pipelines.cfb.quality import Check

NOW = datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("when,season", [
    (datetime(2026, 8, 29), 2026), (datetime(2026, 12, 31), 2026), (datetime(2027, 1, 19), 2026),
    (datetime(2027, 6, 30), 2026), (datetime(2027, 7, 1), 2027),
])
def test_current_season_keeps_january_bowls_in_the_last_season(when, season):
    assert pipeline.current_season(when) == season


def test_freshness_flags_games_without_a_final():
    games = pd.DataFrame({
        "kickoff": pd.to_datetime(["2026-10-09 23:00", "2026-10-10 16:00", "2026-10-16 03:00", "2026-10-17 00:00"], utc=True),
        "completed": [True, False, False, False], "home_points": [24.0, None, None, None]})
    f = pipeline.freshness(games, NOW)
    assert f == {"games_completed": 1, "latest_completed_kickoff": "2026-10-09T23:00:00+00:00", "games_missing_final": 1}


def test_mirror_round_trip_pulls_missing_and_pushes_only_new_files(tmp_path):
    remote, local = tmp_path / "remote", tmp_path / "local"
    (remote / "plays").mkdir(parents=True)
    (remote / "plays" / "week-1_a.json.gz").write_bytes(b"w1")
    (local / "plays").mkdir(parents=True)
    (local / "plays" / "week-2_b.json.gz").write_bytes(b"w2-local")
    old = time.time() - 3600
    os.utime(local / "plays" / "week-2_b.json.gz", (old, old))
    assert cache_sync.pull(local, str(remote)) == 1
    assert (local / "plays" / "week-1_a.json.gz").read_bytes() == b"w1"
    since = time.time()
    (local / "games").mkdir()
    (local / "games" / "year-2026_c.json.gz").write_bytes(b"games")
    (local / "games" / "year-2026_c.tmp").write_bytes(b"partial")
    assert cache_sync.push(local, str(remote), since) == 1
    assert (remote / "games" / "year-2026_c.json.gz").read_bytes() == b"games"
    assert not (remote / "plays" / "week-2_b.json.gz").exists() and not (remote / "games" / "year-2026_c.tmp").exists()


class FakeClient:
    calls_made = 12

    def remaining_calls(self):
        return 2400


class FakeStore:
    def __init__(self):
        self.published = []

    def publish(self, spec, df):
        self.published.append(spec.name)


@pytest.fixture
def stubbed(monkeypatch):
    store = FakeStore()
    games = pd.DataFrame({"kickoff": pd.to_datetime(["2026-10-09 23:00"], utc=True), "completed": [True], "home_points": [24.0]})
    monkeypatch.setattr(pipeline, "CfbdClient", FakeClient)
    monkeypatch.setattr(pipeline.cache_sync, "pull", lambda local, remote: 3)
    monkeypatch.setattr(pipeline.cache_sync, "push", lambda local, remote, since: 5)
    monkeypatch.setattr(pipeline, "fetch", lambda client, years, refresh_current: None)
    monkeypatch.setattr(pipeline, "fetch_context", lambda client, years, refresh_current: None)
    monkeypatch.setattr(pipeline, "build_all", lambda client, years: {"cfb_games": games, "cfb_plays": pd.DataFrame({"x": [1, 2]})})
    monkeypatch.setattr(pipeline, "dataset_store", lambda: store)
    monkeypatch.setattr(pipeline.SourceData, "load", staticmethod(lambda ds: None))
    monkeypatch.setattr(pipeline, "build_features", lambda src, years: {"cfb_team_week_features": pd.DataFrame({"y": [1]})})
    return store


def test_hard_check_failure_publishes_nothing(stubbed, monkeypatch):
    monkeypatch.setattr(pipeline, "run_checks", lambda t: [Check("games present", False, "0 games")])
    with pytest.raises(RuntimeError, match="games present"):
        pipeline.CfbUpdatePipeline().run(CfbUpdateParams(season=2026, cache_root="s3://x/apicache/cfbd"))
    assert stubbed.published == []


def test_clean_run_publishes_datasets_then_features(stubbed, monkeypatch):
    monkeypatch.setattr(pipeline, "run_checks", lambda t: [Check("ok", True, ""), Check("soft", False, "1%", hard=False)])
    out = pipeline.CfbUpdatePipeline().run(CfbUpdateParams(season=2026, cache_root="s3://x/apicache/cfbd")).outputs
    assert stubbed.published == ["cfb_games", "cfb_plays", "cfb_team_week_features"]
    assert out["rows"] == {"cfb_games": 1, "cfb_plays": 2, "cfb_team_week_features": 1} and out["warnings"] == ["soft"]
    assert out["cache_pulled"] == 3 and out["cache_pushed"] == 5 and out["cfbd_remaining"] == 2400
    assert families.pipelines("cfb")["cfb_update"] is pipeline.CfbUpdatePipeline


def test_features_and_context_refresh_can_be_skipped(stubbed, monkeypatch):
    seen = []
    monkeypatch.setattr(pipeline, "run_checks", lambda t: [])
    monkeypatch.setattr(pipeline, "fetch_context", lambda client, years, refresh_current: seen.append(refresh_current))
    pipeline.CfbUpdatePipeline().run(CfbUpdateParams(season=2026, features=False, refresh_context=False,
                                                      cache_root="s3://x/apicache/cfbd"))
    assert stubbed.published == ["cfb_games", "cfb_plays"] and seen == [None]
    pipeline.CfbUpdatePipeline().run(CfbUpdateParams(season=2026, cache_root="s3://x/apicache/cfbd"))
    assert seen == [None, 2026]


def test_schedules_fire_in_season_only_and_the_afternoon_run_spares_context():
    sched = {s.name: s for s in pipeline.CfbUpdatePipeline.schedules}
    assert set(sched) == {"morning", "afternoon"}
    assert sched["afternoon"].params == {"refresh_context": False} and sched["morning"].params == {}
    for s in sched.values():
        CfbUpdateParams.model_validate(s.params)
        assert s.expression.endswith("* 1,8-12 ? *)")
    assert CfbUpdateParams.model_validate({"refresh_context": "false"}).refresh_context is False
