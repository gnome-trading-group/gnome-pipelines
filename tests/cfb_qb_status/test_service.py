from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.health import kickoff_accuracy, news_lag_rows, report_lag_rows
from gnome_pipelines.cfb.qb_status.service import Service, Task
from gnome_pipelines.cfb.qb_status.store import StatusStore

T0 = datetime(2026, 10, 22, 0, 0, tzinfo=timezone.utc)


def ev(eid, team_id=344, at=T0):
    return Evidence(evidence_id=eid, source_type="official_report", source_id=f"src-{eid}", source_org="SEC", first_seen_at=at,
                    team="Mississippi State", status="out", season=2026, team_id=team_id, player_id="T", game_id=8,
                    report_slot="Initial")


def store(tmp_path):
    return StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path)))


def test_add_evidence_is_idempotent(tmp_path):
    s = store(tmp_path)
    assert s.add_evidence([ev("a"), ev("b")]) == 2
    assert s.add_evidence([ev("a"), ev("b"), ev("c")]) == 1
    assert sorted(s.evidence()["evidence_id"]) == ["a", "b", "c"]


def test_put_snapshots_only_publishes_changes(tmp_path):
    s = store(tmp_path)
    snap = pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": "T", "p_start": 0.8, "official_status": None,
                          "official_slot": None, "evidence_ids": [], "reason": "base", "rule_version": "v1"}])
    assert s.put_snapshots(snap, T0) == 1
    assert s.put_snapshots(snap, T0 + timedelta(hours=1)) == 0
    assert s.put_snapshots(snap.assign(p_start=0.2), T0 + timedelta(hours=2)) == 1


def test_a_new_rule_version_republishes_unchanged_numbers(tmp_path):
    s = store(tmp_path)
    snap = pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": "T", "p_start": 0.8, "official_status": None,
                          "official_slot": None, "evidence_ids": [], "reason": "base", "rule_version": "v1"}])
    s.put_snapshots(snap, T0)
    assert s.put_snapshots(snap.assign(rule_version="v2"), T0 + timedelta(hours=1)) == 1
    assert s.snapshots()["rule_version"].tolist() == ["v2"]


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_failing_task_records_health_and_others_run(tmp_path):
    s = store(tmp_path)
    ran = []

    def boom(now):
        raise ValueError("feed down")

    def ok(now):
        ran.append(now)
        return [ev("x", at=now)]
    svc = Service([Task("bad", lambda now: 60, boom), Task("good", lambda now: 60, ok)], s)
    svc.step(T0)
    assert ran == [T0]
    h = s.health()
    assert list(h["kind"]) == ["source_error"] and "bad: ValueError: feed down" in h["detail"].iloc[0]


def test_sleep_gap_records_gap_and_runs_all_tasks_with_wake_time(tmp_path):
    s = store(tmp_path)
    seen = []

    def report(now):
        seen.append(now)
        return [ev(f"r{len(seen)}", at=now)]
    svc = Service([Task("reports", lambda now: 1800, report), Task("espn", lambda now: 30, lambda now: [])], s)
    svc.step(T0)
    wake = T0 + timedelta(hours=5)
    svc.step(wake)
    assert seen == [T0, wake]
    h = s.health()
    gap = h[h["kind"] == "gap"]
    assert len(gap) == 1 and gap["value"].iloc[0] == 5 * 3600
    assert pd.Timestamp(s.evidence().set_index("evidence_id").loc["r2", "first_seen_at"]) == pd.Timestamp(wake)


def test_new_evidence_triggers_snapshot_for_that_team_only(tmp_path):
    s = store(tmp_path)
    calls = []
    svc = Service([Task("t", lambda now: 60, lambda now: [ev("a", team_id=344)])], s,
                  recompute=lambda teams, new, now: calls.append(set(teams)))
    svc.step(T0)
    svc.step(T0 + timedelta(seconds=61))
    assert calls == [{344}]


def test_kickoff_accuracy_compares_top_candidate_to_first_passer():
    snaps = pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": "T", "p_start": 0.3},
                          {"game_id": 8, "team_id": 344, "player_id": "P", "p_start": 0.7},
                          {"game_id": 8, "team_id": 201, "player_id": "M", "p_start": 0.9}])
    actual = pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": "P"},
                           {"game_id": 8, "team_id": 201, "player_id": "Z"}])
    rows = {r["team_id"]: r["value"] for r in kickoff_accuracy(snaps, actual, T0)}
    assert rows == {344: 1.0, 201: 0.0}


def test_report_lag_row_on_arrival():
    sched = pd.DataFrame([{"conference": "SEC", "game_id": 8, "report_slot": "Initial",
                           "expected_at": pd.Timestamp("2026-10-22T00:00Z"), "observed_at": pd.NaT,
                           "state": "expected"}])
    rows = report_lag_rows(sched, {("SEC", 8, "Initial"): T0 + timedelta(minutes=7)})
    assert len(rows) == 1 and rows[0]["kind"] == "report_lag" and rows[0]["value"] == 420


def snap_rows(**players):
    return pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": pid, "p_start": p, "official_status": None,
                          "official_slot": None, "evidence_ids": [], "reason": "r", "rule_version": "v1"}
                         for pid, p in players.items()])


def test_dropped_candidate_is_zeroed_so_latest_rows_sum_to_one(tmp_path):
    s = store(tmp_path)
    s.put_snapshots(snap_rows(A=0.5, unknown=0.5), T0)
    s.put_snapshots(snap_rows(A=1.0), T0 + timedelta(hours=1))
    latest = s.snapshots()
    assert latest["p_start"].sum() == pytest.approx(1.0)
    assert latest.set_index("player_id").loc["unknown", "p_start"] == 0.0


def official(status, at, eid=None):
    return Evidence(evidence_id=eid or f"{status}", source_type="official_report", source_id="SEC:Team:Player:2026-10-24:Update 2",
                    source_org="SEC", first_seen_at=at, team="Mississippi State", status=status, season=2026,
                    team_id=344, player_id="9000001", game_id=8, report_slot="Update 2")


def test_status_flip_back_within_a_slot_is_recorded(tmp_path):
    s = store(tmp_path)
    assert s.add_evidence([official("questionable", T0)]) == 1
    assert s.add_evidence([official("out", T0 + timedelta(minutes=30))]) == 1
    assert s.add_evidence([official("questionable", T0 + timedelta(hours=1))]) == 1
    assert s.add_evidence([official("questionable", T0 + timedelta(hours=2))]) == 0
    latest = s.evidence().sort_values("first_seen_at").iloc[-1]
    assert latest["status"] == "questionable" and pd.Timestamp(latest["first_seen_at"]) == pd.Timestamp(T0 + timedelta(hours=1))


class BrokenStore:
    def load(self, *a, **k):
        raise ConnectionError("catalog unavailable")

    def publish(self, *a, **k):
        raise AssertionError("must not publish after a failed load")


def test_failed_load_is_raised_not_treated_as_empty():
    with pytest.raises(ConnectionError):
        StatusStore(BrokenStore()).add_evidence([ev("a")])


def test_evidence_is_cached_after_first_load(tmp_path):
    s = store(tmp_path)
    s.add_evidence([ev("a")])
    calls = []
    real = s.store.load
    s.store.load = lambda *a, **k: (calls.append(a), real(*a, **k))[1]
    s.evidence()
    s.add_evidence([ev("b")])
    assert sorted(s.evidence()["evidence_id"]) == ["a", "b"] and len(calls) <= 1


def test_news_lag_rows_measure_publish_to_capture():
    e = ev("n")
    e.published_at = T0 - timedelta(minutes=12)
    no_pub = ev("m")
    rows = news_lag_rows([e, no_pub], changed_teams={344})
    assert [(r["kind"], r["value"]) for r in rows] == [("news_lag", 720.0)]
    assert news_lag_rows([e], changed_teams=set()) == []


class SteppingClock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_clock_jump_inside_a_task_restamps_and_records_gap(tmp_path):
    s = store(tmp_path)
    clock = SteppingClock(T0)

    def sleepy(now):
        clock.t = now + timedelta(hours=8)
        return [ev("late", at=now)]
    svc = Service([Task("reports", lambda now: 1800, sleepy)], s, clock=clock)
    svc.step(T0)
    stamped = pd.Timestamp(s.evidence().set_index("evidence_id").loc["late", "first_seen_at"])
    assert stamped == pd.Timestamp(T0 + timedelta(hours=8))
    gap = s.health()
    assert list(gap["kind"]) == ["gap"] and gap["value"].iloc[0] == 8 * 3600


def test_failed_task_retries_soon_with_backoff(tmp_path):
    s = store(tmp_path)
    runs = []

    def flaky(now):
        runs.append(now)
        raise ConnectionError("offline")
    svc = Service([Task("reports", lambda now: 1800, flaky)], s)
    svc.step(T0)
    svc.step(T0 + timedelta(seconds=16))
    svc.step(T0 + timedelta(seconds=30))
    svc.step(T0 + timedelta(seconds=47))
    assert runs == [T0, T0 + timedelta(seconds=16), T0 + timedelta(seconds=47)]


def test_recompute_error_is_recorded_not_raised(tmp_path):
    s = store(tmp_path)

    def broken(teams, new, now):
        raise KeyError("missing column")
    svc = Service([Task("t", lambda now: 60, lambda now: [ev("a")])], s, recompute=broken)
    svc.step(T0)
    assert "recompute: KeyError" in s.health()["detail"].iloc[0]


def test_sources_round_trip_and_republish_only_changes(tmp_path):
    from gnome_pipelines.cfb.qb_status.x_scan import registry_frame
    s = store(tmp_path)
    df = registry_frame(pd.DataFrame([{"platform": "x", "handle": "TestvilleBeat", "teams": "Testville State",
                                       "kind": "beat", "confidence": "normal"}]), vetted_by="user", now=T0)
    s.put_sources(df)
    s.put_sources(df.assign(added_at=pd.Timestamp(T0 + timedelta(days=1))))
    got = s.sources()
    assert got["handle"].tolist() == ["TestvilleBeat"] and list(got["teams"].iloc[0]) == ["Testville State"]
