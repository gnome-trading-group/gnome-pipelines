from datetime import datetime, timedelta, timezone

import pandas as pd
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.health import kickoff_accuracy, report_lag_rows
from gnome_pipelines.cfb.qb_status.service import Service, Task
from gnome_pipelines.cfb.qb_status.store import StatusStore

T0 = datetime(2026, 10, 22, 0, 0, tzinfo=timezone.utc)


def ev(eid, team_id=344, at=T0):
    return Evidence(evidence_id=eid, source_type="official_report", source_id="s", source_org="SEC", first_seen_at=at,
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
