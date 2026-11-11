from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.health import kickoff_accuracy, news_lag_rows, report_lag_rows
from gnome_pipelines.cfb.qb_status.service import Context
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


def test_new_evidence_recomputes_only_the_teams_it_names():
    calls = []

    class Ctx:
        def recompute(self, teams, new, now):
            calls.append(set(teams))
    Context.on_new(Ctx(), [ev("a", team_id=344), ev("b", team_id=None)], T0)
    Context.on_new(Ctx(), [ev("c", team_id=None)], T0)
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


def test_sources_round_trip_and_republish_only_changes(tmp_path):
    from gnome_pipelines.cfb.qb_status.x_scan import registry_frame
    s = store(tmp_path)
    df = registry_frame(pd.DataFrame([{"platform": "x", "handle": "TestvilleBeat", "teams": "Testville State",
                                       "kind": "beat", "confidence": "normal"}]), vetted_by="user", now=T0)
    s.put_sources(df)
    s.put_sources(df.assign(added_at=pd.Timestamp(T0 + timedelta(days=1))))
    got = s.sources()
    assert got["handle"].tolist() == ["TestvilleBeat"] and list(got["teams"].iloc[0]) == ["Testville State"]


def test_copy_tables_replays_every_version_with_its_original_time(tmp_path):
    from gnome_pipelines.cfb.qb_status.store import copy_tables
    src, dst = store(tmp_path / "src"), store(tmp_path / "dst")
    src.add_evidence([ev("a")])
    snap = pd.DataFrame([{"game_id": 8, "team_id": 344, "player_id": "T", "p_start": 0.8, "official_status": None,
                          "official_slot": None, "evidence_ids": [], "reason": "base", "rule_version": "v2"}])
    src.put_snapshots(snap, T0)
    src.put_snapshots(snap.assign(p_start=0.3), T0 + timedelta(hours=1))
    copied = copy_tables(src.store, dst.store)
    assert copied["qb_status_evidence"] == 1 and copied["qb_status_snapshots"] == 2
    meta = src.store.load("qb_status_snapshots", include_metadata=True)["ingested_at"].iloc[0]
    before = meta - timedelta(microseconds=1)
    assert dst.snapshots()["p_start"].tolist() == [0.3]
    assert (dst.snapshots(as_of=before)["p_start"].tolist() == src.snapshots(as_of=before)["p_start"].tolist() == [0.8])
    with pytest.raises(RuntimeError):
        copy_tables(src.store, dst.store)


def test_copy_tables_can_leave_out_a_table_the_destination_already_has(tmp_path):
    from gnome_pipelines.cfb.qb_status.store import copy_tables
    from gnome_pipelines.datasets.qb_status import ALL, EVIDENCE
    src, dst = store(tmp_path / "src"), store(tmp_path / "dst")
    src.add_evidence([ev("a")])
    dst.add_evidence([ev("z")])
    with pytest.raises(RuntimeError):
        copy_tables(src.store, dst.store)
    assert copy_tables(src.store, dst.store, specs=[s for s in ALL if s is not EVIDENCE]) == {}
    assert sorted(dst.evidence()["evidence_id"]) == ["z"]
