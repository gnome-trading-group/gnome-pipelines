from datetime import datetime, timezone

from gnome_pipelines.cfb.qb_status.evidence import AVAILABILITY, Evidence, evidence_frame, evidence_id, parse_report_status
from gnome_pipelines.datasets.qb_status import EVIDENCE

T0 = datetime(2026, 10, 21, 20, 0, tzinfo=timezone.utc)


def test_report_words_map_to_statuses():
    assert parse_report_status("Questionable") == "questionable"
    assert parse_report_status("Game Time Decision") == "game_time_decision"
    assert parse_report_status("Game-Time Decision") == "game_time_decision"
    assert parse_report_status(" Out ") == "out"
    assert parse_report_status("") is None
    assert parse_report_status("TBD") is None


def test_availability_mapping():
    assert AVAILABILITY["probable"] == 0.75
    assert AVAILABILITY["game_time_decision"] == 0.5
    assert AVAILABILITY["out"] == 0.0
    assert AVAILABILITY["available"] == 1.0


def test_evidence_id_is_deterministic_and_order_sensitive():
    a = evidence_id("official_report", "SEC", "Stone", "Out")
    assert a == evidence_id("official_report", "SEC", "Stone", "Out")
    assert a != evidence_id("official_report", "SEC", "Out", "Stone")
    assert len(a) == 24


def test_frame_matches_spec():
    ev = Evidence(evidence_id="x", source_type="official_report", source_id="r1", source_org="SEC", first_seen_at=T0,
                  team="Mississippi State", status="out", season=2026)
    df = evidence_frame([ev])
    assert list(df.columns) == EVIDENCE.schema.names
    assert df.loc[0, "review_state"] == "auto"
    assert bool(df.loc[0, "backfilled"]) is False
