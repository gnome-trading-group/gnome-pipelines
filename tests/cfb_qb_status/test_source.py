from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from gnome_pipelines import families
from gnome_pipelines.cfb.qb_status import source as qs
from gnome_pipelines.cfb.qb_status.source import QbStatusParams, QbStatusSource, season_for
from gnome_pipelines.polling.health import PollHealth


def test_qb_status_is_a_small_cfb_polling_source_with_its_own_secrets():
    assert families.sources("cfb")["cfb_qb_status"] is QbStatusSource
    assert "cfb_qb_status" not in families.pipelines("cfb")
    assert (QbStatusSource.resources.cpu, QbStatusSource.resources.memory) == (256, 2048)
    assert QbStatusSource.secrets == ("x", "anthropic")


def test_its_state_lives_in_the_state_store_not_in_files_serve_mirrors():
    assert not any(hasattr(QbStatusSource, a) for a in ("state_files", "state_dir", "state_lane"))


def test_x_spend_caps_are_parameters_defaulting_to_60_a_month_and_3_a_day():
    p = QbStatusParams()
    assert (p.x_monthly_usd, p.x_daily_usd, p.x, p.claude) == (60.0, 3.0, True, True)
    with pytest.raises(ValidationError):
        QbStatusParams(x_monthly_usd=-1)
    with pytest.raises(ValidationError):
        QbStatusParams(hours=23.75)


class FakeService:
    def __init__(self):
        self.tasks, self.stopped = [], False

    def stop(self):
        self.stopped = True


def built(monkeypatch, rows, seen=None, season_now=datetime(2026, 11, 11, tzinfo=timezone.utc)):
    seen = {} if seen is None else seen
    svc = FakeService()

    def fake_context(research, status, season, state, **k):
        seen.update(k, season=season, state=state)
        k["say"]("X is off: no key")
        return "ctx"
    monkeypatch.setattr(qs, "service_context", fake_context)
    monkeypatch.setattr(qs, "build_service", lambda ctx, health: svc)
    monkeypatch.setattr(qs, "dataset_store", lambda: None)
    monkeypatch.setattr(qs, "StatusStore", lambda store: None)
    monkeypatch.setattr(qs, "utcnow", lambda: season_now)
    monkeypatch.setattr(qs, "state_objects", lambda: "the state store")
    health = PollHealth("cfb_qb_status", publish=lambda df: rows.extend(df.to_dict("records")))
    return QbStatusSource().build(QbStatusParams(x_monthly_usd=40, x_daily_usd=2), health, None, None), seen


def test_build_hands_the_caps_and_season_to_the_context_and_notes_to_poll_health(monkeypatch):
    rows = []
    svc, seen = built(monkeypatch, rows)
    assert (seen["x_monthly_usd"], seen["x_daily_usd"], seen["season"]) == (40, 2, 2026)
    assert seen["state"] == "the state store"
    assert [(r["kind"], r["detail"]) for r in rows] == [("note", "X is off: no key")]


def test_a_run_left_going_past_july_1_ends_itself_to_restart_on_the_new_season(monkeypatch):
    rows = []
    svc, _ = built(monkeypatch, rows)
    check = next(t for t in svc.tasks if t.name == "season_check")
    assert check.lane == "qb_status" and check.interval(None) == 3600.0
    check.run(datetime(2027, 1, 20, tzinfo=timezone.utc))
    assert not svc.stopped, "January is still the 2026 season"
    check.run(datetime(2027, 7, 1, tzinfo=timezone.utc))
    assert svc.stopped and "restarting for season 2027" in rows[-1]["detail"]
    assert season_for(datetime(2027, 1, 20)) == 2026 and season_for(datetime(2027, 7, 1)) == 2027
