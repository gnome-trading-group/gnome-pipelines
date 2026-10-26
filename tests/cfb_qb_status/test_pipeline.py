import pytest
from pydantic import ValidationError

from gnome_pipelines import families
from gnome_pipelines.cfb.qb_status.pipeline import STATE_FILES, QbStatusParams, QbStatusPipeline
from gnome_pipelines.cfb.qb_status.service import X_STATE
from gnome_pipelines.cfb.qb_status.extract import CLAUDE_CACHE


def test_the_service_is_a_small_daily_cfb_pipeline_that_ends_before_the_next_run():
    assert families.pipelines("cfb")["cfb_qb_status"] is QbStatusPipeline
    assert (QbStatusPipeline.resources.cpu, QbStatusPipeline.resources.memory) == (256, 2048)
    (daily,) = QbStatusPipeline.schedules
    assert daily.expression == "cron(0 9 * 1,8-12 ? *)"
    assert 23 < QbStatusParams().hours < 24 and QbStatusParams().prod is True
    with pytest.raises(ValidationError):
        QbStatusParams(hours=25)


def test_the_mirrored_state_is_the_x_spend_and_claudes_reads():
    assert set(STATE_FILES) == {X_STATE.name, CLAUDE_CACHE.name} and X_STATE.parent == CLAUDE_CACHE.parent


def test_prod_writes_carry_the_run_and_its_commit(monkeypatch, tmp_path):
    from gnome_pipelines.cfb.qb_status.pipeline import status_store
    monkeypatch.setenv("GNOME_LOCAL_WAREHOUSE", str(tmp_path / "wh"))
    monkeypatch.setenv("GNOME_CODE_VERSION", "gnome-pipelines@abc1234")
    monkeypatch.setenv("PIPELINE_NAME", "cfb_qb_status")
    monkeypatch.setenv("RUN_ID", "20261023T090000-x")
    store = status_store(prod=True)
    assert (store.code_version, store.run_id) == ("gnome-pipelines@abc1234", "cfb_qb_status/20261023T090000-x")


def test_a_state_root_that_is_not_a_location_fails_before_the_run():
    with pytest.raises(ValidationError):
        QbStatusParams(state_root="default s3://<research bucket>/apicache/cfbd")
    assert QbStatusParams(state_root="s3://bucket/qb_status/state").state_root == "s3://bucket/qb_status/state"
    assert QbStatusParams(state_root="/tmp/state").state_root == "/tmp/state"


def test_x_spend_caps_are_parameters_defaulting_to_60_a_month_and_3_a_day():
    p = QbStatusParams()
    assert (p.x_monthly_usd, p.x_daily_usd) == (60.0, 3.0)
    with pytest.raises(ValidationError):
        QbStatusParams(x_monthly_usd=-1)
    with pytest.raises(ValidationError):
        QbStatusParams(x_daily_usd=-1)


def test_the_run_hands_its_x_caps_to_the_service(monkeypatch):
    from gnome_pipelines.cfb.qb_status import pipeline as qp
    seen = {}

    class Mirror:
        def __init__(self, *a, **k):
            pass

        def pull(self):
            return []

        def push(self):
            pass

    class Svc:
        def run_until(self, deadline):
            pass

    class Ctx:
        x, x_budget = None, None
        extractor = type("E", (), {"name": "rules"})()

    def fake_context(*a, **k):
        seen.update(k)
        return Ctx()
    monkeypatch.setattr(qp, "StateMirror", Mirror)
    monkeypatch.setattr(qp, "service_context", fake_context)
    monkeypatch.setattr(qp, "build_service", lambda ctx, mirror=None: Svc())
    monkeypatch.setattr(qp, "dataset_store", lambda: None)
    monkeypatch.setattr(qp, "status_store", lambda prod: None)
    monkeypatch.setattr(qp, "StatusStore", lambda store: None)
    QbStatusPipeline().run(QbStatusParams(state_root="/tmp/state", x_monthly_usd=40, x_daily_usd=2))
    assert (seen["x_monthly_usd"], seen["x_daily_usd"]) == (40, 2)
