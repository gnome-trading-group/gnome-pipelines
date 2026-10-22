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
