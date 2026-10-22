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
