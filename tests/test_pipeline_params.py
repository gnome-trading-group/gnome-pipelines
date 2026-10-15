import re
from datetime import date

import pytest
from pydantic import ValidationError

from gnome_pipelines import PIPELINE_REGISTRY
from gnome_pipelines.cs2_prematch.predict import CS2PrematchPredictParams
from gnome_pipelines.hltv_cs2.pipeline import HltvCs2Params
from gnome_pipelines.runner import execute


def test_every_registered_pipeline_declares_its_params():
    assert {name: cls.params_type.__name__ for name, cls in PIPELINE_REGISTRY.items()} == {
        "hltv_cs2": "HltvCs2Params",
        "cs2_prematch_train": "CS2PrematchTrainParams",
        "cs2_prematch_predict": "CS2PrematchPredictParams",
    }


def test_text_values_become_their_declared_types():
    params = HltvCs2Params.model_validate({"start_date": "2026-10-11", "end_date": "2026-10-13", "min_stars": "0"})

    assert params.start_date.isoformat() == "2026-10-11" and params.min_stars == 0


def test_the_string_false_is_false():
    assert CS2PrematchPredictParams.model_validate({"dry_run": "false", "launch_queue_url": "q",
                                                    "shutdown_queue_url": "q"}).dry_run is False


def test_a_real_predict_run_needs_both_queues_up_front():
    with pytest.raises(ValidationError, match="launch_queue_url and shutdown_queue_url"):
        CS2PrematchPredictParams.model_validate({"launch_queue_url": "q"})

    assert CS2PrematchPredictParams.model_validate({"dry_run": True}).dry_run


def test_execute_rejects_unknown_and_missing_params_before_running():
    with pytest.raises(ValidationError) as e:
        execute("hltv_cs2", {"days_back": 0, "min_star": 0})

    assert {err["loc"][0] for err in e.value.errors()} == {"days_back", "min_star"}


def test_demos_need_the_chrome_fetcher_and_cache_has_nothing_to_refresh():
    base = {"start_date": "2026-10-11", "end_date": "2026-10-13"}

    with pytest.raises(ValidationError, match="fetcher=chrome"):
        HltvCs2Params.model_validate({**base, "demos": True})
    with pytest.raises(ValidationError, match="refresh"):
        HltvCs2Params.model_validate({**base, "fetcher": "cache", "refresh": True})
    assert HltvCs2Params.model_validate({**base, "fetcher": "chrome", "demos": True}).demos


def test_chrome_is_refused_inside_a_fargate_task(monkeypatch):
    monkeypatch.setenv("GNOME_STAGE", "dev")
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4/x")

    with pytest.raises(ValueError, match="run it locally"):
        execute("hltv_cs2", {"start_date": "2026-10-11", "end_date": "2026-10-13", "fetcher": "chrome"})


# Fargate's valid cpu → memory (MiB) combinations up to 4 vCPU.
FARGATE = {256: range(512, 2049, 512), 512: range(1024, 4097, 1024), 1024: range(2048, 8193, 1024),
           2048: range(4096, 16385, 1024), 4096: range(8192, 30721, 1024)}


@pytest.mark.parametrize("name", sorted(PIPELINE_REGISTRY))
def test_declarations_are_valid(name):
    cls = PIPELINE_REGISTRY[name]

    assert cls.description
    assert cls.resources.memory in FARGATE[cls.resources.cpu]
    assert len({s.name for s in cls.schedules}) == len(cls.schedules)
    for schedule in cls.schedules:
        # EventBridge Scheduler names are <pipeline>.<schedule>, at most 64 of [0-9a-zA-Z-_.].
        assert re.fullmatch(r"[\w-]+", schedule.name) and len(f"{name}.{schedule.name}") <= 64
        assert re.fullmatch(r"(cron|rate)\(.+\)", schedule.expression)
        cls.params_type.model_validate(schedule.params)


def test_hltv_without_dates_runs_the_last_days_back_days():
    params = HltvCs2Params.model_validate({"days_back": 8})

    assert params.window(date(2026, 10, 19)) == (date(2026, 10, 11), date(2026, 10, 19))
    assert HltvCs2Params.model_validate({"start_date": "2026-10-01", "end_date": "2026-10-02"}).window(
        date(2026, 10, 19)) == (date(2026, 10, 1), date(2026, 10, 2))
    with pytest.raises(ValidationError, match="both start_date and end_date"):
        HltvCs2Params.model_validate({"start_date": "2026-10-01"})
