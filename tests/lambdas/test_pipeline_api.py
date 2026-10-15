import json

import pytest

import create_pipeline
import get_pipeline
import list_pipelines
import store
import trigger_pipeline
import update_pipeline
from tests.lambdas import fakes

DEFINITION = {"pk": "PIPELINE", "sk": "hltv_cs2", "pipeline_name": "hltv_cs2", "parameters": {"min_stars": 0},
              "schedule": "cron(0 6 ? * MON *)", "schedule_enabled": False, "cpu": 2048, "memory": 8192}


@pytest.fixture
def table(monkeypatch):
    t = fakes.Table([DEFINITION])
    for module in (store, create_pipeline, get_pipeline, list_pipelines, trigger_pipeline, update_pipeline):
        monkeypatch.setattr(module, "table", t)
    return t


def body(result: dict) -> dict:
    return json.loads(result["body"])


def test_listing_hides_the_table_keys(table):
    result = body(list_pipelines.handler({}, None))

    assert result["count"] == 1
    assert "pk" not in result["pipelines"][0] and "sk" not in result["pipelines"][0]


def test_create_starts_unscheduled_and_refuses_a_duplicate(table):
    request = {"body": json.dumps({"pipeline_name": "cs2_prematch_train", "schedule_enabled": True})}

    created = create_pipeline.handler(request, None)
    again = create_pipeline.handler(request, None)

    assert created["statusCode"] == 201 and body(created)["pipeline"]["schedule_enabled"] is False
    assert again["statusCode"] == 409


def test_get_returns_newest_runs_first(table):
    for run_id in ("20261013T000000-a", "20261014T000000-b"):
        table.put_item(Item={**store.run_key("hltv_cs2", run_id), "run_id": run_id, "status": "SUCCEEDED"})

    result = body(get_pipeline.handler({"pathParameters": {"pipelineName": "hltv_cs2"}}, None))

    assert [r["run_id"] for r in result["runs"]] == ["20261014T000000-b", "20261013T000000-a"]


def test_manual_trigger_records_the_run_and_starts_the_task_with_merged_parameters(table, monkeypatch):
    ecs = fakes.Ecs()
    monkeypatch.setattr(trigger_pipeline, "ecs", ecs)
    event = {"pathParameters": {"pipelineName": "hltv_cs2"},
             "body": json.dumps({"parameters": {"start_date": "2026-10-11"}})}

    result = body(trigger_pipeline.handler(event, None))

    run = table.get_item(Key=store.run_key("hltv_cs2", result["run_id"]))["Item"]
    environment = {e["name"]: e["value"] for e in ecs.calls[0]["overrides"]["containerOverrides"][0]["environment"]}
    assert run["status"] == "PENDING" and run["trigger"] == "manual" and run["ecs_task_arn"] == "arn:task/1"
    assert json.loads(environment["PIPELINE_PARAMS"]) == {"min_stars": 0, "start_date": "2026-10-11"}
    assert ecs.calls[0]["overrides"]["cpu"] == "2048"


def test_scheduled_trigger_is_recorded_as_scheduled(table, monkeypatch):
    monkeypatch.setattr(trigger_pipeline, "ecs", fakes.Ecs())

    result = body(trigger_pipeline.handler({"pipeline_name": "hltv_cs2", "trigger": "scheduled"}, None))

    assert table.get_item(Key=store.run_key("hltv_cs2", result["run_id"]))["Item"]["trigger"] == "scheduled"


def test_a_task_ecs_refuses_to_start_fails_the_run_and_the_invocation(table, monkeypatch):
    monkeypatch.setattr(trigger_pipeline, "ecs", fakes.Ecs(failures=[{"reason": "RESOURCE:MEMORY"}]))

    with pytest.raises(RuntimeError, match="RESOURCE:MEMORY"):
        trigger_pipeline.handler({"pipeline_name": "hltv_cs2"}, None)

    runs = [i for i in table.items.values() if i["pk"] == "RUN#hltv_cs2"]
    assert runs[0]["status"] == "FAILED"


def test_trigger_of_an_unknown_pipeline_is_404(table, monkeypatch):
    monkeypatch.setattr(trigger_pipeline, "ecs", fakes.Ecs())

    assert trigger_pipeline.handler({"pipeline_name": "nope"}, None)["statusCode"] == 404


def test_enabling_a_schedule_creates_it_and_disabling_deletes_it(table, monkeypatch):
    scheduler = fakes.Scheduler()
    monkeypatch.setattr(update_pipeline, "scheduler", scheduler)
    path = {"pathParameters": {"pipelineName": "hltv_cs2"}}

    update_pipeline.handler({**path, "body": json.dumps({"schedule_enabled": True})}, None)
    update_pipeline.handler({**path, "body": json.dumps({"schedule_enabled": False})}, None)

    (created, spec), (deleted, _) = scheduler.calls
    assert created == "create" and deleted == "delete"
    assert spec["GroupName"] == "gnome-pipelines" and spec["Name"] == "hltv_cs2"
    assert json.loads(spec["Target"]["Input"]) == {"pipeline_name": "hltv_cs2", "trigger": "scheduled"}


def test_updating_an_unknown_pipeline_is_404_and_creates_nothing(table, monkeypatch):
    monkeypatch.setattr(update_pipeline, "scheduler", fakes.Scheduler())

    result = update_pipeline.handler({"pathParameters": {"pipelineName": "nope"}, "body": "{}"}, None)

    assert result["statusCode"] == 404 and ("PIPELINE", "nope") not in table.items
