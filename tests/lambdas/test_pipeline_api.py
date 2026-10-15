import json
from datetime import datetime, timezone

import pytest

import api
import store
import sync_schedules
import trigger_pipeline
from tests.lambdas import fakes

SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "start_date": {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}], "default": None},
        "days_back": {"type": "integer", "minimum": 1, "default": 7},
        "fetcher": {"type": "string", "enum": ["zenrows", "chrome", "cache"], "default": "zenrows"},
    },
}
HLTV = {"name": "hltv_cs2", "family": "cs2", "description": "HLTV", "params_schema": SCHEMA,
        "resources": {"cpu": 2048, "memory": 8192}, "code_version": "gnome-pipelines@abc",
        "schedules": [{"name": "weekly", "expression": "cron(0 6 ? * MON *)", "params": {"days_back": 8}, "description": ""}]}
TRAIN = {"name": "cs2_prematch_train", "family": "cs2", "description": "train", "params_schema": {"type": "object", "properties": {}},
         "resources": {"cpu": 1024, "memory": 4096}, "code_version": "gnome-pipelines@abc", "schedules": []}


@pytest.fixture
def aws(monkeypatch):
    env = {"table": fakes.Table(), "ecs": fakes.Ecs(), "scheduler": fakes.Scheduler(), "logs": fakes.Logs({})}
    monkeypatch.setattr(store, "ssm", fakes.Ssm([HLTV, TRAIN]))
    for module in (api, trigger_pipeline, sync_schedules):
        monkeypatch.setattr(module, "table", env["table"], raising=False)
    monkeypatch.setattr(trigger_pipeline, "ecs", env["ecs"])
    monkeypatch.setattr(api, "ecs", env["ecs"])
    monkeypatch.setattr(api, "scheduler", env["scheduler"])
    monkeypatch.setattr(sync_schedules, "scheduler", env["scheduler"])
    monkeypatch.setattr(api, "logs", env["logs"])
    monkeypatch.setattr(api, "lambdas", fakes.Lambdas(trigger_pipeline.handler))
    return env


def call(method, path, body=None, query=None):
    result = api.handler({"httpMethod": method, **event_path(path), "body": json.dumps(body) if body is not None else None,
                          "queryStringParameters": query}, None)
    return result["statusCode"], json.loads(result["body"])


def test_lists_declared_pipelines_with_schedule_state_and_last_run(aws):
    sync_schedules.handler({}, None)
    call("POST", "/research/pipelines/hltv_cs2/runs", {"parameters": {}})

    status, body = call("GET", "/research/pipelines")

    hltv = next(p for p in body["pipelines"] if p["name"] == "hltv_cs2")
    assert status == 200 and body["count"] == 2
    assert hltv["schedules"][0] | {"enabled": False, "synced": True} == hltv["schedules"][0]
    assert hltv["last_run"]["status"] == "PENDING" and "pk" not in hltv["last_run"]


def test_a_run_takes_its_family_task_definition_the_declared_task_size_and_the_given_parameters(aws):
    status, body = call("POST", "/research/pipelines/hltv_cs2/runs", {"parameters": {"days_back": 3}})

    task = aws["ecs"].calls[0]["overrides"]
    environment = {e["name"]: e["value"] for e in task["containerOverrides"][0]["environment"]}
    assert status == 202 and body["run_id"]
    assert aws["ecs"].calls[0]["taskDefinition"] == "arn:task-definition/gnome-pipelines-cs2"
    assert task["cpu"] == "2048" and task["memory"] == "8192"
    assert json.loads(environment["PIPELINE_PARAMS"]) == {"days_back": 3}


def test_bad_parameters_are_refused_before_a_task_starts(aws):
    status, body = call("POST", "/research/pipelines/hltv_cs2/runs",
                        {"parameters": {"days_back": 0, "fetcher": "curl", "start_date": "11/10/2026", "dayz": 1}})

    assert status == 400 and aws["ecs"].calls == []
    for expected in ("days_back: must be >= 1", "fetcher: must be one of zenrows, chrome, cache",
                     "start_date: expected a date", "dayz: not a parameter"):
        assert expected in body["error"]


def test_an_undeclared_pipeline_is_404(aws):
    assert call("POST", "/research/pipelines/nope/runs", {})[0] == 404
    assert call("GET", "/research/pipelines/nope")[0] == 404


def test_a_pipeline_whose_family_has_no_task_definition_is_refused(aws, monkeypatch):
    monkeypatch.setattr(store, "ssm", fakes.Ssm([{**TRAIN, "family": "cfb"}]))

    status, body = call("POST", "/research/pipelines/cs2_prematch_train/runs", {})

    assert status == 500 and "family 'cfb'" in body["error"] and aws["ecs"].calls == []


def test_a_scheduled_trigger_that_cant_start_raises_so_it_alarms(aws):
    with pytest.raises(trigger_pipeline.Rejected):
        trigger_pipeline.handler({"pipeline_name": "nope", "trigger": "scheduled"}, None)


def test_ecs_refusing_a_task_fails_the_run_and_the_request(aws, monkeypatch):
    monkeypatch.setattr(trigger_pipeline, "ecs", fakes.Ecs(failures=[{"reason": "RESOURCE:MEMORY"}]))

    status, body = call("POST", "/research/pipelines/hltv_cs2/runs", {})

    assert status == 502 and "RESOURCE:MEMORY" in body["error"]
    assert [i["status"] for i in aws["table"].items.values()] == ["FAILED"]


def test_switching_a_schedule_keeps_everything_but_its_state(aws):
    sync_schedules.handler({}, None)

    status, _ = call("PUT", "/research/pipelines/hltv_cs2/schedules/weekly", {"enabled": True})

    schedule = aws["scheduler"].schedules["hltv_cs2.weekly"]
    assert status == 200 and schedule["State"] == "ENABLED"
    assert schedule["ScheduleExpression"] == "cron(0 6 ? * MON *)"
    assert call("PUT", "/research/pipelines/hltv_cs2/schedules/daily", {"enabled": True})[0] == 404
    assert call("PUT", "/research/pipelines/hltv_cs2/schedules/weekly", {"enabled": "yes"})[0] == 400


def test_logs_come_from_the_runs_task_stream(aws):
    _, run = call("POST", "/research/pipelines/hltv_cs2/runs", {})
    aws["logs"].streams["pipeline/pipeline/abc123"] = ["Starting", "Done"]

    status, body = call("GET", f"/research/pipelines/hltv_cs2/runs/{run['run_id']}/logs", query={"before": "b/x"})

    assert status == 200 and [line["message"] for line in body["lines"]] == ["Starting", "Done"]
    assert aws["logs"].requests[0]["nextToken"] == "b/x" and body["before"] == "b/earlier"


def test_logs_before_the_container_starts_are_empty(aws):
    _, run = call("POST", "/research/pipelines/hltv_cs2/runs", {})

    assert call("GET", f"/research/pipelines/hltv_cs2/runs/{run['run_id']}/logs") == (200, {"before": None, "lines": []})


def test_cancel_stops_the_task_once(aws):
    _, run = call("POST", "/research/pipelines/hltv_cs2/runs", {})
    path = f"/research/pipelines/hltv_cs2/runs/{run['run_id']}/cancel"

    status, body = call("POST", path)

    assert status == 200 and body["run"]["status"] == "CANCELLED"
    assert aws["ecs"].stopped[0]["task"].endswith("/abc123")
    assert call("POST", path)[0] == 409


def test_unknown_routes_and_bad_bodies(aws):
    assert call("DELETE", "/research/pipelines/hltv_cs2")[0] == 404
    status = api.handler({"httpMethod": "POST", **event_path("/research/pipelines/hltv_cs2/runs"), "body": "{nope"}, None)
    assert status["statusCode"] == 400


def event_path(path: str) -> dict:
    """How API Gateway presents a request to one of the controller's {proxy+} routes."""
    root, _, rest = path.removeprefix("/research/").partition("/")
    if not rest:
        return {"resource": f"/research/{root}", "pathParameters": None}
    return {"resource": f"/research/{root}/{{proxy+}}", "pathParameters": {"proxy": rest}}


def test_a_switched_on_schedule_reports_its_next_run(aws):
    sync_schedules.handler({}, None)
    call("PUT", "/research/pipelines/hltv_cs2/schedules/weekly", {"enabled": True})

    _, body = call("GET", "/research/pipelines/hltv_cs2")

    schedule = body["pipeline"]["schedules"][0]
    upcoming = datetime.fromisoformat(schedule["next_run"])
    assert upcoming.weekday() == 0 and (upcoming.hour, upcoming.minute) == (6, 0)
    assert upcoming > datetime.now(timezone.utc)
