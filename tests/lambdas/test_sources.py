"""A polling source's run end to end through the trigger, the API and the status Lambda: one live run at a time,
started again whenever its task stops unasked, stopped when asked, failed after a crash loop."""
import json

import pytest

import api
import pipeline_status
import sources
import store
import tasks
import trigger_pipeline
from tests.lambdas import fakes

SOURCE = {"name": "qb", "family": "cfb", "kind": "source", "description": "QB status", "code_version": "x",
          "params_schema": {"type": "object", "additionalProperties": False,
                            "properties": {"x": {"type": "boolean", "default": True}}},
          "resources": {"cpu": 256, "memory": 2048}, "secrets": ["x"],
          "schedules": []}
TASK = "arn:aws:ecs:us-east-1:1:task/gnome-pipelines/abc123"


@pytest.fixture
def aws(monkeypatch):
    env = {"table": fakes.Table(), "ecs": fakes.Ecs(), "events": fakes.Events()}
    monkeypatch.setattr(store, "ssm", fakes.Ssm([SOURCE]))
    for module in (api, trigger_pipeline, pipeline_status, sources):
        monkeypatch.setattr(module, "table", env["table"])
    for module in (tasks, trigger_pipeline, pipeline_status):
        monkeypatch.setattr(module, "ecs", env["ecs"])
    monkeypatch.setattr(api, "ecs", env["ecs"])
    monkeypatch.setattr(pipeline_status, "events", env["events"])
    monkeypatch.setattr(api, "lambdas", fakes.Lambdas(trigger_pipeline.handler))
    monkeypatch.setitem(tasks.TASK_DEFINITIONS, "source:qb", "arn:task-definition/gnome-pipelines-qb")
    return env


def post(path):
    rest = path.removeprefix("/research/pipelines/")
    result = api.handler({"httpMethod": "POST", "resource": "/research/pipelines/{proxy+}",
                          "pathParameters": {"proxy": rest}, "body": None}, None)
    return result["statusCode"], json.loads(result["body"])


def start(trigger="manual", **params):
    return trigger_pipeline.handler({"pipeline_name": "qb", "trigger": trigger, "parameters": params}, None)


def run_of(env, run_id):
    return env["table"].items[("RUN#qb", run_id)]


def stopped(task=TASK, exit_code=0, **extra):
    return {"detail": {"taskArn": task, "lastStatus": "STOPPED", "stoppedReason": "Essential container exited",
                       "containers": [{"exitCode": exit_code}], **extra}}


def test_a_source_starts_on_its_own_task_definition_and_holds_the_source(aws):
    started = start(x=False)
    assert started["status"] == 202
    run = run_of(aws, started["run_id"])
    assert run["kind"] == "source" and run["status"] == "PENDING" and run["ecs_task_arn"] == TASK
    (call,) = aws["ecs"].calls
    assert call["taskDefinition"] == "arn:task-definition/gnome-pipelines-qb" and call["startedBy"] == "source:qb"
    assert sources.active_run("qb") == started["run_id"]


def test_a_second_start_is_refused(aws):
    first = start()["run_id"]
    assert start() == {"status": 409, "error": f"qb is already running (run {first}); stop it first"}
    assert len(aws["ecs"].calls) == 1


def test_a_holder_that_ended_without_releasing_does_not_block_a_start(aws):
    aws["table"].put_item(Item={"pk": "SOURCE#qb", "sk": "ACTIVE", "run_id": "old"})
    aws["table"].put_item(Item={"pk": "RUN#qb", "sk": "old", "status": "FAILED", "kind": "source"})
    assert start()["status"] == 202


def test_stopping_marks_stopping_stops_the_task_and_the_stopped_task_ends_the_run(aws):
    run_id = start()["run_id"]
    assert post("/research/pipelines/nope/stop")[0] == 404
    status, body = post("/research/pipelines/qb/stop")
    assert status == 200 and body["run"]["status"] == "STOPPING"
    assert aws["ecs"].stopped[0]["task"] == TASK
    pipeline_status.handler(stopped(), None)
    assert run_of(aws, run_id)["status"] == "STOPPED" and sources.active_run("qb") is None
    assert len(aws["ecs"].calls) == 1, "a stop that was asked for is not restarted"


def test_a_task_stopped_for_a_new_image_restarts_the_same_run_without_counting_a_crash(aws):
    run_id = start(x=False)["run_id"]
    aws["table"].update_item(Key=store.run_key("qb", run_id), UpdateExpression="SET #s = :r",
                             ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":r": "RUNNING"})
    pipeline_status.handler(stopped(stopCode="UserInitiated"), None)
    run = run_of(aws, run_id)
    assert run["status"] == "PENDING" and run["restarts"] == 1 and run["crashes"] == []
    restart = aws["ecs"].calls[1]
    assert restart["startedBy"] == "source:qb"
    env = {e["name"]: e["value"] for e in restart["overrides"]["containerOverrides"][0]["environment"]}
    assert env["RUN_ID"] == run_id and json.loads(env["PIPELINE_PARAMS"]) == {"x": False}
    assert aws["events"].sent == []


def test_each_crash_warns_and_the_third_in_half_an_hour_fails_the_run_releases_it_and_alerts(aws):
    run_id = start()["run_id"]
    for _ in range(2):
        pipeline_status.handler(stopped(run_of(aws, run_id)["ecs_task_arn"], exit_code=1), None)
        assert run_of(aws, run_id)["status"] == "PENDING"
    pipeline_status.handler(stopped(run_of(aws, run_id)["ecs_task_arn"], exit_code=137), None)
    run = run_of(aws, run_id)
    assert run["status"] == "FAILED" and "3 crashes in 30 min" in run["error_message"]
    assert sources.active_run("qb") is None and len(aws["ecs"].calls) == 3
    *warnings, failed = aws["events"].sent
    assert [(w["type"], w["severity"]) for w in warnings] == [("CUSTOM", "WARNING")] * 2
    assert failed["title"] == "Polling source qb failed" and failed["labels"]["kind"] == "source"
    assert failed["labels"]["trigger"] == "manual"


def test_a_restart_ecs_refuses_fails_the_run(aws):
    run_id = start()["run_id"]
    aws["ecs"].failures = [{"reason": "RESOURCE:MEMORY"}]
    pipeline_status.handler(stopped(exit_code=1), None)
    run = run_of(aws, run_id)
    assert run["status"] == "FAILED" and "RESOURCE:MEMORY" in run["error_message"]
    assert sources.active_run("qb") is None and aws["events"].sent[-1]["type"] == "PIPELINE_FAILED"


def test_cancelling_a_source_run_from_the_runs_list_is_a_stop(aws):
    run_id = start()["run_id"]
    status, _ = post(f"/research/pipelines/qb/runs/{run_id}/cancel")
    assert status == 200 and run_of(aws, run_id)["status"] == "STOPPING"
    # A second stop while the task is still stopping stops it again (in case the first stop_task was lost).
    assert post("/research/pipelines/qb/stop")[0] == 200 and run_of(aws, run_id)["status"] == "STOPPING"


def running(env, run_id):
    env["table"].update_item(Key=store.run_key("qb", run_id), UpdateExpression="SET #s = :r",
                             ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":r": "RUNNING"})


def test_a_duplicate_stopped_event_during_a_restart_launches_nothing_more(aws):
    run_id = start()["run_id"]
    running(aws, run_id)
    pipeline_status.handler(stopped(stopCode="UserInitiated"), None)
    pipeline_status.handler(stopped(stopCode="UserInitiated"), None)
    assert len(aws["ecs"].calls) == 2 and run_of(aws, run_id)["ecs_task_arn"] == TASK + "-2"
    assert aws["ecs"].stopped == []


def test_a_stop_while_restarting_stops_the_task_the_restart_starts(aws, monkeypatch):
    run_id = start()["run_id"]
    running(aws, run_id)
    real = pipeline_status.launch

    def launch_after_a_stop(*a, **k):
        assert post("/research/pipelines/qb/stop")[0] == 200
        return real(*a, **k)
    monkeypatch.setattr(pipeline_status, "launch", launch_after_a_stop)
    pipeline_status.handler(stopped(stopCode="UserInitiated"), None)
    assert run_of(aws, run_id)["status"] == "STOPPING"
    assert aws["ecs"].stopped == [{"cluster": "arn:cluster", "task": TASK + "-2", "reason": "Stopped while restarting"}]
    pipeline_status.handler(stopped(TASK + "-2", stopCode="UserInitiated"), None)
    assert run_of(aws, run_id)["status"] == "STOPPED" and sources.active_run("qb") is None


def test_a_restart_superseded_while_launching_stops_its_task(aws, monkeypatch):
    run_id = start()["run_id"]
    real = pipeline_status.launch

    def launch_after_the_run_ended(*a, **k):
        sources.finish("qb", run_id, "FAILED", "ended elsewhere")
        return real(*a, **k)
    monkeypatch.setattr(pipeline_status, "launch", launch_after_the_run_ended)
    pipeline_status.handler(stopped(exit_code=1), None)
    assert aws["ecs"].stopped[-1]["reason"] == "Superseded while restarting"


def test_a_deliberate_stop_of_a_starting_task_is_not_a_crash(aws):
    run_id = start()["run_id"]
    pipeline_status.handler(stopped(exit_code=None, stopCode="UserInitiated"), None)
    run = run_of(aws, run_id)
    assert run["status"] == "PENDING" and run["crashes"] == [] and aws["events"].sent == []


def test_a_run_stuck_stopping_whose_task_is_gone_is_ended_by_the_next_stop_and_start_works(aws):
    run_id = start()["run_id"]
    assert post("/research/pipelines/qb/stop")[0] == 200
    # The stopped event never arrived, and ECS has since forgotten the task.
    aws["ecs"].tasks[TASK] = None
    assert post("/research/pipelines/qb/stop")[0] == 200
    assert run_of(aws, run_id)["status"] == "STOPPED" and sources.active_run("qb") is None
    assert start()["status"] == 202


def test_a_start_takes_over_from_a_live_run_whose_task_stopped_long_ago(aws):
    old = start()["run_id"]
    running(aws, old)
    aws["ecs"].tasks[TASK] = {"lastStatus": "STOPPED", "stoppedAt": "2026-01-01T00:00:00+00:00"}
    new = start()
    assert new["status"] == 202 and run_of(aws, old)["status"] == "FAILED"
    assert sources.active_run("qb") == new["run_id"]


def test_a_live_run_whose_task_just_stopped_still_holds_the_source(aws):
    old = start()["run_id"]
    aws["ecs"].tasks[TASK] = {"lastStatus": "STOPPED", "stoppedAt": store.now()}
    assert start()["status"] == 409 and sources.active_run("qb") == old


def test_a_refused_start_leaves_no_run_behind(aws):
    start()
    start()
    assert len([k for k in aws["table"].items if k[0] == "RUN#qb"]) == 1


def test_a_launch_that_raises_fails_the_run_and_releases_the_source(aws, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("ThrottlingException")
    monkeypatch.setattr(trigger_pipeline, "launch", boom)
    with pytest.raises(RuntimeError, match="ThrottlingException"):
        start()
    (run,) = [v for k, v in aws["table"].items.items() if k[0] == "RUN#qb"]
    assert run["status"] == "FAILED" and sources.active_run("qb") is None


def test_an_error_after_claiming_a_restart_fails_the_run_loudly(aws, monkeypatch):
    run_id = start()["run_id"]

    def broken(*a, **k):
        raise RuntimeError("ThrottlingException")
    monkeypatch.setattr(pipeline_status, "launch", broken)
    pipeline_status.handler(stopped(exit_code=1), None)
    run = run_of(aws, run_id)
    assert run["status"] == "FAILED" and "ThrottlingException" in run["error_message"]
    assert sources.active_run("qb") is None and aws["events"].sent[-1]["type"] == "PIPELINE_FAILED"


def test_the_reconcile_check_ends_a_live_source_whose_task_is_gone_and_leaves_a_healthy_one(aws):
    run_id = start()["run_id"]
    running(aws, run_id)
    assert pipeline_status.handler({"detail-type": "Scheduled Event"}, None) == {"ended": []}
    aws["ecs"].tasks[TASK] = None
    assert pipeline_status.handler({"detail-type": "Scheduled Event"}, None) == {"ended": [run_id]}
    assert run_of(aws, run_id)["status"] == "FAILED" and sources.active_run("qb") is None
    assert aws["events"].sent[-1]["labels"]["kind"] == "source"


def test_crash_warnings_are_keyed_per_run_per_day(aws):
    run_id = start()["run_id"]
    pipeline_status.handler(stopped(exit_code=1), None)
    (warning,) = aws["events"].sent
    assert warning["key"] == f"SOURCE_RESTARTED:qb:{run_id}:{store.now()[:10]}"
