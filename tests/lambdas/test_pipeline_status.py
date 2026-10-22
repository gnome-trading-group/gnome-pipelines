from decimal import Decimal

import pytest

import pipeline_status
import store
import tasks
from tests.lambdas import fakes

PREDICT = {"name": "cs2_prematch_predict", "family": "cs2", "resources": {"cpu": 1024, "memory": 4096}}
PULL_TIMEOUT = "CannotPullContainerError: dial tcp 18.205.220.157:443: i/o timeout"


def run(status, **extra):
    return {"pk": "RUN#cs2_prematch_predict", "sk": "r1", "pipeline_name": "cs2_prematch_predict", "run_id": "r1",
            "status": status, "trigger": "scheduled", "ecs_task_arn": "arn:task/1", **extra}


def stopped(exit_code, reason="Essential container in task exited", **extra):
    return {"detail": {"taskArn": "arn:task/1", "lastStatus": "STOPPED", "stoppedReason": reason,
                       "containers": [{"exitCode": exit_code}], **extra}}


def failed_to_start(reason=PULL_TIMEOUT):
    return stopped(None, reason, stopCode="TaskFailedToStart")


@pytest.fixture
def setup(monkeypatch):
    def make(items):
        table, events, ecs = fakes.Table(items), fakes.Events(), fakes.Ecs()
        monkeypatch.setattr(pipeline_status, "table", table)
        monkeypatch.setattr(pipeline_status, "events", events)
        monkeypatch.setattr(tasks, "ecs", ecs)
        monkeypatch.setattr(store, "ssm", fakes.Ssm([PREDICT]))
        return table, events, ecs
    return make


def test_a_crashed_scheduled_run_is_marked_failed_and_alerted(setup):
    table, events, _ = setup([run("RUNNING")])

    pipeline_status.handler(stopped(137), None)

    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"
    assert events.sent == [{
        "action": "FIRING", "key": "PIPELINE_FAILED:cs2_prematch_predict:r1", "type": "PIPELINE_FAILED",
        "source": "ingest", "title": "Pipeline cs2_prematch_predict failed",
        "body": "ECS task stopped with exit code 137: Essential container in task exited",
        "labels": {"pipeline": "cs2_prematch_predict", "runId": "r1", "trigger": "scheduled"},
        "stateAt": events.sent[0]["stateAt"],
    }]


def test_a_task_that_failed_to_start_is_started_again_for_the_same_run(setup):
    table, events, ecs = setup([run("PENDING", parameters={"days_back": Decimal(3)})])

    pipeline_status.handler(failed_to_start(), None)

    record = table.items[("RUN#cs2_prematch_predict", "r1")]
    assert record["status"] == "PENDING"
    assert record["ecs_task_arn"] == "arn:aws:ecs:us-east-1:1:task/gnome-pipelines/abc123"
    assert record["start_attempts"] == 2 and record["start_errors"] == [PULL_TIMEOUT]
    environment = {e["name"]: e["value"] for e in ecs.calls[0]["overrides"]["containerOverrides"][0]["environment"]}
    assert environment == {"PIPELINE_NAME": "cs2_prematch_predict", "RUN_ID": "r1", "PIPELINE_PARAMS": '{"days_back": 3}'}
    assert events.sent == []


def test_a_run_out_of_start_attempts_fails_and_alerts(setup):
    table, events, ecs = setup([run("PENDING", start_attempts=pipeline_status.MAX_START_ATTEMPTS)])

    pipeline_status.handler(failed_to_start(), None)

    assert ecs.calls == []
    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"
    assert "CannotPullContainerError" in events.sent[0]["body"]


def test_a_repeated_stopped_event_starts_the_run_only_once(setup):
    table, events, ecs = setup([run("PENDING")])

    pipeline_status.handler(failed_to_start(), None)
    # EventBridge's duplicate carries the stopped task, which the run no longer points at.
    pipeline_status._restart(run("PENDING"), "arn:task/1", PULL_TIMEOUT)

    assert len(ecs.calls) == 1 and events.sent == []


def test_ecs_refusing_the_restart_fails_the_run(setup, monkeypatch):
    table, events, _ = setup([run("PENDING")])
    monkeypatch.setattr(tasks, "ecs", fakes.Ecs(failures=[{"reason": "RESOURCE:MEMORY"}]))

    pipeline_status.handler(failed_to_start(), None)

    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"
    assert "CannotPullContainerError" in events.sent[0]["body"]


def test_a_running_task_that_crashed_is_not_restarted(setup):
    table, events, ecs = setup([run("RUNNING")])

    pipeline_status.handler(stopped(137, stopCode="EssentialContainerExited"), None)

    assert ecs.calls == []
    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"


def test_a_failure_the_runner_recorded_is_alerted_with_its_own_error(setup):
    table, events, _ = setup([run("FAILED", trigger="manual", error_message="HLTV returned 403")])

    pipeline_status.handler(stopped(1), None)

    assert table.updates == []
    assert events.sent[0]["body"] == "HLTV returned 403"
    assert events.sent[0]["labels"]["trigger"] == "manual"


def test_a_clean_exit_reports_nothing(setup):
    table, events, _ = setup([run("RUNNING")])

    pipeline_status.handler(stopped(0), None)

    assert table.updates == [] and events.sent == []
