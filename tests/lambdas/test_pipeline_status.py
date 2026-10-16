import pytest

import pipeline_status
from tests.lambdas import fakes


def run(status, **extra):
    return {"pk": "RUN#cs2_prematch_predict", "sk": "r1", "pipeline_name": "cs2_prematch_predict", "run_id": "r1",
            "status": status, "trigger": "scheduled", "ecs_task_arn": "arn:task/1", **extra}


def stopped(exit_code, reason="Essential container in task exited"):
    return {"detail": {"taskArn": "arn:task/1", "lastStatus": "STOPPED", "stoppedReason": reason,
                       "containers": [{"exitCode": exit_code}]}}


@pytest.fixture
def setup(monkeypatch):
    def make(items):
        table, events = fakes.Table(items), fakes.Events()
        monkeypatch.setattr(pipeline_status, "table", table)
        monkeypatch.setattr(pipeline_status, "events", events)
        return table, events
    return make


def test_a_crashed_scheduled_run_is_marked_failed_and_alerted(setup):
    table, events = setup([run("RUNNING")])

    pipeline_status.handler(stopped(137), None)

    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"
    assert events.sent == [{
        "action": "FIRING", "key": "PIPELINE_FAILED:cs2_prematch_predict:r1", "type": "PIPELINE_FAILED",
        "source": "ingest", "title": "Pipeline cs2_prematch_predict failed",
        "body": "ECS task stopped with exit code 137: Essential container in task exited",
        "labels": {"pipeline": "cs2_prematch_predict", "runId": "r1", "trigger": "scheduled"},
        "stateAt": events.sent[0]["stateAt"],
    }]


def test_a_task_that_never_started_the_runner_fails_its_pending_run(setup):
    table, events = setup([run("PENDING")])

    pipeline_status.handler(stopped(None, "CannotPullContainerError: image not found"), None)

    assert table.items[("RUN#cs2_prematch_predict", "r1")]["status"] == "FAILED"
    assert "CannotPullContainerError" in events.sent[0]["body"]


def test_a_failure_the_runner_recorded_is_alerted_with_its_own_error(setup):
    table, events = setup([run("FAILED", trigger="manual", error_message="HLTV returned 403")])

    pipeline_status.handler(stopped(1), None)

    assert table.updates == []
    assert events.sent[0]["body"] == "HLTV returned 403"
    assert events.sent[0]["labels"]["trigger"] == "manual"


def test_a_clean_exit_reports_nothing(setup):
    table, events = setup([run("RUNNING")])

    pipeline_status.handler(stopped(0), None)

    assert table.updates == [] and events.sent == []
