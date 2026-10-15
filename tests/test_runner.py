import json
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest

from gnome_pipelines import PipelineResult, runner
from gnome_pipelines.runner import _storable
from tests.lambdas.fakes import Table


def test_outputs_become_dynamodb_types():
    outputs = {"n_maps": np.int64(812), "auc": 0.71, "skipped": {"no_market": 3}, "version": "20261015T000000Z"}

    assert _storable(outputs) == {"n_maps": 812, "auc": Decimal("0.71"), "skipped": {"no_market": 3},
                                  "version": "20261015T000000Z"}


class _Events:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def put_events(self, Entries):
        if self.fail:
            raise RuntimeError("EventBridge is down")
        self.sent += Entries
        return {"FailedEntryCount": 0, "Entries": [{"EventId": "e1"}]}


class _Boto:
    def __init__(self, table, events=None):
        self.table = table
        self.events = events or _Events()

    def resource(self, name):
        return SimpleNamespace(Table=lambda _name: self.table)

    def client(self, name):
        assert name == "events"
        return self.events


@pytest.fixture
def run_env(monkeypatch):
    monkeypatch.setenv("PIPELINES_TABLE", "t")
    monkeypatch.setenv("PIPELINE_NAME", "probe")
    monkeypatch.setenv("RUN_ID", "r1")
    monkeypatch.setenv("GNOME_STAGE", "dev")
    monkeypatch.setenv("GNOME_CODE_VERSION", "gnome-pipelines@abc1234")
    ran = []
    monkeypatch.setattr(runner, "execute", lambda name, params: ran.append(name) or PipelineResult("succeeded", {"n": 1}))
    return ran


def test_a_run_cancelled_before_it_started_does_nothing(monkeypatch, run_env):
    table = Table([{"pk": "RUN#probe", "sk": "r1", "status": "CANCELLED"}])
    monkeypatch.setattr(runner, "boto3", _Boto(table))

    runner.main()

    assert run_env == [] and table.items[("RUN#probe", "r1")]["status"] == "CANCELLED"


def test_a_succeeded_run_is_announced_for_its_consumers(monkeypatch, run_env):
    boto = _Boto(Table([{"pk": "RUN#probe", "sk": "r1", "status": "PENDING"}]))
    monkeypatch.setattr(runner, "boto3", boto)
    monkeypatch.setenv("PIPELINE_FAMILY", "cs2")

    runner.main()

    [event] = boto.events.sent
    assert event["Source"] == "gnome.pipelines" and event["DetailType"] == "Pipeline Run Succeeded"
    assert json.loads(event["Detail"]) == {"pipeline": "probe", "family": "cs2", "run_id": "r1",
                                           "code_version": "gnome-pipelines@abc1234"}


def test_a_run_that_cant_be_announced_still_succeeds(monkeypatch, run_env):
    table = Table([{"pk": "RUN#probe", "sk": "r1", "status": "PENDING"}])
    monkeypatch.setattr(runner, "boto3", _Boto(table, _Events(fail=True)))

    runner.main()

    assert table.items[("RUN#probe", "r1")]["status"] == "SUCCEEDED"


def test_a_failed_run_is_not_announced(monkeypatch, run_env):
    boto = _Boto(Table([{"pk": "RUN#probe", "sk": "r1", "status": "PENDING"}]))
    monkeypatch.setattr(runner, "boto3", boto)
    monkeypatch.setattr(runner, "execute", lambda name, params: 1 / 0)

    with pytest.raises(SystemExit):
        runner.main()

    assert boto.events.sent == [] and boto.table.items[("RUN#probe", "r1")]["status"] == "FAILED"


def test_a_run_records_the_commit_that_ran_it_and_its_outputs(monkeypatch, run_env):
    table = Table([{"pk": "RUN#probe", "sk": "r1", "status": "PENDING"}])
    monkeypatch.setattr(runner, "boto3", _Boto(table))

    runner.main()

    item = table.items[("RUN#probe", "r1")]
    assert run_env == ["probe"]
    assert item["status"] == "SUCCEEDED" and item["code_version"] == "gnome-pipelines@abc1234" and item["outputs"] == {"n": 1}
