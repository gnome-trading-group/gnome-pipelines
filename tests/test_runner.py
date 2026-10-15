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


class _Boto:
    def __init__(self, table):
        self.table = table

    def resource(self, name):
        return SimpleNamespace(Table=lambda _name: self.table)


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


def test_a_run_records_the_commit_that_ran_it_and_its_outputs(monkeypatch, run_env):
    table = Table([{"pk": "RUN#probe", "sk": "r1", "status": "PENDING"}])
    monkeypatch.setattr(runner, "boto3", _Boto(table))

    runner.main()

    item = table.items[("RUN#probe", "r1")]
    assert run_env == ["probe"]
    assert item["status"] == "SUCCEEDED" and item["code_version"] == "gnome-pipelines@abc1234" and item["outputs"] == {"n": 1}
