import os

import pandas as pd
import pyarrow as pa
import pytest
from click.testing import CliRunner
from gnomepy.data import DatasetStore, TableSpec

from gnome_pipelines import PIPELINE_REGISTRY, Pipeline, PipelineParams, PipelineResult
from gnome_pipelines.cli import main
from gnome_pipelines.environment import Stage, environment
from gnome_pipelines.stores import dataset_store

PROBE = TableSpec("probe", pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("note", pa.string())]),
                  key=("id",))


class _ProbeParams(PipelineParams):
    note: str
    n: int = 1
    dry_run: bool = False


class _Probe(Pipeline[_ProbeParams]):
    name = "probe"

    def run(self, params: _ProbeParams) -> PipelineResult:
        dataset_store().publish(PROBE, pd.DataFrame({"id": [1], "note": [params.note]}))
        env = environment()
        return PipelineResult("succeeded", {"stage": env.stage.value, "in_task": env.in_task,
                                            "params": params.model_dump()})


RUN_VARIABLES = ("GNOME_STAGE", "RUN_ID", "GNOME_CODE_VERSION", "GNOME_LOCAL_WAREHOUSE", "AWS_PROFILE", "PIPELINE_NAME")


@pytest.fixture
def probe(monkeypatch):
    monkeypatch.setitem(PIPELINE_REGISTRY, "probe", _Probe)
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
    saved = {v: os.environ.pop(v) for v in RUN_VARIABLES if v in os.environ}
    yield
    # main() sets these for the rest of the process, as it would for a real run.
    for v in RUN_VARIABLES:
        os.environ.pop(v, None)
    os.environ.update(saved)


def test_local_run_writes_the_local_warehouse_stamped_with_the_checkout(probe, tmp_path):
    result = CliRunner().invoke(main, ["run", "probe", "--stage", "local", "--warehouse", str(tmp_path),
                                       "-p", "note=hi", "-p", "n=3", "-p", "dry_run=false"])

    assert result.exit_code == 0, result.output
    outputs = result.output
    assert '"stage": "local"' in outputs and '"in_task": false' in outputs
    assert '"n": 3' in outputs and '"dry_run": false' in outputs
    stored = DatasetStore().load("probe", include_metadata=True)
    assert stored.note.tolist() == ["hi"]
    assert stored.code_version.iloc[0].startswith("gnome-pipelines@") and stored.code_version.iloc[0] != "gnome-pipelines@local"
    snapshot = DatasetStore()._catalog.load_table("research.probe").current_snapshot()
    assert snapshot.summary["gnome.run_id"].startswith("probe/local-")


def test_prod_asks_first(probe):
    result = CliRunner().invoke(main, ["run", "probe", "--stage", "prod", "-p", "note=hi"], input="n\n")

    assert result.exit_code == 1 and "Aborted" in result.output


def test_environment_requires_a_stage(monkeypatch):
    monkeypatch.delenv("GNOME_STAGE", raising=False)

    with pytest.raises(RuntimeError, match="GNOME_STAGE"):
        environment()


def test_environment_in_a_fargate_task(monkeypatch):
    monkeypatch.setenv("GNOME_STAGE", "prod")
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4/x")

    env = environment()

    assert env.stage is Stage.PROD and env.in_task and not env.is_local


def test_bad_parameters_stop_before_anything_runs(probe):
    result = CliRunner().invoke(main, ["run", "probe", "--stage", "prod", "-p", "nte=hi", "-p", "n=many"])

    assert result.exit_code == 2
    assert "note\n  Field required" in result.output and "nte\n  Extra inputs" in result.output
    assert "n\n  Input should be a valid integer" in result.output
    assert "Run probe against prod" not in result.output


def test_describe_lists_types_defaults_and_descriptions(probe):
    result = CliRunner().invoke(main, ["describe", "probe"])

    assert result.output.splitlines()[:2] == ["note: str (required)", "n: int (default 1)"]
