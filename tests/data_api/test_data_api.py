import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pytest
from gnomepy.data import ArtifactStore, CatalogConfig, DatasetStore, TableSpec

import app

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
SPEC = TableSpec("cs2_match_veto", pa.schema([
    pa.field("match_id", pa.int64(), nullable=False),
    pa.field("order", pa.int64(), nullable=False),
    pa.field("map_name", pa.string()),
]), key=("match_id", "order"))


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    config = CatalogConfig(local_warehouse=tmp_path, artifact_root=str(tmp_path / "files"))
    monkeypatch.setattr(app, "_config", config)
    store = DatasetStore(config, code_version="gnome-pipelines@abc", run_id="hltv_cs2/20261001T000000-aa")
    store.publish(SPEC, pd.DataFrame({"match_id": [1, 1], "order": [1, 2], "map_name": ["de_nuke", "de_inferno"]}), ingested_at=T0)
    store.publish(SPEC, pd.DataFrame({"match_id": [2], "order": [1], "map_name": ["de_mirage"]}), ingested_at=T0 + timedelta(days=7))
    model = tmp_path / "model.joblib"
    model.write_bytes(b"model")
    store.load("cs2_match_veto")
    artifacts = ArtifactStore(config, code_version="gnome-pipelines@abc", run_id="cs2_prematch_train/r1")
    ref = artifacts.publish(model, "xgboost_model", "cs2_prematch_preveto", inputs=store.loaded,
                            description="pre-veto", params={"features": 40})
    artifacts.set_alias(ref, "production")
    return ref


def get(path):
    result = app.handler({"httpMethod": "GET", **event_path(path)}, None)
    return result["statusCode"], json.loads(result["body"])


def test_datasets_list_their_key_size_and_last_publish(warehouse):
    status, body = get("/research/datasets")

    veto = next(d for d in body["datasets"] if d["name"] == "cs2_match_veto")
    assert status == 200
    assert veto["key"] == ["match_id", "order"] and veto["stored_rows"] == 3 and veto["publishes"] == 2
    assert veto["last_publish"]["rows"] == 1 and veto["last_publish"]["run_id"] == "hltv_cs2/20261001T000000-aa"


def test_a_dataset_shows_its_schema_history_and_a_sample(warehouse):
    status, body = get("/research/datasets/cs2_match_veto")

    assert status == 200
    assert [c["name"] for c in body["columns"]] == ["match_id", "order", "map_name", "ingested_at", "code_version"]
    assert [h["rows"] for h in body["history"]] == [1, 2]
    assert len(body["sample"]) == 3 and "row_hash" not in body["sample"][0]
    assert get("/research/datasets/nope")[0] == 404


def test_artifacts_list_their_latest_version_and_aliases(warehouse):
    status, body = get("/research/artifacts")

    assert status == 200 and body["count"] == 1
    assert body["artifacts"][0] | {"latest_version": warehouse.version, "aliases": {"production": warehouse.version},
                                   "versions": 1} == body["artifacts"][0]


def test_an_artifact_shows_each_version_with_its_params_and_inputs(warehouse):
    status, body = get("/research/artifacts/xgboost_model/cs2_prematch_preveto")

    version = body["versions"][0]
    assert status == 200 and version["version"] == warehouse.version
    assert version["params"] == {"features": 40}
    assert [i["dataset"] for i in version["inputs"]] == ["cs2_match_veto"]
    assert get("/research/artifacts/xgboost_model/nope")[0] == 404


def test_unknown_routes_are_404(warehouse):
    assert get("/research/somewhere")[0] == 404
    assert app.handler({"httpMethod": "POST", **event_path("/research/datasets")}, None)["statusCode"] == 404


def event_path(path: str) -> dict:
    """How API Gateway presents a request to one of the controller's {proxy+} routes."""
    root, _, rest = path.removeprefix("/research/").partition("/")
    if not rest:
        return {"resource": f"/research/{root}", "pathParameters": None}
    return {"resource": f"/research/{root}/{{proxy+}}", "pathParameters": {"proxy": rest}}
