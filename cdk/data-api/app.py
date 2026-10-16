"""The datasets and artifacts API behind gnome-controller's /research/datasets and /research/artifacts routes.

  GET /research/datasets                                        every dataset: key, size, last publish
  GET /research/datasets/{name}                                 schema, partitioning, publish history, a sample
  GET /research/artifacts                                       every artifact: latest version, aliases
  GET /research/artifacts/{type}/{name}                         its versions, aliases and the datasets each used
  GET /research/artifacts/{type}/{name}/{version}/download      a short-lived download link

Read-only, straight from the Iceberg tables gnomepy.data writes. A publish's run id is
<pipeline>/<run id> (gnome_pipelines.stores), which the controller turns into a link to the run.
"""
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal

import boto3
import pandas as pd
from gnomepy.data import CatalogConfig, DatasetStore
from gnomepy.data.datasets import KEY_PROPERTY, MANAGED_COLUMNS, ROW_HASH
from pyiceberg.table import Table

HISTORY = 50
SAMPLE_ROWS = 50
DOWNLOAD_SECONDS = 900

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
}

_config: CatalogConfig | None = None


def config() -> CatalogConfig:
    # Kept across invocations: from_env reads SSM, and a warm container keeps its config.
    global _config
    if _config is None:
        _config = CatalogConfig.from_env()
    return _config


ROUTES = [
    (r"/research/datasets/?", lambda m: list_datasets()),
    (r"/research/datasets/(?P<name>[\w-]+)", lambda m: get_dataset(m["name"])),
    (r"/research/artifacts/?", lambda m: list_artifacts()),
    (r"/research/artifacts/(?P<type>[\w-]+)/(?P<name>[\w.-]+)", lambda m: get_artifact(m["type"], m["name"])),
    (r"/research/artifacts/(?P<type>[\w-]+)/(?P<name>[\w.-]+)/(?P<version>[\w.-]+)/download",
     lambda m: download_artifact(m["type"], m["name"], m["version"])),
]


def handler(event: dict, context) -> dict:
    method, path = event.get("httpMethod", ""), api_path(event)
    if method == "GET":
        for pattern, action in ROUTES:
            match = re.fullmatch(pattern, path)
            if match:
                return action(match.groupdict())
    return response(404, {"error": f"No route for {method} {path}"})


def list_datasets() -> dict:
    store = DatasetStore(config())
    names = store.list()
    with ThreadPoolExecutor(max_workers=8) as pool:
        tables = list(pool.map(lambda n: store._catalog.load_table(f"{store.namespace}.{n}"), names))
    return response(200, {"datasets": [_summary(name, table) for name, table in zip(names, tables)], "count": len(names)})


def get_dataset(name: str) -> dict:
    store = DatasetStore(config())
    if name not in store.list():
        return response(404, {"error": f"No dataset named '{name}'"})
    table = store._catalog.load_table(f"{store.namespace}.{name}")
    sample = table.scan(limit=SAMPLE_ROWS).to_pandas().drop(columns=[ROW_HASH], errors="ignore")
    return response(200, {
        **_summary(name, table),
        "columns": [{"name": f.name, "type": str(f.field_type), "required": f.required,
                     "managed": f.name in MANAGED_COLUMNS} for f in table.schema().fields if f.name != ROW_HASH],
        "history": _publishes(table)[:HISTORY],
        "sample": _records(sample),
    })


def list_artifacts() -> dict:
    versions, aliases = _artifact_frames()
    artifacts = []
    for (artifact_type, name), group in versions.groupby(["artifact_type", "name"], sort=True):
        latest = group.sort_values("created_at").iloc[-1]
        artifacts.append({
            "artifact_type": artifact_type, "name": name, "versions": len(group),
            "latest_version": latest["version"], "latest_created_at": latest["created_at"],
            "description": latest["description"],
            "aliases": _aliases_of(aliases, artifact_type, name),
        })
    return response(200, {"artifacts": artifacts, "count": len(artifacts)})


def get_artifact(artifact_type: str, name: str) -> dict:
    versions, aliases = _artifact_frames()
    mine = versions[(versions.artifact_type == artifact_type) & (versions.name == name)]
    if mine.empty:
        return response(404, {"error": f"No artifact {artifact_type}/{name}"})
    inputs = _load("artifacts", "artifact_inputs")
    if not inputs.empty:
        inputs = inputs[(inputs.artifact_type == artifact_type) & (inputs.name == name)]
    rows = []
    for record in _records(mine.sort_values("created_at", ascending=False)):
        used = inputs[inputs.version == record["version"]] if not inputs.empty else inputs
        rows.append({**record, "params": _json(record.get("params")), "metrics": _json(record.get("metrics")),
                     "inputs": _records(used.drop(columns=["artifact_type", "name", "version"]))})
    return response(200, {"artifact_type": artifact_type, "name": name, "versions": rows,
                           "aliases": _aliases_of(aliases, artifact_type, name)})


def download_artifact(artifact_type: str, name: str, version: str) -> dict:
    versions, _ = _artifact_frames()
    match = versions[(versions.artifact_type == artifact_type) & (versions.name == name) & (versions.version == version)]
    if match.empty:
        return response(404, {"error": f"No version {version} of {artifact_type}/{name}"})
    bucket, key = match.iloc[0]["uri"].removeprefix("s3://").split("/", 1)
    url = boto3.client("s3").generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=DOWNLOAD_SECONDS)
    return response(200, {"url": url, "expires_in": DOWNLOAD_SECONDS})


def _summary(name: str, table: Table) -> dict:
    snapshot = table.current_snapshot()
    publishes = _publishes(table)
    return {
        "name": name,
        "key": table.properties.get(KEY_PROPERTY, "").split(","),
        "partitioned_by": [f"{table.schema().find_column_name(f.source_id)} ({f.transform})" for f in table.spec().fields],
        # Every row ever appended, corrections included; a load returns the newest per key, so fewer.
        "stored_rows": int(snapshot.summary["total-records"]) if snapshot else 0,
        "publishes": len(publishes),
        "last_publish": publishes[0] if publishes else None,
    }


def _publishes(table: Table) -> list[dict]:
    # S3 Tables' own compaction commits snapshots too; only appends are publishes.
    appends = [s for s in table.metadata.snapshots if s.summary and s.summary.operation.value == "append"]
    return [{
        "at": datetime.fromtimestamp(s.timestamp_ms / 1000, tz=timezone.utc).isoformat(),
        "rows": int(s.summary["added-records"]),
        "code_version": s.summary.get("gnome.code_version"),
        "run_id": s.summary.get("gnome.run_id"),
    } for s in sorted(appends, key=lambda s: s.timestamp_ms, reverse=True)]


def _artifact_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    return _load("artifacts", "artifacts"), _load("artifacts", "artifact_aliases")


def _load(namespace: str, table: str) -> pd.DataFrame:
    store = DatasetStore(config(), namespace=namespace)
    if table not in store.list():
        return pd.DataFrame(columns=["artifact_type", "name", "version"])
    return store.load(table)


def _aliases_of(aliases: pd.DataFrame, artifact_type: str, name: str) -> dict[str, str]:
    if aliases.empty:
        return {}
    mine = aliases[(aliases.artifact_type == artifact_type) & (aliases.name == name)]
    return dict(zip(mine.alias, mine.version))


def _json(text: object) -> object:
    try:
        return json.loads(text) if isinstance(text, str) and text else None
    except json.JSONDecodeError:
        return text


def _records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))


def api_path(event: dict) -> str:
    # The resource with its {proxy+} filled in: unlike event["path"], it never carries a custom domain's base path.
    resource = event.get("resource", "")
    proxy = (event.get("pathParameters") or {}).get("proxy")
    return resource.replace("{proxy+}", proxy) if proxy is not None else resource


def response(status_code: int, body: dict) -> dict:
    return {"statusCode": status_code, "headers": CORS_HEADERS, "body": json.dumps(body, default=_jsonable, sort_keys=True)}


def _jsonable(value: object) -> object:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "item"):
        return value.item()
    return str(value)
