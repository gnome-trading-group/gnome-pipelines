"""What every handler shares: the runs table, the pipeline manifest in SSM, and the API's response shape.

Runs are items in the gnome-pipelines table: pk = "RUN#<pipeline>", sk = <run id>, which sorts by start
time. Pipelines themselves are not stored here: they are declared in code and published by CI as one SSM
parameter each under MANIFEST_PREFIX (gnome_pipelines/manifest.py).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3

TASK_INDEX = "by-task"
KEY_ATTRIBUTES = ("pk", "sk")
MANIFEST_PREFIX = "/gnome/pipelines/types/"
SCHEDULE_GROUP = "gnome-pipelines"

table = boto3.resource("dynamodb").Table(os.environ["PIPELINES_TABLE"])
ssm = boto3.client("ssm")

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


def run_key(pipeline_name: str, run_id: str) -> dict:
    return {"pk": f"RUN#{pipeline_name}", "sk": run_id}


def schedule_name(pipeline_name: str, schedule: str) -> str:
    return f"{pipeline_name}.{schedule}"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def manifest() -> dict[str, dict]:
    pages = ssm.get_paginator("get_parameters_by_path").paginate(Path=MANIFEST_PREFIX)
    entries = (json.loads(p["Value"]) for page in pages for p in page["Parameters"])
    return {e["name"]: e for e in entries}


def pipeline(pipeline_name: str) -> dict | None:
    try:
        return json.loads(ssm.get_parameter(Name=MANIFEST_PREFIX + pipeline_name)["Parameter"]["Value"])
    except ssm.exceptions.ParameterNotFound:
        return None


def public(item: dict) -> dict:
    return native({k: v for k, v in item.items() if k not in KEY_ATTRIBUTES})


def api_path(event: dict) -> str:
    # The resource with its {proxy+} filled in: unlike event["path"], it never carries a custom domain's base path.
    resource = event.get("resource", "")
    proxy = (event.get("pathParameters") or {}).get("proxy")
    return resource.replace("{proxy+}", proxy) if proxy is not None else resource


def response(status_code: int, body: dict) -> dict:
    return {"statusCode": status_code, "headers": CORS_HEADERS, "body": json.dumps(native(body), sort_keys=True)}


def native(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [native(v) for v in obj]
    return obj
