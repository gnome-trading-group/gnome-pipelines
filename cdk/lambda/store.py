"""The gnome-pipelines table and the API's response shape, shared by every handler.

One table holds both kinds of item:
  definitions  pk = "PIPELINE",       sk = <pipeline name>
  runs         pk = "RUN#<pipeline>", sk = <run id>, which sorts by start time
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3

DEFINITION_PK = "PIPELINE"
TASK_INDEX = "by-task"
KEY_ATTRIBUTES = ("pk", "sk")

table = boto3.resource("dynamodb").Table(os.environ["PIPELINES_TABLE"])

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


def definition_key(pipeline_name: str) -> dict:
    return {"pk": DEFINITION_PK, "sk": pipeline_name}


def run_key(pipeline_name: str, run_id: str) -> dict:
    return {"pk": f"RUN#{pipeline_name}", "sk": run_id}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def public(item: dict) -> dict:
    return _native({k: v for k, v in item.items() if k not in KEY_ATTRIBUTES})


def response(status_code: int, body: dict) -> dict:
    return {"statusCode": status_code, "headers": CORS_HEADERS, "body": json.dumps(_native(body), sort_keys=True)}


def _native(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_native(v) for v in obj]
    return obj
