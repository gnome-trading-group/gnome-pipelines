"""POST /research/pipelines: add a pipeline definition, unscheduled until an update enables its schedule."""
from __future__ import annotations

import json

from botocore.exceptions import ClientError

from store import definition_key, now, public, response, table

DEFAULT_CPU = 1024
DEFAULT_MEMORY = 4096


def handler(event: dict, context) -> dict:
    body = json.loads(event.get("body") or "{}")
    pipeline_name = body.get("pipeline_name", "").strip()
    if not pipeline_name:
        return response(400, {"error": "pipeline_name is required"})

    stamp = now()
    item = {
        **definition_key(pipeline_name),
        "pipeline_name": pipeline_name,
        "description": body.get("description", ""),
        "schedule": body.get("schedule"),
        "schedule_enabled": False,
        "parameters": body.get("parameters", {}),
        "cpu": body.get("cpu", DEFAULT_CPU),
        "memory": body.get("memory", DEFAULT_MEMORY),
        "created_at": stamp,
        "updated_at": stamp,
    }
    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return response(409, {"error": f"Pipeline '{pipeline_name}' already exists"})
    return response(201, {"pipeline": public(item)})
