"""PUT /research/pipelines/{pipelineName}: change a definition and keep its EventBridge schedule in step."""
from __future__ import annotations

import json
import os

import boto3
from botocore.exceptions import ClientError

from store import definition_key, now, public, response, table

TRIGGER_FUNCTION_ARN = os.environ["TRIGGER_FUNCTION_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
SCHEDULE_GROUP = os.environ["SCHEDULE_GROUP"]
EDITABLE = ("description", "schedule", "schedule_enabled", "parameters", "cpu", "memory")

scheduler = boto3.client("scheduler")


def handler(event: dict, context) -> dict:
    pipeline_name = (event.get("pathParameters") or {}).get("pipelineName", "")
    body = json.loads(event.get("body") or "{}")
    updates = {k: body[k] for k in EDITABLE if k in body} | {"updated_at": now()}

    try:
        item = table.update_item(
            Key=definition_key(pipeline_name),
            UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in updates),
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeNames={f"#{k}": k for k in updates},
            ExpressionAttributeValues={f":{k}": v for k, v in updates.items()},
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return response(404, {"error": f"Pipeline '{pipeline_name}' not found"})

    sync_schedule(pipeline_name, item.get("schedule"), bool(item.get("schedule_enabled")))
    return response(200, {"pipeline": public(item)})


def sync_schedule(pipeline_name: str, schedule: str | None, enabled: bool) -> None:
    if not enabled or not schedule:
        try:
            scheduler.delete_schedule(Name=pipeline_name, GroupName=SCHEDULE_GROUP)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        return

    spec = dict(
        Name=pipeline_name,
        GroupName=SCHEDULE_GROUP,
        ScheduleExpression=schedule,
        ScheduleExpressionTimezone="UTC",
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        ActionAfterCompletion="NONE",
        Target={
            "Arn": TRIGGER_FUNCTION_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"pipeline_name": pipeline_name, "trigger": "scheduled"}),
        },
    )
    try:
        scheduler.update_schedule(**spec)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        scheduler.create_schedule(**spec)
