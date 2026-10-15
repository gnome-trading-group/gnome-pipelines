"""Start a pipeline run: record it, then start the Fargate task that runs gnome_pipelines.runner.

Called by the API (POST /research/pipelines/{pipelineName}/trigger) and by EventBridge Scheduler, which
sends {"pipeline_name": ..., "trigger": "scheduled"} directly.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import boto3

from store import definition_key, now, response, run_key, table

CLUSTER_ARN = os.environ["CLUSTER_ARN"]
TASK_DEFINITION_ARN = os.environ["TASK_DEFINITION_ARN"]
SUBNET_IDS = os.environ["SUBNET_IDS"].split(",")
SECURITY_GROUP_ID = os.environ["SECURITY_GROUP_ID"]
CONTAINER_NAME = "pipeline"

ecs = boto3.client("ecs")


def handler(event: dict, context) -> dict:
    path = event.get("pathParameters")
    if path:
        pipeline_name = path.get("pipelineName", "")
        trigger = "manual"
        params = json.loads(event.get("body") or "{}").get("parameters", {})
    else:
        pipeline_name = event.get("pipeline_name", "")
        trigger = event.get("trigger", "manual")
        params = event.get("parameters", {})

    definition = table.get_item(Key=definition_key(pipeline_name)).get("Item")
    if not definition:
        return response(404, {"error": f"Pipeline '{pipeline_name}' not found"})

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    merged = {**definition.get("parameters", {}), **params}
    table.put_item(Item={
        **run_key(pipeline_name, run_id),
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "status": "PENDING",
        "trigger": trigger,
        "parameters": merged,
        "queued_at": now(),
    })

    cpu, memory = int(definition.get("cpu", 1024)), int(definition.get("memory", 4096))
    started = ecs.run_task(
        cluster=CLUSTER_ARN,
        taskDefinition=TASK_DEFINITION_ARN,
        launchType="FARGATE",
        networkConfiguration={"awsvpcConfiguration": {
            "subnets": SUBNET_IDS, "securityGroups": [SECURITY_GROUP_ID], "assignPublicIp": "ENABLED",
        }},
        overrides={
            "containerOverrides": [{
                "name": CONTAINER_NAME,
                "cpu": cpu,
                "memory": memory,
                "environment": [
                    {"name": "PIPELINE_NAME", "value": pipeline_name},
                    {"name": "RUN_ID", "value": run_id},
                    {"name": "PIPELINE_PARAMS", "value": json.dumps(merged, default=_number)},
                ],
            }],
            "cpu": str(cpu),
            "memory": str(memory),
        },
    )
    if not started["tasks"]:
        # No task means no stopped event either, so the run would sit at PENDING forever.
        error = f"ECS didn't start the task: {started['failures']}"
        table.update_item(
            Key=run_key(pipeline_name, run_id),
            UpdateExpression="SET #s = :failed, completed_at = :ts, error_message = :err",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "FAILED", ":ts": now(), ":err": error},
        )
        raise RuntimeError(error)

    task_arn = started["tasks"][0]["taskArn"]
    table.update_item(
        Key=run_key(pipeline_name, run_id),
        UpdateExpression="SET ecs_task_arn = :arn",
        ExpressionAttributeValues={":arn": task_arn},
    )
    return response(202, {"run_id": run_id, "ecs_task_arn": task_arn})


def _number(value):
    # DynamoDB hands numbers back as Decimal.
    return int(value) if value % 1 == 0 else float(value)
