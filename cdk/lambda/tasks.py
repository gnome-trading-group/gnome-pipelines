"""Start the Fargate task that runs one pipeline run, for the trigger and for the status Lambda's restarts.

A run's task size is its pipeline's declared resources, and its task definition is its family's, whose image holds
only that family's dependencies. A polling source has a task definition of its own (same image), whose role reads
only that source's secrets, and its tasks are started by "source:<name>" so CI can find them to restart on a new image.
"""
from __future__ import annotations

import json
import os

import boto3

CLUSTER_ARN = os.environ["CLUSTER_ARN"]
# {family: task definition ARN} per family image, and {"source:<name>": task definition ARN} per polling source.
TASK_DEFINITIONS = json.loads(os.environ["TASK_DEFINITIONS"])
SOURCE_PREFIX = "source:"
SUBNET_IDS = os.environ["SUBNET_IDS"].split(",")
SECURITY_GROUP_ID = os.environ["SECURITY_GROUP_ID"]
CONTAINER_NAME = "pipeline"

ecs = boto3.client("ecs")


def task_definition(declared: dict) -> str | None:
    if declared.get("kind") == "source":
        return TASK_DEFINITIONS.get(SOURCE_PREFIX + declared["name"])
    return TASK_DEFINITIONS.get(declared.get("family"))


def launch(pipeline_name: str, run_id: str, params: dict, declared: dict) -> dict:
    cpu, memory = declared["resources"]["cpu"], declared["resources"]["memory"]
    extra = {"startedBy": SOURCE_PREFIX + pipeline_name} if declared.get("kind") == "source" else {}
    return ecs.run_task(
        **extra,
        cluster=CLUSTER_ARN,
        taskDefinition=task_definition(declared),
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
                    {"name": "PIPELINE_PARAMS", "value": json.dumps(params)},
                ],
            }],
            "cpu": str(cpu),
            "memory": str(memory),
        },
    )
