"""Start the Fargate task that runs one pipeline run, for the trigger and for the status Lambda's restarts.

A run's task size is its pipeline's declared resources, and its task definition is its family's, whose image holds
only that family's dependencies.
"""
from __future__ import annotations

import json
import os

import boto3

CLUSTER_ARN = os.environ["CLUSTER_ARN"]
# {family: task definition ARN}, one per family image.
TASK_DEFINITIONS = json.loads(os.environ["TASK_DEFINITIONS"])
SUBNET_IDS = os.environ["SUBNET_IDS"].split(",")
SECURITY_GROUP_ID = os.environ["SECURITY_GROUP_ID"]
CONTAINER_NAME = "pipeline"

ecs = boto3.client("ecs")


def task_definition(declared: dict) -> str | None:
    return TASK_DEFINITIONS.get(declared.get("family"))


def launch(pipeline_name: str, run_id: str, params: dict, declared: dict) -> dict:
    cpu, memory = declared["resources"]["cpu"], declared["resources"]["memory"]
    return ecs.run_task(
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
