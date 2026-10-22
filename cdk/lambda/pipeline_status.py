"""Mark a run failed when its Fargate task stops abnormally, and report a failed run as an alert.

Triggered by the EventBridge rule on the cluster's task-stopped events. The runner records a failure itself before
exiting; this marks the ones it couldn't (OOM kills, container crashes). A task that failed to start ran none of the
pipeline, so its run is started again, up to MAX_START_ATTEMPTS: Fargate's image pulls occasionally time out reaching
ECR. Either way a failed run is put on the alert
bus: a scheduled run's output can feed trading (the CS2 strategies and launcher read cs2_upcoming), so it's an ERROR; a manual
one is only for the record, since whoever started it sees it fail.
"""
from __future__ import annotations

import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from store import TASK_INDEX, native, now, pipeline, run_key, table
from tasks import launch

# The registry's alerting stack names its bus; any service in the account may put alerts on it.
ALERT_BUS_NAME = os.environ.get("ALERT_BUS_NAME", "gnome-alerts")
MAX_START_ATTEMPTS = 3

logger = logging.getLogger(__name__)
events = boto3.client("events")


def handler(event: dict, context) -> dict:
    detail = event.get("detail", {})
    if detail.get("lastStatus") != "STOPPED":
        return {}
    containers = detail.get("containers") or [{}]
    exit_code = containers[0].get("exitCode")
    if exit_code == 0:
        return {}

    task_arn = detail.get("taskArn", "")
    runs = table.query(IndexName=TASK_INDEX, KeyConditionExpression=Key("ecs_task_arn").eq(task_arn))
    for run in runs["Items"]:
        if (run.get("status") == "PENDING" and detail.get("stopCode") == "TaskFailedToStart"
                and _restart(run, task_arn, detail.get("stoppedReason", ""))):
            continue
        if run.get("status") in ("RUNNING", "PENDING"):
            error = f"ECS task stopped with exit code {exit_code}: {detail.get('stoppedReason', '')}".rstrip(": ")
            try:
                table.update_item(
                    Key=run_key(run["pipeline_name"], run["run_id"]),
                    UpdateExpression="SET #s = :failed, completed_at = :ts, error_message = :err",
                    ConditionExpression="#s IN (:running, :pending)",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":failed": "FAILED", ":ts": now(), ":err": error, ":running": "RUNNING", ":pending": "PENDING",
                    },
                )
            except ClientError as e:
                if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                continue
            _report_failure(run, error)
        elif run.get("status") == "FAILED":
            _report_failure(run, run.get("error_message") or f"ECS task stopped with exit code {exit_code}")
    return {}


def _restart(run: dict, stopped_task: str, reason: str) -> bool:
    """Start a run's task again; False when it's out of attempts or ECS refused, so the run fails as usual."""
    attempts = int(run.get("start_attempts", 1))
    declared = pipeline(run["pipeline_name"])
    if attempts >= MAX_START_ATTEMPTS or declared is None:
        return False
    key = run_key(run["pipeline_name"], run["run_id"])
    # Claim the restart before launching: EventBridge can deliver a stopped event twice, and only one may relaunch.
    try:
        table.update_item(
            Key=key,
            UpdateExpression="SET start_attempts = :attempts, start_errors = :errors",
            ConditionExpression="ecs_task_arn = :stopped AND #s = :pending",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":attempts": attempts + 1, ":errors": [*run.get("start_errors", []), reason],
                ":stopped": stopped_task, ":pending": "PENDING",
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return True
    started = launch(run["pipeline_name"], run["run_id"], native(run.get("parameters") or {}), declared)
    if not started["tasks"]:
        logger.warning("ECS didn't restart %s run %s: %s", run["pipeline_name"], run["run_id"], started["failures"])
        return False
    table.update_item(
        Key=key, UpdateExpression="SET ecs_task_arn = :arn", ExpressionAttributeValues={":arn": started["tasks"][0]["taskArn"]},
    )
    return True


def _report_failure(run: dict, error: str) -> None:
    pipeline, run_id = run.get("pipeline_name", ""), run.get("run_id", "")
    alert = {
        "action": "FIRING",
        "key": f"PIPELINE_FAILED:{pipeline}:{run_id}",
        "type": "PIPELINE_FAILED",
        "source": "ingest",
        "title": f"Pipeline {pipeline} failed",
        "body": error,
        "labels": {"pipeline": pipeline, "runId": run_id, "trigger": run.get("trigger", "manual")},
        "stateAt": now(),
    }
    # Alerting must never fail the status update.
    try:
        events.put_events(Entries=[{
            "EventBusName": ALERT_BUS_NAME, "Source": "gnome.alerts", "DetailType": "Alert", "Detail": json.dumps(alert),
        }])
    except Exception:
        logger.warning("Couldn't report pipeline %s run %s failing", pipeline, run_id, exc_info=True)
