"""The pipelines API behind gnome-controller's /research/pipelines routes.

  GET  /research/pipelines                                     every pipeline, its schedules and its last run
  GET  /research/pipelines/{name}                              one pipeline, its schedules and recent runs
  POST /research/pipelines/{name}/runs                         start a run: {"parameters": {...}}
  PUT  /research/pipelines/{name}/schedules/{schedule}         switch a schedule: {"enabled": true}
  GET  /research/pipelines/{name}/runs/{run_id}/logs           the run's latest log lines (?before=<token>)
  POST /research/pipelines/{name}/runs/{run_id}/cancel         stop a pending or running run

Pipelines and their schedules are declared in code (the manifest); only runs and a schedule's on/off
state change at runtime.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from cron import next_fire
from store import SCHEDULE_GROUP, api_path, manifest, now, pipeline, public, response, run_key, schedule_name, table

TRIGGER_FUNCTION = os.environ["TRIGGER_FUNCTION"]
CLUSTER_ARN = os.environ["CLUSTER_ARN"]
LOG_GROUP = os.environ["LOG_GROUP"]
# trigger_pipeline.py's container name; ECS's awslogs stream is <prefix>/<container>/<task id>.
LOG_STREAM_PREFIX = "pipeline/pipeline/"
RECENT_RUNS = 20
LOG_LINES = 500

scheduler = boto3.client("scheduler")
lambdas = boto3.client("lambda")
logs = boto3.client("logs")
ecs = boto3.client("ecs")

ROUTES = [
    ("GET", r"/research/pipelines/?", lambda m, e: list_pipelines()),
    ("GET", r"/research/pipelines/(?P<name>[\w-]+)", lambda m, e: get_pipeline(m["name"])),
    ("POST", r"/research/pipelines/(?P<name>[\w-]+)/runs", lambda m, e: start_run(m["name"], _body(e))),
    ("PUT", r"/research/pipelines/(?P<name>[\w-]+)/schedules/(?P<schedule>[\w-]+)",
     lambda m, e: set_schedule(m["name"], m["schedule"], _body(e))),
    ("GET", r"/research/pipelines/(?P<name>[\w-]+)/runs/(?P<run_id>[\w-]+)/logs",
     lambda m, e: run_logs(m["name"], m["run_id"], (e.get("queryStringParameters") or {}).get("before"))),
    ("POST", r"/research/pipelines/(?P<name>[\w-]+)/runs/(?P<run_id>[\w-]+)/cancel",
     lambda m, e: cancel_run(m["name"], m["run_id"])),
]


def handler(event: dict, context) -> dict:
    method, path = event.get("httpMethod", ""), api_path(event)
    for route_method, pattern, action in ROUTES:
        match = re.fullmatch(pattern, path)
        if match and route_method == method:
            try:
                return action(match.groupdict(), event)
            except BadRequest as e:
                return response(400, {"error": str(e)})
    return response(404, {"error": f"No route for {method} {path}"})


class BadRequest(Exception):
    pass


def _body(event: dict) -> dict:
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError as e:
        raise BadRequest(f"body is not JSON: {e}") from None
    if not isinstance(body, dict):
        raise BadRequest("body must be a JSON object")
    return body


def list_pipelines() -> dict:
    states = _schedule_states()
    pipelines = []
    for name, declared in sorted(manifest().items()):
        last = table.query(KeyConditionExpression=Key("pk").eq(f"RUN#{name}"), ScanIndexForward=False, Limit=1)
        pipelines.append({**_describe(declared, states), "last_run": public(last["Items"][0]) if last["Items"] else None})
    return response(200, {"pipelines": pipelines, "count": len(pipelines)})


def get_pipeline(name: str) -> dict:
    declared = pipeline(name)
    if declared is None:
        return response(404, {"error": f"No pipeline named '{name}'"})
    runs = table.query(KeyConditionExpression=Key("pk").eq(f"RUN#{name}"), ScanIndexForward=False, Limit=RECENT_RUNS)
    return response(200, {"pipeline": _describe(declared, _schedule_states()), "runs": [public(r) for r in runs["Items"]]})


def start_run(name: str, body: dict) -> dict:
    result = lambdas.invoke(
        FunctionName=TRIGGER_FUNCTION,
        Payload=json.dumps({"pipeline_name": name, "trigger": "manual", "parameters": body.get("parameters") or {}}),
    )
    payload = json.loads(result["Payload"].read() or b"{}")
    if result.get("FunctionError"):
        return response(502, {"error": payload.get("errorMessage", "the trigger failed")})
    status = payload.pop("status")
    return response(status, payload)


def set_schedule(name: str, schedule: str, body: dict) -> dict:
    if not isinstance(body.get("enabled"), bool):
        raise BadRequest('send {"enabled": true} or {"enabled": false}')
    try:
        current = scheduler.get_schedule(Name=schedule_name(name, schedule), GroupName=SCHEDULE_GROUP)
    except scheduler.exceptions.ResourceNotFoundException:
        return response(404, {"error": f"{name} has no schedule '{schedule}'"})
    state = "ENABLED" if body["enabled"] else "DISABLED"
    scheduler.update_schedule(**_updatable(current), State=state)
    return response(200, {"pipeline": name, "schedule": schedule, "enabled": body["enabled"]})


def run_logs(name: str, run_id: str, before: str | None) -> dict:
    run = table.get_item(Key=run_key(name, run_id)).get("Item")
    if run is None:
        return response(404, {"error": f"No run {run_id} of {name}"})
    if not run.get("ecs_task_arn"):
        return response(200, {"lines": [], "before": None})
    stream = LOG_STREAM_PREFIX + run["ecs_task_arn"].rsplit("/", 1)[-1]
    request = {"logGroupName": LOG_GROUP, "logStreamName": stream, "limit": LOG_LINES, "startFromHead": False}
    if before:
        request["nextToken"] = before
    try:
        page = logs.get_log_events(**request)
    except logs.exceptions.ResourceNotFoundException:
        # The stream appears once the container starts.
        return response(200, {"lines": [], "before": None})
    lines = [{"at": e["timestamp"], "message": e["message"]} for e in page["events"]]
    return response(200, {"lines": lines, "before": page["nextBackwardToken"] if lines else None})


def cancel_run(name: str, run_id: str) -> dict:
    try:
        run = table.update_item(
            Key=run_key(name, run_id),
            UpdateExpression="SET #s = :cancelled, completed_at = :ts",
            ConditionExpression="#s IN (:pending, :running)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":cancelled": "CANCELLED", ":ts": now(), ":pending": "PENDING", ":running": "RUNNING"},
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return response(409, {"error": f"Run {run_id} isn't pending or running"})
    if run.get("ecs_task_arn"):
        ecs.stop_task(cluster=CLUSTER_ARN, task=run["ecs_task_arn"], reason="Cancelled from the Pipelines page")
    return response(200, {"run": public(run)})


def _schedule_states() -> dict[str, dict]:
    """Each schedule's summary: its State, and LastModificationDate, which a rate schedule counts from."""
    pages = scheduler.get_paginator("list_schedules").paginate(GroupName=SCHEDULE_GROUP)
    return {s["Name"]: s for page in pages for s in page["Schedules"]}


def _describe(declared: dict, states: dict[str, dict]) -> dict:
    now = datetime.now(timezone.utc)
    schedules = []
    for s in declared["schedules"]:
        summary = states.get(schedule_name(declared["name"], s["name"]), {})
        enabled = summary.get("State") == "ENABLED"
        upcoming = next_fire(s["expression"], now, summary.get("LastModificationDate")) if enabled else None
        schedules.append({
            **s, "enabled": enabled,
            # A schedule the sync hasn't created yet (or failed to) can't be switched on.
            "synced": schedule_name(declared["name"], s["name"]) in states,
            "next_run": upcoming.isoformat() if upcoming else None,
        })
    return {**declared, "schedules": schedules}


def _updatable(current: dict) -> dict:
    keep = ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow",
            "Target", "Description", "ActionAfterCompletion")
    return {k: current[k] for k in keep if k in current}
