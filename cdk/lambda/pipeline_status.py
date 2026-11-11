"""Mark a run failed when its Fargate task stops abnormally, and report a failed run as an alert.

Triggered by the EventBridge rule on the cluster's task-stopped events. The runner records a failure itself before
exiting; this marks the ones it couldn't (OOM kills, container crashes). A task that failed to start ran none of the
pipeline, so its run is started again, up to MAX_START_ATTEMPTS: Fargate's image pulls occasionally time out reaching
ECR. Either way a failed run is put on the alert
bus: a scheduled run's output can feed trading (the CS2 strategies and launcher read cs2_upcoming), so it's an ERROR; a manual
one is only for the record, since whoever started it sees it fail.

A polling source's run (sources.py) has no end of its own, so every stop of its current task is handled: asked for
(STOPPING) -> STOPPED; otherwise the same run is started again on a new task, whatever the exit code, since that is
how a new image reaches it (CI stops the task) and how it outlives a crash. A crash (a non-zero exit, or a task that
failed to start, unless the stop was asked for with stop_task) puts a WARNING and counts toward MAX_CRASHES within
CRASH_WINDOW or MAX_CRASHES_PER_DAY; the one that reaches either fails the run and alerts as an ERROR, however the run
was started, because nothing is polling until someone acts. An EventBridge schedule also invokes this every few
minutes (reconcile_sources) to end, with an ERROR, any source run left live with no task.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import sources
from store import TASK_INDEX, manifest, native, now, pipeline, run_key, table
from tasks import CLUSTER_ARN, ecs, launch

# The registry's alerting stack names its bus; any service in the account may put alerts on it.
ALERT_BUS_NAME = os.environ.get("ALERT_BUS_NAME", "gnome-alerts")
MAX_START_ATTEMPTS = 3
MAX_CRASHES = 3
CRASH_WINDOW = timedelta(minutes=30)
# A crash every few hours never trips the window above but is still a broken source.
MAX_CRASHES_PER_DAY = 10
CRASH_DAY = timedelta(days=1)

logger = logging.getLogger(__name__)
events = boto3.client("events")


def handler(event: dict, context) -> dict:
    if event.get("detail-type") == "Scheduled Event":
        return {"ended": reconcile_sources()}
    detail = event.get("detail", {})
    if detail.get("lastStatus") != "STOPPED":
        return {}
    containers = detail.get("containers") or [{}]
    exit_code = containers[0].get("exitCode")
    task_arn = detail.get("taskArn", "")
    runs = table.query(IndexName=TASK_INDEX, KeyConditionExpression=Key("ecs_task_arn").eq(task_arn))
    for run in runs["Items"]:
        if sources.is_source(run):
            _source_stopped(run, task_arn, exit_code, detail)
            continue
        if exit_code == 0:
            continue
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


def _source_stopped(run: dict, task_arn: str, exit_code, detail: dict) -> None:
    name, run_id, status = run["pipeline_name"], run["run_id"], run.get("status")
    reason = detail.get("stoppedReason", "")
    if status == "STOPPING":
        sources.finish(name, run_id, "STOPPED")
        return
    if status not in ("PENDING", "RUNNING"):
        return
    # A stop_task (CI restarting it on a new image) ends the task on purpose, whatever its exit code says.
    asked = detail.get("stopCode") == "UserInitiated"
    crashed = not asked and (detail.get("stopCode") == "TaskFailedToStart" or exit_code != 0)
    at = datetime.fromisoformat(now())
    recent = [c for c in run.get("crashes", []) if at - datetime.fromisoformat(c) < CRASH_DAY]
    if crashed:
        recent.append(at.isoformat())
    in_window = [c for c in recent if at - datetime.fromisoformat(c) < CRASH_WINDOW]
    why = f"task stopped with exit code {exit_code}: {reason}".rstrip(": ")
    if len(in_window) >= MAX_CRASHES or len(recent) >= MAX_CRASHES_PER_DAY:
        error = (f"{len(in_window)} crashes in {CRASH_WINDOW.seconds // 60} min" if len(in_window) >= MAX_CRASHES
                 else f"{len(recent)} crashes in a day") + f"; last {why}"
        failed = sources.finish(name, run_id, "FAILED", error)
        if failed is not None:
            _report_failure(failed, error)
        return
    # Before the claim: once claimed, the old task no longer maps to the run, so a retry of this event does nothing.
    declared = pipeline(name)
    restarting = sources.RESTARTING + task_arn
    # Claim the restart first, moving the run off the stopped task: a duplicate stopped event for it then matches
    # nothing, so only this one launches.
    try:
        table.update_item(
            Key=run_key(name, run_id),
            UpdateExpression="SET #s = :pending, crashes = :crashes, restarts = :restarts, last_stop = :why, "
                             "ecs_task_arn = :restarting, restarting_at = :at",
            ConditionExpression="ecs_task_arn = :stopped AND #s IN (:pending, :running)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":pending": "PENDING", ":running": "RUNNING", ":crashes": recent,
                                       ":restarts": int(run.get("restarts", 0)) + 1, ":why": why,
                                       ":restarting": restarting, ":at": at.isoformat(), ":stopped": task_arn},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return
    try:
        _relaunch(run, declared, restarting, why, crashed, len(recent))
    except Exception as e:
        # Nothing retries a claimed restart, so anything that goes wrong now ends the run loudly rather than leaving
        # it live with no task.
        logger.exception("restarting %s run %s failed", name, run_id)
        error = f"couldn't start it again after {why}: {type(e).__name__}: {e}"
        failed = sources.finish(name, run_id, "FAILED", error)
        if failed is not None:
            _report_failure(failed, error)


def _relaunch(run: dict, declared: dict | None, restarting: str, why: str, crashed: bool, today: int) -> None:
    name, run_id = run["pipeline_name"], run["run_id"]
    if crashed:
        _report_crash(run, why, today)
    started = launch(name, run_id, native(run.get("parameters") or {}), declared) if declared else None
    if not started or not started["tasks"]:
        error = (f"couldn't start it again after {why}: "
                 f"{started['failures'] if started else 'no longer declared in the manifest'}")
        failed = sources.finish(name, run_id, "FAILED", error)
        if failed is not None:
            _report_failure(failed, error)
        return
    new_task = started["tasks"][0]["taskArn"]
    try:
        current = table.update_item(Key=run_key(name, run_id), UpdateExpression="SET ecs_task_arn = :arn",
                                    ConditionExpression="ecs_task_arn = :restarting AND #s IN (:pending, :stopping)",
                                    ExpressionAttributeNames={"#s": "status"},
                                    ExpressionAttributeValues={":arn": new_task, ":restarting": restarting,
                                                               ":pending": "PENDING", ":stopping": "STOPPING"},
                                    ReturnValues="ALL_NEW")["Attributes"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # The run moved on without this restart (ended as dead by a start or a stop): its task must not poll.
        ecs.stop_task(cluster=CLUSTER_ARN, task=new_task, reason="Superseded while restarting")
        return
    if current.get("status") == "STOPPING":
        # Stopped while restarting: the stop had no task to stop, so stop the one just started.
        ecs.stop_task(cluster=CLUSTER_ARN, task=new_task, reason="Stopped while restarting")


def reconcile_sources() -> list[str]:
    """Every few minutes: end each source run that is live but has no running task and nothing moving it on (a lost
    stopped event, a status Lambda that failed mid-restart), with an ERROR alert, so a dead source is never silent.
    Returns the runs ended."""
    ended = []
    for name, declared in manifest().items():
        if declared.get("kind") != sources.KIND:
            continue
        run_id = sources.active_run(name)
        run = table.get_item(Key=run_key(name, run_id)).get("Item") if run_id else None
        if not run or run.get("status") not in sources.LIVE or not sources.dead(run, ecs, CLUSTER_ARN):
            continue
        if run.get("status") == "STOPPING":
            sources.finish(name, run_id, "STOPPED")
        else:
            error = "its task is gone and nothing started it again"
            failed = sources.finish(name, run_id, "FAILED", error)
            if failed is not None:
                _report_failure(failed, error)
        ended.append(run_id)
    return ended


def _report_crash(run: dict, why: str, today: int) -> None:
    """A crash the run survived: WARNING, one alert per run (its key), so a slow crash loop is seen."""
    name, run_id = run.get("pipeline_name", ""), run.get("run_id", "")
    _put_alert({
        # One alert per run per day: a crash loop slow enough to survive is surfaced again each day it goes on.
        "action": "FIRING", "key": f"SOURCE_RESTARTED:{name}:{run_id}:{now()[:10]}", "type": "CUSTOM",
        "severity": "WARNING",
        "source": "ingest", "title": f"Polling source {name} crashed and was restarted",
        "body": f"{why}. {today} crash{'es' if today != 1 else ''} in the last 24 h; it fails after "
                f"{MAX_CRASHES} in {CRASH_WINDOW.seconds // 60} min or {MAX_CRASHES_PER_DAY} in a day.",
        "labels": {"pipeline": name, "runId": run_id, "kind": "source"}, "stateAt": now(),
    })


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
    _put_alert({
        "action": "FIRING",
        "key": f"PIPELINE_FAILED:{pipeline}:{run_id}",
        "type": "PIPELINE_FAILED",
        "source": "ingest",
        "title": f"{'Polling source' if sources.is_source(run) else 'Pipeline'} {pipeline} failed",
        "body": error,
        "labels": {"pipeline": pipeline, "runId": run_id, "trigger": run.get("trigger", "manual"),
                   "kind": run.get("kind", "pipeline")},
        "stateAt": now(),
    })


def _put_alert(alert: dict) -> None:
    # Alerting must never fail the status update.
    try:
        events.put_events(Entries=[{
            "EventBusName": ALERT_BUS_NAME, "Source": "gnome.alerts", "DetailType": "Alert", "Detail": json.dumps(alert),
        }])
    except Exception:
        logger.warning("Couldn't put alert %s", alert.get("key"), exc_info=True)
