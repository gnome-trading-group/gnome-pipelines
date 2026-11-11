"""Polling sources' runs: at most one at a time per source, stopping it, and clearing a run whose task is gone.

A source's run never finishes on its own. Its states: PENDING (task starting) -> RUNNING -> STOPPING (someone asked)
-> STOPPED, or FAILED (it couldn't start, or crashed too often); the status Lambda starts a task that stopped
without being asked to again, on the same run. One item per source, pk "SOURCE#<name>", sk "ACTIVE", names the run
that may have a task. The trigger writes the run, then claims the item with a conditional write, so two starts in
the same second can't both launch; whatever ends a run releases it.

Every way a run ends goes through a task-stopped event, so a lost event (a failed status Lambda, a stop_task that
errored) would leave a run live forever and block every later start. A live run whose task is gone, or that has had
no task for STALE_AFTER, is "dead": claim() and stop() check ECS for that and end it instead of waiting.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from store import now, run_key, table

KIND = "source"
LIVE = ("PENDING", "RUNNING", "STOPPING")
# The status Lambda's ecs_task_arn while it starts a run's next task: a duplicate stopped event for the old task no
# longer matches the run, and only the restart that wrote it may replace it.
RESTARTING = "RESTARTING#"
# Longer than a task takes to start or stop, and than the status Lambda takes to act on a stopped event.
STALE_AFTER = timedelta(minutes=5)


def is_source(item: dict | None) -> bool:
    return bool(item) and item.get("kind") == KIND


def active_key(name: str) -> dict:
    return {"pk": f"SOURCE#{name}", "sk": "ACTIVE"}


def has_task(run: dict) -> bool:
    arn = run.get("ecs_task_arn") or ""
    return bool(arn) and not arn.startswith(RESTARTING)


def _conditional(write) -> bool:
    try:
        write()
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return False
    return True


def _older_than(stamp: str | None, age: timedelta) -> bool:
    return stamp is not None and datetime.fromisoformat(now()) - datetime.fromisoformat(stamp) > age


def dead(run: dict, ecs, cluster: str) -> bool:
    """A live run nothing will move on: its task has stopped and no restart followed, or it has had no task for a
    while. A run whose task is still running, or stopped moments ago, is not dead yet."""
    if not has_task(run):
        return _older_than(run.get("restarting_at") or run.get("queued_at"), STALE_AFTER)
    found = ecs.describe_tasks(cluster=cluster, tasks=[run["ecs_task_arn"]])
    if not found.get("tasks"):
        # ECS forgets a task about an hour after it stops.
        return True
    task = found["tasks"][0]
    if task.get("lastStatus") != "STOPPED":
        return False
    stopped_at = task.get("stoppedAt")
    stamp = stopped_at.isoformat() if hasattr(stopped_at, "isoformat") else stopped_at
    return stamp is None or _older_than(stamp, STALE_AFTER)


def claim(name: str, run_id: str, ecs, cluster: str) -> str | None:
    """Make run_id (already written) the source's active run. None when claimed; else the live run holding it."""
    item = {**active_key(name), "run_id": run_id, "claimed_at": now()}
    # A few rounds at most: each either claims, finds a live holder, or clears a holder that had ended.
    for _ in range(3):
        if _conditional(lambda: table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")):
            return None
        holder = active_run(name)
        if holder is None:
            continue
        run = table.get_item(Key=run_key(name, holder)).get("Item")
        if run is not None and run.get("status") in LIVE:
            if not dead(run, ecs, cluster):
                return holder
            # Ends it and releases the source, so the next round claims it.
            finish(name, holder, "FAILED", "its task stopped and was never started again or marked stopped")
            continue
        # The holder ended without releasing: take over, unless someone else just did.
        if _conditional(lambda: table.put_item(Item=item, ConditionExpression="run_id = :held",
                                               ExpressionAttributeValues={":held": holder})):
            return None
    # Something else keeps claiming it; report whoever holds it now, never a claim that didn't happen.
    return active_run(name) or "another start"


def active_run(name: str) -> str | None:
    return (table.get_item(Key=active_key(name)).get("Item") or {}).get("run_id")


def release(name: str, run_id: str) -> None:
    _conditional(lambda: table.delete_item(Key=active_key(name), ConditionExpression="run_id = :run",
                                           ExpressionAttributeValues={":run": run_id}))


def finish(name: str, run_id: str, status: str, error: str | None = None) -> dict | None:
    """Move a live run to STOPPED or FAILED and release the source. The run as it is now, or None if it had ended."""
    names, values = {"#s": "status"}, {":to": status, ":ts": now(), **{f":l{i}": s for i, s in enumerate(LIVE)}}
    update = "SET #s = :to, completed_at = :ts"
    if error:
        update += ", error_message = :err"
        values[":err"] = error
    try:
        run = table.update_item(Key=run_key(name, run_id), UpdateExpression=update,
                                ConditionExpression=f"#s IN ({', '.join(f':l{i}' for i in range(len(LIVE)))})",
                                ExpressionAttributeNames=names, ExpressionAttributeValues=values,
                                ReturnValues="ALL_NEW")["Attributes"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return None
    release(name, run_id)
    return run


def stop(name: str, run_id: str, ecs, cluster: str, reason: str) -> dict | None:
    """Ask a live run to stop: STOPPING and its task stopped (the status Lambda then marks it STOPPED), or STOPPED at
    once when it has no task. Asking again stops the task again (stop_task is idempotent), or ends the run if its
    task is gone. The run as it is now, or None when it wasn't live."""
    try:
        run = table.update_item(
            Key=run_key(name, run_id), UpdateExpression="SET #s = :stopping, stop_requested_at = :ts",
            ConditionExpression="#s IN (:pending, :running)", ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":stopping": "STOPPING", ":ts": now(), ":pending": "PENDING",
                                       ":running": "RUNNING"},
            ReturnValues="ALL_NEW")["Attributes"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        run = table.get_item(Key=run_key(name, run_id)).get("Item")
        if not run or run.get("status") != "STOPPING":
            return None
        if dead(run, ecs, cluster):
            return finish(name, run_id, "STOPPED")
    if not has_task(run):
        # A restart in flight (RESTARTING#) stops the task it starts once it sees STOPPING.
        return finish(name, run_id, "STOPPED") if not run.get("ecs_task_arn") else run
    ecs.stop_task(cluster=cluster, task=run["ecs_task_arn"], reason=reason)
    return run
