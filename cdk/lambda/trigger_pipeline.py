"""Start a pipeline run: check its parameters, record it, then start the Fargate task that runs gnome_pipelines.runner.

Invoked by EventBridge Scheduler ({"pipeline_name", "trigger": "scheduled", "schedule", "parameters"}) and by
the API Lambda ({"pipeline_name", "trigger": "manual", "parameters"}). A run's parameters are its schedule's
or the caller's, over the pipeline's own defaults.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from params import problems
from store import now, pipeline, run_key, table
from tasks import launch, task_definition


class Rejected(Exception):
    def __init__(self, status: int, error: str) -> None:
        super().__init__(error)
        self.status = status


def handler(event: dict, context) -> dict:
    trigger = event.get("trigger", "manual")
    try:
        return {"status": 202, **start_run(event.get("pipeline_name", ""), event.get("parameters") or {},
                                            trigger, event.get("schedule"))}
    except Rejected as e:
        if trigger == "scheduled":
            # A schedule that can't start is a broken deploy, so it fails the invocation and alarms.
            raise
        return {"status": e.status, "error": str(e)}


def start_run(pipeline_name: str, params: dict, trigger: str, schedule: str | None) -> dict:
    declared = pipeline(pipeline_name)
    if declared is None:
        raise Rejected(404, f"No pipeline named '{pipeline_name}'")
    found = problems(declared["params_schema"], params)
    if found:
        raise Rejected(400, "Invalid parameters: " + "; ".join(found))
    if task_definition(declared) is None:
        raise Rejected(500, f"No task definition for {pipeline_name}'s family {declared.get('family')!r}")

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    record = {
        **run_key(pipeline_name, run_id),
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "status": "PENDING",
        "trigger": trigger,
        "parameters": params,
        "queued_at": now(),
    }
    if schedule:
        record["schedule"] = schedule
    table.put_item(Item=record)

    started = launch(pipeline_name, run_id, params, declared)
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
    return {"run_id": run_id, "ecs_task_arn": task_arn}
