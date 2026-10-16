"""Make the EventBridge schedules match the manifest. CI invokes this after publishing it.

One schedule per declared schedule, named <pipeline>.<schedule> in the gnome-pipelines group. New ones
start disabled; a changed expression or parameters is updated in place keeping its on/off state; a
schedule that is no longer declared is deleted. The on/off state lives only in EventBridge.
"""
from __future__ import annotations

import json
import os

import boto3

from store import SCHEDULE_GROUP, manifest, schedule_name

TRIGGER_FUNCTION_ARN = os.environ["TRIGGER_FUNCTION_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]

scheduler = boto3.client("scheduler")


def handler(event: dict, context) -> dict:
    desired = {
        schedule_name(name, s["name"]): _spec(name, s)
        for name, declared in manifest().items() for s in declared["schedules"]
    }
    pages = scheduler.get_paginator("list_schedules").paginate(GroupName=SCHEDULE_GROUP)
    existing = {s["Name"] for page in pages for s in page["Schedules"]}

    created, updated = [], []
    for name, spec in desired.items():
        if name not in existing:
            scheduler.create_schedule(**spec, State="DISABLED")
            created.append(name)
            continue
        current = scheduler.get_schedule(Name=name, GroupName=SCHEDULE_GROUP)
        if _differs(current, spec):
            scheduler.update_schedule(**spec, State=current["State"])
            updated.append(name)
    deleted = sorted(existing - desired.keys())
    for name in deleted:
        scheduler.delete_schedule(Name=name, GroupName=SCHEDULE_GROUP)
    return {"created": created, "updated": updated, "deleted": deleted}


def _spec(pipeline_name: str, schedule: dict) -> dict:
    return {
        "Name": schedule_name(pipeline_name, schedule["name"]),
        "GroupName": SCHEDULE_GROUP,
        "ScheduleExpression": schedule["expression"],
        "ScheduleExpressionTimezone": "UTC",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Description": schedule.get("description", "")[:512],
        "Target": {
            "Arn": TRIGGER_FUNCTION_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"pipeline_name": pipeline_name, "trigger": "scheduled",
                                 "schedule": schedule["name"], "parameters": schedule["params"]}, sort_keys=True),
        },
    }


def _differs(current: dict, spec: dict) -> bool:
    return (current["ScheduleExpression"] != spec["ScheduleExpression"]
            or current.get("Description", "") != spec["Description"]
            or current["Target"]["Arn"] != spec["Target"]["Arn"]
            or current["Target"]["RoleArn"] != spec["Target"]["RoleArn"]
            or json.loads(current["Target"].get("Input") or "{}") != json.loads(spec["Target"]["Input"]))
