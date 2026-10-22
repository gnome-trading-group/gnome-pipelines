"""In-memory stand-ins for the AWS clients and table the handlers call."""
from __future__ import annotations

import io
import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

from botocore.exceptions import ClientError


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


class _NotFound(Exception):
    pass


class _Pages:
    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages

    def paginate(self, **_):
        return self.pages


class Table:
    def __init__(self, items: list[dict] | None = None) -> None:
        self.items = {(i["pk"], i["sk"]): dict(i) for i in items or []}
        self.updates: list[dict] = []

    def get_item(self, Key):
        item = self.items.get((Key["pk"], Key["sk"]))
        return {"Item": dict(item)} if item else {}

    def put_item(self, Item, ConditionExpression=None):
        self.items[(Item["pk"], Item["sk"])] = dict(Item)

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames=None,
                    ConditionExpression=None, ReturnValues=None):
        names = ExpressionAttributeNames or {}
        key = (Key["pk"], Key["sk"])
        item = self.items.get(key, dict(Key))
        if ConditionExpression and not _holds(ConditionExpression, item, names, ExpressionAttributeValues):
            raise client_error("ConditionalCheckFailedException")
        self.updates.append({"Key": Key, "values": ExpressionAttributeValues})
        for assignment in UpdateExpression.removeprefix("SET ").split(", "):
            name, value = (part.strip() for part in assignment.split("="))
            item[names.get(name, name)] = ExpressionAttributeValues[value]
        self.items[key] = item
        return {"Attributes": dict(item)}

    def query(self, KeyConditionExpression, IndexName=None, ScanIndexForward=True, Limit=None):
        condition = KeyConditionExpression.get_expression()
        attribute, value = condition["values"][0].name, condition["values"][1]
        matched = sorted((dict(i) for i in self.items.values() if i.get(attribute) == value),
                         key=lambda i: i["sk"], reverse=not ScanIndexForward)
        return {"Items": matched[:Limit]}


def _holds(condition: str, item: dict, names: dict, values: dict) -> bool:
    return all(_clause_holds(clause, item, names, values) for clause in condition.split(" AND "))


def _clause_holds(clause: str, item: dict, names: dict, values: dict) -> bool:
    attribute, op, rest = re.match(r"(#?\w+) (=|IN) (.+)", clause).groups()
    current = item.get(names.get(attribute, attribute))
    allowed = [values[v.strip()] for v in rest.strip("()").split(",")]
    return current in allowed


class Ssm:
    exceptions = SimpleNamespace(ParameterNotFound=_NotFound)

    def __init__(self, entries: list[dict]) -> None:
        self.parameters = {f"/gnome/pipelines/types/{e['name']}": json.dumps(e) for e in entries}

    def get_paginator(self, name):
        return _Pages([{"Parameters": [{"Name": k, "Value": v} for k, v in self.parameters.items()]}])

    def get_parameter(self, Name):
        if Name not in self.parameters:
            raise _NotFound(Name)
        return {"Parameter": {"Name": Name, "Value": self.parameters[Name]}}


class Ecs:
    def __init__(self, failures: list | None = None) -> None:
        self.calls: list[dict] = []
        self.stopped: list[dict] = []
        self.failures = failures

    def run_task(self, **kwargs):
        self.calls.append(kwargs)
        if self.failures:
            return {"tasks": [], "failures": self.failures}
        return {"tasks": [{"taskArn": "arn:aws:ecs:us-east-1:1:task/gnome-pipelines/abc123"}], "failures": []}

    def stop_task(self, **kwargs):
        self.stopped.append(kwargs)


class Events:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def put_events(self, Entries):
        self.sent.extend(json.loads(e["Detail"]) for e in Entries)


class Scheduler:
    exceptions = SimpleNamespace(ResourceNotFoundException=_NotFound)

    def __init__(self, schedules: dict[str, dict] | None = None) -> None:
        self.schedules = {name: dict(spec) for name, spec in (schedules or {}).items()}
        self.calls: list[tuple[str, dict]] = []

    def get_paginator(self, name):
        return _Pages([{"Schedules": [{"Name": n, "State": s["State"], "LastModificationDate": s.get("LastModificationDate")}
                                      for n, s in self.schedules.items()]}])

    def get_schedule(self, Name, GroupName):
        if Name not in self.schedules:
            raise _NotFound(Name)
        return dict(self.schedules[Name])

    def create_schedule(self, **kwargs):
        self.calls.append(("create", kwargs))
        self.schedules[kwargs["Name"]] = {**kwargs, "LastModificationDate": datetime.now(timezone.utc)}

    def update_schedule(self, **kwargs):
        self.calls.append(("update", kwargs))
        self.schedules[kwargs["Name"]] = {**kwargs, "LastModificationDate": datetime.now(timezone.utc)}

    def delete_schedule(self, Name, GroupName):
        self.calls.append(("delete", {"Name": Name}))
        del self.schedules[Name]


class Lambdas:
    """Invokes a local handler the way lambda.invoke would."""

    def __init__(self, handler) -> None:
        self.handler = handler

    def invoke(self, FunctionName, Payload):
        try:
            result, error = self.handler(json.loads(Payload), None), None
        except Exception as e:
            result, error = {"errorMessage": str(e)}, "Unhandled"
        response = {"Payload": io.BytesIO(json.dumps(result).encode())}
        if error:
            response["FunctionError"] = error
        return response


class Logs:
    exceptions = SimpleNamespace(ResourceNotFoundException=_NotFound)

    def __init__(self, streams: dict[str, list[str]]) -> None:
        self.streams = streams
        self.requests: list[dict] = []

    def get_log_events(self, **request):
        self.requests.append(request)
        if request["logStreamName"] not in self.streams:
            raise _NotFound(request["logStreamName"])
        events = [{"timestamp": i, "message": m} for i, m in enumerate(self.streams[request["logStreamName"]])]
        return {"events": events, "nextBackwardToken": "b/earlier"}
