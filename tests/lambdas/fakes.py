"""In-memory stand-ins for the DynamoDB table and AWS clients the handlers call."""
from __future__ import annotations

import json

from botocore.exceptions import ClientError


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


class Table:
    def __init__(self, items: list[dict] | None = None) -> None:
        self.items = {(i["pk"], i["sk"]): dict(i) for i in items or []}
        self.updates: list[dict] = []

    def get_item(self, Key):
        item = self.items.get((Key["pk"], Key["sk"]))
        return {"Item": dict(item)} if item else {}

    def put_item(self, Item, ConditionExpression=None):
        if ConditionExpression and (Item["pk"], Item["sk"]) in self.items:
            raise client_error("ConditionalCheckFailedException")
        self.items[(Item["pk"], Item["sk"])] = dict(Item)

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames=None,
                    ConditionExpression=None, ReturnValues=None):
        key = (Key["pk"], Key["sk"])
        if ConditionExpression == "attribute_exists(pk)" and key not in self.items:
            raise client_error("ConditionalCheckFailedException")
        self.updates.append({"Key": Key, "values": ExpressionAttributeValues})
        names = ExpressionAttributeNames or {}
        item = self.items.setdefault(key, dict(Key))
        for assignment in UpdateExpression.removeprefix("SET ").split(", "):
            name, value = (part.strip() for part in assignment.split("="))
            item[names.get(name, name)] = ExpressionAttributeValues[value]
        return {"Attributes": dict(item)}

    def query(self, KeyConditionExpression, IndexName=None, ScanIndexForward=True, Limit=None):
        condition = KeyConditionExpression.get_expression()
        attribute, value = condition["values"][0].name, condition["values"][1]
        matched = sorted((dict(i) for i in self.items.values() if i.get(attribute) == value),
                         key=lambda i: i["sk"], reverse=not ScanIndexForward)
        return {"Items": matched[:Limit]}


class Ecs:
    def __init__(self, failures: list | None = None) -> None:
        self.calls: list[dict] = []
        self.failures = failures

    def run_task(self, **kwargs):
        self.calls.append(kwargs)
        if self.failures:
            return {"tasks": [], "failures": self.failures}
        return {"tasks": [{"taskArn": "arn:task/1"}], "failures": []}


class Events:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def put_events(self, Entries):
        self.sent.extend(json.loads(e["Detail"]) for e in Entries)


class Scheduler:
    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing = set(existing or ())
        self.calls: list[tuple[str, dict]] = []

    def create_schedule(self, **kwargs):
        self.calls.append(("create", kwargs))
        self.existing.add(kwargs["Name"])

    def update_schedule(self, **kwargs):
        if kwargs["Name"] not in self.existing:
            raise client_error("ResourceNotFoundException")
        self.calls.append(("update", kwargs))

    def delete_schedule(self, Name, GroupName):
        if Name not in self.existing:
            raise client_error("ResourceNotFoundException")
        self.calls.append(("delete", {"Name": Name, "GroupName": GroupName}))
        self.existing.discard(Name)
