"""GET /research/pipelines: every pipeline definition."""
from __future__ import annotations

from boto3.dynamodb.conditions import Key

from store import DEFINITION_PK, public, response, table


def handler(event: dict, context) -> dict:
    items = table.query(KeyConditionExpression=Key("pk").eq(DEFINITION_PK))["Items"]
    pipelines = [public(i) for i in items]
    return response(200, {"pipelines": pipelines, "count": len(pipelines)})
