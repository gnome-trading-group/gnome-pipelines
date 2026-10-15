"""GET /research/pipelines/{pipelineName}: a definition and its 20 most recent runs."""
from __future__ import annotations

from boto3.dynamodb.conditions import Key

from store import definition_key, public, response, table

RECENT_RUNS = 20


def handler(event: dict, context) -> dict:
    pipeline_name = (event.get("pathParameters") or {}).get("pipelineName", "")
    definition = table.get_item(Key=definition_key(pipeline_name)).get("Item")
    if not definition:
        return response(404, {"error": f"Pipeline '{pipeline_name}' not found"})
    runs = table.query(
        KeyConditionExpression=Key("pk").eq(f"RUN#{pipeline_name}"), ScanIndexForward=False, Limit=RECENT_RUNS,
    )["Items"]
    return response(200, {"pipeline": public(definition), "runs": [public(r) for r in runs]})
