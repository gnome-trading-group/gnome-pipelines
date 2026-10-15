"""Runs a pipeline: execute() for any caller, main() as the Fargate container's entry point.

main() reads PIPELINE_NAME, RUN_ID, PIPELINE_PARAMS from the environment and records the run's status in
the PIPELINES_TABLE item that the trigger Lambda created (cdk/lambda/store.py has the layout).
`gnome-pipelines run` (cli.py) calls execute() directly and records nothing.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from gnome_pipelines import PipelineResult, families
from gnome_pipelines.environment import environment
from gnome_pipelines.stores import code_version

logger = logging.getLogger(__name__)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _storable(value: object) -> object:
    # DynamoDB takes neither floats nor numpy scalars, both of which pipeline outputs carry.
    return json.loads(json.dumps(value, default=lambda v: v.item() if hasattr(v, "item") else str(v)), parse_float=Decimal)


def _update_run(pipeline_name: str, run_id: str, expect: str, **attrs: object) -> bool:
    """Move the run on from status `expect`; False if it has moved elsewhere (cancelled from the UI)."""
    table = boto3.resource("dynamodb").Table(os.environ["PIPELINES_TABLE"])
    try:
        table.update_item(
            Key={"pk": f"RUN#{pipeline_name}", "sk": run_id},
            UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in attrs),
            ConditionExpression="#status = :expect",
            ExpressionAttributeNames={"#status": "status", **{f"#{k}": k for k in attrs}},
            ExpressionAttributeValues={":expect": expect, **{f":{k}": v for k, v in attrs.items()}},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return False
    return True


def execute(pipeline_name: str, params: dict) -> PipelineResult:
    available = families.available()
    pipeline_cls = available.get(pipeline_name)
    if pipeline_cls is None:
        raise ValueError(f"Unknown pipeline '{pipeline_name}'. Available here: {sorted(available)}")
    return pipeline_cls().run(pipeline_cls.params_type.model_validate(params))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    pipeline_name = os.environ["PIPELINE_NAME"]
    run_id = os.environ["RUN_ID"]
    params = json.loads(os.environ.get("PIPELINE_PARAMS", "{}"))

    logger.info("Starting pipeline=%s run_id=%s stage=%s params=%s", pipeline_name, run_id, environment().stage.value, params)

    if not _update_run(pipeline_name, run_id, "PENDING", status="RUNNING", started_at=_now(), code_version=code_version()):
        logger.info("Run %s was cancelled before it started", run_id)
        return
    try:
        result = execute(pipeline_name, params)
        _update_run(pipeline_name, run_id, "RUNNING", status="SUCCEEDED", outputs=_storable(result.outputs), completed_at=_now())
        logger.info("Pipeline %s succeeded: %s", pipeline_name, result.outputs)
    except Exception:
        logger.exception("Pipeline %s failed", pipeline_name)
        _update_run(pipeline_name, run_id, "RUNNING",
                    status="FAILED",
                    error_message=traceback.format_exc()[-2000:],
                    completed_at=_now())
        sys.exit(1)


if __name__ == "__main__":
    main()
