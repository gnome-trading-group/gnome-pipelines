"""Runs a registered pipeline: execute() for any caller, main() as the Fargate container's entry point.

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

from gnome_pipelines import PIPELINE_REGISTRY, PipelineResult
from gnome_pipelines.environment import environment
from gnome_pipelines.cs2_prematch import CS2PrematchPredictPipeline, CS2PrematchTrainPipeline  # noqa: F401 — registers pipelines
from gnome_pipelines.hltv_cs2 import HltvCs2Pipeline  # noqa: F401 — registers pipeline

logger = logging.getLogger(__name__)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _storable(value: object) -> object:
    # DynamoDB takes neither floats nor numpy scalars, both of which pipeline outputs carry.
    return json.loads(json.dumps(value, default=lambda v: v.item() if hasattr(v, "item") else str(v)), parse_float=Decimal)


def _update_run(pipeline_name: str, run_id: str, **attrs: object) -> None:
    table = boto3.resource("dynamodb").Table(os.environ["PIPELINES_TABLE"])
    update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in attrs)
    table.update_item(
        Key={"pk": f"RUN#{pipeline_name}", "sk": run_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames={f"#{k}": k for k in attrs},
        ExpressionAttributeValues={f":{k}": v for k, v in attrs.items()},
    )


def execute(pipeline_name: str, params: dict) -> PipelineResult:
    pipeline_cls = PIPELINE_REGISTRY.get(pipeline_name)
    if pipeline_cls is None:
        raise ValueError(f"Unknown pipeline '{pipeline_name}'. Registered: {sorted(PIPELINE_REGISTRY)}")
    return pipeline_cls().run(pipeline_cls.params_type.model_validate(params))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    pipeline_name = os.environ["PIPELINE_NAME"]
    run_id = os.environ["RUN_ID"]
    params = json.loads(os.environ.get("PIPELINE_PARAMS", "{}"))

    logger.info("Starting pipeline=%s run_id=%s stage=%s params=%s", pipeline_name, run_id, environment().stage.value, params)

    _update_run(pipeline_name, run_id, status="RUNNING", started_at=_now())
    try:
        result = execute(pipeline_name, params)
        _update_run(pipeline_name, run_id, status="SUCCEEDED", outputs=_storable(result.outputs), completed_at=_now())
        logger.info("Pipeline %s succeeded: %s", pipeline_name, result.outputs)
    except Exception:
        logger.exception("Pipeline %s failed", pipeline_name)
        _update_run(pipeline_name, run_id,
                    status="FAILED",
                    error_message=traceback.format_exc()[-2000:],
                    completed_at=_now())
        sys.exit(1)


if __name__ == "__main__":
    main()
