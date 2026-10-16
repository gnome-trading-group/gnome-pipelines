"""ECS Fargate container entry point.

Reads PIPELINE_NAME, RUN_ID, PIPELINE_PARAMS from environment, dispatches
to the registered pipeline class, and updates run status in DynamoDB.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

import boto3

from gnome_pipelines import PIPELINE_REGISTRY
from gnome_pipelines.cfb.pipeline import CfbUpdatePipeline  # noqa: F401 — registers pipeline
from gnome_pipelines.cs2_prematch import CS2PrematchPredictPipeline, CS2PrematchTrainPipeline  # noqa: F401 — registers pipelines
from gnome_pipelines.hltv_cs2 import HltvCs2Pipeline  # noqa: F401 — registers pipeline

logger = logging.getLogger(__name__)

_TABLE_NAME = os.environ["DYNAMODB_TABLE"]
_PIPELINES_PK = "__pipelines__"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_run(pipeline_name: str, run_id: str, **attrs: object) -> None:
    table = boto3.resource("dynamodb").Table(_TABLE_NAME)
    sk = f"RUN#{pipeline_name}#{run_id}"
    update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in attrs)
    table.update_item(
        Key={"session_name": _PIPELINES_PK, "sk": sk},
        UpdateExpression=update_expr,
        ExpressionAttributeNames={f"#{k}": k for k in attrs},
        ExpressionAttributeValues={f":{k}": v for k, v in attrs.items()},
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    pipeline_name = os.environ["PIPELINE_NAME"]
    run_id = os.environ["RUN_ID"]
    params = json.loads(os.environ.get("PIPELINE_PARAMS", "{}"))

    logger.info("Starting pipeline=%s run_id=%s params=%s", pipeline_name, run_id, params)

    pipeline_cls = PIPELINE_REGISTRY.get(pipeline_name)
    if pipeline_cls is None:
        msg = f"Unknown pipeline '{pipeline_name}'. Registered: {list(PIPELINE_REGISTRY)}"
        logger.error(msg)
        _update_run(pipeline_name, run_id, status="FAILED", error_message=msg, completed_at=_now())
        sys.exit(1)

    _update_run(pipeline_name, run_id, status="RUNNING", started_at=_now())

    pipeline = pipeline_cls()
    try:
        result = pipeline.run(params)
        _update_run(pipeline_name, run_id, status="SUCCEEDED", outputs=result.outputs, completed_at=_now())
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
