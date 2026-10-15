"""The pipelines' declarations as JSON, published to SSM for the API and the scheduler.

One parameter per pipeline under /gnome/pipelines/types/: its description, parameter JSON schema,
task size and schedules. CI publishes it from the image it has just pushed to an account, so the
API and the schedules always describe the code that will run, then invokes the sync Lambda, which
makes the EventBridge schedules match.

    python -m gnome_pipelines.manifest publish   # CI, with the account's credentials
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict

import boto3

from gnome_pipelines import PIPELINE_REGISTRY, Pipeline
from gnome_pipelines import runner  # noqa: F401 — importing it registers every pipeline
from gnome_pipelines.stores import code_version

PARAMETER_PREFIX = "/gnome/pipelines/types/"
SYNC_FUNCTION = "gnome-pipelines-sync"
# The pipelines' infrastructure lives in one region, whatever region CI runs in.
REGION = "us-east-1"

logger = logging.getLogger(__name__)


def entry(cls: type[Pipeline]) -> dict:
    return {
        "name": cls.name,
        "description": cls.description,
        "params_schema": cls.params_type.model_json_schema(),
        "resources": asdict(cls.resources),
        "schedules": [asdict(s) for s in cls.schedules],
        "code_version": code_version(),
    }


def manifest() -> dict[str, dict]:
    return {name: entry(cls) for name, cls in sorted(PIPELINE_REGISTRY.items())}


def publish() -> None:
    ssm = boto3.client("ssm", region_name=REGION)
    published = {
        p["Name"] for page in ssm.get_paginator("get_parameters_by_path").paginate(Path=PARAMETER_PREFIX)
        for p in page["Parameters"]
    }
    entries = manifest()
    for name, item in entries.items():
        ssm.put_parameter(
            Name=PARAMETER_PREFIX + name, Value=json.dumps(item, sort_keys=True), Type="String",
            # A parameter schema can outgrow the 4 KB standard tier; this moves it up only when it does.
            Tier="Intelligent-Tiering", Overwrite=True,
        )
    for stale in published - {PARAMETER_PREFIX + name for name in entries}:
        ssm.delete_parameter(Name=stale)
    logger.info("published %d pipeline(s), removed %d", len(entries), len(published - {PARAMETER_PREFIX + n for n in entries}))

    response = boto3.client("lambda", region_name=REGION).invoke(FunctionName=SYNC_FUNCTION, Payload=b"{}")
    result = response["Payload"].read().decode()
    if response.get("FunctionError"):
        raise RuntimeError(f"{SYNC_FUNCTION} failed: {result}")
    logger.info("schedules synced: %s", result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if sys.argv[1:] != ["publish"]:
        sys.exit("usage: python -m gnome_pipelines.manifest publish")
    os.environ.setdefault("AWS_REGION", REGION)
    publish()
