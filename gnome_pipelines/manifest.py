"""The pipelines' declarations as JSON, published to SSM for the API and the scheduler.

One parameter per pipeline under /gnome/pipelines/types/: its family, description, parameter JSON
schema, task size and schedules. CI publishes each family's entries from the family image it has just
pushed to an account, so the API and the schedules always describe the code that will run, then invokes
the sync Lambda, which makes the EventBridge schedules match. A family's publish replaces only its own
entries; the trigger Lambda starts a run in its family's task definition.

    python -m gnome_pipelines.manifest publish   # CI, in a family image, with the account's credentials
    python -m gnome_pipelines.manifest check     # the image build: the family's pipelines import with its extra alone
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict

import boto3

from gnome_pipelines import Pipeline, families
from gnome_pipelines.stores import code_version

PARAMETER_PREFIX = "/gnome/pipelines/types/"
SYNC_FUNCTION = "gnome-pipelines-sync"
# The pipelines' infrastructure lives in one region, whatever region CI runs in.
REGION = "us-east-1"

logger = logging.getLogger(__name__)


def entry(family: str, cls: type[Pipeline]) -> dict:
    return {
        "name": cls.name,
        "family": family,
        "description": cls.description,
        "params_schema": cls.params_type.model_json_schema(),
        "resources": asdict(cls.resources),
        "schedules": [asdict(s) for s in cls.schedules],
        "code_version": code_version(),
    }


def manifest(family: str) -> dict[str, dict]:
    return {name: entry(family, cls) for name, cls in sorted(families.pipelines(family).items())}


def publish(family: str) -> None:
    ssm = boto3.client("ssm", region_name=REGION)
    published = {
        p["Name"] for page in ssm.get_paginator("get_parameters_by_path").paginate(Path=PARAMETER_PREFIX)
        for p in page["Parameters"] if json.loads(p["Value"]).get("family") == family
    }
    entries = manifest(family)
    for name, item in entries.items():
        ssm.put_parameter(
            Name=PARAMETER_PREFIX + name, Value=json.dumps(item, sort_keys=True), Type="String",
            # A parameter schema can outgrow the 4 KB standard tier; this moves it up only when it does.
            Tier="Intelligent-Tiering", Overwrite=True,
        )
    for stale in published - {PARAMETER_PREFIX + name for name in entries}:
        ssm.delete_parameter(Name=stale)
    logger.info("published %d %s pipeline(s), removed %d", len(entries), family,
                len(published - {PARAMETER_PREFIX + n for n in entries}))

    response = boto3.client("lambda", region_name=REGION).invoke(FunctionName=SYNC_FUNCTION, Payload=b"{}")
    result = response["Payload"].read().decode()
    if response.get("FunctionError"):
        raise RuntimeError(f"{SYNC_FUNCTION} failed: {result}")
    logger.info("schedules synced: %s", result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    family = os.environ[families.FAMILY_VARIABLE]
    match sys.argv[1:]:
        case ["publish"]:
            os.environ.setdefault("AWS_REGION", REGION)
            publish(family)
        case ["check"]:
            logger.info("%s: %s", family, ", ".join(manifest(family)))
        case _:
            sys.exit("usage: python -m gnome_pipelines.manifest publish|check")
