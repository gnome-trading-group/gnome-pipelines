"""The pipelines' declarations as JSON, published to SSM for the API and the scheduler.

One parameter per pipeline or polling source under /gnome/pipelines/types/: its family, kind ("pipeline" or
"source"), description, parameter JSON schema, task size and schedules. CI publishes each family's entries from the family image it has just
pushed to an account, so the API and the schedules always describe the code that will run, then invokes
the sync Lambda, which makes the EventBridge schedules match. A family's publish replaces only its own
entries; the trigger Lambda starts a run in its family's task definition.

    python -m gnome_pipelines.manifest publish   # CI, in a family image, with the account's credentials
    python -m gnome_pipelines.manifest restart-sources   # CI, after publish: running polling sources take the new image
    python -m gnome_pipelines.manifest check     # the image build: the family's pipelines and polling sources import
                                                 # with its extra alone
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict

import boto3

from gnome_pipelines import Pipeline, families
from gnome_pipelines.polling.source import PollingSource
from gnome_pipelines.stores import code_version

PARAMETER_PREFIX = "/gnome/pipelines/types/"
SYNC_FUNCTION = "gnome-pipelines-sync"
# The pipelines' infrastructure lives in one region, whatever region CI runs in.
REGION = "us-east-1"
# Source tasks are started with startedBy "source:<name>" (cdk/lambda/tasks.py), in this cluster.
CLUSTER = "gnome-pipelines"
STARTED_BY_PREFIX = "source:"

logger = logging.getLogger(__name__)


def entry(family: str, cls: type[Pipeline] | type[PollingSource]) -> dict:
    source = issubclass(cls, PollingSource)
    if source and getattr(cls, "schedules", ()):
        raise ValueError(f"{cls.name}: polling sources have no schedules; they are started and stopped by hand")
    return {
        "name": cls.name,
        "family": family,
        "kind": "source" if source else "pipeline",
        "description": cls.description,
        "params_schema": cls.params_type.model_json_schema(),
        "resources": asdict(cls.resources),
        "schedules": [] if source else [asdict(s) for s in cls.schedules],
        **({"secrets": list(cls.secrets)} if source else {}),
        "code_version": code_version(),
    }


def manifest(family: str) -> dict[str, dict]:
    declared = {**families.pipelines(family), **families.sources(family)}
    return {name: entry(family, cls) for name, cls in sorted(declared.items())}


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


def restart_sources(family: str, ecs=None) -> list[str]:
    """Stop the running task of each of the family's polling sources, so they run the <family>-latest image just
    pushed: the status Lambda starts a stopped source's task again, with the same run and parameters, unless someone
    asked for the stop. A source with no running task is left stopped. Returns the tasks stopped."""
    ecs = ecs or boto3.client("ecs", region_name=REGION)
    stopped = []
    for name in sorted(families.sources(family)):
        pages = ecs.get_paginator("list_tasks").paginate(cluster=CLUSTER, startedBy=STARTED_BY_PREFIX + name,
                                                         desiredStatus="RUNNING")
        for arn in (a for page in pages for a in page["taskArns"]):
            ecs.stop_task(cluster=CLUSTER, task=arn, reason=f"New {family} image {code_version()}")
            stopped.append(arn)
    logger.info("restarted %d polling source task(s)", len(stopped))
    return stopped


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    family = os.environ[families.FAMILY_VARIABLE]
    match argv:
        case ["publish"]:
            os.environ.setdefault("AWS_REGION", REGION)
            publish(family)
        case ["restart-sources"]:
            os.environ.setdefault("AWS_REGION", REGION)
            restart_sources(family)
        case ["check"]:
            logger.info("%s: %s", family, ", ".join(f"{n} ({e['kind']})" for n, e in manifest(family).items()))
        case _:
            sys.exit("usage: python -m gnome_pipelines.manifest publish|restart-sources|check")


if __name__ == "__main__":
    main(sys.argv[1:])
