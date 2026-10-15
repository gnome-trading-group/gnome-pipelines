"""Where a pipeline is running, and whose data it reads and writes.

The task definition sets GNOME_STAGE to its account's stage; `gnome-pipelines run --stage` sets it for a
local run, along with the AWS profile of the same name, so the stage can't disagree with the credentials.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class Stage(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


@dataclass(frozen=True)
class Environment:
    stage: Stage
    in_task: bool

    @property
    def is_local(self) -> bool:
        return not self.in_task


def environment() -> Environment:
    stage = os.environ.get("GNOME_STAGE")
    if not stage:
        raise RuntimeError("GNOME_STAGE isn't set; run pipelines locally with `gnome-pipelines run --stage ...`")
    # ECS sets this in every Fargate container.
    return Environment(Stage(stage), in_task="ECS_CONTAINER_METADATA_URI_V4" in os.environ)
