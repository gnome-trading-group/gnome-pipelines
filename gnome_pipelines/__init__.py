"""Research pipeline framework.

Pipelines are recurring automated jobs that collect data, build datasets,
and optionally train models. Each pipeline is a Python class listed in exactly
one family (gnome_pipelines/families/) and executed by runner.execute(), in its
family's ECS Fargate task or through `gnome-pipelines run`.

Everything about a pipeline is declared in code, as an orchestrator like Dagster would have it:
its parameters (a PipelineParams model, Pipeline[ThatModel], which execute() validates before the
pipeline starts), its description, the task size it needs and its schedules. manifest.py publishes
those declarations for the API and the scheduler; at runtime a schedule is only switched on or off.
"""
from __future__ import annotations

import typing
from dataclasses import dataclass, field
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

class PipelineParams(BaseModel):
    # A misspelled parameter fails the run instead of silently taking the default.
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True)
class Resources:
    """The Fargate task size: cpu in vCPU units (1024 = 1 vCPU), memory in MiB, a valid Fargate pair."""

    cpu: int = 1024
    memory: int = 4096


@dataclass(frozen=True)
class Schedule:
    """A run on an EventBridge Scheduler expression (UTC), with its parameters. Starts switched off."""

    name: str
    expression: str
    params: dict = field(default_factory=dict)
    description: str = ""


class Pipeline[P: PipelineParams]:
    name: ClassVar[str]
    description: ClassVar[str]
    params_type: ClassVar[type[PipelineParams]]
    resources: ClassVar[Resources] = Resources()
    schedules: ClassVar[tuple[Schedule, ...]] = ()

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            if typing.get_origin(base) is Pipeline:
                cls.params_type = typing.get_args(base)[0]

    def run(self, params: P) -> PipelineResult:
        raise NotImplementedError


@dataclass
class PipelineResult:
    status: str
    outputs: dict = field(default_factory=dict)
    error: str | None = None
