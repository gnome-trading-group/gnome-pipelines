"""Research pipeline framework.

Pipelines are recurring automated jobs that collect data, build datasets,
and optionally train models. Each pipeline is a Python class registered via
@register_pipeline and executed by runner.execute(), in an ECS Fargate task or
through `gnome-pipelines run`.

A pipeline declares its parameters as a PipelineParams model, Pipeline[ThatModel];
execute() validates the raw parameters against it before the pipeline starts.
Operational config (schedule, cpu/memory, parameter values) is stored in DynamoDB
and managed through the web UI — not in code.
"""
from __future__ import annotations

import typing
from dataclasses import dataclass, field
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

PIPELINE_REGISTRY: dict[str, type[Pipeline]] = {}


def register_pipeline[T: type[Pipeline]](cls: T) -> T:
    PIPELINE_REGISTRY[cls.name] = cls
    return cls


class PipelineParams(BaseModel):
    # A misspelled parameter fails the run instead of silently taking the default.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Pipeline[P: PipelineParams]:
    name: ClassVar[str]
    params_type: ClassVar[type[PipelineParams]]

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
