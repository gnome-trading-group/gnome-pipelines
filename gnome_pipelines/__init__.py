"""Research pipeline framework.

Pipelines are recurring automated jobs that collect data, build datasets,
and optionally train models. Each pipeline is a Python class registered via
@register_pipeline and executed by runner.py inside an ECS Fargate task.

Operational config (schedule, cpu/memory, parameters) is stored in DynamoDB
and managed through the web UI — not in code.
"""
from __future__ import annotations

from dataclasses import dataclass, field

PIPELINE_REGISTRY: dict[str, type[Pipeline]] = {}


def register_pipeline(cls: type[Pipeline]) -> type[Pipeline]:
    PIPELINE_REGISTRY[cls.name] = cls
    return cls


class Pipeline:
    name: str

    def run(self, params: dict) -> PipelineResult:
        raise NotImplementedError


@dataclass
class PipelineResult:
    status: str
    outputs: dict = field(default_factory=dict)
    error: str | None = None
