"""A polling source: an alternative-data feed whose run never ends.

A source is a PollingSource[Params] subclass listed in its family's SOURCES. It is started, stopped and given its
parameters on the Pipelines page like a pipeline, and its run lasts until someone stops it there; if its task stops
any other way the status Lambda starts it again. It declares its parameters, description and task size in code,
plus the secrets its task role may read. What must outlive a restart it keeps in its own tables or objects.
build() returns the Service that polls it. An orchestrator only observes the tables a source writes.
"""
from __future__ import annotations

import typing
from typing import ClassVar

from gnome_pipelines import PipelineParams, Resources
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Service


class PollingSource[P: PipelineParams]:
    name: ClassVar[str]
    description: ClassVar[str]
    params_type: ClassVar[type[PipelineParams]]
    resources: ClassVar[Resources] = Resources(cpu=256, memory=512)
    # Names under gnome/pipelines/; the source's task role may read these and no others.
    secrets: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            if typing.get_origin(base) is PollingSource:
                cls.params_type = typing.get_args(base)[0]

    def build(self, params: P, health: PollHealth) -> Service:
        raise NotImplementedError
