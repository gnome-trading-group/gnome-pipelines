"""
Pipeline families: pipelines that share one image and one set of dependencies.

Each family is a module here listing its pipelines in PIPELINES and its polling sources in SOURCES, with the
extra of the same name in pyproject.toml holding what they need beyond the base. CI builds one image per family with only that
extra installed, and PIPELINE_FAMILY in the image says which family it runs. Moving to Dagster, each
family module becomes a code location: PIPELINES becomes its Definitions and the image stays the same.
"""
from __future__ import annotations

import importlib
import os
import pkgutil

from gnome_pipelines import Pipeline
from gnome_pipelines.polling.source import PollingSource

FAMILY_VARIABLE = "PIPELINE_FAMILY"


def names() -> list[str]:
    return sorted(m.name for m in pkgutil.iter_modules(__path__))


def _module(family: str):
    if family not in names():
        raise ValueError(f"no pipeline family '{family}'; there are {', '.join(names())}")
    # By name, not at the top: a family's modules need its extra, and an image installs only its own family's.
    return importlib.import_module(f"{__name__}.{family}")


def pipelines(family: str) -> dict[str, type[Pipeline]]:
    return {p.name: p for p in _module(family).PIPELINES}


def sources(family: str) -> dict[str, type[PollingSource]]:
    return {s.name: s for s in _module(family).SOURCES}


def available() -> dict[str, type[Pipeline]]:
    """The pipelines this environment can run: its image's family, or every family on a laptop with all extras."""
    family = os.environ.get(FAMILY_VARIABLE)
    if family:
        return pipelines(family)
    return {name: cls for f in names() for name, cls in pipelines(f).items()}



def available_sources() -> dict[str, type[PollingSource]]:
    """The polling sources this environment can serve, as available() is for pipelines."""
    family = os.environ.get(FAMILY_VARIABLE)
    if family:
        return sources(family)
    return {name: cls for f in names() for name, cls in sources(f).items()}
