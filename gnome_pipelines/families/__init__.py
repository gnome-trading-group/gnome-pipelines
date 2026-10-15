"""
Pipeline families: pipelines that share one image and one set of dependencies.

Each family is a module here listing its pipelines in PIPELINES, with the extra of the same name in
pyproject.toml holding what they need beyond the base. CI builds one image per family with only that
extra installed, and PIPELINE_FAMILY in the image says which family it runs. Moving to Dagster, each
family module becomes a code location: PIPELINES becomes its Definitions and the image stays the same.
"""
from __future__ import annotations

import importlib
import os
import pkgutil

from gnome_pipelines import Pipeline

FAMILY_VARIABLE = "PIPELINE_FAMILY"


def names() -> list[str]:
    return sorted(m.name for m in pkgutil.iter_modules(__path__))


def pipelines(family: str) -> dict[str, type[Pipeline]]:
    if family not in names():
        raise ValueError(f"no pipeline family '{family}'; there are {', '.join(names())}")
    # By name, not at the top: a family's modules need its extra, and an image installs only its own family's.
    module = importlib.import_module(f"{__name__}.{family}")
    return {p.name: p for p in module.PIPELINES}


def available() -> dict[str, type[Pipeline]]:
    """The pipelines this environment can run: its image's family, or every family on a laptop with all extras."""
    family = os.environ.get(FAMILY_VARIABLE)
    if family:
        return pipelines(family)
    return {name: cls for f in names() for name, cls in pipelines(f).items()}

