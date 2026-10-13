"""Stores stamped with this build's code version and the current run, for every writer."""
from __future__ import annotations

import os

from gnomepy.data import ArtifactStore, DatasetStore

# The image build sets GNOME_CODE_VERSION to the commit; local runs say so instead.
CODE_VERSION = os.environ.get("GNOME_CODE_VERSION", "gnome-pipelines@local")


def dataset_store() -> DatasetStore:
    return DatasetStore(code_version=CODE_VERSION, run_id=os.environ.get("RUN_ID"))


def artifact_store() -> ArtifactStore:
    return ArtifactStore(code_version=CODE_VERSION, run_id=os.environ.get("RUN_ID"))
