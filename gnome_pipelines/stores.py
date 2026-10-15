"""Stores stamped with this build's code version and the current run, for every writer."""
from __future__ import annotations

import os

from gnomepy.data import ArtifactStore, DatasetStore


def code_version() -> str:
    # The image build sets GNOME_CODE_VERSION to the commit, and `gnome-pipelines run` to the local checkout's.
    return os.environ.get("GNOME_CODE_VERSION", "gnome-pipelines@local")


def dataset_store() -> DatasetStore:
    return DatasetStore(code_version=code_version(), run_id=os.environ.get("RUN_ID"))


def artifact_store() -> ArtifactStore:
    return ArtifactStore(code_version=code_version(), run_id=os.environ.get("RUN_ID"))
