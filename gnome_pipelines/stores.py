"""Stores stamped with this build's code version and the current run, for every writer."""
from __future__ import annotations

import os

from gnomepy.data import ArtifactStore, DatasetStore


def code_version() -> str:
    # The image build sets GNOME_CODE_VERSION to the commit, and `gnome-pipelines run` to the local checkout's.
    return os.environ.get("GNOME_CODE_VERSION", "gnome-pipelines@local")


def run_id() -> str | None:
    # Stamped on every publish as <pipeline>/<run id>, so a dataset's history or an artifact links back to its run.
    run = os.environ.get("RUN_ID")
    return f"{os.environ['PIPELINE_NAME']}/{run}" if run and os.environ.get("PIPELINE_NAME") else run


def dataset_store() -> DatasetStore:
    return DatasetStore(code_version=code_version(), run_id=run_id())


def artifact_store() -> ArtifactStore:
    return ArtifactStore(code_version=code_version(), run_id=run_id())
