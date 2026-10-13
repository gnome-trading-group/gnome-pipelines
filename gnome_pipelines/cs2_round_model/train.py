"""
Train the CS2 round win probability model on cs2_round_features and publish it.

Usage:
    poetry run python -m gnome_pipelines.cs2_round_model.train
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from gnome_pipelines.datasets import cs2
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.win_probability.model import train

logger = logging.getLogger(__name__)

ARTIFACT_TYPE = "xgboost_model"
MODEL_NAME = "cs2_round_win_prob"


def main() -> None:
    ds = dataset_store()
    df = ds.load(cs2.ROUND_FEATURES.name)
    logger.info("Loaded %d round rows from %s", len(df), cs2.ROUND_FEATURES.name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "model.xgb"
        metrics = train(df, str(path))
        logger.info("Training complete: %s", metrics)
        ref = artifact_store().publish(path, ARTIFACT_TYPE, MODEL_NAME, metrics=metrics, inputs=ds.loaded)
    logger.info("Published %s", ref)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
