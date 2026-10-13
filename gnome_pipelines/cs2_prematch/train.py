"""
Monthly refit of the pre-veto model on all history, published as a versioned artifact.

Params: n_estimators (1000), max_depth (5).
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import joblib

from gnome_pipelines import Pipeline, PipelineResult, register_pipeline
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.sessions.cs2_win_probability.pre_map_features import PREVETO_FEATURE_NAMES
from gnomepy_research.sessions.cs2_win_probability.pre_map_model import fit_production, load_training_frame

logger = logging.getLogger(__name__)

ARTIFACT_TYPE = "xgboost_model"
MODEL_NAME = "cs2_prematch_preveto"


@register_pipeline
class CS2PrematchTrainPipeline(Pipeline):
    name = "cs2_prematch_train"

    def run(self, params: dict) -> PipelineResult:
        ds = dataset_store()
        df = load_training_frame(ds)
        bundle = fit_production(df, PREVETO_FEATURE_NAMES, params.get("n_estimators", 1000), params.get("max_depth", 5))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.joblib"
            joblib.dump(bundle, path)
            ref = artifact_store().publish(
                path, ARTIFACT_TYPE, MODEL_NAME, inputs=ds.loaded,
                description=f"pre-veto model, {bundle['n_maps']} maps through {bundle['trained_through']:%Y-%m-%d}",
                params={"features": len(PREVETO_FEATURE_NAMES), "best_iteration": int(bundle["model"].best_iteration)},
            )
        logger.info("published %s/%s v%s", ARTIFACT_TYPE, MODEL_NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={
            "version": ref.version, "n_maps": bundle["n_maps"],
            "trained_through": f"{bundle['trained_through']:%Y-%m-%d}",
            "calibrator": bundle["calibrator"].kind,
        })
