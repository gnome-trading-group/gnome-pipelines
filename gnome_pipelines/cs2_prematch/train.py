"""Monthly refit of the pre-veto model on all history, published as a versioned artifact."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import joblib
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, register_pipeline
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.win_probability.pre_map_features import PREVETO_FEATURE_NAMES
from gnomepy_research.models.cs2.win_probability.pre_map_model import fit_production, load_training_frame

logger = logging.getLogger(__name__)

ARTIFACT_TYPE = "xgboost_model"
MODEL_NAME = "cs2_prematch_preveto"


class CS2PrematchTrainParams(PipelineParams):
    n_estimators: int = Field(1000, gt=0, description="XGBoost boosting rounds, before early stopping")
    max_depth: int = Field(5, gt=0, description="XGBoost tree depth")


@register_pipeline
class CS2PrematchTrainPipeline(Pipeline[CS2PrematchTrainParams]):
    name = "cs2_prematch_train"

    def run(self, params: CS2PrematchTrainParams) -> PipelineResult:
        ds = dataset_store()
        df = load_training_frame(ds)
        bundle = fit_production(df, PREVETO_FEATURE_NAMES, params.n_estimators, params.max_depth)
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
