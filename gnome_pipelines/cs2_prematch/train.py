"""Monthly refit of the pre-veto model on all history, published as a versioned artifact."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import xgboost
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.win_probability.pre_map_features import PREVETO_FEATURE_NAMES
from gnomepy_research.models.cs2.win_probability.pre_map_model import fit_production, load_training_frame, write_bundle

logger = logging.getLogger(__name__)

ARTIFACT_TYPE = "xgboost_model"
MODEL_NAME = "cs2_prematch_preveto"


class CS2PrematchTrainParams(PipelineParams):
    n_estimators: int = Field(2000, gt=0, description="XGBoost boosting rounds, before early stopping")
    max_depth: int = Field(2, gt=0, description="XGBoost tree depth")


class CS2PrematchTrainPipeline(Pipeline[CS2PrematchTrainParams]):
    name = "cs2_prematch_train"
    description = "Refit the CS2 pre-veto model on all history and publish it as a new artifact version"
    resources = Resources(cpu=2048, memory=8192)
    schedules = (Schedule("monthly", "cron(0 7 1 * ? *)", {}, "07:00 UTC on the 1st of each month"),)

    def run(self, params: CS2PrematchTrainParams) -> PipelineResult:
        ds = dataset_store()
        df = load_training_frame(ds)
        bundle = fit_production(df, PREVETO_FEATURE_NAMES, params.n_estimators, params.max_depth)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.json"
            write_bundle(bundle, path)
            ref = artifact_store().publish(
                path, ARTIFACT_TYPE, MODEL_NAME, inputs=ds.loaded,
                description=f"pre-veto model, {bundle['n_maps']} maps through {bundle['trained_through']:%Y-%m-%d}",
                params={"features": len(PREVETO_FEATURE_NAMES), "best_iteration": int(bundle["model"].best_iteration),
                        "xgboost": xgboost.__version__},
            )
        logger.info("published %s/%s v%s", ARTIFACT_TYPE, MODEL_NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={
            "version": ref.version, "n_maps": bundle["n_maps"],
            "trained_through": f"{bundle['trained_through']:%Y-%m-%d}",
            "calibrator": bundle["calibrator"].kind,
        })
