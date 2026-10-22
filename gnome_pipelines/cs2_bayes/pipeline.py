"""
The Bayesian CS2 series model in production: a weekly posterior, and the weights that blend it with the pre-veto model.

cs2_bayes_fit samples the model (gnomepy_research.models.cs2.bayes) on the last twelve months before the run day
and publishes a thinned posterior. The cs2_series sessions price from the newest one, and stop trading on one more
than two weeks old, so a missed week costs nothing and two missed weeks stop trading rather than trade stale. A fit
that failed to converge is not published: the sessions keep the previous week's.

cs2_bayes_blend publishes the blend weights. They come from a walk-forward research job (fitted on six months of
out-of-sample series, scored on the next seven), not from this pipeline, so it only records them as an artifact.
"""
from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

import pandas as pd
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.datasets import cs2
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.bayes.data import prepare_maps
from gnomepy_research.models.cs2.bayes.model import BayesSeriesConfig, fit_month
from gnomepy_research.models.cs2.hltv.bayes_series import RankStrength, SeriesBlend

logger = logging.getLogger(__name__)

POSTERIOR_TYPE, BLEND_TYPE, NAME = "bayes_posterior", "blend", "cs2_series"
STAT_COLUMNS = ["match_id", "map_name", "player_id", "team_id", "side", "rating"]


class CS2BayesFitParams(PipelineParams):
    as_of: str | None = Field(None, description="Fit through this date (YYYY-MM-DD, exclusive); default: the run day (UTC)")
    keep_draws: int = Field(500, gt=0, description="Posterior draws kept in the published artifact")
    max_rhat: float = Field(1.15, gt=1, description="Refuse to publish if a hyperparameter's r-hat exceeds this")
    max_skill_rhat: float = Field(1.05, gt=1, description="Refuse to publish if a player skill's r-hat exceeds this")


class CS2BayesFitPipeline(Pipeline[CS2BayesFitParams]):
    name = "cs2_bayes_fit"
    description = "Fit the Bayesian CS2 series model on the last twelve months and publish its posterior"
    # NUTS runs four chains in parallel; 8 vCPUs gives each one a core with room for the rest.
    resources = Resources(cpu=8192, memory=16384)
    schedules = (Schedule("weekly", "cron(0 6 ? * MON *)", {}, "06:00 UTC every Monday"),)

    def run(self, params: CS2BayesFitParams) -> PipelineResult:
        as_of = pd.Timestamp(params.as_of) if params.as_of else pd.Timestamp.now(tz="UTC").tz_convert(None).normalize()
        ds = dataset_store()
        maps = prepare_maps(ds.load(cs2.MATCH_HISTORY.name), ds.load(cs2.MATCH_LINEUPS.name))
        stats = ds.load(cs2.PLAYER_MAP_STATS.name, columns=STAT_COLUMNS)
        ranks = RankStrength(ds.load(cs2.TEAM_RANKINGS.name))
        cfg = BayesSeriesConfig()
        posterior, diag = fit_month(maps, stats, ranks, as_of, cfg)
        logger.info("fit through %s: %s", as_of.date(), diag)
        problems = [f"{diag['divergences']} divergences"] if diag["divergences"] else []
        if diag["max_rhat"] > params.max_rhat:
            problems.append(f"r-hat {diag['max_rhat']:.3f} on {diag['worst_rhat_param']}")
        if diag["skill_max_rhat"] > params.max_skill_rhat:
            problems.append(f"player-skill r-hat {diag['skill_max_rhat']:.3f}")
        if problems:
            raise RuntimeError(f"not publishing a posterior that did not converge: {', '.join(problems)}")

        thin = posterior.thinned(params.keep_draws)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "posterior.npz"
            thin.save(path)
            ref = artifact_store().publish(
                path, POSTERIOR_TYPE, NAME, inputs=ds.loaded,
                description=f"Bayesian series posterior, fit window {cfg.window_months} months to {thin.as_of}",
                params={"as_of": thin.as_of, "window_months": cfg.window_months, "tau": cfg.tau,
                        "debut_prior": cfg.debut_prior, "draws": thin.n_draws},
                metrics=diag,
            )
        logger.info("published %s/%s v%s", POSTERIOR_TYPE, NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={"version": ref.version, "as_of": thin.as_of, **{
            k: diag[k] for k in ("maps", "players", "divergences", "max_rhat", "skill_max_rhat", "gamma", "a_new")
            if k in diag}})


class CS2BayesBlendParams(PipelineParams):
    w_bayes: float = Field(description="Weight on the Bayesian series model's logit")
    w_xgb: float = Field(description="Weight on the pre-veto model's logit")
    source: str = Field(description="Where the weights were fitted, e.g. the research job's run id")


class CS2BayesBlendPipeline(Pipeline[CS2BayesBlendParams]):
    name = "cs2_bayes_blend"
    description = "Publish the weights that blend the Bayesian series model with the pre-veto model"

    def run(self, params: CS2BayesBlendParams) -> PipelineResult:
        blend = SeriesBlend(params.w_bayes, params.w_xgb)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "blend.json"
            path.write_text(json.dumps(blend.to_dict()))
            ref = artifact_store().publish(path, BLEND_TYPE, NAME, description=f"blend weights from {params.source}",
                                           params={**blend.to_dict(), "source": params.source})
        logger.info("published %s/%s v%s", BLEND_TYPE, NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={"version": ref.version, **blend.to_dict()})
