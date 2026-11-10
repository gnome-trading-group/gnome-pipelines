"""
The e12-wide-mm CS2 series model in production (gnomepy_research.models.cs2.e12).

cs2_e12_fit fits the model every morning, at its own start time, on the HLTV and FACEIT rows known before it and
publishes it as artifact e12_model/cs2_series; the cs2_series sessions price announced fives from the newest one. A
daily fit beat a weekly one at kickoff in research (it sees the week's results, which the market already has). It runs
after hltv_cs2 (06:00) and faceit_daily (07:30), and refuses to publish on stale inputs (the newest HLTV map or FACEIT
game more than max_input_age_days before the cutoff), an implausibly small fit or one that didn't converge, so the
sessions keep the previous day's model rather than price from a gap in the data.

cs2_e12_blend publishes the weights that blend the model with the market's own price (blend/cs2_series_e12). They are
fitted in research on past series' kickoff prices and outcomes, so this pipeline only records them.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pandas as pd
from pydantic import Field
from threadpoolctl import threadpool_limits

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.e12.blend import MarketBlend
from gnomepy_research.models.cs2.e12.model import E12Model
from gnomepy_research.models.cs2.e12.tables import DatasetTables

logger = logging.getLogger(__name__)

MODEL_TYPE, BLEND_TYPE, NAME, BLEND_NAME = "e12_model", "blend", "cs2_series", "cs2_series_e12"


def default_cutoff() -> pd.Timestamp:
    """Now, naive UTC, to the minute."""
    return pd.Timestamp.now(tz="UTC").tz_convert(None).floor("min")


class CS2E12FitParams(PipelineParams):
    cutoff: str | None = Field(None, description="Fit at this UTC time (YYYY-MM-DDTHH:MM); default: now")
    max_input_age_days: float = Field(2.0, gt=0, description="Refuse when the newest HLTV map or FACEIT game is older than "
                                                              "this before the cutoff")
    min_players: int = Field(2000, ge=0, description="Refuse when the HLTV skill fit has fewer players")
    min_mm_players: int = Field(1000, ge=0, description="Refuse when fewer linked players have a matchmaking skill")


class CS2E12FitPipeline(Pipeline[CS2E12FitParams]):
    name = "cs2_e12_fit"
    description = "Fit the e12-wide-mm CS2 series model now and publish it"
    # The matchmaking fit reads ~25M FACEIT player rows; the skill fits are single-threaded.
    resources = Resources(cpu=4096, memory=30720)
    schedules = (Schedule("daily", "cron(30 8 * * ? *)", {}, "08:30 UTC every day, after hltv_cs2 (06:00) and faceit_daily (07:30)"),)

    def run(self, params: CS2E12FitParams) -> PipelineResult:
        cutoff = pd.Timestamp(params.cutoff) if params.cutoff else default_cutoff()
        ds = dataset_store()
        tables = DatasetTables(ds)
        newest_hltv = tables.load("match_history", cutoff, ["match_date"]).match_date.max()
        newest_faceit = pd.Timestamp(int(tables.load("faceit_history_matches", cutoff, ["finished_at"]).finished_at.max()), unit="s")
        problems = [f"newest {what} is {(cutoff - when).total_seconds() / 86400:.1f} days before the cutoff"
                    for what, when in (("HLTV map", pd.Timestamp(newest_hltv)), ("FACEIT game", newest_faceit))
                    if cutoff - when > pd.Timedelta(days=params.max_input_age_days)]
        if problems:
            raise RuntimeError(f"not fitting on stale inputs: {'; '.join(problems)}")

        with threadpool_limits(1):
            model = E12Model.build(tables, cutoff)
        info = model.info
        logger.info("fit at %s: %s", cutoff, info)
        if info["hltv"]["players"] < params.min_players:
            problems.append(f"HLTV fit has {info['hltv']['players']} players")
        if info["mm_players"] < params.min_mm_players:
            problems.append(f"{info['mm_players']} players with a matchmaking skill")
        for fit in ("hltv", "faceit"):
            if not info[fit]["converged"]:
                problems.append(f"the {fit} skill fit stopped short of its optimum (largest gradient {info[fit]['grad_max']:.1e}, "
                                f"{info[fit]['stop']})")
        if problems:
            raise RuntimeError(f"not publishing an implausible fit: {'; '.join(problems)}")

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "e12.npz"
            model.save(path)
            # The freshness checks and the fit read some tables more than once; the index takes one row per dataset.
            inputs = list({r.dataset: r for r in ds.loaded}.values())
            ref = artifact_store().publish(path, MODEL_TYPE, NAME, inputs=inputs,
                                           description=f"e12-wide-mm fit at {cutoff:%Y-%m-%d %H:%M}",
                                           params={"cutoff": cutoff.isoformat()}, metrics=info)
        logger.info("published %s/%s v%s", MODEL_TYPE, NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={"version": ref.version, "cutoff": cutoff.isoformat(), **info})


class CS2E12BlendParams(PipelineParams):
    market: float = Field(description="Weight on logit(market mid)")
    model: float = Field(description="Weight on logit(e12-wide-mm price)")
    intercept: float = Field(description="Intercept, in team A's (HLTV's) favour")
    source: str = Field(description="Where the weights were fitted, e.g. the research run or file")


class CS2E12BlendPipeline(Pipeline[CS2E12BlendParams]):
    name = "cs2_e12_blend"
    description = "Publish the weights that blend e12-wide-mm with the market's own price"

    def run(self, params: CS2E12BlendParams) -> PipelineResult:
        blend = MarketBlend(params.market, params.model, params.intercept)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "blend.json"
            blend.save(path)
            ref = artifact_store().publish(path, BLEND_TYPE, BLEND_NAME, description=f"blend weights from {params.source}",
                                           params={"market": blend.market, "model": blend.model,
                                                   "intercept": blend.intercept, "source": params.source})
        logger.info("published %s/%s v%s", BLEND_TYPE, BLEND_NAME, ref.version)
        return PipelineResult(status="succeeded", outputs={"version": ref.version, "market": blend.market,
                                                           "model": blend.model, "intercept": blend.intercept})
