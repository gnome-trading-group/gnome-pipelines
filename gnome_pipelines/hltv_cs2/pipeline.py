"""Weekly HLTV CS2 data collection pipeline — runs on ECS Fargate via gnome-controller.

Params (passed via PIPELINE_PARAMS env var):
  start_date: str  — YYYY-MM-DD
  end_date: str    — YYYY-MM-DD
  min_stars: int   — 0=all matches, 2=top-tier+, 3=big events only (default 2).
                     The models are trained on all tiers, so 0 keeps the data consistent.
"""
from __future__ import annotations

import datetime
import logging

from gnome_pipelines import Pipeline, PipelineResult, register_pipeline
from gnome_pipelines.hltv_cs2.publish import publish_priors
from gnome_pipelines.hltv_cs2.scraper import run_matches, run_rankings

logger = logging.getLogger(__name__)


@register_pipeline
class HltvCs2Pipeline(Pipeline):
    name = "hltv_cs2"

    def run(self, params: dict) -> PipelineResult:
        start_date = datetime.date.fromisoformat(params["start_date"])
        end_date = datetime.date.fromisoformat(params["end_date"])
        min_stars = params.get("min_stars", 2)

        match_df = run_matches(start_date, end_date, min_stars=min_stars)
        rankings_df = run_rankings(start_date, end_date)
        priors = publish_priors()

        return PipelineResult(
            status="succeeded",
            outputs={
                "matches": len(match_df),
                "rankings": len(rankings_df),
                "priors": priors,
            },
        )
