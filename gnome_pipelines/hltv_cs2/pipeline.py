"""Weekly HLTV CS2 data collection: matches and rankings in a date range, then the priors rebuilt from everything."""
from __future__ import annotations

import datetime
import logging

from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, register_pipeline
from gnome_pipelines.hltv_cs2.publish import publish_priors
from gnome_pipelines.hltv_cs2.scraper import run_matches, run_rankings

logger = logging.getLogger(__name__)


class HltvCs2Params(PipelineParams):
    start_date: datetime.date = Field(description="first match day to scrape")
    end_date: datetime.date = Field(description="last match day to scrape")
    min_stars: int = Field(2, ge=0, le=5, description=(
        "HLTV event stars: 0 = every match, 2 = top tier, 3 = big events. "
        "The models are trained on all tiers, so 0 keeps the data consistent."))


@register_pipeline
class HltvCs2Pipeline(Pipeline[HltvCs2Params]):
    name = "hltv_cs2"

    def run(self, params: HltvCs2Params) -> PipelineResult:
        match_df = run_matches(params.start_date, params.end_date, min_stars=params.min_stars)
        rankings_df = run_rankings(params.start_date, params.end_date)
        priors = publish_priors()

        return PipelineResult(
            status="succeeded",
            outputs={
                "matches": len(match_df),
                "rankings": len(rankings_df),
                "priors": priors,
            },
        )
