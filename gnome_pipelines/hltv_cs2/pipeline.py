"""HLTV CS2 collection: matches and rankings in a date range, then the priors rebuilt from everything.

The scheduled run fetches through ZenRows. Locally, `fetcher=chrome` uses a visible Chrome window
instead (and is the only way to collect demos, which feed the round features), and `fetcher=cache`
re-parses already-cached pages without touching the network, e.g. after a parser change.
"""
from __future__ import annotations

import datetime
import logging
from typing import Literal

from pydantic import Field, model_validator

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule, register_pipeline
from gnome_pipelines.environment import environment
from gnome_pipelines.hltv_cs2.chrome import ChromeSource
from gnome_pipelines.hltv_cs2.collect import collect_matches, collect_rankings
from gnome_pipelines.hltv_cs2.fetch import CacheSource, HltvFetcher, PageSource, ZenRowsSource, run_with_fetcher
from gnome_pipelines.hltv_cs2.html_cache import CachePolicy
from gnome_pipelines.hltv_cs2.publish import publish_priors

logger = logging.getLogger(__name__)


class HltvCs2Params(PipelineParams):
    start_date: datetime.date | None = Field(None, description="first match day to scrape; with end_date, overrides days_back")
    end_date: datetime.date | None = Field(None, description="last match day to scrape; with start_date, overrides days_back")
    days_back: int = Field(7, ge=1, description="without dates: the last N days, ending today (UTC)")
    min_stars: int = Field(2, ge=0, le=5, description=(
        "HLTV event stars: 0 = every match, 2 = top tier, 3 = big events. "
        "The models are trained on all tiers, so 0 keeps the data consistent."))
    fetcher: Literal["zenrows", "chrome", "cache"] = Field("zenrows", description=(
        "zenrows (anywhere), chrome (a visible local window; click through Cloudflare when asked) "
        "or cache (only pages already cached, no network)"))
    demos: bool = Field(False, description="download and parse demos for the round features; needs chrome")
    min_stars_demo: int = Field(2, ge=0, le=5, description="only download demos for matches with this many stars")
    match_ids: list[int] | None = Field(None, description="only these matches (still listed day by day)")
    refresh: bool = Field(False, description="ignore cached pages and fetch everything again")

    @model_validator(mode="after")
    def _consistent(self) -> HltvCs2Params:
        if (self.start_date is None) != (self.end_date is None):
            raise ValueError("give both start_date and end_date, or neither to use days_back")
        if self.start_date is not None and self.end_date < self.start_date:
            raise ValueError("end_date is before start_date")
        if self.demos and self.fetcher != "chrome":
            raise ValueError("demos are downloaded through Chrome; set fetcher=chrome")
        if self.refresh and self.fetcher == "cache":
            raise ValueError("refresh has nothing to fetch with fetcher=cache")
        return self

    def window(self, today: datetime.date) -> tuple[datetime.date, datetime.date]:
        if self.start_date is not None:
            return self.start_date, self.end_date
        return today - datetime.timedelta(days=self.days_back), today


@register_pipeline
class HltvCs2Pipeline(Pipeline[HltvCs2Params]):
    name = "hltv_cs2"
    description = "HLTV matches, player stats, veto, head-to-head, form and rankings; then the match priors rebuilt"
    resources = Resources(cpu=2048, memory=8192)
    schedules = (
        # Daily, not weekly: cs2_upcoming builds live features from this history, and a week-old
        # history leaves Elo and form a week behind the matches being priced.
        Schedule("daily", "cron(0 6 * * ? *)", {"days_back": 2, "min_stars": 0},
                 "06:00 UTC: the last 2 days, every tier (one day of overlap; cached pages cost nothing)"),
    )

    def run(self, params: HltvCs2Params) -> PipelineResult:
        if params.fetcher == "chrome" and environment().in_task:
            raise ValueError("fetcher=chrome needs a screen for the Cloudflare challenge; run it locally")
        start_date, end_date = params.window(datetime.datetime.now(datetime.timezone.utc).date())

        async def collect(fetcher: HltvFetcher) -> dict:
            matches = await collect_matches(
                fetcher, start_date, end_date, min_stars_match=params.min_stars,
                demos=params.demos, min_stars_demo=params.min_stars_demo,
                match_ids=set(params.match_ids) if params.match_ids else None,
            )
            rankings = await collect_rankings(fetcher, start_date, end_date)
            return {**matches, "rankings": rankings, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()}

        outputs = run_with_fetcher(_source(params.fetcher), _policy(params), collect)
        outputs["priors"] = publish_priors()
        return PipelineResult(status="succeeded", outputs=outputs)


def _source(fetcher: str) -> PageSource:
    return {"zenrows": ZenRowsSource, "chrome": ChromeSource, "cache": CacheSource}[fetcher]()


def _policy(params: HltvCs2Params) -> CachePolicy:
    if params.fetcher == "cache":
        # Any cached copy will do, however old: there is nothing to replace it with.
        return CachePolicy(ttl=datetime.timedelta.max, write=False)
    return CachePolicy(refresh_all=params.refresh)
