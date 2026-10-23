"""CFB season refresh: games, plays, drives, lines and context fetched from CFBD, checked, published, and the season's
point-in-time feature tables rebuilt.

One run, for one season:
1. pull the CFBD response cache from S3, fetch the season (its season-level endpoints refreshed), push back what
   was written;
2. build the cfb_* tables and run the quality checks — a hard failure fails the run before anything is published;
3. publish the cfb_* datasets, then rebuild and publish the season's feature tables.

The CFBD key is gnome/pipelines/cfbd in Secrets Manager (CFBD_API_KEY locally). Measured 2026-10-16 for one season:
about 10 minutes end to end and 2 GB peak (the feature rebuild).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import pandas as pd
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.cfb import cache_sync
from gnome_pipelines.cfb.cfbd_client import CACHE_DIR, CfbdClient
from gnome_pipelines.cfb.context import fetch_context
from gnome_pipelines.cfb.features import SPECS as FEATURE_SPECS
from gnome_pipelines.cfb.ingest import SPECS, build_all, fetch
from gnome_pipelines.cfb.quality import run_checks
from gnome_pipelines.stores import dataset_store
from gnomepy_research.models.cfb.features.snapshots import build_all as build_features
from gnomepy_research.models.cfb.features.source import SourceData

logger = logging.getLogger(__name__)

NEW_SEASON_MONTH = 7
# A final should be in CFBD well within this long after kickoff; older games without one mean the data lags.
FINAL_GRACE = pd.Timedelta(hours=6)


def current_season(now: datetime) -> int:
    return now.year if now.month >= NEW_SEASON_MONTH else now.year - 1


def freshness(games: pd.DataFrame, now: datetime) -> dict:
    """Completed games, the latest completed kickoff, and games that kicked off over FINAL_GRACE ago with no final."""
    kickoff = pd.to_datetime(games["kickoff"], utc=True)
    done = games["completed"].fillna(False).astype(bool) & games["home_points"].notna()
    missing = (~done) & (kickoff < pd.Timestamp(now) - FINAL_GRACE)
    return {"games_completed": int(done.sum()),
            "latest_completed_kickoff": kickoff[done].max().isoformat() if done.any() else None,
            "games_missing_final": int(missing.sum())}


class CfbUpdateParams(PipelineParams):
    season: int | None = Field(None, ge=2014, description=(
        "season to refresh; default the current one (July onwards is the new season, January bowls belong to the last)"))
    features: bool = Field(True, description="rebuild and publish the season's point-in-time feature tables")
    refresh_context: bool = Field(True, description=(
        "refetch the season's context endpoints (talent, recruiting, portal, rosters, coaches, game PPA; about 9 CFBD "
        "calls) instead of reusing the S3 mirror"))
    cache_root: str | None = Field(None, description="S3 root of the CFBD response mirror; default s3://<research bucket>/apicache/cfbd")


class CfbUpdatePipeline(Pipeline[CfbUpdateParams]):
    name = "cfb_update"
    description = "CFB season refresh from CFBD (games, plays, drives, lines, context), quality-gated publish, feature tables rebuilt"
    resources = Resources(cpu=1024, memory=4096)
    schedules = (
        Schedule("morning", "cron(0 8 * 1,8-12 ? *)", {},
                 "08:00 UTC (4am EDT, 3am EST), Jan and Aug-Dec: the night's finals, context refetched"),
        # The context endpoints barely move in season, so the second run reuses the mirror and spends about half the calls.
        Schedule("afternoon", "cron(0 20 * 1,8-12 ? *)", {"refresh_context": False},
                 "20:00 UTC (4pm EDT, 3pm EST), Jan and Aug-Dec: late West Coast finals and early kickoffs"),
    )

    def run(self, params: CfbUpdateParams) -> PipelineResult:
        now = datetime.now(timezone.utc)
        season = params.season or current_season(now)
        remote = params.cache_root or cache_sync.default_remote_root()
        client = CfbdClient()
        pulled = cache_sync.pull(CACHE_DIR, remote)
        since = time.time()
        fetch(client, [season], refresh_current=season)
        fetch_context(client, [season], refresh_current=season if params.refresh_context else None)
        pushed = cache_sync.push(CACHE_DIR, remote, since)

        tables = build_all(client, [season])
        checks = run_checks(tables)
        failed = [c for c in checks if c.hard and not c.passed]
        if failed:
            raise RuntimeError("quality checks failed, nothing published: "
                               + "; ".join(f"{c.name}: {c.detail}" for c in failed))
        ds = dataset_store()
        rows = {}
        for name, df in tables.items():
            rows[name] = len(df)
            # Before 2021 CFBD has no transfer portal, and before 2015 no talent: nothing to publish for that season.
            if not df.empty:
                ds.publish(SPECS[name], df)
        if params.features:
            for name, df in build_features(SourceData.load(ds), [season]).items():
                ds.publish(FEATURE_SPECS[name], df)
                rows[name] = len(df)
        fresh = freshness(tables["cfb_games"], now)
        if fresh["games_missing_final"]:
            logger.warning("%d games kicked off over %s ago without a final in CFBD", fresh["games_missing_final"], FINAL_GRACE)
        return PipelineResult(status="succeeded", outputs={
            "season": season, "rows": rows, "warnings": [c.name for c in checks if not c.hard and not c.passed],
            "cfbd_calls": client.calls_made, "cfbd_remaining": client.remaining_calls(),
            "cache_pulled": pulled, "cache_pushed": pushed, **fresh,
        })
