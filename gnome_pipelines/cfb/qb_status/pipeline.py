"""The QB Status Service as a daily pipeline: one Fargate run a day that polls until just before the next one starts.

The pollers need 15 to 60 second cadences, so the service runs as one long task rather than many scheduled ones. A
run pulls its state (X spend and since_ids, Claude's read cache) from S3, serves for `hours`, and pushes the state
back every minute and on the way out. Keys come from gnome/pipelines/x and gnome/pipelines/anthropic in Secrets
Manager; without one, that source is off and the run says so. Measured on the laptop: about 560 MB and 4% of a core.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gnomepy.data import DatasetStore
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.cfb.qb_status.extract import CLAUDE_CACHE
from gnome_pipelines.cfb.qb_status.service import X_STATE, build_service, service_context
from gnome_pipelines.cfb.qb_status.state_sync import StateMirror, default_remote_root
from gnome_pipelines.cfb.qb_status.store import StatusStore, open_store
from gnome_pipelines.cfb.qb_status.x_scan import DAILY_CAP_USD, MONTHLY_USD
from gnome_pipelines.stores import dataset_store

STATE_DIR = X_STATE.parent
STATE_FILES = (X_STATE.name, CLAUDE_CACHE.name)
NEW_SEASON_MONTH = 7


def status_store(prod: bool) -> DatasetStore:
    """Prod writes carry the run's id and commit (as cfb_update's do), so the datasets page links rows to the run."""
    return dataset_store() if prod else open_store(False)


class QbStatusParams(PipelineParams):
    hours: float = Field(23.75, gt=0, le=24, description=(
        "how long to serve; under 24 so a run ends before the next day's starts"))
    prod: bool = Field(True, description="write the QB status tables to the prod catalog (false: a local warehouse)")
    x: bool = Field(True, description="search X when its key is set")
    claude: bool = Field(True, description="have Claude read posts and headlines when its key is set")
    state_root: str | None = Field(None, pattern=r"^(s3://|/)\S+$", description=(
        "S3 root of the mirrored state; leave empty for s3://<research bucket>/qb_status/state"))
    x_monthly_usd: float = Field(MONTHLY_USD, ge=0, description=(
        "X spend cap per calendar month, at $0.005 per post returned; sweeps stop at 80% of it"))
    x_daily_usd: float = Field(DAILY_CAP_USD, ge=0, description="X spend cap per UTC day, so one busy day can't spend the month")


class QbStatusPipeline(Pipeline[QbStatusParams]):
    name = "cfb_qb_status"
    description = ("QB status service: official reports, X, news, game-night flags and market alarms into "
                   "timestamped start probabilities per team-game")
    resources = Resources(cpu=256, memory=2048)
    schedules = (
        Schedule("daily", "cron(0 9 * 1,8-12 ? *)", {},
                 "09:00 UTC (5am EDT, 4am EST), Jan and Aug-Dec: serves until 08:45 the next day"),
    )

    def run(self, params: QbStatusParams) -> PipelineResult:
        now = datetime.now(timezone.utc)
        mirror = StateMirror(STATE_DIR, params.state_root or default_remote_root(), STATE_FILES)
        pulled = mirror.pull()
        said: list[str] = []
        season = now.year if now.month >= NEW_SEASON_MONTH else now.year - 1
        ctx = service_context(dataset_store(), StatusStore(status_store(params.prod)), season, use_x=params.x,
                              use_claude=params.claude, say=said.append, x_monthly_usd=params.x_monthly_usd,
                              x_daily_usd=params.x_daily_usd)
        svc = build_service(ctx, mirror=mirror)
        try:
            svc.run_until(now + timedelta(hours=params.hours))
        finally:
            mirror.push()
        return PipelineResult(status="succeeded", outputs={
            "season": season, "state_pulled": pulled, "x": ctx.x is not None, "extractor": ctx.extractor.name,
            "x_spent_usd": ctx.x_budget.spent_usd() if ctx.x_budget is not None else 0.0,
            "claude_usd": getattr(ctx.extractor, "usd", lambda: 0.0)(), "notes": said,
        })
