"""The QB Status Service as a polling source, started on the Pipelines page for the season and stopped after it.

The pollers need 15 to 60 second cadences. Its state (X spend and since_ids, Claude's reads) lives in
s3://<research bucket>/qb_status/state (state_store.py), written as it goes. Keys come from gnome/pipelines/x and gnome/pipelines/anthropic in Secrets Manager; without one, that source is off
and the service says so in poll_health. Measured on the laptop: about 560 MB and 4% of a core.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import Field

from gnome_pipelines import PipelineParams, Resources
from gnome_pipelines.cfb.qb_status.service import LANE, SOURCE, build_service, service_context
from gnome_pipelines.cfb.qb_status.state_store import state_objects
from gnome_pipelines.cfb.qb_status.store import StatusStore
from gnome_pipelines.cfb.qb_status.x_scan import DAILY_CAP_USD, MONTHLY_USD
from gnome_pipelines.polling.alerts import Alerts
from gnome_pipelines.polling.health import PollHealth
from gnome_pipelines.polling.service import Service, Task, utcnow
from gnome_pipelines.polling.source import PollingSource
from gnome_pipelines.polling.writer import TableWriter
from gnome_pipelines.stores import dataset_store

NEW_SEASON_MONTH = 7
SEASON_CHECK_EVERY = 3600.0


def season_for(now: datetime) -> int:
    """The college-football season a date belongs to: bowls and the title game in January are last year's season."""
    return now.year if now.month >= NEW_SEASON_MONTH else now.year - 1


class QbStatusParams(PipelineParams):
    x: bool = Field(True, description="search X when its key is set")
    claude: bool = Field(True, description="have Claude read posts and headlines when its key is set")
    x_monthly_usd: float = Field(MONTHLY_USD, ge=0, description=(
        "X spend cap per calendar month, at $0.005 per post returned; sweeps stop at 80% of it"))
    x_daily_usd: float = Field(DAILY_CAP_USD, ge=0, description="X spend cap per UTC day, so one busy day can't spend the month")


class QbStatusSource(PollingSource[QbStatusParams]):
    name = SOURCE
    description = ("QB status service: official reports, X, news, game-night flags and market alarms into "
                   "timestamped start probabilities per team-game")
    resources = Resources(cpu=256, memory=2048)
    secrets = ("x", "anthropic")

    def build(self, params: QbStatusParams, health: PollHealth, writer: TableWriter, alerts: Alerts) -> Service:
        # QB status's own tables go through StatusStore, which reads back what it writes; only its health rows use
        # the writer, through `health`.
        season = season_for(utcnow())
        store = dataset_store()
        ctx = service_context(store, StatusStore(store), season, state_objects(), use_x=params.x,
                              use_claude=params.claude,
                              say=lambda note: health.record("note", utcnow(), note),
                              x_monthly_usd=params.x_monthly_usd, x_daily_usd=params.x_daily_usd)
        service = build_service(ctx, health)
        service.alerts = alerts

        def check_season(now: datetime) -> list:
            # The run loaded one season's games and rosters at start. Left running past July 1 it would match
            # nothing in the new season, so it ends cleanly instead and is started again (on the new season).
            if season_for(now) != season:
                health.record("note", now, f"season {season} is over; restarting for season {season_for(now)}")
                service.stop()
            return []
        service.tasks.append(Task("season_check", lambda now: SEASON_CHECK_EVERY, check_season, LANE))
        return service
