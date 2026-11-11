"""CS2: HLTV collection (matches and rosters), FACEIT collection, the weekly e12 series model fit and its market blend,
and the upcoming-match feed for gnome-launcher."""
from gnome_pipelines.cs2_e12.pipeline import CS2E12BlendPipeline, CS2E12FitPipeline
from gnome_pipelines.cs2_upcoming.pipeline import CS2UpcomingPipeline
from gnome_pipelines.faceit.pipeline import FaceitBackfillPipeline, FaceitDailyPipeline
from gnome_pipelines.hltv_cs2.pipeline import HltvCs2Pipeline
from gnome_pipelines.hltv_rosters.pipeline import HltvRostersPipeline

PIPELINES = (HltvCs2Pipeline, HltvRostersPipeline, FaceitDailyPipeline, FaceitBackfillPipeline, CS2E12FitPipeline,
             CS2E12BlendPipeline, CS2UpcomingPipeline)
SOURCES = ()
