"""CS2: HLTV collection (matches and rosters), the upcoming-match feed for gnome-launcher, the pre-veto model refit, and the
Bayesian series model's weekly posterior and blend weights."""
from gnome_pipelines.cs2_bayes.pipeline import CS2BayesBlendPipeline, CS2BayesFitPipeline
from gnome_pipelines.cs2_prematch.train import CS2PrematchTrainPipeline
from gnome_pipelines.cs2_upcoming.pipeline import CS2UpcomingPipeline
from gnome_pipelines.hltv_cs2.pipeline import HltvCs2Pipeline
from gnome_pipelines.hltv_rosters.pipeline import HltvRostersPipeline

PIPELINES = (HltvCs2Pipeline, HltvRostersPipeline, CS2UpcomingPipeline, CS2PrematchTrainPipeline, CS2BayesFitPipeline,
             CS2BayesBlendPipeline)
