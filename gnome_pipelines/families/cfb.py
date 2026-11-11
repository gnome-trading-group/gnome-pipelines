"""CFB: the CFBD season refresh behind the cfb_* datasets and feature tables, and the QB status polling source."""
from gnome_pipelines.cfb.pipeline import CfbUpdatePipeline
from gnome_pipelines.cfb.qb_status.source import QbStatusSource

PIPELINES = (CfbUpdatePipeline,)
SOURCES = (QbStatusSource,)
