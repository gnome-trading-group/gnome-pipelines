"""CFB: the CFBD season refresh behind the cfb_* datasets and feature tables, and the QB status service."""
from gnome_pipelines.cfb.pipeline import CfbUpdatePipeline
from gnome_pipelines.cfb.qb_status.pipeline import QbStatusPipeline

PIPELINES = (CfbUpdatePipeline, QbStatusPipeline)
