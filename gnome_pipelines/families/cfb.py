"""CFB: the CFBD season refresh behind the cfb_* datasets and feature tables."""
from gnome_pipelines.cfb.pipeline import CfbUpdatePipeline

PIPELINES = (CfbUpdatePipeline,)
