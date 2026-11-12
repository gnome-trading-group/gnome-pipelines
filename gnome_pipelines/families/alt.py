"""Alternative data: polling sources that read the web on cadences of seconds to minutes, starting with Rotten
Tomatoes reviews (rt_reviews) and its one-off backfill from the research run."""
from gnome_pipelines.rt.backfill import RtBackfillPipeline
from gnome_pipelines.rt.source import RtReviewsSource

PIPELINES = (RtBackfillPipeline,)
SOURCES = (RtReviewsSource,)
