"""Long-running pollers: alternative-data sources that need cadences of seconds to minutes.

A source (source.py) declares its tasks. It is started and stopped on the Pipelines page like a pipeline, but its
run lasts until stopped (cdk/lambda/sources.py); runner.py serves it with serve.py, through the Service loop
(service.py), and its health in poll_health (health.py). claude.py is an optional helper for sources that have Claude read text; nothing else here imports it,
so a family without the anthropic extra can still run pollers.
"""
