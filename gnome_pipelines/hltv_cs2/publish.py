"""
Publishing for the HLTV datasets, used by every hltv_cs2 fetcher (zenrows, chrome, cache).

One path on purpose: the production pipeline once published only match history,
so its priors were rebuilt without the harvested datasets and player stats, veto,
head-to-head and form went stale. Both callers now publish through here.
"""
from __future__ import annotations

import logging

import pandas as pd
from gnomepy.data import TableSpec

from gnome_pipelines.datasets import cs2
from gnome_pipelines.stores import dataset_store
from gnomepy_research.models.cs2.hltv.build_priors import build_priors
from gnomepy_research.models.cs2.hltv.sides import attach_team_keyed_scores

logger = logging.getLogger(__name__)

# attach_team_keyed_scores merges these in from match history, which already stores them.
HISTORY_COLUMNS = ["team_a_started_ct", "team_a_score", "team_b_score"]


def publish(spec: TableSpec, rows: list[dict] | pd.DataFrame) -> None:
    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No rows — %s not updated", spec.name)
        return
    if "match_date" in df:
        df["match_date"] = pd.to_datetime(df["match_date"])
    result = dataset_store().publish(spec, df)
    logger.info("Published %s: %d rows written, %d unchanged", spec.name, result.rows_written, result.rows_skipped)


def publish_match_data(match_rows: list[dict], demo_rows: list[dict], player_rows: list[dict] | None = None, veto_rows: list[dict] | None = None,
                        h2h_rows: list[dict] | None = None,
                        form_rows: list[dict] | None = None) -> None:
    publish(cs2.MATCH_HISTORY, match_rows)
    publish(cs2.PLAYER_MAP_STATS, player_rows or [])
    publish(cs2.MATCH_VETO, veto_rows or [])
    # A meeting on a map outside the pool (a forfeit's "Default", a retired map) parses with no
    # map_name, but still counts toward the head-to-head features and map_name is part of the key.
    # The migrated table stores those as "", so live rows must too or they'd land under new keys.
    publish(cs2.H2H_HISTORY, [{**r, "map_name": r["map_name"] or ""} for r in h2h_rows or []])
    publish(cs2.TEAM_RECENT_FORM, form_rows or [])

    if demo_rows:
        df = pd.DataFrame(demo_rows)
        df["match_date"] = pd.to_datetime(df["match_date"])
        if match_rows:
            history = pd.DataFrame(match_rows)
        else:
            history = dataset_store().load(cs2.MATCH_HISTORY.name)
        df, dropped = attach_team_keyed_scores(df, history)
        if len(dropped):
            logger.warning(
                "dropped %d map(s) that could not be reconciled with HLTV scores: %s",
                len(dropped), dropped.reason.value_counts().to_dict(),
            )
        publish(cs2.ROUND_FEATURES, df.drop(columns=HISTORY_COLUMNS, errors="ignore"))


def publish_priors() -> int:
    """Rebuild cs2_match_priors from everything published so far; returns the row count."""
    ds = dataset_store()
    # The harvested blocks (player form, veto, page H2H) need their datasets; without
    # them build_priors still runs but publishes those features blank.
    priors = build_priors(
        ds.load(cs2.MATCH_HISTORY.name), ds.load(cs2.TEAM_RANKINGS.name),
        player_stats=ds.load(cs2.PLAYER_MAP_STATS.name),
        veto=ds.load(cs2.MATCH_VETO.name),
        h2h=ds.load(cs2.H2H_HISTORY.name),
    )
    publish(cs2.MATCH_PRIORS, priors)
    return len(priors)
