"""
Pre-veto prices for upcoming BO3 series: series winner and map 1 winner.

Before kickoff the veto is not public, so every map of the series is priced as an
unknown map: the pre-veto model is asked for P(team A wins the next map) at each
series score, and the series DP composes those into the series price. Map 1 is
the lattice's (0, 0) node. This is the construction the walk-forward backtest
scored (holding/preveto_checks.py).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from gnomepy_research.sessions.cs2_win_probability.pre_map_model import CS2PreMapModel, _matrix
from gnomepy_research.sessions.cs2_win_probability.series_dp import series_nodes, series_win_prob

MAPS_TO_WIN = 2
PROB_CLIP = (0.01, 0.99)


def node_frame(priors_row: pd.Series) -> tuple[pd.DataFrame, list[tuple[int, int]]]:
    """One feature row per series score, carrying the series-level features of the match."""
    rows, nodes = [], []
    for a, b in series_nodes(MAPS_TO_WIN):
        row = priors_row.to_dict()
        position = a + b + 1
        row.update({"team_a_series_score": float(a), "team_b_series_score": float(b),
                    "map_position_in_series": float(position), "map_name": "",
                    "is_decider": float(position == 2 * MAPS_TO_WIN - 1)})
        rows.append(row)
        nodes.append((a, b))
    return pd.DataFrame(rows), nodes


def price(model: CS2PreMapModel, priors: pd.DataFrame) -> pd.DataFrame:
    """
    priors: one row per upcoming series (build_priors_for output).
    Returns match_id, series, game1 - each P(HLTV team A wins), clipped.
    """
    if priors.empty:
        return pd.DataFrame(columns=["match_id", "series", "game1"])
    frames, nodes = zip(*(node_frame(r) for _, r in priors.iterrows()))
    p = model.predict_batch(_matrix(pd.concat(frames, ignore_index=True), model.feature_names))
    out, cur = [], 0
    for (_, r), f, nd in zip(priors.iterrows(), frames, nodes):
        lattice = dict(zip(nd, p[cur:cur + len(f)]))
        cur += len(f)
        out.append({"match_id": int(r.match_id),
                    "series": series_win_prob(lattice, MAPS_TO_WIN, n_veto_maps=2 * MAPS_TO_WIN - 1),
                    "game1": lattice[(0, 0)]})
    df = pd.DataFrame(out)
    df[["series", "game1"]] = np.clip(df[["series", "game1"]], *PROB_CLIP)
    return df
