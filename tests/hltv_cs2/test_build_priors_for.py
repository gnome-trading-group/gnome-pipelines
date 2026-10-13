"""
Live features must equal training features.

build_priors_for serves the live predictor; build_priors built the training
data. For the same rows they must agree exactly, or the model sees inputs at
prediction time that it never saw in training.
"""
import numpy as np
import pandas as pd

from gnomepy_research.hltv_cs2.build_priors import build_priors, build_priors_for

from tests.hltv_cs2.test_feature_blocks import _synthetic_history, _synthetic_players


def _history(n_days=10):
    hist = _synthetic_history(n_days=n_days)
    for side in ("a", "b"):
        hist[f"team_{side}_rank"] = hist[f"team_{side}_id"] + 1
        hist[f"team_{side}_vrs_rank"] = hist[f"team_{side}_id"] + 2
    return hist


def _rankings(hist):
    teams = sorted(set(hist.team_a_id) | set(hist.team_b_id))
    return pd.DataFrame([{"date": pd.Timestamp("2026-03-01"), "team_id": t, "team_name": f"T{t}",
                          "rank": i + 1, "points": 1000 - 50 * i} for i, t in enumerate(teams)])


def _upcoming(hist, day_offset):
    """The last day's series re-dated as upcoming, with outcomes unknown."""
    last = hist[hist.match_date == hist.match_date.max()].copy()
    last["match_id"] += 1000
    last["match_date"] += pd.Timedelta(days=day_offset)
    for col in ("team_a_won", "team_a_score", "team_b_score"):
        last[col] = np.nan
    return last


def _assert_same(a, b):
    a = a.sort_values(["match_id", "map_name"]).reset_index(drop=True)
    b = b.sort_values(["match_id", "map_name"]).reset_index(drop=True)[a.columns]
    pd.testing.assert_frame_equal(a, b, check_dtype=False)


def test_targets_equal_a_full_rebuild():
    hist = _history()
    players = _synthetic_players(hist)
    targets = _upcoming(hist, day_offset=1)
    full = build_priors(pd.concat([hist, targets], ignore_index=True), _rankings(hist), player_stats=players)
    live = build_priors_for(hist, _rankings(hist), targets, player_stats=players)
    assert len(live) == len(targets)
    _assert_same(live, full[full.match_id.isin(targets.match_id)])


def test_same_day_targets_do_not_see_that_days_results():
    """Matches already finished today are not yet observed, exactly as in training."""
    hist = _history()
    targets = _upcoming(hist, day_offset=0)
    full = build_priors(pd.concat([hist, targets], ignore_index=True), _rankings(hist))
    live = build_priors_for(hist, _rankings(hist), targets)
    _assert_same(live, full[full.match_id.isin(targets.match_id)])


def test_unknown_outcomes_do_not_leak_into_target_features():
    hist = _history()
    targets = _upcoming(hist, day_offset=1)
    a = build_priors_for(hist, _rankings(hist), targets)
    flipped = targets.assign(team_a_won=1.0, team_a_score=13, team_b_score=0)
    b = build_priors_for(hist, _rankings(hist), flipped)
    _assert_same(a, b)
