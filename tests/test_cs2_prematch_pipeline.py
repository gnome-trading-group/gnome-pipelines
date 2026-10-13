"""
The live prediction pipeline: pricing, market legs, state and launcher triggers.

The I/O (HLTV, DatasetStore, registry, SQS) lives in the pipeline class; these
are the decisions it makes, which are where a mistake would cost money or
launch a duplicate session.
"""
import json

import joblib
import numpy as np
import pandas as pd
import pytest

from gnome_pipelines.cs2_prematch import predict
from gnome_pipelines.cs2_prematch.model import node_frame, price
from gnomepy_research.sessions.cs2_prematch import identity
from gnomepy_research.sessions.cs2_prematch.polymarket import index_markets
from gnomepy_research.sessions.cs2_win_probability.pre_map_features import PREVETO_FEATURE_NAMES
from gnomepy_research.sessions.cs2_win_probability.pre_map_model import CS2PreMapModel, _matrix, fit_production
from gnomepy_research.sessions.cs2_win_probability.symmetry import build_swap_plan, swap_features

NOW = pd.Timestamp("2026-10-05 01:00", tz="UTC")
KICK = pd.Timestamp("2026-10-05 16:30", tz="UTC")
LEAD = pd.Timedelta(hours=12)
MATCH = {"match_id": 2398739, "team_a_id": 9565, "team_a_name": "Vitality", "team_b_id": 11283,
         "team_b_name": "Falcons", "match_time": KICK}


@pytest.fixture(scope="module")
def model(tmp_path_factory):
    rng = np.random.default_rng(0)
    n = 1200
    df = pd.DataFrame(rng.normal(size=(n, len(PREVETO_FEATURE_NAMES))), columns=PREVETO_FEATURE_NAMES)
    df["match_id"] = np.repeat(np.arange(n // 2), 2)
    df["match_date"] = pd.Timestamp("2026-01-01") + pd.to_timedelta(df.match_id // 4, unit="D")
    df["map_name"] = ""
    df["team_a_won"] = (rng.uniform(size=n) < 1 / (1 + np.exp(-df.elo_diff))).astype(int)
    path = tmp_path_factory.mktemp("m") / "model.joblib"
    joblib.dump(fit_production(df, PREVETO_FEATURE_NAMES, n_estimators=60, max_depth=3), path)
    return CS2PreMapModel(str(path))


def _mirror(row: pd.Series) -> pd.Series:
    plan = build_swap_plan(PREVETO_FEATURE_NAMES)
    swapped = swap_features(_matrix(pd.DataFrame([row]), PREVETO_FEATURE_NAMES), plan)[0]
    return row.copy().pipe(lambda r: r.where(~r.index.isin(PREVETO_FEATURE_NAMES), pd.Series(swapped, PREVETO_FEATURE_NAMES)))


def test_lattice_covers_every_bo3_score():
    frame, nodes = node_frame(pd.Series({"match_id": 1, "elo_diff": 30.0}))
    assert sorted(nodes) == [(0, 0), (0, 1), (1, 0), (1, 1)]
    for (a, b), r in zip(nodes, frame.itertuples()):
        assert (r.team_a_series_score, r.team_b_series_score) == (a, b)
        assert r.map_position_in_series == a + b + 1 and r.is_decider == (a + b == 2)
    assert (frame.elo_diff == 30.0).all() and (frame.map_name == "").all()


def test_prices_are_order_invariant(model):
    rng = np.random.default_rng(1)
    row = pd.Series(rng.normal(size=len(PREVETO_FEATURE_NAMES)), index=PREVETO_FEATURE_NAMES)
    row["match_id"] = 1
    for c in ("team_a_series_score", "team_b_series_score", "map_position_in_series", "is_decider"):
        row[c] = 0.0
    a = price(model, pd.DataFrame([row]))
    b = price(model, pd.DataFrame([_mirror(row)]))
    assert a.series.iloc[0] + b.series.iloc[0] == pytest.approx(1.0, abs=0.02)
    assert a.game1.iloc[0] + b.game1.iloc[0] == pytest.approx(1.0, abs=0.02)


def test_a_series_is_more_lopsided_than_one_map(model):
    rows = []
    for elo in (-2.5, 2.5):
        r = pd.Series(0.0, index=PREVETO_FEATURE_NAMES)
        r["elo_diff"], r["match_id"] = elo, int(elo > 0)
        rows.append(r)
    out = price(model, pd.DataFrame(rows)).set_index("match_id")
    assert out.series[1] > out.game1[1] > 0.5 > out.game1[0] > out.series[0]


def test_prediction_rows_carry_kickoff_and_rank():
    prices = pd.DataFrame({"match_id": [7], "series": [0.6], "game1": [0.55]})
    targets = pd.DataFrame({"match_id": [7], "match_time": [KICK]})
    priors = pd.DataFrame({"match_id": [7], "rank_known_both": [1.0]})
    rows = predict.prediction_rows(prices, targets, priors, "m:3", NOW).set_index("market")
    assert rows.p_team_a.to_dict() == {"series": 0.6, "game1": 0.55}
    assert (rows.kickoff == KICK).all() and (rows.generated_at == NOW).all() and (rows.model_version == "m:3").all()


def _pm():
    rows = [("cs2-vit-fal-2026-10-05", ["Vitality", "Falcons"], ["s_vit", "s_fal"]),
            ("cs2-vit-fal-2026-10-05-game1", ["Falcons", "Vitality"], ["g_fal", "g_vit"])]
    return index_markets(pd.DataFrame([{"market_slug": s, "condition_id": "0x" + s, "outcomes": o, "tokens": t,
                                        "final": [], "closed": False, "kickoff": KICK} for s, o, t in rows]))


def test_map1_tokens_are_oriented_by_name_not_position():
    ids = {"s_vit": 1, "s_fal": 2, "g_vit": 3, "g_fal": 4}
    res, legs, reason = predict.resolve_legs(MATCH, _pm(), identity.empty_aliases(), lambda c, t: ids[t])
    assert reason is None and res.market_slug == "cs2-vit-fal-2026-10-05"
    assert legs == [{"market": "series", "listing_team_a": 1, "listing_team_b": 2},
                    {"market": "game1", "listing_team_a": 3, "listing_team_b": 4}]


def test_unregistered_market_is_skipped_with_a_reason():
    res, legs, reason = predict.resolve_legs(MATCH, _pm(), identity.empty_aliases(), lambda c, t: None)
    assert legs == [] and reason == "markets not in registry"


def _state_with_legs(now=NOW, match=MATCH):
    res = identity.Resolution("cs2-vit-fal-2026-10-05", 0)
    legs = [{"market": "series", "listing_team_a": 1, "listing_team_b": 2}]
    return predict.upsert_state(predict.empty_state(), match, res, legs, now, LEAD)


def test_each_trigger_is_sent_exactly_once():
    state = _state_with_legs()
    launches, shutdowns, state = predict.plan_triggers(state, NOW, pd.Timedelta(minutes=30))
    assert len(launches) == 1 and shutdowns == []
    assert launches[0] == {"rule_type": "cs2_prematch", "data": {
        "match_id": 2398739, "launch_at": (KICK - LEAD).isoformat(),
        "markets": [{"market": "series", "listing_team_a": 1, "listing_team_b": 2}]}}
    again, _, state = predict.plan_triggers(state, NOW + pd.Timedelta(minutes=30), pd.Timedelta(minutes=30))
    assert again == [], "a repeat could schedule a second session"
    _, shutdowns, state = predict.plan_triggers(state, KICK + pd.Timedelta(minutes=29), pd.Timedelta(minutes=30))
    assert shutdowns == []
    _, shutdowns, state = predict.plan_triggers(state, KICK + pd.Timedelta(minutes=31), pd.Timedelta(minutes=30))
    assert len(shutdowns) == 1 and shutdowns[0]["data"]["match_id"] == 2398739
    _, again, _ = predict.plan_triggers(state, KICK + pd.Timedelta(hours=1), pd.Timedelta(minutes=30))
    assert again == []


def test_launch_is_immediate_when_first_seen_inside_the_lead():
    state = _state_with_legs(now=KICK - pd.Timedelta(hours=3))
    assert pd.Timestamp(state.launch_at.iloc[0]) == KICK - pd.Timedelta(hours=3)


def test_kickoff_follows_hltv_but_the_sent_trigger_does_not_change():
    state = _state_with_legs()
    launches, _, state = predict.plan_triggers(state, NOW, pd.Timedelta(minutes=30))
    moved = {**MATCH, "match_time": KICK + pd.Timedelta(hours=2)}
    state = predict.upsert_state(state, moved, identity.Resolution(None, None), [], NOW + pd.Timedelta(hours=1), LEAD)
    assert pd.Timestamp(state.kickoff.iloc[0]) == KICK + pd.Timedelta(hours=2)
    assert pd.Timestamp(state.launch_at.iloc[0]) == KICK - LEAD
    assert json.loads(state.markets.iloc[0]) == launches[0]["data"]["markets"]
    _, shutdowns, _ = predict.plan_triggers(state, KICK + pd.Timedelta(hours=1), pd.Timedelta(minutes=30))
    assert shutdowns == [], "shutdown waits for the new kickoff"


def test_no_launch_without_legs():
    state = predict.upsert_state(predict.empty_state(), MATCH, identity.Resolution(None, None), [], NOW, LEAD)
    launches, _, _ = predict.plan_triggers(state, NOW, pd.Timedelta(minutes=30))
    assert launches == []


def test_settlement_confirms_learned_aliases_once():
    res = identity.Resolution("cs2-vit-fal-2026-10-05", 1, learned=(("vitalityx", 9565),))
    legs = [{"market": "series", "listing_team_a": 1, "listing_team_b": 2}]
    state = predict.upsert_state(predict.empty_state(), MATCH, res, legs, NOW, LEAD)
    aliases = identity.learn(identity.empty_aliases(), res.learned, NOW)
    pm = _pm()
    pm.loc[pm.market_slug == "cs2-vit-fal-2026-10-05", "final"] = pd.Series([["0", "1"]], index=pm.index[:1])
    won = pd.Series({2398739: 1})
    state, aliases = predict.settle_aliases(state, pm, won, aliases)
    assert aliases.evidence_count.iloc[0] == 1 and bool(state.alias_settled.iloc[0])
    _, aliases = predict.settle_aliases(state, pm, won, aliases)
    assert aliases.evidence_count.iloc[0] == 1, "a match counts once"


def test_settlement_disagreement_rejects_the_alias():
    res = identity.Resolution("cs2-vit-fal-2026-10-05", 0, learned=(("vitalityx", 9565),))
    legs = [{"market": "series", "listing_team_a": 1, "listing_team_b": 2}]
    state = predict.upsert_state(predict.empty_state(), MATCH, res, legs, NOW, LEAD)
    aliases = identity.learn(identity.empty_aliases(), res.learned, NOW)
    pm = _pm()
    pm.loc[pm.market_slug == "cs2-vit-fal-2026-10-05", "final"] = pd.Series([["0", "1"]], index=pm.index[:1])
    _, aliases = predict.settle_aliases(state, pm, pd.Series({2398739: 1}), aliases)
    assert aliases.status.tolist() == [identity.REJECTED]
