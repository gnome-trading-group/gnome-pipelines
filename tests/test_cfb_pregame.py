"""cfb_pregame_predictions: the column contract the research predictor writes, and the publisher's checks."""
import numpy as np
import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb import pregame
from gnome_pipelines.datasets import cfb

CONTRACT = [
    "game_id", "season", "week", "season_type", "kickoff", "home_id", "away_id", "home_team", "away_team",
    "neutral_site", "p_home", "p_home_lo90", "p_home_hi90", "home_qb_id", "away_qb_id", "home_qb_p", "away_qb_p",
    "home_qb_delta", "away_qb_delta", "home_rank", "away_rank", "fbs_teams", "home_newcomer", "away_newcomer", "model",
    "model_version", "qb_status_at", "generated_at",
]


def frame(p_home=(0.6, 0.62), generated=("2026-10-24T13:30:00Z", "2026-10-24T14:30:00Z")) -> pd.DataFrame:
    n = len(p_home)
    return pd.DataFrame({
        "game_id": [401858274] * n, "season": [2026] * n, "week": [8] * n, "season_type": ["regular"] * n,
        "kickoff": [pd.Timestamp("2026-10-24T16:00:00Z")] * n, "home_id": [2390] * n, "away_id": [221] * n,
        "home_team": ["Miami"] * n, "away_team": ["Pittsburgh"] * n, "neutral_site": [False] * n,
        "p_home": list(p_home), "p_home_lo90": [0.5] * n, "p_home_hi90": [0.7] * n, "home_qb_id": ["1"] * n,
        "away_qb_id": ["2"] * n, "home_qb_p": [0.8] * n, "away_qb_p": [np.nan] * n, "home_qb_delta": [0.0] * n,
        "away_qb_delta": [0.0] * n, "home_rank": [3] * n, "away_rank": [40] * n, "fbs_teams": [136] * n,
        "home_newcomer": [False] * n, "away_newcomer": [False] * n, "model": ["B0"] * n, "model_version": ["abc"] * n,
        "qb_status_at": [pd.Timestamp("2026-10-24T02:55:00Z")] * n,
        "generated_at": [pd.Timestamp(g) for g in generated],
    })


def test_the_spec_has_the_contract_columns():
    assert cfb.PREGAME_PREDICTIONS.schema.names == CONTRACT
    assert cfb.PREGAME_PREDICTIONS.key == ("game_id", "generated_at")
    assert cfb.PREGAME_PREDICTIONS in cfb.ALL


def test_publish_round_trips_every_refresh(tmp_path):
    path = tmp_path / "preds.parquet"
    frame().to_parquet(path)
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh"))
    assert pregame.publish(path, store).rows_written == 2
    loaded = store.load("cfb_pregame_predictions")
    assert sorted(loaded["p_home"]) == [0.6, 0.62], "one row per refresh, keyed by generated_at"


def test_publish_refuses_a_probability_outside_0_1(tmp_path):
    path = tmp_path / "preds.parquet"
    frame(p_home=(0.6, 1.2)).to_parquet(path)
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh"))
    with pytest.raises(ValueError, match="p_home"):
        pregame.publish(path, store)
    with pytest.raises(Exception):
        store.load("cfb_pregame_predictions")


def test_publish_refuses_missing_columns(tmp_path):
    path = tmp_path / "preds.parquet"
    frame().drop(columns=["home_rank"]).to_parquet(path)
    with pytest.raises(ValueError, match="home_rank"):
        pregame.publish(path, DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))


def test_publish_refuses_rows_without_a_key(tmp_path):
    path = tmp_path / "preds.parquet"
    f = frame()
    f.loc[1, "game_id"] = None
    f.to_parquet(path)
    with pytest.raises(ValueError, match="game_id"):
        pregame.publish(path, DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
