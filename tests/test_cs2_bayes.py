"""The Bayesian series pipelines: the fit publishes only a converged posterior, thinned; the blend publishes its weights.
The NUTS fit itself is replaced by a stand-in (models/cs2/bayes is tested in gnomepy-research)."""
import json

import numpy as np
import pandas as pd
import pytest
from gnomepy.data import ArtifactStore, CatalogConfig, DatasetStore
from gnomepy_research.models.cs2.hltv.bayes_series import SeriesPosterior

from gnome_pipelines.cs2_bayes import pipeline as mod
from gnome_pipelines.datasets import cs2

S = 40


def _posterior(as_of="2026-10-19"):
    return SeriesPosterior(
        player_ids=np.arange(1, 11), theta=np.zeros((S, 10)), tau=0.055, b=np.full(S, 0.3), sigma0=np.full(S, 0.5),
        strength_mu=-4.0, strength_sd=1.0, rho=np.zeros((S, 7)), sigma_m=np.full(S, 0.37), sigma_s=np.full(S, 0.15),
        home_team_ids=np.array([1]), home_region=np.array([1]), tm_team_ids=np.array([1]), tm_maps=np.array(["de_nuke"]),
        beta=np.zeros((S, 1)), plays_team_ids=np.array([1]), plays_maps=np.array(["de_nuke"]), plays_counts=np.array([3]),
        a_new=np.full(S, -0.35), sigma_new=np.full(S, 0.4), career_ids=np.arange(1, 11), as_of=as_of)


GOOD = {"divergences": 0, "max_rhat": 1.03, "worst_rhat_param": "sigma_s", "skill_max_rhat": 1.01, "maps": 10,
        "players": 10, "gamma": 0.28, "a_new": -0.35}


@pytest.fixture
def local(tmp_path, monkeypatch):
    config = CatalogConfig(local_warehouse=tmp_path, artifact_root=str(tmp_path / "files"))
    monkeypatch.setattr(mod, "dataset_store", lambda: DatasetStore(config))
    monkeypatch.setattr(mod, "artifact_store", lambda: ArtifactStore(config))
    monkeypatch.setattr(mod, "prepare_maps", lambda history, lineups: history)
    store = DatasetStore(config)
    for spec in (cs2.MATCH_HISTORY, cs2.MATCH_LINEUPS, cs2.PLAYER_MAP_STATS, cs2.TEAM_RANKINGS):
        store.create(spec)
    return config


def _fit(monkeypatch, diag):
    seen = {}

    def fake(maps, stats, ranks, as_of, cfg):
        seen["as_of"] = as_of
        return _posterior(f"{as_of:%Y-%m-%d}"), dict(diag)
    monkeypatch.setattr(mod, "fit_month", fake)
    return seen


def test_a_converged_fit_is_published_thinned_and_dated(local, monkeypatch):
    seen = _fit(monkeypatch, GOOD)
    out = mod.CS2BayesFitPipeline().run(mod.CS2BayesFitParams(as_of="2026-10-19", keep_draws=10)).outputs
    assert seen["as_of"] == pd.Timestamp("2026-10-19") and out["as_of"] == "2026-10-19"
    store = ArtifactStore(local)
    ref = store.latest(mod.POSTERIOR_TYPE, mod.NAME)
    published = SeriesPosterior.load(store.resolve(ref))
    assert published.n_draws == 10 and published.theta.dtype == np.float32 and published.as_of == "2026-10-19"


@pytest.mark.parametrize("bad", [{"divergences": 3}, {"max_rhat": 1.4}, {"skill_max_rhat": 1.2}])
def test_a_fit_that_did_not_converge_is_not_published(local, monkeypatch, bad):
    _fit(monkeypatch, {**GOOD, **bad})
    with pytest.raises(RuntimeError, match="did not converge"):
        mod.CS2BayesFitPipeline().run(mod.CS2BayesFitParams(as_of="2026-10-19"))
    with pytest.raises(KeyError):
        ArtifactStore(local).latest(mod.POSTERIOR_TYPE, mod.NAME)


def test_the_blend_weights_are_published_as_json(local):
    mod.CS2BayesBlendPipeline().run(mod.CS2BayesBlendParams(w_bayes=0.48, w_xgb=0.57, source="research job r1"))
    store = ArtifactStore(local)
    blend = json.loads(open(store.resolve(store.latest(mod.BLEND_TYPE, mod.NAME))).read())
    assert blend == {"w_bayes": 0.48, "w_xgb": 0.57}


def test_the_fit_is_weekly_and_sized_for_four_chains():
    assert mod.CS2BayesFitPipeline.schedules[0].expression == "cron(0 6 ? * MON *)"
    assert mod.CS2BayesFitPipeline.resources.cpu >= 4096
