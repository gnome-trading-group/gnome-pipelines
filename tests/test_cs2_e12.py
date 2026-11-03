"""The e12 pipelines: the fit publishes only on fresh inputs and a plausible fit, at a Monday cutoff; the blend publishes its
weights. The fit itself is replaced by a stand-in (models/cs2/e12 is tested in gnomepy-research)."""
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest
from gnomepy.data import ArtifactStore, CatalogConfig
from gnomepy.data.datasets import InputRecord
from gnomepy_research.models.cs2.e12.blend import MarketBlend
from gnomepy_research.models.cs2.e12.model import E12Model, TeamState
from gnomepy_research.models.cs2.e12.skill import SkillFit

from gnome_pipelines.cs2_e12 import pipeline as mod

CUTOFF = pd.Timestamp("2026-11-02")


def _model(cutoff, players=3000, mm_players=2000) -> E12Model:
    fit = SkillFit(np.arange(1, 11), np.zeros(10), np.full(10, 20), np.zeros(12), {"players": players})
    return E12Model(cutoff, fit, fit, {1: TeamState(five_hltv=(1, 2, 3, 4, 5))}, {1: 0.1},
                    {"hltv": {"players": players}, "faceit": {"players": players}, "mm_players": mm_players})


class _Tables:
    def __init__(self, hltv_age_days=1.0, faceit_age_days=1.0):
        self.newest = {"match_history": CUTOFF - pd.Timedelta(days=hltv_age_days),
                       "faceit_history_matches": CUTOFF - pd.Timedelta(days=faceit_age_days)}

    def load(self, name, before=None, columns=None):
        t = self.newest[name]
        if name == "match_history":
            return pd.DataFrame({"match_date": [t]})
        return pd.DataFrame({"finished_at": [int(t.tz_localize("UTC").timestamp())]})


@pytest.fixture
def local(tmp_path, monkeypatch):
    config = CatalogConfig(local_warehouse=tmp_path, artifact_root=str(tmp_path / "files"))
    monkeypatch.setattr(mod, "artifact_store", lambda: ArtifactStore(config))
    # A table read twice (freshness check, then the fit) leaves two input records for one dataset.
    loaded = [InputRecord("cs2_match_history", datetime(2026, 11, 2, tzinfo=timezone.utc), 1, 10, "a"),
              InputRecord("cs2_match_history", datetime(2026, 11, 2, 1, tzinfo=timezone.utc), 2, 11, "b")]
    monkeypatch.setattr(mod, "dataset_store", lambda: type("Store", (), {"loaded": loaded})())
    return config


def _run(monkeypatch, tables, model=_model, **params):
    seen = {}
    monkeypatch.setattr(mod, "DatasetTables", lambda store: tables)

    def build(tables_, cutoff):
        seen["cutoff"] = cutoff
        return model(cutoff)
    monkeypatch.setattr(mod.E12Model, "build", staticmethod(build))
    out = mod.CS2E12FitPipeline().run(mod.CS2E12FitParams(cutoff="2026-11-02", **params)).outputs
    return seen, out


def test_a_fit_on_fresh_inputs_is_published_at_its_monday(local, monkeypatch):
    seen, out = _run(monkeypatch, _Tables())
    assert seen["cutoff"] == CUTOFF and out["cutoff"] == "2026-11-02"
    store = ArtifactStore(local)
    published = E12Model.load(store.resolve(store.latest(mod.MODEL_TYPE, mod.NAME)))
    assert published.cutoff == CUTOFF and published.mm == {1: 0.1}
    assert store.inputs(store.latest(mod.MODEL_TYPE, mod.NAME)).rows_hash.tolist() == ["b"]


@pytest.mark.parametrize("tables,match", [(_Tables(hltv_age_days=5), "HLTV map"), (_Tables(faceit_age_days=6), "FACEIT game")])
def test_stale_inputs_are_not_fitted(local, monkeypatch, tables, match):
    with pytest.raises(RuntimeError, match=match):
        _run(monkeypatch, tables)


def test_an_implausibly_small_fit_is_not_published(local, monkeypatch):
    with pytest.raises(RuntimeError, match="matchmaking"):
        _run(monkeypatch, _Tables(), model=lambda c: _model(c, mm_players=10))
    with pytest.raises(LookupError):
        ArtifactStore(local).latest(mod.MODEL_TYPE, mod.NAME)


def test_the_cutoff_must_be_a_monday(local, monkeypatch):
    monkeypatch.setattr(mod, "DatasetTables", lambda store: _Tables())
    with pytest.raises(ValueError, match="not a Monday"):
        mod.CS2E12FitPipeline().run(mod.CS2E12FitParams(cutoff="2026-11-03"))


def test_default_cutoff_is_this_weeks_monday():
    assert mod.monday(pd.Timestamp("2026-11-05 17:30")) == CUTOFF


def test_blend_publishes_its_weights(local):
    out = mod.CS2E12BlendPipeline().run(mod.CS2E12BlendParams(market=0.612, model=0.513, intercept=0.009, source="prereg")).outputs
    store = ArtifactStore(local)
    assert MarketBlend.load(store.resolve(store.latest(mod.BLEND_TYPE, mod.BLEND_NAME))) == MarketBlend(0.612, 0.513, 0.009)
    assert out["market"] == 0.612
