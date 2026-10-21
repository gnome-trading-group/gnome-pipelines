import gzip
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from bs4 import BeautifulSoup

from gnomepy_research.models.cs2.hltv import context_features as cf
from gnomepy_research.models.cs2.hltv.pit import assert_pit_consistent
from gnomepy_research.models.cs2.hltv.player_skill import PLAYER_SKILL_FEATURES, compute_player_skill_features
from gnome_pipelines.hltv_cs2.scraper import parse_match_detail_html
from gnomepy_research.models.cs2.win_probability.symmetry import build_swap_plan, swap_features

FIXTURES = Path(__file__).parent / "fixtures"


def _parsed():
    out = {"rows": [], "players": [], "veto": [], "h2h": [], "recent_form": []}
    for path in sorted(FIXTURES.glob("*.html.gz")):
        html = gzip.open(path, "rt", errors="ignore").read()
        result = parse_match_detail_html(BeautifulSoup(html, "lxml"), abs(hash(path.name)) % 10**7, stars=1)
        if result is None:
            continue
        for key in out:
            out[key].extend(result.get(key, []))
    return {k: pd.DataFrame(v) for k, v in out.items()}


@pytest.fixture(scope="module")
def parsed():
    frames = _parsed()
    hist = frames["rows"]
    hist["match_date"] = pd.to_datetime(hist["match_date"])
    frames["rows"] = hist
    return frames


def _synthetic_history(n_days=8, per_day=2, maps=3):
    rows = []
    mid = 5000
    for day in range(1, n_days + 1):
        for _ in range(per_day):
            mid += 1
            for pos in range(1, maps + 1):
                rows.append({
                    "match_id": mid,
                    "match_date": pd.Timestamp("2026-03-01") + pd.Timedelta(days=day),
                    "map_name": f"de_m{pos}",
                    "map_position_in_series": pos,
                    "team_a_id": mid % 5,
                    "team_b_id": (mid + 2) % 5,
                    "team_a_name": f"T{mid % 5}",
                    "team_b_name": f"T{(mid + 2) % 5}",
                    "team_a_won": pos % 2,
                    "team_a_score": 13,
                    "team_b_score": 9,
                    "team_a_lineup_ids": [mid % 5 * 10 + i for i in range(5)],
                    "team_b_lineup_ids": [(mid + 2) % 5 * 10 + i for i in range(5)],
                    "team_a_player_ids": [mid % 5 * 10 + i for i in range(5)],
                    "team_b_player_ids": [(mid + 2) % 5 * 10 + i for i in range(5)],
                })
    return pd.DataFrame(rows)


def _synthetic_lineups(hist):
    """cs2_match_lineups rows for every series, with team A's last player a stand-in every third match."""
    rows = []
    for r in hist.drop_duplicates("match_id").itertuples():
        for side, team in (("a", r.team_a_id), ("b", r.team_b_id)):
            ids = getattr(r, f"team_{side}_lineup_ids")
            for slot, pid in enumerate(ids):
                rows.append({"match_id": r.match_id, "team_id": team, "player_id": pid, "slot": slot,
                             "is_standin": side == "a" and slot == 4 and r.match_id % 3 == 0})
    return pd.DataFrame(rows)


# --- block shapes -----------------------------------------------------------

def test_every_block_is_keyed_one_row_per_map(parsed):
    hist, veto, h2h = parsed["rows"], parsed["veto"], parsed["h2h"]
    blocks = {
        "schedule": cf.compute_schedule_features(hist),
        "event": cf.compute_event_features(hist),
        "veto": cf.compute_veto_features(hist, veto),
        "page_h2h": cf.compute_page_h2h_features(hist, h2h),
    }
    for name, df in blocks.items():
        assert len(df) == len(hist), name
        assert {"match_id", "map_name"} <= set(df.columns), name
        assert not df.duplicated(["match_id", "map_name"]).any(), name


def test_declared_feature_lists_match_what_is_emitted(parsed):
    hist, veto, h2h = parsed["rows"], parsed["veto"], parsed["h2h"]
    for df, declared in (
        (cf.compute_schedule_features(hist), cf.SCHEDULE_FEATURES),
        (cf.compute_event_features(hist), cf.EVENT_FEATURES),
        (cf.compute_veto_features(hist, veto), cf.VETO_FEATURES),
        (cf.compute_page_h2h_features(hist, h2h), cf.PAGE_H2H_FEATURES),
    ):
        emitted = set(df.columns) - {"match_id", "map_name"}
        assert emitted == set(declared), emitted.symmetric_difference(declared)


# --- point-in-time ----------------------------------------------------------

def test_schedule_accumulator_is_point_in_time():
    assert_pit_consistent(_synthetic_history(), cf.ScheduleAccumulator, sample=4)


def test_player_skill_is_frozen_within_a_series():
    hist = _synthetic_history()
    out = compute_player_skill_features(hist, lineups=_synthetic_lineups(hist))
    assert (out.groupby("match_id")[PLAYER_SKILL_FEATURES].nunique() <= 1).all().all()


def test_player_skill_requires_announced_lineups():
    hist = _synthetic_history().drop(columns=["team_a_lineup_ids", "team_b_lineup_ids"])
    out = compute_player_skill_features(hist)
    assert out.empty, "post-match player lists must not be used as a fallback"


def test_page_h2h_excludes_same_day_meetings(parsed):
    hist = parsed["rows"]
    subject = hist.groupby("match_id").agg(d=("match_date", "min"),
                                           a=("team_a_name", "first"), b=("team_b_name", "first"))
    mid = subject.index[0]
    same_day = pd.DataFrame([{
        "match_id": mid, "h2h_date": subject.loc[mid, "d"],
        "team1_name": subject.loc[mid, "a"], "team2_name": subject.loc[mid, "b"],
        "team1_score": 13, "team2_score": 4, "team1_won": True,
        "map_name": "de_nuke", "match_date": subject.loc[mid, "d"], "event_name": "x",
    }])
    out = cf.compute_page_h2h_features(hist, same_day)
    assert (out.loc[out.match_id == mid, "page_h2h_maps"] == 0).all()
    assert (out.loc[out.match_id == mid, "page_h2h_never_met"] == 1).all()


def test_audit_flags_a_box_that_renders_as_of_fetch(parsed):
    hist = parsed["rows"]
    mid = hist.match_id.iloc[0]
    future = pd.DataFrame([{
        "match_id": mid,
        "h2h_date": hist.match_date.max() + pd.Timedelta(days=30),
    }])
    result = cf.audit_page_box_pit(hist, future, "h2h_date", label="synthetic")
    assert not result["pit_safe"]
    assert result["after_rows"] == 1


def test_audit_treats_same_day_as_safe_but_counts_it(parsed):
    hist = parsed["rows"]
    mid = hist.match_id.iloc[0]
    same = pd.DataFrame([{"match_id": mid,
                          "h2h_date": hist.loc[hist.match_id == mid, "match_date"].min()}])
    result = cf.audit_page_box_pit(hist, same, "h2h_date", label="synthetic")
    assert result["pit_safe"]
    assert result["same_day_rows"] == 1


# --- block semantics --------------------------------------------------------

def test_veto_pick_is_exact_and_deciders_are_null(parsed):
    hist, veto = parsed["rows"], parsed["veto"]
    out = cf.compute_veto_features(hist, veto).merge(
        hist[["match_id", "map_name", "is_decider"]], on=["match_id", "map_name"])
    picked = out["team_a_picked_map_exact"]
    assert picked.dropna().isin([0.0, 1.0]).all()
    assert out.loc[picked.isna(), "is_decider"].eq(1.0).all(), "only deciders may lack a picker"


def test_veto_is_symmetric_across_the_two_teams(parsed):
    hist, veto = parsed["rows"], parsed["veto"]
    out = cf.compute_veto_features(hist, veto)
    assert "team_a_bans_before_pick" in out.columns
    assert "team_b_bans_before_pick" in out.columns


def test_rest_days_are_nonnegative_and_grow_with_gaps():
    hist = _synthetic_history()
    out = cf.compute_schedule_features(hist).merge(
        hist[["match_id", "map_name", "match_date"]], on=["match_id", "map_name"])
    rest = out["team_a_rest_days"].dropna()
    assert (rest >= 0).all()


def test_own_h2h_rate_is_a_share_of_prior_meetings():
    hist = _synthetic_history()
    out = cf.compute_schedule_features(hist)
    rate = out["own_h2h_rate"].dropna()
    assert ((rate >= 0) & (rate <= 1)).all()
    assert (out.loc[out["own_h2h_maps"] == 0, "own_h2h_never_met"] == 1).all()


# --- symmetry ---------------------------------------------------------------

def test_new_features_all_have_a_mirror():
    names = (PLAYER_SKILL_FEATURES + cf.SCHEDULE_FEATURES
             + cf.EVENT_FEATURES + cf.VETO_FEATURES + cf.PAGE_H2H_FEATURES)
    plan = build_swap_plan(names)
    assert len(plan.names) == len(names)


def test_swapping_twice_is_the_identity():
    names = (PLAYER_SKILL_FEATURES + cf.SCHEDULE_FEATURES
             + cf.EVENT_FEATURES + cf.VETO_FEATURES + cf.PAGE_H2H_FEATURES)
    plan = build_swap_plan(names)
    rng = np.random.default_rng(0)
    x = rng.normal(size=(16, len(names)))
    np.testing.assert_allclose(swap_features(swap_features(x, plan), plan), x, atol=1e-12)


def test_veto_ban_features_are_not_silently_constant(parsed):
    """A ban filter that matches nothing yields a dense column of zeros, not NaN."""
    hist, veto = parsed["rows"], parsed["veto"]
    out = cf.compute_veto_features(hist, veto)
    for col in ("team_a_banned_first", "team_a_bans_before_pick", "team_b_bans_before_pick"):
        assert out[col].notna().any(), col
        assert out[col].nunique() > 1, f"{col} is constant — check the veto action vocabulary"


def test_veto_action_vocabulary_is_what_the_parser_emits(parsed):
    assert set(parsed["veto"]["action"].dropna().unique()) <= cf._VETO_ACTIONS
    assert cf._BAN_ACTION in set(parsed["veto"]["action"].dropna().unique())


def test_unknown_veto_action_is_reported(parsed, caplog):
    hist, veto = parsed["rows"], parsed["veto"].copy()
    veto.loc[veto.index[0], "action"] = "vetoed"
    with caplog.at_level("WARNING"):
        cf.compute_veto_features(hist, veto)
    assert "unrecognised veto actions" in caplog.text
