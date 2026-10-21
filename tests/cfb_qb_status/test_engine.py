import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.engine import RULE_VERSION, team_status
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_frame
from gnome_pipelines.datasets.qb_status import SNAPSHOTS

T0 = datetime(2026, 10, 21, 0, 0, tzinfo=timezone.utc)
CANDS = pd.DataFrame([
    {"player_id": "T", "passer_key": "T", "base": 0.80, "last_starter": True, "season_starter": True},
    {"player_id": "P", "passer_key": "P", "base": 0.20, "last_starter": False, "season_starter": False},
])


def ev(player, status, at, *, slot=None, source="official_report", state="auto", game=8, n=[0]):
    n[0] += 1
    return Evidence(evidence_id=f"e{n[0]}", source_type=source, source_id="s", source_org="SEC", first_seen_at=at,
                    team="Mississippi State", status=status, season=2026, team_id=344, player_id=player, game_id=game,
                    report_slot=slot, review_state=state)


def status(evidence, as_of=T0 + timedelta(days=5), cands=CANDS):
    df = team_status(8, 344, cands, evidence_frame(evidence), as_of)
    return dict(zip(df["player_id"], df["p_start"])), df


def test_no_evidence_returns_base_weights():
    p, df = status([])
    assert p == pytest.approx({"T": 0.80, "P": 0.20})
    assert set(df.columns) == set(SNAPSHOTS.schema.names) - {"computed_at"}
    assert (df["rule_version"] == RULE_VERSION).all()


def test_doubtful_starter_moves_mass_to_backup():
    p, _ = status([ev("T", "doubtful", T0, slot="Update 2")])
    assert p == pytest.approx({"T": 0.20, "P": 0.80})


def test_out_starter_gives_backup_one():
    p, _ = status([ev("T", "out", T0, slot="Game Day")])
    assert p == pytest.approx({"T": 0.0, "P": 1.0})


def test_later_official_slot_overrides_earlier():
    p, df = status([ev("T", "out", T0 + timedelta(hours=1), slot="Update 2"),
                    ev("T", "questionable", T0 + timedelta(hours=2), slot="Initial")])
    assert p["T"] == pytest.approx(0.0)
    assert df.loc[df.player_id == "T", "official_status"].item() == "out"


def test_newer_non_official_claim_beats_older_official():
    p, _ = status([ev("T", "questionable", T0, slot="Initial"),
                   ev("T", "out", T0 + timedelta(hours=3), source="news", state="confirmed")])
    assert p["T"] == pytest.approx(0.0)


def test_evidence_seen_after_as_of_is_ignored():
    p, _ = status([ev("T", "out", T0 + timedelta(days=2), slot="Game Day")], as_of=T0 + timedelta(days=1))
    assert p["T"] == pytest.approx(0.80)


def test_pending_evidence_is_ignored():
    p, _ = status([ev("T", "out", T0, state="pending", source="news")])
    assert p["T"] == pytest.approx(0.80)


def test_expected_starter_takes_090():
    p, _ = status([ev("P", "expected_starter", T0, source="news", state="confirmed")])
    assert p == pytest.approx({"P": 0.90, "T": 0.10})


def test_qb_change_flag_without_game_counts_for_next_game():
    p, _ = status([ev("P", "qb_change", T0, source="pbp_flag", game=None)])
    assert p["P"] > p["T"]


def test_single_candidate_out_leaves_mass_to_unknown():
    p, _ = status([ev("T", "out", T0, slot="Game Day")], cands=CANDS.iloc[:1].assign(base=1.0))
    assert p["T"] == pytest.approx(0.0) and p["unknown"] == pytest.approx(1.0)


def test_probabilities_sum_to_one():
    rng = random.Random(7)
    statuses = ["available", "probable", "questionable", "doubtful", "out", "game_time_decision",
                "left_game_injured", "not_starting", "expected_starter", "qb_change"]
    for _ in range(200):
        n = rng.randint(1, 4)
        w = [rng.random() for _ in range(n)]
        cands = pd.DataFrame({"player_id": [f"q{i}" for i in range(n)], "passer_key": [f"q{i}" for i in range(n)],
                              "base": [x / sum(w) for x in w], "last_starter": [i == 0 for i in range(n)],
                              "season_starter": [i == 0 for i in range(n)]})
        evs = [ev(f"q{rng.randrange(n)}", rng.choice(statuses), T0 + timedelta(minutes=rng.randrange(600)),
                  slot=rng.choice(["Initial", "Update 1", "Update 2", "Game Day"]),
                  source=rng.choice(["official_report", "news", "pbp_flag"]), state="auto")
               for _ in range(rng.randint(0, 6))]
        p, _ = status(evs, cands=cands)
        assert sum(p.values()) == pytest.approx(1.0)
        assert all(0.0 <= v <= 1.0 for v in p.values())


def test_flags_from_before_the_previous_kickoff_are_ignored():
    old = ev("P", "qb_change", T0 - timedelta(days=28), source="pbp_flag", game=None)
    p, _ = status([old], as_of=T0 + timedelta(days=1))
    assert p["P"] > p["T"]
    df = team_status(8, 344, CANDS, evidence_frame([old]), T0 + timedelta(days=1), since=T0 - timedelta(days=6))
    p = dict(zip(df["player_id"], df["p_start"]))
    assert p == pytest.approx({"T": 0.80, "P": 0.20})


def test_out_backup_gets_nothing_and_healthy_starter_gets_the_rest():
    p, _ = status([ev("P", "out", T0, slot="Game Day")])
    assert p == pytest.approx({"T": 1.0, "P": 0.0})


def test_questionable_starter_with_out_backup_leaves_rest_unknown():
    p, _ = status([ev("T", "questionable", T0, slot="Update 1"), ev("P", "out", T0, slot="Game Day")])
    assert p["T"] == pytest.approx(0.5) and p["P"] == 0.0 and p["unknown"] == pytest.approx(0.5)


def test_evidence_only_players_never_hand_mass_to_an_out_player():
    empty = CANDS.iloc[0:0]
    p, _ = status([ev("A1", "out", T0, slot="Initial"), ev("A2", "questionable", T0, slot="Initial")], cands=empty)
    assert p["A1"] == 0.0 and p["A2"] > 0.0
