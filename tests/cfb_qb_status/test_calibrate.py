import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.calibrate import rates, transitions
from gnome_pipelines.cfb.qb_status.candidates import start_state


def test_start_state_reads_streak_season_starter_and_games_missed():
    assert start_state(["A", "A", "B"]) == ("B", 1, "A", 1)
    assert start_state(["A", "B"]) == ("B", 1, "B", 0)
    assert start_state(["A", "A", "A"]) == ("A", 3, "A", 0)


def test_transitions_and_rates_follow_who_started_next():
    t = transitions(["A", "A", "B", "A"])
    assert t[["streak", "missed", "next_is_last", "next_is_season"]].values.tolist() == [
        [1, 0, True, True], [2, 0, False, False], [1, 1, False, True]]
    r = rates(pd.concat([t, transitions(["C", "C", "C"])]))
    assert r["KEEP_RATE"][1] == pytest.approx(1.0) and r["KEEP_RATE"][2] == pytest.approx(0.5)
    assert r["RETURN_RATE"][1] == pytest.approx(1.0) and r["HOLD_RATE"][1] == pytest.approx(0.0)
