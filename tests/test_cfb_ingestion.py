from __future__ import annotations

import pandas as pd
import pytest

from gnome_pipelines.cfb.efficiency import (
    MIN_PLAYS,
    garbage_time,
    successful,
    team_game_efficiency,
)
from gnome_pipelines.cfb.ingest import (
    american_to_prob,
    drive_outcome,
    passer_key,
    passer_name,
)


class TestDriveOutcome:
    def test_explicit_label_beats_misrecorded_score_delta(self):
        # CFBD sometimes records a TD drive's start score after the TD: 0 points on a TD drive.
        assert drive_outcome("TD", 0, 0) == "TD"
        assert drive_outcome("PUNT", 7, 0) == "PUNT"

    def test_score_delta_classifies_unlabelled_drives(self):
        assert drive_outcome("Uncategorized", 7, 0) == "TD"
        assert drive_outcome("Uncategorized", 0, 6) == "DEF_TD"
        assert drive_outcome("Uncategorized", 3, 0) == "FG"
        assert drive_outcome("Uncategorized", 0, 2) == "SAFETY"
        assert drive_outcome("KICKOFF", 0, 0) == "OTHER"

    @pytest.mark.parametrize("label,expected", [
        ("INT TD", "DEF_TD"), ("PUNT RETURN TD", "DEF_TD"), ("SF", "SAFETY"),
        ("END OF 4TH QUARTER", "END_HALF"), ("MISSED FG", "MISSED_FG"), ("FUMBLE", "TURNOVER"),
    ])
    def test_label_mapping(self, label, expected):
        assert drive_outcome(label, 0, 0) == expected


class TestPasser:
    @pytest.mark.parametrize("play_type,text,key", [
        ("Pass Reception", "Payton Thorne pass complete to Jay Fair for 2 yds", "p.thorne"),
        ("Pass Reception", "(07:39) No Huddle-Shotgun #17 J.Kohl pass complete short middle", "j.kohl"),
        ("Passing Touchdown", "Jasaiah Gathings 21 Yd pass from Joe Fagnano (Chris Freeman Kick)", "j.fagnano"),
        ("Pass Reception", "Shotgun #2 J.Fagnano pass complete short left", "j.fagnano"),
        ("Sack", "Payton Thorne sacked by Xavian Sorey Jr. for a loss of 2 yards", "p.thorne"),
        ("Pass Reception", "(01:38) Shotgun #16 R.Tisdale Jr. pass complete short left", "r.tisdale"),
    ])
    def test_all_text_formats_map_to_one_key(self, play_type, text, key):
        assert passer_key(passer_name(play_type, text)) == key

    def test_non_pass_plays_have_no_passer(self):
        assert passer_name("Rush", "Joe Fagnano run for 5 yds") is None


def test_american_odds_to_probability():
    assert american_to_prob(-150) == pytest.approx(0.6)
    assert american_to_prob(150) == pytest.approx(0.4)


class TestEfficiency:
    def test_garbage_time_thresholds_by_quarter(self):
        period = pd.Series([1, 2, 2, 3, 3, 4, 4])
        lead = pd.Series([50, 38, 39, 28, -29, 22, -23])
        assert garbage_time(period, lead).tolist() == [False, False, True, False, True, False, True]

    def test_success_thresholds_by_down(self):
        down = pd.Series([1, 1, 2, 2, 3, 3])
        dist = pd.Series([10, 10, 10, 10, 5, 5])
        gained = pd.Series([5, 4, 7, 6, 5, 4])
        assert successful(down, dist, gained).tolist() == [True, False, True, False, True, False]

    def test_one_row_per_offense_with_mirrored_venue_and_postgame_availability(self):
        kickoff = pd.Timestamp("2024-09-07 16:00", tz="UTC")
        games = pd.DataFrame([{
            "game_id": 1, "season": 2024, "week": 2, "season_type": "regular", "kickoff": kickoff,
            "neutral_site": False, "home_team": "A", "home_id": 10, "away_team": "B", "away_id": 20,
            "home_classification": "fbs", "away_classification": "fcs",
        }])
        rows = []
        for team, other, ppa in (("A", "B", 0.3), ("B", "A", -0.1)):
            for i in range(MIN_PLAYS):
                rows.append({"game_id": 1, "offense": team, "defense": other, "period": 1, "offense_score": 0,
                             "defense_score": 0, "ppa": ppa, "play_type": "Rush", "down": 1, "distance": 10,
                             "yards_gained": 6})
        # A garbage-time play that must be ignored.
        rows.append({**rows[0], "period": 4, "offense_score": 50, "ppa": 5.0})
        eff = team_game_efficiency(pd.DataFrame(rows), games).set_index("offense")
        assert eff.loc["A", "off_ppa"] == pytest.approx(0.3)
        assert eff.loc["A", "plays"] == MIN_PLAYS
        assert eff.loc["A", "venue"] == 1 and eff.loc["B", "venue"] == -1
        assert eff.loc["A", "opponent_classification"] == "fcs"
        assert eff.loc["A", "success_rate"] == 1.0
        assert bool(eff.loc["A", "usable"])
        assert (eff["available_at"] > kickoff).all()
