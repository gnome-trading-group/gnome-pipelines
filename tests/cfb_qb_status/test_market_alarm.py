from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.crosswalk import TeamIndex
from gnome_pipelines.cfb.qb_status.market_alarm import Alarm, alarms, kalshi_home_probs, missed_news

T = datetime(2026, 10, 24, 15, 0, tzinfo=timezone.utc)
GAMES = pd.DataFrame([{"game_id": 8, "season": 2026, "kickoff": pd.Timestamp("2026-10-24T23:30Z"),
                       "home_team": "Mississippi State", "home_id": 344, "away_team": "Oklahoma", "away_id": 201}])


def market(team, bid, ask):
    return {"ticker": f"KXNCAAFGAME-26OCT24OUMSST-{team[:4].upper()}", "yes_sub_title": team,
            "yes_bid_dollars": str(bid), "yes_ask_dollars": str(ask)}


def test_twin_markets_combine_to_home_prob():
    events = [{"event_ticker": "KXNCAAFGAME-26OCT24OUMSST", "title": "Oklahoma vs Mississippi St.",
               "markets": [market("Mississippi St.", 0.36, 0.37), market("Oklahoma", 0.63, 0.65)]}]
    p = kalshi_home_probs(events, GAMES, TeamIndex(GAMES))
    assert p[8] == pytest.approx((0.365 + 1 - 0.64) / 2)


def hist(*points):
    return pd.DataFrame([{"game_id": 8, "at": T + timedelta(minutes=m), "p_home": p} for m, p in points])


def test_move_of_016_logit_in_45min_alarms():
    a = alarms(hist((-45, 0.40), (0, 0.4392)), {}, T)
    assert [x.game_id for x in a] == [8] and a[0].move == pytest.approx(0.16, abs=0.005)


def test_slow_drift_over_3h_does_not_alarm():
    assert alarms(hist((-180, 0.40), (-90, 0.42), (-30, 0.43), (0, 0.44)), {}, T) == []


def test_recent_status_change_suppresses_alarm():
    assert alarms(hist((-45, 0.40), (0, 0.45)), {8: T - timedelta(hours=2)}, T) == []
    assert alarms(hist((-45, 0.40), (0, 0.45)), {8: T - timedelta(hours=7)}, T)


def test_alarm_without_change_after_30min_is_missed_news():
    a = [Alarm(8, T, 0.2)]
    assert missed_news(a, {}, T + timedelta(minutes=31)) == a
    assert missed_news(a, {8: T + timedelta(minutes=10)}, T + timedelta(minutes=31)) == []
    assert missed_news(a, {}, T + timedelta(minutes=20)) == []


def test_in_game_swings_never_alarm():
    kick = {8: T - timedelta(minutes=30)}
    assert alarms(hist((-45, 0.40), (0, 0.60)), {}, T, kickoffs=kick) == []
    later = {8: T + timedelta(hours=3)}
    assert alarms(hist((-45, 0.40), (0, 0.60)), {}, T, kickoffs=later)


def test_moves_more_than_two_days_before_kickoff_do_not_alarm():
    from gnome_pipelines.cfb.qb_status.market_alarm import HORIZON
    kick = {8: T + HORIZON + timedelta(hours=1)}
    assert alarms(hist((-45, 0.40), (0, 0.4392)), {}, T, kick) == []
    assert [x.game_id for x in alarms(hist((-45, 0.40), (0, 0.4392)), {}, T, {8: T + HORIZON - timedelta(hours=1)})] == [8]
