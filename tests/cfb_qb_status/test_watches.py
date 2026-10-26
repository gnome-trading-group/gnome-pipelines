from datetime import datetime, timedelta, timezone

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.watches import opens_watch, update_watches, watched_teams
from gnome_pipelines.datasets.qb_status import WATCHES

T0 = datetime(2026, 10, 21, 22, 0, tzinfo=timezone.utc)
KICK = {8: datetime(2026, 10, 24, 23, 30, tzinfo=timezone.utc)}
EMPTY = pd.DataFrame(columns=WATCHES.schema.names)


def ev(status, at=T0, source="official_report", slot="Initial", player="T", eid=None):
    return Evidence(evidence_id=eid or f"{status}-{at.isoformat()}-{slot}", source_type=source, source_id="s",
                    source_org="SEC", first_seen_at=at, team="Mississippi State", status=status, season=2026,
                    team_id=344, player_id=player, game_id=8, report_slot=slot)


def test_questionable_starter_opens_watch():
    assert opens_watch(ev("questionable"), {"T", "P"})
    w = update_watches(EMPTY, [ev("questionable")], KICK, T0)
    assert list(w.columns) == WATCHES.schema.names
    assert watched_teams(w, T0) == {344}


def test_available_does_not_open():
    assert not opens_watch(ev("available"), {"T"})
    assert update_watches(EMPTY, [ev("available")], KICK, T0).empty


def test_new_evidence_extends_expiry():
    w = update_watches(EMPTY, [ev("questionable")], KICK, T0)
    later = T0 + timedelta(hours=20)
    w = update_watches(w, [ev("available", at=later, slot="Update 1")], KICK, later)
    assert pd.Timestamp(w["expires_at"].iloc[0]) == pd.Timestamp(later + timedelta(hours=24))


def test_closes_at_kickoff():
    w = update_watches(EMPTY, [ev("questionable")], KICK, T0)
    after = KICK[8] + timedelta(minutes=1)
    w = update_watches(w, [], KICK, after)
    assert w["close_reason"].iloc[0] == "kickoff" and watched_teams(w, after) == set()


def test_expires_after_24h_quiet():
    w = update_watches(EMPTY, [ev("questionable")], KICK, T0)
    w = update_watches(w, [], {}, T0 + timedelta(hours=25))
    assert w["close_reason"].iloc[0] == "expired"


def test_game_day_official_closes():
    w = update_watches(EMPTY, [ev("questionable")], KICK, T0)
    gd = T0 + timedelta(hours=20)
    w = update_watches(w, [ev("out", at=gd, slot="Game Day")], KICK, gd)
    assert w["close_reason"].iloc[0] == "official_game_day" and len(w) == 1


def test_game_day_official_never_opens_a_watch():
    assert not opens_watch(ev("out", slot="Game Day"), {"T"})


def test_market_alarm_and_pbp_open():
    assert opens_watch(ev("market_alarm", source="market_alarm", player=None), set())
    assert opens_watch(ev("left_game_injured", source="pbp_flag"), set())


def test_a_qb_change_or_no_clear_favourite_is_unsettled():
    from gnome_pipelines.cfb.qb_status.watches import UNSETTLED_TOP, is_unsettled
    assert is_unsettled(0.95, qb_change=True)
    assert is_unsettled(UNSETTLED_TOP - 0.01, qb_change=False)
    assert not is_unsettled(UNSETTLED_TOP, qb_change=False)


def test_an_unsettled_starter_opens_a_watch_inside_the_window():
    from gnome_pipelines.cfb.qb_status.watches import unsettled_watches
    now = KICK[8] - timedelta(days=2)
    w = unsettled_watches(EMPTY, {(8, 344)}, KICK, now)
    assert watched_teams(w, now) == {344}
    assert w["reason"].tolist() == ["unsettled"]
    assert list(w.columns) == WATCHES.schema.names


def test_a_far_game_opens_nothing():
    from gnome_pipelines.cfb.qb_status.watches import unsettled_watches
    assert unsettled_watches(EMPTY, set(), KICK, KICK[8] - timedelta(days=2)).empty
    assert unsettled_watches(EMPTY, {(8, 344)}, KICK, KICK[8] - timedelta(days=5)).empty


def test_an_open_watch_is_not_duplicated():
    from gnome_pipelines.cfb.qb_status.watches import unsettled_watches
    now = KICK[8] - timedelta(days=2)
    w = unsettled_watches(EMPTY, {(8, 344)}, KICK, now)
    assert len(unsettled_watches(w, {(8, 344)}, KICK, now + timedelta(hours=1))) == 1


def test_unsettled_sits_between_a_new_starter_and_an_established_one():
    from gnome_pipelines.cfb.qb_status.candidates import HOLD_RATE, KEEP_RATE
    from gnome_pipelines.cfb.qb_status.watches import UNSETTLED_TOP
    assert max(HOLD_RATE.values()) < UNSETTLED_TOP < min(KEEP_RATE.values())
