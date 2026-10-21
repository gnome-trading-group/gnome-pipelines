from datetime import datetime, timezone

from gnome_pipelines.cfb.qb_status.pbp_flags import game_flags, live_games, passer_last_name

SEEN = datetime(2026, 10, 18, 1, 0, tzinfo=timezone.utc)
STARTERS = {"Mississippi State": "stone", "LSU": "holt"}


def play(text, period=1, home=0, away=0):
    return {"text": text, "period": {"number": period}, "homeScore": home, "awayScore": away}


def drive(team, *plays):
    return {"team": {"location": team, "displayName": team}, "plays": list(plays)}


def summary(*drives):
    return {"header": {"id": "401999", "season": {"year": 2026}, "competitions": [{"competitors": [
        {"homeAway": "home", "team": {"location": "LSU", "id": "99"}},
        {"homeAway": "away", "team": {"location": "Mississippi State", "id": "344"}}]}]},
            "drives": {"previous": list(drives)}}


NORMAL = [drive("Mississippi State", play("Avery Stone pass complete to J. Doe for 12 yds")),
          drive("LSU", play("Garrett Holt pass incomplete")),
          drive("Mississippi State", play("Avery Stone sacked by A. B for a loss of 5 yards"))]


def test_passer_names_parse_both_espn_formats():
    assert passer_last_name("(10:22) No Huddle-Shotgun #12 R.Quill pass incomplete deep right") == "quill"
    assert passer_last_name("Avery Stone pass complete to J. Doe for 12 yds") == "stone"
    assert passer_last_name("A.Stone sacked by X for a loss of 5 yards") == "stone"
    assert passer_last_name("Avery Stone run for 4 yds") is None


def test_no_flags_in_normal_game():
    assert game_flags(summary(*NORMAL), STARTERS, SEEN) == []


def test_backup_dropback_raises_qb_change():
    s = summary(*NORMAL, drive("Mississippi State", play("Blake Rowe pass complete to J. Doe for 3 yds", 2)))
    flags = game_flags(s, STARTERS, SEEN)
    assert [(f.status, f.player_raw, f.team) for f in flags] == [("qb_change", "Blake Rowe", "Mississippi State")]
    assert flags[0].game_id is None and flags[0].source_type == "pbp_flag" and flags[0].source_id == "401999"


def test_injury_text_raises_left_game_injured():
    s = summary(drive("Mississippi State", play("Avery Stone pass incomplete, Avery Stone injured on the play")))
    flags = game_flags(s, STARTERS, SEEN)
    assert [f.status for f in flags] == ["left_game_injured"]


def test_two_possessions_without_starter_raise_flag():
    s = summary(*NORMAL, drive("Mississippi State", play("J. Back run for 3 yds", 2)),
                drive("LSU", play("Garrett Holt pass complete", 2)),
                drive("Mississippi State", play("J. Back run for 2 yds", 2)))
    assert [f.status for f in game_flags(s, STARTERS, SEEN)] == ["left_game_injured"]


def test_flags_are_emitted_once_per_game():
    s = summary(*NORMAL, drive("Mississippi State", play("Blake Rowe pass complete", 2),
                               play("Blake Rowe pass incomplete", 2)))
    a = game_flags(s, STARTERS, SEEN)
    assert len(a) == 1
    assert [f.evidence_id for f in game_flags(s, STARTERS, SEEN)] == [f.evidence_id for f in a]


def test_garbage_time_backup_is_ignored():
    s = summary(*NORMAL, drive("Mississippi State", play("Blake Rowe pass complete", 4, home=45, away=10)))
    assert game_flags(s, STARTERS, SEEN) == []


def test_live_games_lists_in_progress_events():
    board = {"events": [{"id": "1", "competitions": [{"status": {"type": {"state": "in"}}}]},
                        {"id": "2", "competitions": [{"status": {"type": {"state": "pre"}}}]}]}
    assert live_games(board) == ["1"]
