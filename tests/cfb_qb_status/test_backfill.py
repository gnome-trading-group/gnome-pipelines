import json
from datetime import datetime, timezone

import pandas as pd

from gnome_pipelines.cfb.qb_status.backfill import capture_evidence
from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex, TeamIndex

GAMES = pd.DataFrame([
    {"game_id": 8, "season": 2026, "week": 8, "kickoff": pd.Timestamp("2026-10-24T23:30Z"),
     "home_team": "Mississippi State", "home_id": 344, "away_team": "Oklahoma", "away_id": 201},
])
ROSTERS = pd.DataFrame([
    {"season": 2026, "player_id": "9000001", "first_name": "Avery", "last_name": "Stone", "team": "Mississippi State",
     "position": "QB", "jersey": 1.0, "team_id": 344},
])
PASSERS = pd.DataFrame(columns=["season", "game_id", "offense", "passer", "passer_key", "play_seq"])


def row(**over):
    base = {"Game Day": "", "Initial": "Questionable", "Number": "#1", "Opponent": "vs. Oklahoma",
            "OpponentDisplay": "vs. Oklahoma", "Player": "Avery Stone", "Team": "Mississippi St.",
            "TeamDisplay": "Mississippi State", "Update 1": "", "Update 2": "",
            "Week": "Sat, 24 Oct 2026 00:00:00 GMT", "WeekNum": 8}
    base.update(over)
    return base


def write(dirpath, stamp, rows):
    (dirpath / "SEC").mkdir(parents=True, exist_ok=True)
    body = {"fetched_at": stamp, "sha256": stamp, "data": rows,
            "report_days": ["Initial", "Update 1", "Update 2", "Game Day"]}
    (dirpath / "SEC" / f"{stamp}.json").write_text(json.dumps(body))


def load(tmp_path):
    return capture_evidence(tmp_path, TeamIndex(GAMES), {2026: PlayerIndex(ROSTERS, PASSERS, 2026)}, GAMES)


def test_first_sighting_time_is_kept(tmp_path):
    write(tmp_path, "2026-10-22T001339Z", [row()])
    write(tmp_path, "2026-10-23T004339Z", [row(**{"Update 1": "Doubtful"})])
    ev = {e.report_slot: e for e in load(tmp_path)}
    assert ev["Initial"].first_seen_at == datetime(2026, 10, 22, 0, 13, 39, tzinfo=timezone.utc)
    assert ev["Update 1"].first_seen_at == datetime(2026, 10, 23, 0, 43, 39, tzinfo=timezone.utc)
    assert not ev["Initial"].backfilled


def test_duplicate_rows_across_files_emit_once(tmp_path):
    write(tmp_path, "2026-10-22T001339Z", [row()])
    write(tmp_path, "2026-10-22T004339Z", [row()])
    assert len(load(tmp_path)) == 1


def test_rows_in_the_first_capture_carry_an_estimated_release(tmp_path):
    write(tmp_path, "2026-10-22T001339Z", [row()])
    write(tmp_path, "2026-10-23T004339Z", [row(**{"Update 1": "Doubtful"})])
    ev = {e.report_slot: e for e in load(tmp_path)}
    assert ev["Initial"].estimated_release_at == datetime(2026, 10, 22, 0, 0, tzinfo=timezone.utc)
    assert ev["Update 1"].estimated_release_at is None
