from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.review import decide, is_json_post, pending, safe_url
from gnome_pipelines.cfb.qb_status.store import StatusStore

T0 = datetime(2026, 10, 22, 13, 0, tzinfo=timezone.utc)


@pytest.fixture
def store(tmp_path):
    s = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path)))
    s.add_evidence([Evidence(evidence_id="n1", source_type="news", source_id="g1", source_org="Example Ledger",
                             first_seen_at=T0, team="Mississippi State", status="doubtful", season=2026, team_id=344,
                             player_id="9000001", game_id=8, paraphrase="Stone doubtful", review_state="pending")])
    return s


def test_confirm_writes_manual_confirmed_copy(store):
    ev = decide(store, "n1", "confirm", "user", now=T0 + timedelta(minutes=5))
    assert (ev.source_type, ev.source_id, ev.review_state, ev.status) == ("manual", "n1", "confirmed", "doubtful")
    assert ev.reviewed_at == T0 + timedelta(minutes=5) and ev.first_seen_at == T0 + timedelta(minutes=5)
    assert pending(store).empty


def test_reject_removes_from_pending(store):
    ev = decide(store, "n1", "reject", "user", now=T0)
    assert ev.review_state == "rejected" and pending(store).empty


def test_edit_records_new_status(store):
    ev = decide(store, "n1", "edit", "user", status="out", now=T0)
    assert (ev.status, ev.review_state) == ("out", "confirmed")


def test_original_row_unchanged(store):
    decide(store, "n1", "confirm", "user", now=T0)
    original = store.evidence().set_index("evidence_id").loc["n1"]
    assert original["review_state"] == "pending" and original["status"] == "doubtful"


def test_pending_lists_undecided_items(store):
    assert pending(store)["evidence_id"].tolist() == ["n1"]


def test_unknown_decision_is_refused(store):
    with pytest.raises(ValueError):
        decide(store, "n1", "maybe", "user", now=T0)


def test_confirm_can_fill_missing_ids(store):
    ev = decide(store, "n1", "confirm", "user", player_id="9000009", game_id=9, team_id=345, now=T0)
    assert (ev.player_id, ev.game_id, ev.team_id) == ("9000009", 9, 345)


def test_only_web_links_are_kept_and_posts_must_be_json():
    assert safe_url("https://example.com/a") == "https://example.com/a"
    assert safe_url("javascript:alert(1)") is None
    assert is_json_post({"Content-Type": "application/json; charset=utf-8"})
    assert not is_json_post({"Content-Type": "text/plain"})


def test_pending_fills_missing_names_from_the_players_table(store):
    from gnome_pipelines.datasets.qb_status import PLAYERS
    store.put_players(pd.DataFrame([{"season": 2026, "player_id": "9000001", "team_id": 344, "team": "Mississippi State",
                                     "first_name": "Avery", "last_name": "Stone", "jersey": "1", "position": "QB",
                                     "passer_key": "a.stone", "aliases": ["Avery Stone"]}], columns=PLAYERS.schema.names))
    assert pending(store)["player_raw"].tolist() == ["Avery Stone"]
