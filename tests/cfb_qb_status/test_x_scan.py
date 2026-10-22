from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.x_scan import (QUERY_LIMIT, Registry, XBudget, lookback_query, posts_to_evidence,
                                                   registry_frame, sweep_queries, watch_queries)
from gnome_pipelines.cfb.qb_status.x_source import XSource
from gnome_pipelines.datasets.qb_status import SOURCES

T0 = datetime(2026, 10, 22, 18, 0, tzinfo=timezone.utc)
CSV_ROWS = [
    {"platform": "x", "handle": "NatOne", "teams": "", "kind": "national", "confidence": "normal"},
    {"platform": "x", "handle": "TestvilleBeat", "teams": "Testville State", "kind": "beat", "confidence": "normal"},
    {"platform": "x", "handle": "TestvilleSite", "teams": "Testville State", "kind": "outlet", "confidence": "low"},
    {"platform": "x", "handle": "ExampleBeat", "teams": "Example Tech", "kind": "beat", "confidence": "normal"},
]
QBS = {"Testville State": [("Avery", "Stone"), ("Blake", "Rowe")], "Example Tech": [("Casey", "Vale")],
       "Nowhere A&M": [("Dana", "Quill")]}


@pytest.fixture
def registry():
    return Registry(registry_frame(pd.DataFrame(CSV_ROWS), vetted_by="user", now=T0))


def test_registry_frame_matches_the_sources_table(registry):
    df = registry_frame(pd.DataFrame(CSV_ROWS), vetted_by="user", now=T0)
    assert list(df.columns) == SOURCES.schema.names
    row = df.set_index("handle").loc["TestvilleSite"]
    assert list(row["teams"]) == ["Testville State"] and row["credibility"] == 0.5 and row["vetted_by"] == "user"
    assert registry.handles_for("Testville State") == ["TestvilleBeat", "TestvilleSite"]
    assert registry.nationals == ["NatOne"] and registry.teams_of("ExampleBeat") == ["Example Tech"]


def test_sweep_covers_every_team_once_within_the_length_limit(registry):
    qs = sweep_queries(registry, QBS, list(QBS))
    assert all(len(q.text) <= QUERY_LIMIT for q in qs)
    team_qs = [q for q in qs if q.kind == "sweep"]
    covered = [t for q in team_qs for t in q.teams]
    assert sorted(covered) == ["Example Tech", "Testville State"]
    tq = next(q for q in team_qs if "Testville State" in q.teams)
    assert "from:TestvilleBeat" in tq.text and "Stone" in tq.text and "quarterback" in tq.text
    assert "-is:retweet" in tq.text


def test_teams_without_reporters_search_full_names_from_anyone(registry):
    gap = [q for q in sweep_queries(registry, QBS, list(QBS)) if q.kind == "gap"]
    assert len(gap) == 1 and gap[0].teams == ("Nowhere A&M",)
    assert '"Dana Quill"' in gap[0].text and "from:" not in gap[0].text and "lang:en" in gap[0].text


def test_national_insiders_get_their_own_sweep(registry):
    nat = [q for q in sweep_queries(registry, QBS, list(QBS)) if q.kind == "national"]
    assert len(nat) == 1 and "from:NatOne" in nat[0].text and "quarterback" in nat[0].text


def test_query_keys_survive_a_change_of_qb_names(registry):
    a = {q.key for q in sweep_queries(registry, QBS, list(QBS))}
    b = {q.key for q in sweep_queries(registry, {**QBS, "Testville State": [("Avery", "Stone"), ("Eli", "New")]},
                                      list(QBS))}
    assert a == b


def test_long_handle_lists_are_split_to_fit(registry):
    many = pd.DataFrame([{"platform": "x", "handle": f"Reporter{i:03d}", "teams": "Testville State", "kind": "beat",
                          "confidence": "normal"} for i in range(60)])
    reg = Registry(registry_frame(many, vetted_by="user", now=T0))
    qs = sweep_queries(reg, QBS, ["Testville State"])
    assert len(qs) > 1 and all(len(q.text) <= QUERY_LIMIT for q in qs)
    assert len({q.key for q in qs}) == len(qs)


def test_watch_adds_insiders_and_practice_words(registry):
    qs = watch_queries(registry, "Testville State", QBS["Testville State"])
    text = " ".join(q.text for q in qs)
    assert "from:TestvilleBeat" in text and "from:NatOne" in text and '"did not practice"' in text
    assert all(len(q.text) <= QUERY_LIMIT and q.kind == "watch" for q in qs)


def test_lookback_searches_anyone_by_full_name():
    q = lookback_query("Testville State", QBS["Testville State"])
    assert q.kind == "lookback" and '"Avery Stone"' in q.text and "from:" not in q.text and len(q.text) <= QUERY_LIMIT


def test_budget_stops_sweeps_at_80_percent_and_everything_at_the_cap(tmp_path):
    b = XBudget(tmp_path / "x.json", monthly_usd=1.0)        # 200 posts
    b.charge(159, T0)
    assert b.allow("sweep", T0) and b.allow("watch", T0)
    b.charge(1, T0)
    assert not b.allow("sweep", T0) and b.allow("watch", T0)
    b.charge(40, T0)
    assert not b.allow("watch", T0) and not b.allow("lookback", T0)


def test_budget_persists_and_resets_each_month(tmp_path):
    b = XBudget(tmp_path / "x.json", monthly_usd=1.0)
    b.charge(200, T0)
    assert not XBudget(tmp_path / "x.json", monthly_usd=1.0).allow("watch", T0)
    assert XBudget(tmp_path / "x.json", monthly_usd=1.0).allow("watch", datetime(2026, 11, 1, 0, 1, tzinfo=timezone.utc))


def test_budget_flags_an_expensive_day(tmp_path):
    b = XBudget(tmp_path / "x.json", monthly_usd=25.0, daily_alert_usd=1.5)
    assert b.charge(299, T0) is False
    assert b.charge(2, T0) is True                           # crosses $1.50 once
    assert b.charge(5, T0) is False


def test_since_ids_persist_per_query(tmp_path):
    b = XBudget(tmp_path / "x.json")
    b.set_since("sweep:a", "123")
    assert XBudget(tmp_path / "x.json").since("sweep:a") == "123"
    assert XBudget(tmp_path / "x.json").since("sweep:b") is None


class Response:
    def __init__(self, body, status=200):
        self.body, self.status_code = body, status

    def json(self):
        return self.body


def test_poll_sends_since_or_start_time_and_raises_on_rate_limit():
    calls = []

    def transport(url, params, headers, timeout):
        calls.append(params)
        return Response({"data": [{"id": "9", "text": "x", "author_id": "1", "created_at": "2026-10-22T17:00:00Z"}],
                         "includes": {"users": [{"id": "1", "username": "TestvilleBeat"}]}})
    x = XSource(transport=transport, secret_fn=lambda *a: "token")
    posts = x.poll("q", since_id="5")
    assert calls[-1]["since_id"] == "5" and posts[0]["handle"] == "TestvilleBeat"
    x.poll("q", start_time=T0 - timedelta(hours=24), max_results=50, sort_order="relevancy")
    assert calls[-1]["start_time"] == "2026-10-21T18:00:00Z" and calls[-1]["sort_order"] == "relevancy"
    assert calls[-1]["max_results"] == 50 and "since_id" not in calls[-1]
    limited = XSource(transport=lambda *a, **k: Response({"title": "Too Many Requests"}, 429), secret_fn=lambda *a: "t")
    with pytest.raises(RuntimeError):
        limited.poll("q")


QB_FRAMES = {"Testville State": pd.DataFrame([{"player_id": "9000001", "name": "Avery Stone"},
                                               {"player_id": "9000002", "name": "Blake Rowe"}]),
             "Example Tech": pd.DataFrame([{"player_id": "9000010", "name": "Casey Vale"}])}
TEAM_IDS = {"Testville State": 501, "Example Tech": 502}


def post(pid, handle, text):
    return {"id": pid, "handle": handle, "text": text, "created_at": "2026-10-22T17:30:00Z"}


def to_ev(posts, kind="sweep", teams=("Testville State",)):
    from gnome_pipelines.cfb.qb_status.x_scan import Query
    q = Query(key="k", text="q", kind=kind, teams=tuple(teams))
    reg = Registry(registry_frame(pd.DataFrame(CSV_ROWS), vetted_by="user", now=T0))
    return posts_to_evidence(posts, q, reg, QB_FRAMES, TEAM_IDS, T0, game_for=lambda tid: 77)


def test_a_beat_writers_post_becomes_pending_evidence_for_their_team():
    ev = to_ev([post("100", "TestvilleBeat", "Stone will not play Saturday, coach says.")])
    assert [(e.player_id, e.status, e.team_id, e.game_id, e.review_state) for e in ev] == [
        ("9000001", "out", 501, 77, "pending")]
    assert ev[0].source_type == "x_post" and ev[0].url.endswith("/status/100")
    assert ev[0].player_raw == "Avery Stone"


def test_an_insiders_post_is_matched_to_a_team_by_full_name():
    ev = to_ev([post("101", "NatOne", "Sources: Casey Vale is expected to start this week.")], kind="national",
               teams=("Testville State", "Example Tech"))
    assert [(e.player_id, e.status, e.team_id) for e in ev] == [("9000010", "expected_starter", 502)]


def test_posts_without_a_named_qb_produce_nothing():
    assert to_ev([post("102", "TestvilleBeat", "Injury report is out; the QB room is healthy.")]) == []


def test_insider_posts_need_the_full_name_not_just_a_surname():
    assert to_ev([post("103", "NatOne", "Stone will not play Saturday.")], kind="national",
                 teams=("Testville State", "Example Tech")) == []


class Recording:
    name = "fake-model"

    def __init__(self):
        self.teams = []

    def extract(self, text, team_id, qbs, team=None):
        self.teams.append(team)
        return [(str(qbs["player_id"].iloc[0]), "out", "confirmed", 0.9)]


def test_posts_are_read_with_the_team_name_and_labelled_with_the_reader():
    from gnome_pipelines.cfb.qb_status.x_scan import Query
    rec = Recording()
    reg = Registry(registry_frame(pd.DataFrame(CSV_ROWS), vetted_by="user", now=T0))
    ev = posts_to_evidence([post("104", "TestvilleBeat", "Stone will not play.")],
                           Query(key="k", text="q", kind="sweep", teams=("Testville State",)), reg, QB_FRAMES,
                           TEAM_IDS, T0, game_for=lambda tid: 77, extractor=rec)
    assert rec.teams == ["Testville State"] and [e.extractor for e in ev] == ["fake-model"]


def test_unlisted_accounts_get_low_credibility(registry):
    assert registry.credibility("TestvilleBeat") == 1.0 and registry.credibility("TestvilleSite") == 0.5
    assert registry.credibility("SomeoneElse") == 0.25
