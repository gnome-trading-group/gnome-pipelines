from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.cfb.qb_status.crosswalk import PlayerIndex
from gnome_pipelines.cfb.qb_status.evidence import Evidence
from gnome_pipelines.cfb.qb_status.extract import ExtractionUnavailable
from gnome_pipelines.cfb.qb_status.market_alarm import Alarm
from gnome_pipelines.cfb.qb_status.review import decide
from gnome_pipelines.cfb.qb_status.service import (Context, build_service, fbs_games, fresh_alarms, resolve_flags)
from gnome_pipelines.cfb.qb_status.store import StatusStore

T0 = datetime(2026, 10, 24, 20, 0, tzinfo=timezone.utc)
ROSTERS = pd.DataFrame([
    {"season": 2026, "player_id": "9000001", "first_name": "Avery", "last_name": "Stone", "team": "Testville State",
     "position": "QB", "jersey": 1.0, "team_id": 501},
    {"season": 2026, "player_id": "9000002", "first_name": "Blake", "last_name": "Rowe", "team": "Testville State",
     "position": "QB", "jersey": 12.0, "team_id": 501},
    {"season": 2026, "player_id": "9000003", "first_name": "Casey", "last_name": "Vale", "team": "Testville State",
     "position": "WR", "jersey": 4.0, "team_id": 501},
])
PASSERS = pd.DataFrame(columns=["season", "game_id", "offense", "passer", "passer_key", "play_seq"])


def flag(status, raw):
    return Evidence(evidence_id=f"{status}-{raw}", source_type="pbp_flag", source_id="401", source_org="espn",
                    first_seen_at=T0, team="Testville State", status=status, season=2026, player_raw=raw)


def test_fbs_games_keep_fcs_opponents():
    games = pd.DataFrame([{"game_id": 1, "home_classification": "fbs", "away_classification": "fcs"},
                          {"game_id": 2, "home_classification": "fcs", "away_classification": "fcs"},
                          {"game_id": 3, "home_classification": "fbs", "away_classification": "fbs"}])
    assert fbs_games(games)["game_id"].tolist() == [1, 3]


def test_resolve_flags_keeps_rostered_qbs_only():
    index = PlayerIndex(ROSTERS, PASSERS, 2026)
    out = resolve_flags([flag("qb_change", "Blake Rowe"), flag("qb_change", "Casey Vale"),
                         flag("left_game_injured", "Stone")], {"Testville State": 501}, index)
    assert [(f.status, f.player_id, f.team_id) for f in out] == [("qb_change", "9000002", 501),
                                                                 ("left_game_injured", "9000001", 501)]


def test_fresh_alarms_fire_once_per_move():
    until: dict[int, datetime] = {}
    a = fresh_alarms([Alarm(8, T0, 0.2)], until, T0)
    b = fresh_alarms([Alarm(8, T0 + timedelta(minutes=40), 0.2)], until, T0 + timedelta(minutes=40))
    c = fresh_alarms([Alarm(8, T0 + timedelta(minutes=70), 0.2)], until, T0 + timedelta(minutes=70))
    assert (len(a), len(b), len(c)) == (1, 0, 1)


class FakeCtx:
    status = None

    def __init__(self, watched):
        self._watched = watched

    def watched_count(self, now):
        return self._watched

    def __getattr__(self, name):
        return lambda *a, **k: 600.0 if name.endswith("interval") else []


def test_watched_news_rotation_runs_every_ten_minutes_per_team():
    svc = build_service(FakeCtx(2), espn=False, market=False)
    tasks = {t.name: t for t in svc.tasks}
    assert {"news_watched", "news_sweep", "manual"} <= set(tasks)
    assert Context.news_watched_interval(FakeCtx(2), T0) == pytest.approx(300.0)
    assert Context.news_watched_interval(FakeCtx(0), T0) == pytest.approx(600.0)


class Research:
    def __init__(self, fail):
        self.fail = fail

    def load(self, name, **kw):
        if self.fail:
            raise RuntimeError("SSO session expired")
        if name == "cfb_games":
            return pd.DataFrame([{"game_id": 1, "season": 2026, "kickoff": pd.Timestamp("2026-10-24T19:00Z"),
                                  "home_team": "Testville State", "home_id": 501, "away_team": "Example Tech",
                                  "away_id": 502, "home_classification": "fbs", "away_classification": "fbs",
                                  "completed": False, "conference_game": True, "home_conference": "SEC"}])
        if name == "cfb_rosters":
            return ROSTERS
        return PASSERS


def test_context_starts_from_cache_when_research_reads_fail(tmp_path):
    status = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
    Context(Research(fail=False), status, 2026, cache_dir=tmp_path / "cache")
    ctx = Context(Research(fail=True), status, 2026, cache_dir=tmp_path / "cache")
    assert ctx.team_names == {501: "Testville State", 502: "Example Tech"}
    with pytest.raises(RuntimeError):
        Context(Research(fail=True), status, 2026, cache_dir=tmp_path / "empty")


def test_manual_decisions_are_pulled_from_another_process(tmp_path):
    wh = tmp_path / "wh"
    service_side = StatusStore(DatasetStore(CatalogConfig(local_warehouse=wh)))
    service_side.add_evidence([Evidence(evidence_id="n1", source_type="news", source_id="g", source_org="x",
                                        first_seen_at=T0, team="Testville State", status="out", season=2026,
                                        team_id=501, review_state="pending")])
    review_side = StatusStore(DatasetStore(CatalogConfig(local_warehouse=wh)))
    decide(review_side, "n1", "confirm", "user", player_id="9000001", game_id=1, now=T0 + timedelta(minutes=1))
    pulled = service_side.pull_new("manual")
    assert [(e.player_id, e.game_id, e.review_state) for e in pulled] == [("9000001", 1, "confirmed")]
    assert service_side.pull_new("manual") == []


class ResearchWithPlays(Research):
    def load(self, name, **kw):
        if name == "cfb_games":
            past = {"game_id": 0, "season": 2026, "kickoff": pd.Timestamp("2026-10-17T19:00Z"),
                    "home_team": "Testville State", "home_id": 501, "away_team": "Example Tech", "away_id": 502,
                    "home_classification": "fbs", "away_classification": "fbs", "completed": True,
                    "conference_game": True, "home_conference": "SEC"}
            return pd.concat([super().load(name, **kw), pd.DataFrame([past])], ignore_index=True)
        if name == "cfb_passers":
            qb = [{"season": 2026, "game_id": 0, "offense": "Testville State", "passer": "A.Stone",
                   "passer_key": "a.stone", "play_seq": i} for i in range(1, 31)]
            trick = [{"season": 2026, "game_id": 0, "offense": "Testville State", "passer": "C.Vale",
                      "passer_key": "c.vale", "play_seq": 99}]
            return pd.DataFrame(qb + trick)
        return super().load(name, **kw)


def test_trick_play_passers_and_non_qb_report_rows_stay_out_of_the_status(tmp_path):
    status = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
    ctx = Context(ResearchWithPlays(fail=False), status, 2026, cache_dir=tmp_path / "cache")
    now = T0 - timedelta(days=1)
    assert ctx.team_candidates(501, now)["player_id"].tolist() == ["9000001"]
    status.add_evidence([Evidence(evidence_id="wr", source_type="official_report", source_id="r", source_org="SEC",
                                  first_seen_at=now - timedelta(hours=1), team="Testville State", status="out",
                                  season=2026, team_id=501, player_id="9000003", game_id=1, report_slot="Initial")])
    ctx.recompute({501}, [], now)
    assert "9000003" not in set(status.snapshots()["player_id"])


class FakeX:
    """Stands in for the X recent-search endpoint: records params, returns the posts it's given once."""

    def __init__(self, posts=None):
        self.calls, self.posts = [], list(posts or [])

    def __call__(self, url, params, headers, timeout):
        self.calls.append(params)
        posts, self.posts = self.posts, []

        class R:
            status_code = 200

            def json(self_inner):
                return {"data": [{k: v for k, v in p.items() if k != "handle"} | {"author_id": "1"} for p in posts],
                        "includes": {"users": [{"id": "1", "username": posts[0]["handle"]}]} if posts else {}}
        return R()


def x_context(tmp_path, posts=None, monthly_usd=25.0, extractor=None):
    from gnome_pipelines.cfb.qb_status.x_scan import XBudget, registry_frame
    from gnome_pipelines.cfb.qb_status.x_source import XSource
    status = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
    status.put_sources(registry_frame(pd.DataFrame([
        {"platform": "x", "handle": "TestvilleBeat", "teams": "Testville State", "kind": "beat", "confidence": "normal"},
        {"platform": "x", "handle": "NatOne", "teams": "", "kind": "national", "confidence": "normal"}]),
        vetted_by="user", now=T0))
    fake = FakeX(posts)
    x = XSource(transport=fake, secret_fn=lambda *a: "token")
    ctx = Context(ResearchWithPlays(fail=False), status, 2026, cache_dir=tmp_path / "cache", x=x,
                  x_budget=XBudget(tmp_path / "x.json", monthly_usd=monthly_usd), extractor=extractor)
    return ctx, fake, status


def run_sweep(ctx, now):
    out = []
    for _ in range(len(ctx.x_queries)):
        out += ctx.x_sweep_run(now)
    return out


def test_x_sweep_turns_a_reporters_post_into_evidence_and_resumes_from_it(tmp_path):
    now = T0 - timedelta(days=1)
    post = {"id": "500", "handle": "TestvilleBeat", "text": "Stone will not play Saturday.",
            "created_at": "2026-10-23T19:00:00Z"}
    ctx, fake, _ = x_context(tmp_path, [post])
    ev = run_sweep(ctx, now)
    assert [(e.player_id, e.status, e.team_id, e.source_type) for e in ev] == [("9000001", "out", 501, "x_post")]
    assert "start_time" in fake.calls[0] and "since_id" not in fake.calls[0]
    team_query = next(q for q in ctx.x_queries if q.kind == "sweep")
    assert "Stone" in team_query.text and "from:TestvilleBeat" in team_query.text
    run_sweep(ctx, now + timedelta(minutes=3))
    assert any(c.get("since_id") == "500" for c in fake.calls)


def test_x_budget_stops_the_sweep_before_any_call(tmp_path):
    ctx, fake, status = x_context(tmp_path, monthly_usd=0.0)
    assert run_sweep(ctx, T0 - timedelta(days=1)) == [] and fake.calls == []
    assert "x_budget_stop" in set(status.health()["kind"])


def test_an_alarm_queues_one_relevancy_lookback_per_team(tmp_path):
    ctx, fake, _ = x_context(tmp_path)
    now = T0 - timedelta(days=1)
    ctx.queue_lookback(501, now)
    ctx.queue_lookback(501, now + timedelta(minutes=5))
    ctx.x_lookback_run(now + timedelta(minutes=6))
    ctx.x_lookback_run(now + timedelta(minutes=7))
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["sort_order"] == "relevancy" and call["max_results"] == 50 and '"Avery Stone"' in call["query"]


def test_without_x_the_service_builds_no_x_tasks(tmp_path):
    status = StatusStore(DatasetStore(CatalogConfig(local_warehouse=tmp_path / "wh")))
    ctx = Context(ResearchWithPlays(fail=False), status, 2026, cache_dir=tmp_path / "cache")
    names = {t.name for t in build_service(ctx, espn=False, market=False, news=False).tasks}
    assert not any(n.startswith("x_") for n in names)


def test_load_registry_command_fills_the_sources_table(tmp_path, monkeypatch):
    from gnome_pipelines.cfb.qb_status import service
    from gnome_pipelines.cfb.qb_status.store import open_store
    csv_path = tmp_path / "registry.csv"
    pd.DataFrame([{"platform": "x", "handle": "TestvilleBeat", "name": "T B", "teams": "Testville State",
                   "kind": "beat", "confidence": "normal"},
                  {"platform": "x", "handle": "NatOne", "name": "N O", "teams": "", "kind": "national",
                   "confidence": "low"}]).to_csv(csv_path, index=False)
    monkeypatch.setenv("QB_STATUS_WAREHOUSE", str(tmp_path / "wh"))
    service.main(["--load-registry", str(csv_path)])
    got = StatusStore(open_store(False)).sources().set_index("handle")
    assert sorted(got.index) == ["NatOne", "TestvilleBeat"] and got.loc["NatOne", "credibility"] == 0.5
    assert got.loc["TestvilleBeat", "vetted_by"] == "user"


class FakeReader:
    name = "claude-haiku-5-5"

    def __init__(self, fail=False):
        self.fail = fail

    def extract(self, text, team_id, qbs, team=None):
        if self.fail:
            raise ExtractionUnavailable("APIConnectionError: down")
        return [("9000001", "out", "confirmed", 0.95)] if "Stone" in text else []


BEAT_POST = {"id": "600", "handle": "TestvilleBeat", "text": "Coach says Stone will not play Saturday.",
             "created_at": "2026-10-23T19:00:00Z"}


def test_a_credible_reporters_plain_post_applies_without_review(tmp_path):
    ctx, _, _ = x_context(tmp_path, [BEAT_POST], extractor=FakeReader())
    ev = run_sweep(ctx, T0 - timedelta(days=1))
    assert [(e.review_state, e.reviewer, e.extractor) for e in ev] == [("auto", "auto:credible", "claude-haiku-5-5")]


def test_when_the_model_is_unavailable_posts_are_read_by_rules_and_wait(tmp_path):
    ctx, _, status = x_context(tmp_path, [BEAT_POST], extractor=FakeReader(fail=True))
    ev = run_sweep(ctx, T0 - timedelta(days=1))
    assert [(e.review_state, e.extractor) for e in ev] == [("pending", "rules")]
    assert "extractor_fallback" in set(status.health()["kind"])


class FakeSession:
    def __init__(self, xml):
        self.xml = xml

    def get(self, url, timeout):
        return type("R", (), {"text": self.xml})()


def test_a_headline_that_matches_the_official_report_applies(tmp_path):
    now = T0 - timedelta(days=1)
    ctx, _, status = x_context(tmp_path, extractor=FakeReader())
    status.add_evidence([Evidence(evidence_id="off", source_type="official_report", source_id="cell",
                                  source_org="SEC", first_seen_at=now - timedelta(hours=2), team="Testville State",
                                  status="out", season=2026, team_id=501, player_id="9000001", game_id=1,
                                  report_slot="Initial")])
    ctx.session = FakeSession("""<?xml version="1.0"?><rss><channel><item>
<title>Avery Stone ruled out vs. Example Tech - The Example Ledger</title><link>https://example.com/a</link>
<guid>g1</guid><pubDate>Fri, 23 Oct 2026 19:00:00 GMT</pubDate></item></channel></rss>""")
    ev = ctx._news_for(501, now)
    assert [(e.source_type, e.review_state, e.reviewer) for e in ev] == [("news", "auto", "auto:official")]


def test_the_hourly_refresh_records_what_the_model_has_cost(tmp_path):
    class Priced(FakeReader):
        def usd(self):
            return 0.0123
    ctx, _, status = x_context(tmp_path, extractor=Priced())
    ctx.refresh_run(T0)
    spend = status.health().set_index("kind").loc["claude_spend"]
    assert spend["value"] == pytest.approx(0.0123) and "claude-haiku-5-5" in spend["detail"]
