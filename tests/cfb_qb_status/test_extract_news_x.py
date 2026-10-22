import json
from datetime import datetime, timezone
from types import SimpleNamespace

import anthropic
import httpx2
import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.extract import (CLAUDE_MODEL, ClaudeExtractor, ExtractionUnavailable, NotConfigured,
                                                   RuleExtractor)
from gnome_pipelines.cfb.qb_status.news import news_evidence, parse_rss, rss_url
from gnome_pipelines.cfb.qb_status.x_source import ReadBudget, XSource, build_queries, x_evidence

SEEN = datetime(2026, 10, 22, 14, 0, tzinfo=timezone.utc)
QBS = pd.DataFrame([{"player_id": "9000001", "name": "Avery Stone", "jersey": "1", "passer_key": "9000001"},
                    {"player_id": "9000002", "name": "Blake Rowe", "jersey": "12", "passer_key": "9000002"}])


def test_rule_extractor_finds_doubtful_with_name():
    claims = RuleExtractor().extract("Mississippi State QB Avery Stone is doubtful for Saturday, per sources.",
                                     344, QBS)
    assert claims == [("9000001", "doubtful", "expected", 0.6)]


def test_rule_extractor_needs_name_and_phrase_in_same_sentence():
    text = "Avery Stone practiced Tuesday. The kicker is doubtful."
    assert RuleExtractor().extract(text, 344, QBS) == []


def test_rule_extractor_prefers_specific_phrases():
    claims = RuleExtractor().extract("Blake Rowe will start Saturday; Avery Stone won't start.", 344, QBS)
    assert set(claims) == {("9000002", "expected_starter", "unclear", 0.6), ("9000001", "not_starting", "unclear", 0.6)}


def test_claude_extractor_requires_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(NotConfigured):
        ClaudeExtractor()


class FakeClient:
    def __init__(self, *replies):
        self.replies, self.calls = list(replies), []
        self.messages = self

    def create(self, **kw):
        self.calls.append(kw)
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def reply(*claims, stop="end_turn"):
    return SimpleNamespace(stop_reason=stop, usage=SimpleNamespace(input_tokens=700, output_tokens=90),
                           content=[SimpleNamespace(type="thinking", thinking=""),
                                    SimpleNamespace(type="text", text=json.dumps({"claims": list(claims)}))])


def said(pid, status, hedge="confirmed", timing="upcoming", confidence=0.95):
    return {"player_id": pid, "status": status, "hedge": hedge, "timing": timing, "confidence": confidence}


def claude(tmp_path, *replies, **kw):
    client = FakeClient(*replies)
    return ClaudeExtractor(client=client, cache_path=tmp_path / "claude.jsonl", **kw), client


def test_claude_reads_a_post_into_claims(tmp_path):
    x, client = claude(tmp_path, reply(said("9000001", "out")))
    text = "Coach says Avery Stone will not play Saturday."
    assert x.extract(text, 344, QBS, team="Mississippi State") == [("9000001", "out", "confirmed", 0.95)]
    kw = client.calls[0]
    assert kw["model"] == CLAUDE_MODEL == "claude-haiku-5-5" and kw["output_config"]["effort"] == "medium"
    schema = kw["output_config"]["format"]["schema"]
    assert schema["properties"]["claims"]["items"]["properties"]["player_id"]["enum"] == ["9000001", "9000002"]
    user = kw["messages"][-1]["content"]
    assert text in user and "Mississippi State" in user and "Blake Rowe" in user
    assert not {"temperature", "top_p", "top_k"} & set(kw) and kw["messages"][-1]["role"] == "user"


def test_claude_is_not_asked_about_text_naming_no_listed_qb(tmp_path):
    x, client = claude(tmp_path)
    assert x.extract("Bulldogs prep for Sooners; injury report out Wednesday.", 344, QBS) == []
    assert client.calls == []


def test_past_game_claims_are_dropped_and_unclear_timing_is_capped(tmp_path):
    x, _ = claude(tmp_path, reply(said("9000001", "questionable", timing="past"),
                                  said("9000001", "left_game_injured", timing="past"),
                                  said("9000002", "expected_starter", hedge="per_sources", timing="unclear")))
    assert x.extract("Stone hurt last week; Rowe could start.", 344, QBS) == [
        ("9000001", "left_game_injured", "confirmed", 0.95), ("9000002", "expected_starter", "per_sources", 0.5)]


def test_a_refused_or_cut_off_reading_gives_no_claims(tmp_path):
    x, _ = claude(tmp_path, reply(stop="refusal"), reply(stop="max_tokens"))
    assert x.extract("Avery Stone is out.", 344, QBS) == []
    assert x.extract("Blake Rowe is out.", 344, QBS) == []


def test_each_text_is_read_once_even_across_restarts(tmp_path):
    x, client = claude(tmp_path, reply(said("9000001", "doubtful", hedge="per_sources", confidence=0.85)))
    first = x.extract("Avery Stone doubtful, per sources.", 344, QBS)
    assert x.extract("Avery Stone doubtful, per sources.", 344, QBS) == first and len(client.calls) == 1
    again, client2 = claude(tmp_path)
    assert again.extract("Avery Stone doubtful, per sources.", 344, QBS) == first and client2.calls == []
    assert "Avery Stone doubtful" not in (tmp_path / "claude.jsonl").read_text()


def test_api_errors_and_the_daily_cap_make_the_reader_unavailable(tmp_path):
    down = anthropic.APIConnectionError(request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages"))
    x, _ = claude(tmp_path, down)
    with pytest.raises(ExtractionUnavailable):
        x.extract("Avery Stone is out.", 344, QBS)
    capped, client = claude(tmp_path, reply(), daily_calls=1)
    capped.extract("Blake Rowe is out.", 344, QBS)
    with pytest.raises(ExtractionUnavailable):
        capped.extract("Blake Rowe will start.", 344, QBS)
    assert len(client.calls) == 1


RSS = """<?xml version="1.0"?><rss><channel>
<item><title>Avery Stone doubtful vs. Oklahoma - The Example Ledger</title><link>https://example.com/a</link>
<guid>g1</guid><pubDate>Thu, 22 Oct 2026 13:05:00 GMT</pubDate></item>
<item><title>Bulldogs prep for Sooners - Example Daily</title><link>https://example.com/b</link>
<guid>g2</guid><pubDate>Thu, 22 Oct 2026 12:00:00 GMT</pubDate></item>
</channel></rss>"""


def test_rss_url_quotes_team():
    assert "%22Mississippi%20State%22" in rss_url("Mississippi State")


def test_parse_rss_reads_items():
    items = parse_rss(RSS)
    assert [i["guid"] for i in items] == ["g1", "g2"]
    assert items[0]["published"] == datetime(2026, 10, 22, 13, 5, tzinfo=timezone.utc)


def test_news_evidence_is_pending():
    ev = news_evidence(parse_rss(RSS), "Mississippi State", 344, QBS, RuleExtractor(), SEEN, game_id=8)
    assert [(e.status, e.player_id, e.review_state, e.source_org, e.game_id) for e in ev] == [
        ("doubtful", "9000001", "pending", "The Example Ledger", 8)]
    assert ev[0].published_at == datetime(2026, 10, 22, 13, 5, tzinfo=timezone.utc)


def test_news_is_read_with_the_team_name_and_labelled_with_the_reader():
    seen = []

    class Reader:
        name = "fake-model"

        def extract(self, text, team_id, qbs, team=None):
            seen.append(team)
            return [("9000001", "doubtful", "confirmed", 0.9)] if "Stone" in text else []
    ev = news_evidence(parse_rss(RSS), "Mississippi State", 344, QBS, Reader(), SEEN, game_id=8)
    assert [e.extractor for e in ev] == ["fake-model"] and seen == ["Mississippi State"] * 2


def test_query_split_respects_512_chars():
    handles = [f"beatwriter{i:03d}" for i in range(60)]
    queries = build_queries(handles, ["Stone", "Rowe"], ["out", "doubtful", "injured"])
    assert len(queries) > 1 and all(len(q) <= 512 for q in queries)
    joined = " ".join(queries)
    assert all(f"from:{h}" in joined for h in handles)


def test_budget_blocks_sweep_before_watches():
    b = ReadBudget(100)
    b.charge(85)
    assert not b.allow(1, "sweep") and b.allow(1, "watch")
    b.charge(15)
    assert not b.allow(1, "watch")


def test_x_source_disabled_without_key():
    def missing(name, env):
        raise RuntimeError("no secret")
    assert XSource(secret_fn=missing).enabled is False


def test_x_evidence_has_no_raw_text():
    post = {"id": "1850", "text": "SECRET RAW TEXT: Avery Stone is doubtful", "created_at": "2026-10-22T13:00:00.000Z"}
    ev = x_evidence(post, "msbeat", [("9000001", "doubtful", "expected", 0.6)], "Mississippi State", 344, SEEN, 8)
    assert len(ev) == 1 and "SECRET RAW TEXT" not in (ev[0].paraphrase or "")
    assert len(ev[0].paraphrase) <= 140 and ev[0].source_id == "1850" and ev[0].review_state == "pending"


def test_rss_dates_without_zone_are_utc():
    xml = RSS.replace("Thu, 22 Oct 2026 13:05:00 GMT", "Thu, 22 Oct 2026 13:05:00 -0000")
    assert parse_rss(xml)[0]["published"] == datetime(2026, 10, 22, 13, 5, tzinfo=timezone.utc)
