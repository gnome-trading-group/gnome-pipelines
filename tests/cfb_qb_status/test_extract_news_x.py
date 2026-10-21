from datetime import datetime, timezone

import pandas as pd
import pytest

from gnome_pipelines.cfb.qb_status.extract import ClaudeExtractor, NotConfigured, RuleExtractor
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
