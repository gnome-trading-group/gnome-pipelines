from types import SimpleNamespace

import anthropic
import httpx2
import pytest

from gnome_pipelines.polling.claude import ClaudeReader, MemoryCache, NotConfigured, Unavailable, cache_key

SCHEMA = {"type": "object", "properties": {"fresh": {"type": "boolean"}}}


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


def reply(body, stop="end_turn"):
    return SimpleNamespace(stop_reason=stop, usage=SimpleNamespace(input_tokens=1000, output_tokens=100),
                           content=[SimpleNamespace(type="text", text=body)])


def reader(tmp_path, *replies, cache=None, **kw):
    client = FakeClient(*replies)
    return ClaudeReader("claude-haiku-5-5", kw.pop("daily_calls", 10), cache=cache, client=client, **kw), client


def test_a_read_returns_the_json_answer_and_meters_its_cost(tmp_path):
    r, client = reader(tmp_path, reply('{"fresh": true}'))
    assert r.read("k1", "system", "Review text", SCHEMA, effort="low") == {"fresh": True}
    kw = client.calls[0]
    assert kw["model"] == "claude-haiku-5-5" and kw["output_config"] == {
        "effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}}
    assert r.meter().name == "claude:claude-haiku-5-5" and r.meter().usd() == pytest.approx(1000 * 0.1e-6 + 100 * 0.5e-6)


def test_each_key_is_read_once_and_a_cache_that_outlives_the_reader_keeps_it_read(tmp_path):
    cache = MemoryCache()
    r, client = reader(tmp_path, reply('{"fresh": false}'), cache=cache)
    key = cache_key("model", "system", "Secret review text")
    assert r.read(key, "s", "Secret review text", SCHEMA) == r.read(key, "s", "Secret review text", SCHEMA)
    again, client2 = reader(tmp_path, cache=cache)
    assert again.read(key, "s", "Secret review text", SCHEMA) == {"fresh": False} and client2.calls == []
    assert len(client.calls) == 1 and cache.reads == {key: {"fresh": False}}


def test_without_a_cache_reads_are_remembered_only_by_this_reader(tmp_path):
    r, client = reader(tmp_path, reply('{"fresh": true}'), reply('{"fresh": true}'))
    r.read("k", "s", "u", SCHEMA)
    r.read("k", "s", "u", SCHEMA)
    reader(tmp_path, reply('{"fresh": true}'))[0].read("k", "s", "u", SCHEMA)
    assert len(client.calls) == 1


def test_a_refusal_a_cut_off_or_bad_json_reads_as_none_and_is_cached(tmp_path):
    r, client = reader(tmp_path, reply("", stop="refusal"), reply('{"fresh":', stop="max_tokens"), reply("[1, 2]"))
    assert [r.read(k, "s", "u", SCHEMA) for k in ("a", "b", "c")] == [None, None, None]
    assert r.read("a", "s", "u", SCHEMA) is None and len(client.calls) == 3


def test_api_errors_and_the_daily_cap_make_the_reader_unavailable(tmp_path):
    down = anthropic.APIConnectionError(request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages"))
    r, _ = reader(tmp_path, down)
    with pytest.raises(Unavailable):
        r.read("a", "s", "u", SCHEMA)
    capped, client = reader(tmp_path, reply("{}"), daily_calls=1)
    capped.read("b", "s", "u", SCHEMA)
    with pytest.raises(Unavailable):
        capped.read("c", "s", "u", SCHEMA)
    assert len(client.calls) == 1


def test_without_a_key_the_reader_is_not_configured(tmp_path):
    def missing(name, env):
        raise RuntimeError("gnome/pipelines/anthropic has no value in this account")
    with pytest.raises(NotConfigured):
        ClaudeReader("claude-haiku-5-5", 10, secret_fn=missing)
