"""Optional: structured-output Claude calls for sources that have Claude read text.

Only a source that uses Claude imports this module (it needs the anthropic package, which only some family extras
install). ClaudeReader reads each distinct input once: results are kept by a hash the caller supplies (never the text
itself) in a ReadCache. The default only remembers reads for the life of the process; a source that wants them to
outlive a restart passes its own (QB status keeps them on S3). A daily call cap, counted per process, bounds spend;
past it, or on an API error, reads raise Unavailable and the source decides what to do instead. meter() reports its
spend through the Service.
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Callable, Protocol

import anthropic
from botocore.exceptions import BotoCoreError, ClientError

from gnome_pipelines import credentials
from gnome_pipelines.polling.service import Meter

# Haiku 5.5 list prices per token.
USD_PER_TOKEN = {"input": 0.10e-6, "output": 0.50e-6}


class NotConfigured(RuntimeError):
    """No Anthropic key in the environment or gnome/pipelines/anthropic."""


class Unavailable(RuntimeError):
    """Claude can't read right now: an API error or the daily cap."""


class ReadCache(Protocol):
    def lookup(self, key: str) -> tuple[bool, dict | None]:
        """(True, the cached answer, which may be None) or (False, None) when this key was never read."""

    def store(self, key: str, output: dict | None) -> None: ...


class MemoryCache:
    def __init__(self) -> None:
        self.reads: dict[str, dict | None] = {}

    def lookup(self, key: str) -> tuple[bool, dict | None]:
        return (True, self.reads[key]) if key in self.reads else (False, None)

    def store(self, key: str, output: dict | None) -> None:
        self.reads[key] = output


def cache_key(*parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()


class ClaudeReader:
    def __init__(self, model: str, daily_calls: int, cache: ReadCache | None = None, client=None,
                 clock: Callable[[], datetime] | None = None,
                 secret_fn: Callable[[str, str], str] = credentials.secret,
                 usd_per_token: dict[str, float] = USD_PER_TOKEN) -> None:
        if client is None:
            try:
                key = secret_fn("anthropic", "ANTHROPIC_API_KEY")
            except (RuntimeError, BotoCoreError, ClientError) as e:
                raise NotConfigured(f"no Anthropic key: {e}") from e
            client = anthropic.Anthropic(api_key=key, timeout=30.0, max_retries=2)
        self.model, self.client, self.daily_calls = model, client, daily_calls
        self.cache = cache if cache is not None else MemoryCache()
        self.usd_per_token = usd_per_token
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.tokens = {"input": 0, "output": 0}
        self.day = self.clock().strftime("%Y-%m-%d")
        self.calls_today = 0
        # Lanes read concurrently: the lock covers the counters. Cache lookups stay outside it (they may be slow),
        # so two lanes asking for the same new key at once may both call the model.
        self._lock = threading.Lock()

    def usd(self) -> float:
        with self._lock:
            return sum(n * self.usd_per_token[k] for k, n in self.tokens.items())

    def meter(self) -> Meter:
        return Meter(f"claude:{self.model}", self.usd)

    def read(self, key: str, system: str, user: str, schema: dict, effort: str = "low",
             max_tokens: int = 2048) -> dict | None:
        """The model's JSON answer, or None when it stopped early (refusal, max_tokens) or returned no valid JSON.
        Both outcomes are cached under key."""
        found, cached = self.cache.lookup(key)
        if found:
            return cached
        with self._lock:
            now = self.clock()
            if now.strftime("%Y-%m-%d") != self.day:
                self.day, self.calls_today = now.strftime("%Y-%m-%d"), 0
            if self.calls_today >= self.daily_calls:
                raise Unavailable(f"daily cap of {self.daily_calls} reads reached")
            self.calls_today += 1
        try:
            response = self.client.messages.create(
                model=self.model, max_tokens=max_tokens, system=system,
                messages=[{"role": "user", "content": user}],
                output_config={"effort": effort, "format": {"type": "json_schema", "schema": schema}})
        except anthropic.APIError as e:
            raise Unavailable(f"{type(e).__name__}: {e}") from e
        output = _output(response)
        usage = getattr(response, "usage", None)
        used = {"input": getattr(usage, "input_tokens", 0) or 0, "output": getattr(usage, "output_tokens", 0) or 0}
        with self._lock:
            for k, n in used.items():
                self.tokens[k] += n
        self.cache.store(key, output)
        return output


def _output(response) -> dict | None:
    if response.stop_reason != "end_turn":
        return None
    body = next((b.text for b in response.content if b.type == "text"), None)
    try:
        value = json.loads(body) if body else None
    except ValueError:
        return None
    return value if isinstance(value, dict) else None
