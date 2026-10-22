"""X (beat reporters and national insiders), through the official API with the user's pay-per-use key.

Disabled until the key exists (X_BEARER_TOKEN locally, gnome/pipelines/x in Secrets Manager). A monthly read budget
degrades gracefully: the hourly sweep stops at 80% of the budget, watches at 100%. Following X's terms on deletions,
evidence keeps the post id, the handle, our claim and a short paraphrase of the claim, never the post's text.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import requests

from gnome_pipelines import credentials
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id

SEARCH = "https://api.x.com/2/tweets/search/recent"
QUERY_LIMIT = 512
SWEEP_SHARE = 0.8


class ReadBudget:
    def __init__(self, monthly_reads: int) -> None:
        self.monthly_reads, self.used = monthly_reads, 0

    def allow(self, n: int, kind: str = "watch") -> bool:
        cap = self.monthly_reads * (SWEEP_SHARE if kind == "sweep" else 1.0)
        return self.used + n <= cap and self.used < cap

    def charge(self, n: int) -> None:
        self.used += n


def _group(words: list[str], fmt: str) -> str:
    inner = " OR ".join(fmt.format(w) for w in words)
    return f"({inner})" if len(words) > 1 else inner


def build_query(handles: list[str], names: list[str], terms: list[str]) -> str:
    parts = [_group(handles, "from:{}")]
    if names:
        parts.append(_group(names, '"{}"' if any(" " in n for n in names) else "{}"))
    if terms:
        parts.append(_group(terms, '"{}"'))
    return " ".join(parts) + " -is:retweet"


def build_queries(handles: list[str], names: list[str], terms: list[str], limit: int = QUERY_LIMIT) -> list[str]:
    queries, chunk = [], []
    for h in handles:
        trial = build_query(chunk + [h], names, terms)
        if chunk and len(trial) > limit:
            queries.append(build_query(chunk, names, terms))
            chunk = [h]
        else:
            chunk.append(h)
    if chunk:
        queries.append(build_query(chunk, names, terms))
    return queries


class XSource:
    def __init__(self, transport: Callable | None = None,
                 secret_fn: Callable[[str, str], str] = credentials.secret, budget: ReadBudget | None = None) -> None:
        try:
            self._token = secret_fn("x", "X_BEARER_TOKEN")
        except Exception:
            self._token = None
        self.enabled = bool(self._token)
        self.budget = budget or ReadBudget(20_000)
        self._transport = transport or requests.get

    def poll(self, query: str, since_id: str | None = None, kind: str = "watch", start_time: datetime | None = None,
             max_results: int = 100, sort_order: str | None = None) -> list[dict]:
        """One recent-search request. since_id resumes a query; a query we haven't run before starts at start_time
        instead, so it never pays for a week of history. A rate limit or error raises, and the service retries."""
        if not self.enabled or not self.budget.allow(1, kind):
            return []
        params = {"query": query, "max_results": max_results, "tweet.fields": "created_at,author_id",
                  "expansions": "author_id", "user.fields": "username"}
        if since_id:
            params["since_id"] = since_id
        elif start_time is not None:
            params["start_time"] = start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if sort_order:
            params["sort_order"] = sort_order
        r = self._transport(SEARCH, params=params, headers={"Authorization": f"Bearer {self._token}"}, timeout=30)
        body = r.json() if hasattr(r, "json") else r
        status = getattr(r, "status_code", 200)
        if status >= 400:
            raise RuntimeError(f"X search failed with HTTP {status}: {(body or {}).get('title', '')}")
        posts = body.get("data", []) or []
        users = {u["id"]: u["username"] for u in (body.get("includes") or {}).get("users", [])}
        for p in posts:
            p["handle"] = users.get(p.get("author_id"))
        self.budget.charge(len(posts))
        return posts


def x_evidence(post: dict, handle: str, claims: list[tuple[str, str, str, float]], team: str, team_id: int,
               seen_at: datetime, game_id: int | None = None, extractor: str = "rules") -> list[Evidence]:
    published = datetime.fromisoformat(post["created_at"].replace("Z", "+00:00")) if post.get("created_at") else None
    season = seen_at.year if seen_at.month >= 7 else seen_at.year - 1
    out = []
    for player_id, status, hedge, confidence in claims:
        out.append(Evidence(
            evidence_id=evidence_id("x_post", post["id"], player_id, status), source_type="x_post",
            source_id=str(post["id"]), source_org=handle, first_seen_at=seen_at, team=team, status=status,
            season=season, published_at=published, team_id=team_id, player_id=player_id, match_confidence=1.0,
            game_id=game_id, hedge=hedge, paraphrase=f"@{handle}: {status.replace('_', ' ')} ({hedge})"[:140],
            url=f"https://x.com/{handle}/status/{post['id']}", extractor=extractor, extraction_confidence=confidence,
            review_state="pending"))
    return out
