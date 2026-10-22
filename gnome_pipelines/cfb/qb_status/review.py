"""Human review of uncertain claims: a local page listing pending evidence with confirm, reject and edit.

A decision never mutates the original row. It writes a new `manual` evidence row (source_id = the original's id)
stamped with the decision time, which the engine then applies like any other claim.

    poetry run python -m gnome_pipelines.cfb.qb_status.review     # http://localhost:8767
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import STATUSES, Evidence, evidence_id
from gnome_pipelines.cfb.qb_status.store import StatusStore, open_store
from gnome_pipelines.datasets.qb_status import PLAYERS

HERE = Path(__file__).resolve().parent
DECISIONS = {"confirm": "confirmed", "edit": "confirmed", "reject": "rejected"}
FIELDS = ("team", "season", "team_id", "player_raw", "jersey", "player_id", "match_confidence", "game_id", "week",
          "report_slot", "hedge", "paraphrase", "url", "published_at")


def pending(store: StatusStore) -> pd.DataFrame:
    ev = store.evidence()
    decided = set(ev.loc[ev["source_type"] == "manual", "source_id"])
    out = ev[(ev["review_state"] == "pending") & ~ev["evidence_id"].isin(decided)].copy()
    players = store.players()
    if len(out) and len(players):
        names = dict(zip(players["player_id"].astype(str),
                         (players["first_name"].fillna("") + " " + players["last_name"].fillna("")).str.strip()))
        missing = out["player_raw"].isna() & out["player_id"].notna()
        out.loc[missing, "player_raw"] = out.loc[missing, "player_id"].astype(str).map(names)
    return out.sort_values("first_seen_at", ascending=False).reset_index(drop=True)


def _value(v):
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def safe_url(url) -> str | None:
    return url if isinstance(url, str) and url.lower().startswith(("https://", "http://")) else None


def is_json_post(headers) -> bool:
    """Only JSON posts are accepted: a cross-site form can't send that content type without a CORS preflight."""
    return (headers.get("Content-Type") or "").split(";")[0].strip().lower() == "application/json"


def decide(store: StatusStore, original_id: str, decision: str, reviewer: str, status: str | None = None,
           now: datetime | None = None, *, team_id: int | None = None, game_id: int | None = None,
           player_id: str | None = None) -> Evidence:
    """Write the decision as a new manual row. team_id, game_id and player_id fill in what the original couldn't
    resolve (that is usually why it was pending); the engine needs all three to apply the claim."""
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {sorted(DECISIONS)}")
    if decision == "edit" and status not in STATUSES:
        raise ValueError(f"edit needs a status from {STATUSES}")
    now = now or datetime.now(timezone.utc)
    rows = store.evidence()
    match = rows[rows["evidence_id"] == original_id]
    if match.empty:
        raise KeyError(original_id)
    orig = {k: _value(match.iloc[0][k]) for k in FIELDS}
    for key, value in (("team_id", team_id), ("game_id", game_id), ("player_id", player_id)):
        if value is not None:
            orig[key] = value
    new_status = status if decision == "edit" else _value(match.iloc[0]["status"])
    ev = Evidence(evidence_id=evidence_id("manual", original_id, decision, new_status, now.isoformat()),
                  source_type="manual", source_id=original_id, source_org=reviewer, first_seen_at=now,
                  status=new_status, extractor="human", extraction_confidence=1.0,
                  review_state=DECISIONS[decision], reviewer=reviewer, reviewed_at=now,
                  **{k: orig[k] for k in FIELDS if k not in ("team", "season")},
                  team=orig["team"] or "", season=int(orig["season"] or now.year))
    store.add_evidence([ev])
    return ev


def _load_players(store: StatusStore) -> dict[int, list[list[str]]]:
    try:
        df = store.store.load(PLAYERS.name)
    except Exception:
        return {}
    out: dict[int, list[list[str]]] = {}
    for r in df.itertuples(index=False):
        if pd.notna(r.team_id):
            out.setdefault(int(r.team_id), []).append([str(r.player_id), f"{r.first_name} {r.last_name}".strip()])
    return out


def _team_games(store: StatusStore) -> dict[int, int]:
    snaps = store.snapshots()
    if snaps.empty:
        return {}
    latest = snaps.sort_values("computed_at").groupby("team_id")["game_id"].last()
    return {int(t): int(g) for t, g in latest.items()}


def _records(df: pd.DataFrame) -> list[dict]:
    cols = ["evidence_id", "source_type", "source_org", "first_seen_at", "published_at", "team", "team_id",
            "player_raw", "player_id", "status", "hedge", "paraphrase", "url", "report_slot", "match_confidence",
            "game_id"]
    out = df[cols].astype(object).where(df[cols].notna(), None).to_dict("records")
    for r in out:
        r["url"] = safe_url(r["url"])
        for k in ("team_id", "game_id"):
            r[k] = int(r[k]) if r[k] is not None else None
        r["match_confidence"] = float(r["match_confidence"]) if r["match_confidence"] is not None else None
        for k in ("first_seen_at", "published_at"):
            if r[k] is not None:
                r[k] = pd.Timestamp(r[k]).isoformat()
    return out


def serve(store: StatusStore, port: int = 8767) -> None:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self._send(200, (HERE / "review.html").read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/pending":
                items = _records(pending(store))
                players = _load_players(store)
                games = _team_games(store)
                for it in items:
                    tid = it.get("team_id")
                    it["options"] = {"players": players.get(tid, []), "game_id": games.get(tid)}
                body = {"items": items, "statuses": list(STATUSES)}
                self._send(200, json.dumps(body).encode(), "application/json")
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path != "/decide":
                self._send(404, b"not found", "text/plain")
                return
            if not is_json_post(self.headers):
                self._send(415, b"JSON only", "text/plain")
                return
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                game = body.get("game_id")
                ev = decide(store, body["evidence_id"], body["decision"], "user", body.get("status"),
                            player_id=body.get("player_id") or None, game_id=int(game) if game else None)
            except (KeyError, ValueError, TypeError) as e:
                self._send(400, json.dumps({"error": str(e)}).encode(), "application/json")
                return
            self._send(200, json.dumps({"ok": True, "evidence_id": ev.evidence_id}).encode(), "application/json")

    print(f"review page on http://localhost:{port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Local review page for pending QB claims")
    ap.add_argument("--warehouse", help="status warehouse directory (default: QB_STATUS_WAREHOUSE or ~/.cache)")
    ap.add_argument("--port", type=int, default=8767)
    a = ap.parse_args(argv)
    if a.warehouse:
        os.environ["QB_STATUS_WAREHOUSE"] = a.warehouse
    serve(StatusStore(open_store()), a.port)


if __name__ == "__main__":
    main()
