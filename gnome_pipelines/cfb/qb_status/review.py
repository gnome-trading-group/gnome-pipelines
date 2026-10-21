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

HERE = Path(__file__).resolve().parent
DECISIONS = {"confirm": "confirmed", "edit": "confirmed", "reject": "rejected"}
FIELDS = ("team", "season", "team_id", "player_raw", "jersey", "player_id", "match_confidence", "game_id", "week",
          "report_slot", "hedge", "paraphrase", "url", "published_at")


def pending(store: StatusStore) -> pd.DataFrame:
    ev = store.evidence()
    decided = set(ev.loc[ev["source_type"] == "manual", "source_id"])
    out = ev[(ev["review_state"] == "pending") & ~ev["evidence_id"].isin(decided)]
    return out.sort_values("first_seen_at", ascending=False).reset_index(drop=True)


def _value(v):
    if v is None or (not isinstance(v, (list, dict)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def decide(store: StatusStore, original_id: str, decision: str, reviewer: str, status: str | None = None,
           now: datetime | None = None) -> Evidence:
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
    new_status = status if decision == "edit" else _value(match.iloc[0]["status"])
    ev = Evidence(evidence_id=evidence_id("manual", original_id, decision, new_status, now.isoformat()),
                  source_type="manual", source_id=original_id, source_org=reviewer, first_seen_at=now,
                  status=new_status, extractor="human", extraction_confidence=1.0,
                  review_state=DECISIONS[decision], reviewer=reviewer, reviewed_at=now,
                  **{k: orig[k] for k in FIELDS if k not in ("team", "season")},
                  team=orig["team"], season=int(orig["season"]))
    store.add_evidence([ev])
    return ev


def _records(df: pd.DataFrame) -> list[dict]:
    cols = ["evidence_id", "source_type", "source_org", "first_seen_at", "published_at", "team", "player_raw",
            "player_id", "status", "hedge", "paraphrase", "url", "report_slot", "match_confidence", "game_id"]
    out = df[cols].astype(object).where(df[cols].notna(), None).to_dict("records")
    for r in out:
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
                body = {"items": _records(pending(store)), "statuses": list(STATUSES)}
                self._send(200, json.dumps(body).encode(), "application/json")
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path != "/decide":
                self._send(404, b"not found", "text/plain")
                return
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
            try:
                ev = decide(store, body["evidence_id"], body["decision"], "user", body.get("status"))
            except (KeyError, ValueError) as e:
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
