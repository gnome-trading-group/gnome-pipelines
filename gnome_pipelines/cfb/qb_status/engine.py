"""Start probabilities for a team's candidate QBs, as a pure function of the evidence known at as_of (rule v1).

Rule v1:
1. Each candidate's latest availability claim wins. Among official reports a later slot beats an earlier one; a
   non-official claim beats the official one only if we saw it later.
2. Availability comes from AVAILABILITY; left_game_injured 0.5, not_starting 0, returned 1, no claim 1.
3. Role claims, applied in the order seen: expected_starter gives that QB a 0.90 base (the rest share 0.10);
   qb_change gives the new QB the starter's base and halves the old starter's.
4. The starter (highest base) gets base x availability; the rest of the mass goes to the others in proportion to
   base x availability, or equally if those are all zero. With no other known QB it goes to "unknown".
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import AVAILABILITY

RULE_VERSION = "v1"
SLOT_ORDER = {"Initial": 0, "Update 1": 1, "Update 2": 2, "Game Day": 3}
EXTRA_AVAILABILITY = {"left_game_injured": 0.5, "not_starting": 0.0, "returned": 1.0}
AVAILABILITY_STATUSES = set(AVAILABILITY) | set(EXTRA_AVAILABILITY)
ROLE_STATUSES = {"expected_starter", "qb_change"}
TEAM_LEVEL = {"left_game_injured", "qb_change"}
USED_STATES = {"auto", "confirmed"}
UNKNOWN = "unknown"
COLUMNS = ["game_id", "team_id", "player_id", "p_start", "official_status", "official_slot", "evidence_ids", "reason",
           "rule_version"]


def _availability(status: str) -> float:
    return AVAILABILITY.get(status, EXTRA_AVAILABILITY.get(status, 1.0))


def _relevant(evidence: pd.DataFrame, game_id: int, team_id: int, as_of: datetime) -> pd.DataFrame:
    if evidence.empty:
        return evidence.assign(_t=pd.Series(dtype="datetime64[ns, UTC]"))
    seen = pd.to_datetime(evidence["first_seen_at"], utc=True)
    keep = ((seen <= pd.Timestamp(as_of)) & evidence["review_state"].isin(USED_STATES)
            & (evidence["team_id"] == team_id) & evidence["player_id"].notna()
            & ((evidence["game_id"] == game_id) | (evidence["game_id"].isna() & evidence["status"].isin(TEAM_LEVEL))))
    return evidence[keep].assign(_t=seen[keep]).sort_values("_t", kind="stable")


def _apply_roles(base: dict[str, float], ev: pd.DataFrame, used: dict[str, list[str]]) -> dict[str, float]:
    for r in ev[ev["status"].isin(ROLE_STATUSES)].itertuples():
        pid = str(r.player_id)
        if r.status == "expected_starter":
            others = {k: v for k, v in base.items() if k != pid}
            total = sum(others.values())
            for k in others:
                base[k] = 0.10 * (others[k] / total if total > 0 else 1 / len(others))
            base[pid] = 0.90 if others else 1.0
        else:
            starter = max(base, key=lambda k: (base[k], k))
            if starter != pid:
                base[pid], base[starter] = base[starter], base[starter] / 2
                total = sum(base.values())
                base = {k: v / total for k, v in base.items()}
        used[pid].append(r.evidence_id)
    return base


def _latest_claims(ev: pd.DataFrame, pid: str) -> tuple[pd.Series | None, pd.Series | None]:
    claims = ev[(ev["player_id"].astype(str) == pid) & ev["status"].isin(AVAILABILITY_STATUSES)]
    official = claims[claims["source_type"] == "official_report"]
    other = claims[claims["source_type"] != "official_report"]
    latest_official = None
    if len(official):
        ranked = official.assign(_o=official["report_slot"].map(SLOT_ORDER).fillna(-1))
        latest_official = ranked.sort_values(["_o", "_t"], kind="stable").iloc[-1]
    latest_other = other.iloc[-1] if len(other) else None
    winner = latest_official
    if latest_other is not None and (latest_official is None or latest_other["_t"] > latest_official["_t"]):
        winner = latest_other
    return winner, latest_official


def team_status(game_id: int, team_id: int, cands: pd.DataFrame, evidence: pd.DataFrame,
                as_of: datetime) -> pd.DataFrame:
    ev = _relevant(evidence, game_id, team_id, as_of)
    base = {str(r.player_id): float(r.base) for r in cands.itertuples()}
    for pid in ev["player_id"].astype(str):
        base.setdefault(pid, 0.0)
    used: dict[str, list[str]] = {pid: [] for pid in base}
    base = _apply_roles(base, ev, used)

    avail, official, why = {}, {}, {}
    for pid in base:
        winner, latest_official = _latest_claims(ev, pid)
        avail[pid] = _availability(winner["status"]) if winner is not None else 1.0
        if latest_official is not None:
            official[pid] = (latest_official["status"], latest_official["report_slot"])
        if winner is not None:
            used[pid].append(winner["evidence_id"])
            where = f"{winner['source_org']} {winner['report_slot']}" if winner["source_type"] == "official_report" \
                else winner["source_type"]
            why[pid] = f"{winner['status']} ({where})"

    p = {k: 0.0 for k in base}
    if base:
        starter = max(base, key=lambda k: (base[k], k))
        p[starter] = base[starter] * avail[starter]
        rest = 1.0 - p[starter]
        others = [k for k in base if k != starter]
        weights = {k: base[k] * avail[k] for k in others}
        total = sum(weights.values())
        if total > 0:
            for k in others:
                p[k] = rest * weights[k] / total
        elif others:
            pool = [k for k in others if avail[k] > 0] or others
            for k in pool:
                p[k] = rest / len(pool)
        elif rest > 1e-12:
            p[UNKNOWN] = rest
            why[UNKNOWN] = "no other known QB"

    rows = []
    for pid, prob in p.items():
        status, slot = official.get(pid, (None, None))
        rows.append({"game_id": game_id, "team_id": team_id, "player_id": pid, "p_start": prob,
                     "official_status": status, "official_slot": slot, "evidence_ids": used.get(pid, []),
                     "reason": why.get(pid, "base weight, no news"), "rule_version": RULE_VERSION})
    return pd.DataFrame(rows, columns=COLUMNS)
