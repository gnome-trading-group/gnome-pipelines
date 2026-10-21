"""The service's own health: report lags, kickoff accuracy, gaps and errors, and a plain-text weekly summary."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import evidence_id
from gnome_pipelines.datasets.qb_status import HEALTH


def health_row(kind: str, at: datetime, detail: str, value: float | None = None, team_id: int | None = None,
               game_id: int | None = None) -> dict:
    return {"health_id": evidence_id("health", kind, pd.Timestamp(at).isoformat(), team_id, game_id, detail),
            "kind": kind, "at": pd.Timestamp(at), "team_id": team_id, "game_id": game_id, "value": value,
            "detail": detail[:500]}


def report_lag_rows(schedule: pd.DataFrame, observed: dict[tuple[str, int, str], datetime]) -> list[dict]:
    rows = []
    for r in schedule.itertuples(index=False):
        seen = observed.get((r.conference, int(r.game_id), r.report_slot))
        if seen is None or r.state == "arrived" or pd.isna(r.expected_at):
            continue
        lag = (pd.Timestamp(seen) - pd.Timestamp(r.expected_at)).total_seconds()
        rows.append(health_row("report_lag", seen, f"{r.conference} {r.report_slot}", lag, game_id=int(r.game_id)))
    return rows


def kickoff_accuracy(snapshots: pd.DataFrame, actual: pd.DataFrame, at: datetime) -> list[dict]:
    rows = []
    starters = {(int(r.game_id), int(r.team_id)): str(r.player_id) for r in actual.itertuples(index=False)}
    for (game_id, team_id), g in snapshots.groupby(["game_id", "team_id"]):
        truth = starters.get((int(game_id), int(team_id)))
        if truth is None:
            continue
        top = str(g.sort_values("p_start", ascending=False)["player_id"].iloc[0])
        rows.append(health_row("kickoff_accuracy", at, f"expected {top}, started {truth}", 1.0 if top == truth else 0.0,
                               team_id=int(team_id), game_id=int(game_id)))
    return rows


def health_report(health: pd.DataFrame, since: datetime) -> str:
    h = health[pd.to_datetime(health["at"], utc=True) >= pd.Timestamp(since)] if len(health) else health
    if h.empty:
        return f"No health rows since {pd.Timestamp(since):%Y-%m-%d %H:%M} UTC."
    lines = [f"QB status health since {pd.Timestamp(since):%Y-%m-%d %H:%M} UTC"]
    for kind, g in h.groupby("kind"):
        v = g["value"].dropna()
        stats = f", median {v.median():.1f}, worst {v.max():.1f}" if len(v) else ""
        lines.append(f"- {kind}: {len(g)}{stats}")
    return "\n".join(lines)


__all__ = ["HEALTH", "health_report", "health_row", "kickoff_accuracy", "report_lag_rows"]
