"""Official conference availability reports, from the HD Intelligence feed behind the conference report pages.

One call returns a conference's whole season. Each (player, game, report slot) status becomes one evidence row whose
id is deterministic, so re-polling the same report adds nothing and a changed status adds a new row.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import requests

from gnome_pipelines.cfb.qb_status.crosswalk import MATCH_OK, PlayerIndex, TeamIndex, _split_name, norm_team
from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id, parse_report_status

FEED = "https://app.hdintelligence.com/api/get-archive-public"
CONFERENCES = ("SEC", "ACC", "B10", "B12", "American", "MAC")
HEADERS = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "Mozilla/5.0",
           "Origin": "https://app.hdintelligence.com"}


# A conference's own feed sometimes uses a bare name another FBS school owns
CONFERENCE_TEAM_ALIASES = {("MAC", "miami"): "Miami (OH)"}
PAST_GRACE = timedelta(days=2)


class FeedError(RuntimeError):
    pass


def _team(teams: TeamIndex, conference: str, name: str, season: int) -> int | None:
    return teams.team_id(CONFERENCE_TEAM_ALIASES.get((conference, norm_team(name)), name), season)


def fetch_conference(session: requests.Session, conference: str) -> dict:
    body = {"sport": "Football", "organization": conference, "conference": conference}
    r = session.post(FEED, json=body, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        raise FeedError(f"{conference}: HTTP {r.status_code}")
    try:
        d = r.json()
    except ValueError as e:
        raise FeedError(f"{conference}: not JSON") from e
    if not isinstance(d.get("data"), list) or not isinstance(d.get("report_days"), list):
        raise FeedError(f"{conference}: unexpected shape, keys {sorted(d)[:8]}")
    return d


def _season(d: date) -> int:
    return d.year if d.month >= 7 else d.year - 1


def _week_date(text: str) -> date | None:
    try:
        return datetime.strptime(text.strip(), "%a, %d %b %Y %H:%M:%S %Z").date()
    except (ValueError, AttributeError):
        return None


def _strip_venue(text: str) -> str:
    t = (text or "").strip()
    for prefix in ("at ", "vs. ", "vs "):
        if t.lower().startswith(prefix):
            return t[len(prefix):]
    return t


def find_game(games: pd.DataFrame, team_id: int, opponent_text: str, week_date: date, teams: TeamIndex,
              conference: str = "") -> int | None:
    kick = pd.to_datetime(games["kickoff"], utc=True).dt.date
    near = games[((games["home_id"] == team_id) | (games["away_id"] == team_id))
                 & kick.between(week_date - timedelta(days=1), week_date + timedelta(days=1))]
    opp = _team(teams, conference, _strip_venue(opponent_text), _season(week_date))
    if opp is not None:
        near = near[(near["home_id"] == opp) | (near["away_id"] == opp)]
    return int(near["game_id"].iloc[0]) if len(near) == 1 else None


def _could_be_qb(players: PlayerIndex, team_id: int, name: str) -> bool:
    _, last = _split_name(name)
    qbs = players.qbs(team_id)
    return any(_split_name(n)[1] == last for n in qbs["name"])


def report_evidence(conference: str, payload: dict, seen_at: datetime, teams: TeamIndex,
                    players: dict[int, PlayerIndex], games: pd.DataFrame, *, backfilled: bool = False) -> list[Evidence]:
    slots = payload.get("report_days") or []
    out: list[Evidence] = []
    for row in payload.get("data") or []:
        wd = _week_date(row.get("Week", ""))
        if wd is None:
            continue
        season = _season(wd)
        team_name = row.get("TeamDisplay") or row.get("Team") or ""
        team_id = _team(teams, conference, team_name, season)
        if team_id is None and row.get("Team"):
            team_id = _team(teams, conference, row["Team"], season)
        player_raw = (row.get("Player") or "").strip()
        jersey = (row.get("Number") or "").strip().lstrip("#") or None
        player_id, confidence = None, 0.0
        if team_id is not None:
            index = players.get(season)
            if index is None:
                continue
            m = index.match(team_id, player_raw, jersey)
            if m.player_id is not None and m.confidence >= MATCH_OK:
                if m.player_id not in set(index.qbs(team_id)["player_id"]):
                    continue
                player_id, confidence = m.player_id, m.confidence
            elif not _could_be_qb(index, team_id, player_raw):
                continue
            else:
                player_id, confidence = m.player_id, m.confidence
        game_id = find_game(games, team_id, row.get("OpponentDisplay") or row.get("Opponent", ""), wd, teams,
                            conference) if team_id is not None else None
        if team_id is not None and game_id is None and wd < seen_at.date() - PAST_GRACE:
            continue
        sure = team_id is not None and game_id is not None and player_id is not None and confidence >= MATCH_OK
        for slot in slots:
            status = parse_report_status(row.get(slot))
            if status is None:
                continue
            out.append(Evidence(
                evidence_id=evidence_id("official_report", conference, team_name, player_raw, wd.isoformat(), slot,
                                        status),
                source_type="official_report", source_id=f"{conference}:{team_name}:{player_raw}:{wd}:{slot}",
                source_org=conference, first_seen_at=seen_at, team=team_name, status=status, season=season,
                backfilled=backfilled, team_id=team_id, player_raw=player_raw, jersey=jersey, player_id=player_id,
                match_confidence=confidence, game_id=game_id, week=row.get("WeekNum"), report_slot=slot,
                hedge="confirmed", extractor="report", extraction_confidence=1.0,
                review_state="auto" if sure else "pending"))
    return out
