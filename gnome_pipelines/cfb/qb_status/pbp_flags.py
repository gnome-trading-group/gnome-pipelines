"""Game-night flags from ESPN's live play-by-play: the starter left, or another QB took over.

ESPN runs about 40 seconds behind the field, which is fine for flags. Flags are team-level evidence (game_id None, the
ESPN event id in source_id) that the engine applies to the team's next game. Fourth-quarter plays with a margin of 21
or more are ignored, because backups routinely mop up in blowouts. With qb_names, only the team's rostered QBs count
as passers, so trick plays and penalty text ("Pass Interference") never flag; a QB change also needs two dropbacks by
the new QB, so a one-play cameo doesn't count.
"""
from __future__ import annotations

import re
from datetime import datetime

from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary"
DROPBACK = re.compile(r"\b(pass|sacked|scrambles?)\b", re.IGNORECASE)
SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}
GARBAGE_MARGIN, POSSESSIONS_WITHOUT_STARTER, CHANGE_DROPBACKS = 21, 2, 2


def live_games(scoreboard: dict) -> list[str]:
    return [e["id"] for e in scoreboard.get("events", [])
            if e.get("competitions") and e["competitions"][0].get("status", {}).get("type", {}).get("state") == "in"]


def _name_tokens(text: str) -> list[str] | None:
    m = DROPBACK.search(text)
    if not m:
        return None
    tokens = text[:m.start()].split()
    while tokens and tokens[-1].lower().rstrip(",") in SUFFIXES:
        tokens = tokens[:-1]
    if not tokens or not re.search(r"[A-Za-z]", tokens[-1]):
        return None
    name = [tokens[-1].strip(",")]
    if len(tokens) > 1 and re.fullmatch(r"[A-Z][a-z'\-]+", tokens[-2]) and "." not in name[0]:
        name.insert(0, tokens[-2])
    return name


def passer_last_name(text: str) -> str | None:
    tokens = _name_tokens(text or "")
    if not tokens:
        return None
    return tokens[-1].split(".")[-1].strip("'").lower() or None


def _raw_name(text: str) -> str:
    return " ".join(_name_tokens(text) or [])


def _compact(text: str | None) -> str:
    return re.sub(r"[^a-z]", "", (text or "").lower())


def _garbage(play: dict) -> bool:
    period = (play.get("period") or {}).get("number") or 0
    margin = abs((play.get("homeScore") or 0) - (play.get("awayScore") or 0))
    return period >= 4 and margin >= GARBAGE_MARGIN


def game_flags(summary: dict, expected_starters: dict[str, str], seen_at: datetime,
               qb_names: dict[str, set[str]] | None = None) -> list[Evidence]:
    header = summary.get("header") or {}
    event, season = str(header.get("id")), (header.get("season") or {}).get("year")
    d = summary.get("drives") or {}
    drives = list(d.get("previous") or []) + ([d["current"]] if d.get("current") else [])
    state = {team: {"seen": False, "since": 0, "others": {}} for team in expected_starters}
    qbs = {team: {_compact(n) for n in names} for team, names in (qb_names or {}).items()}
    out: dict[str, Evidence] = {}

    def flag(team: str, status: str, name: str, text: str) -> None:
        last = name.split()[-1].split(".")[-1].lower() if name else ""
        eid = evidence_id("pbp_flag", event, team, status, last)
        if eid not in out:
            out[eid] = Evidence(evidence_id=eid, source_type="pbp_flag", source_id=event, source_org="espn",
                                first_seen_at=seen_at, team=team, status=status, season=season, player_raw=name,
                                game_id=None, hedge="unclear", paraphrase=text[:140], extractor="pbp_rules",
                                extraction_confidence=0.7, review_state="auto")

    for drive in drives:
        team_info = drive.get("team") or {}
        team = team_info.get("location") or team_info.get("displayName")
        if team not in expected_starters:
            continue
        starter, st = _compact(expected_starters[team]), state[team]
        plays = drive.get("plays") or []
        starter_dropback, garbage = False, False
        for play in plays:
            text = play.get("text") or ""
            garbage = _garbage(play)
            low = text.lower()
            if "injur" in low and (starter in low or starter in _compact(text)):
                flag(team, "left_game_injured", expected_starters[team].title(), text)
            last = _compact(passer_last_name(text))
            if not last or (qb_names is not None and last not in qbs.get(team, set()) and last != starter):
                continue
            if last == starter:
                st["seen"], starter_dropback, st["since"] = True, True, 0
            elif st["seen"] and not garbage:
                st["others"][last] = st["others"].get(last, 0) + 1
                if st["others"][last] >= CHANGE_DROPBACKS:
                    flag(team, "qb_change", _raw_name(text), text)
        if st["seen"] and plays and not starter_dropback and not garbage:
            st["since"] += 1
            if st["since"] >= POSSESSIONS_WITHOUT_STARTER:
                flag(team, "left_game_injured", expected_starters[team].title(),
                     f"{expected_starters[team].title()}: no dropbacks for {st['since']} straight possessions")
    return list(out.values())
