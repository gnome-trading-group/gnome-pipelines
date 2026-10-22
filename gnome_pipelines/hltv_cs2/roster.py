"""Roster history from HLTV team pages and the transfers feed.

Every transfer row is a dated status transition for one person between two sides: the first team container is
where he was, the second where he went, and each carries his status there (blank = active, BENCH, SUB, COACH;
"No team" = none). A team page's all-time transfer list rebuilds its roster timeline exactly (benched, substitute
and coach periods included), so the timeline itself isn't parsed.

The dates are HLTV's effective dates, usually the announcement. The datasets these rows go to only rewrite
changed rows, so a dataset read as of a time shows the moves we had seen by then; that, not HLTV's date, is what
makes them safe for live features.

A team page's players table and coach show the roster as it stands when fetched, nothing older, so they are
stored as dated snapshots.
"""
from __future__ import annotations

import datetime
import logging
import re

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

NO_TEAM = 0
STATUS_TAGS = {"": "active", "BENCH": "benched", "SUB": "substitute", "COACH": "coach"}
_PERSON = re.compile(r"/(player|coach)/(\d+)")
_TEAM = re.compile(r"/team/(\d+)")


def _date(text: str) -> datetime.date | None:
    try:
        return datetime.datetime.strptime(re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text.strip()), "%b %d %Y").date()
    except ValueError:
        return None


def _kind(text: str) -> str:
    """The event, from the movement text; the from/to statuses carry the rest."""
    checks = (
        (" on loan from ", "loan"),
        (" from loan at ", "loan_return"),
        (" is benched on ", "benched"),
        (" is moved to the active roster", "activated"),
        (" is moved to substitute", "to_substitute"),
        (" is moved to coach", "to_coach"),
        (" parts ways with ", "leave"),
        (" transfers from ", "transfer"),
        (" joins ", "join"),
        (" returns to ", "return"),
    )
    for needle, kind in checks:
        if needle in text:
            return kind
    return "other"


def _side(container: Tag | None) -> tuple[int, str | None, str | None]:
    """(team id or NO_TEAM, team name, status on that side or None when there is no team)."""
    if container is None:
        return NO_TEAM, None, None
    link = container if container.name == "a" else container.select_one("a[href*='/team/']")
    match = _TEAM.search(link.get("href", "")) if link else None
    if match is None:
        return NO_TEAM, None, None
    logo = container.select_one(".transfer-team-logo")
    name = (logo.get("title") or logo.get("alt")) if logo else None
    tag = container.select_one(".transfer-team-status")
    status = STATUS_TAGS.get(tag.get_text(strip=True) if tag else "", "other")
    return int(match.group(1)), name, status


def parse_transfer_rows(rows: list[Tag]) -> list[dict]:
    """One cs2_roster_moves row per transfer row, deduplicated on the dataset key."""
    out: dict[tuple, dict] = {}
    for row in rows:
        movement, when = row.select_one(".transfer-movement"), row.select_one(".transfer-date")
        if movement is None or when is None:
            continue
        event_date = _date(when.get_text())
        person = next((a for a in movement.select("a") if _PERSON.search(a.get("href", ""))), None)
        if event_date is None or person is None:
            logger.warning("skipping a transfer row without a date or person: %s", movement.get_text(" ", strip=True)[:80])
            continue
        role, person_id = _PERSON.search(person["href"]).groups()
        containers = row.select(".transfer-team-container")
        from_id, from_name, from_status = _side(containers[0] if containers else None)
        to_id, to_name, to_status = _side(containers[1] if len(containers) > 1 else None)
        text = re.sub(r"\s+", " ", movement.get_text(" ", strip=True))
        move = {
            "event_date": event_date, "person_id": int(person_id), "person_name": person.get_text(strip=True),
            "person_role": role, "kind": _kind(text),
            "from_team_id": from_id, "from_team_name": from_name, "from_status": from_status,
            "to_team_id": to_id, "to_team_name": to_name, "to_status": to_status, "text": text,
        }
        out[(move["person_id"], event_date, from_id, to_id, move["kind"])] = move
    return list(out.values())


def parse_transfers_page_html(soup: BeautifulSoup, page: int) -> dict:
    """The site-wide transfers feed (/transfers, then ?page=1, 2, ... going back): its moves, newest first, and
    whether it links to the next older page."""
    moves = parse_transfer_rows(soup.select(".transfer-row"))
    older = re.compile(rf"/transfers\?(?:.*&)?page={page + 1}(?:&|$)")
    return {"moves": moves, "has_next": any(older.search(a.get("href", "")) for a in soup.select("a[href*='/transfers']"))}


def parse_team_page_html(soup: BeautifulSoup, team_id: int, snapshot_date: datetime.date) -> dict:
    """
    A team page's all-time transfer list (cs2_roster_moves rows) and its current players table and coach
    (cs2_team_roster_snapshots rows for snapshot_date, the day the page was fetched).
    """
    all_time = soup.select_one('[data-toggle-target="all-time"] .transfer-row')
    rows = (all_time.find_parent(attrs={"data-toggle-target": "all-time"}).select(".transfer-row")
            if all_time else soup.select(".transfer-row"))
    moves = parse_transfer_rows(rows)

    roster = []
    for table, role in ((soup.select_one("table.players-table"), "player"), (soup.select_one("table.coach-table"), "coach")):
        if table is None:
            continue
        for tr in table.select("tbody tr"):
            link = tr.select_one("a[href*='/player/'], a[href*='/coach/']")
            match = _PERSON.search(link["href"]) if link else None
            if match is None:
                continue
            cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
            name = link.select_one(".text-ellipsis")
            status = cells[1].upper() if role == "player" and len(cells) > 1 else "COACH"
            time_on_team = cells[2] if role == "player" and len(cells) > 2 else (cells[1] if len(cells) > 1 else None)
            maps_col = 3 if role == "player" else 2
            maps = re.sub(r"\D", "", cells[maps_col]) if len(cells) > maps_col else ""
            roster.append({
                "snapshot_date": snapshot_date, "team_id": team_id, "person_id": int(match.group(2)),
                "person_name": name.get_text(strip=True) if name else link.get_text(" ", strip=True), "role": role,
                "status": status, "time_on_team": time_on_team, "maps_played": int(maps) if maps else None,
            })
    return {"moves": moves, "roster": roster}
