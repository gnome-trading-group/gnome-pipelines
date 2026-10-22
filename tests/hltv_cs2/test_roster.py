"""Roster parsing against saved HLTV pages: team pages (Rooster; Natus Vincere, with loans and benchings), a match
with three stand-ins, and the transfers feed. Fetched 2026-10-19."""
import collections
import datetime
import gzip
import pathlib
import re

import pytest
from bs4 import BeautifulSoup

from gnome_pipelines.hltv_cs2.roster import NO_TEAM, parse_team_page_html, parse_transfers_page_html
from gnome_pipelines.hltv_cs2.scraper import parse_match_detail_html

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
FETCHED = datetime.date(2026, 10, 19)
ROOSTER, NAVI, MOUZ = 9881, 4608, 4494
TIMELINE_STATUS = {"player-active": "active", "player-benched": "benched", "player-coach": "coach",
                   "player-substitute": "substitute"}


def page(name: str) -> BeautifulSoup:
    return BeautifulSoup(gzip.open(FIXTURES / f"{name}.html.gz", "rt", errors="ignore").read(), "lxml")


@pytest.fixture(scope="module")
def navi():
    return parse_team_page_html(page("hltv_team_navi"), NAVI, FETCHED)


@pytest.fixture(scope="module")
def rooster():
    return parse_team_page_html(page("hltv_team_rooster"), ROOSTER, FETCHED)


def moves_of(parsed: dict, name: str) -> list[tuple]:
    return sorted((m["event_date"].isoformat(), m["kind"], m["from_team_id"], m["from_status"], m["to_team_id"], m["to_status"])
                  for m in parsed["moves"] if m["person_name"] == name)


def test_a_loan_and_return_are_transitions_between_the_owning_and_borrowing_team(navi):
    assert moves_of(navi, "jL")[-3:] == [
        ("2026-04-17", "loan", NAVI, "benched", MOUZ, "active"),
        ("2026-05-23", "loan_return", MOUZ, "active", NAVI, "benched"),
        ("2026-06-30", "leave", NAVI, "benched", NO_TEAM, None),
    ]


def test_status_changes_within_a_team_keep_the_team_on_both_sides(navi):
    kinds = {m["kind"]: m for m in navi["moves"]}
    for kind, before, after in (("benched", "active", "benched"), ("activated", "substitute", "active"),
                                ("to_substitute", "active", "substitute"), ("to_coach", "active", "coach")):
        move = kinds[kind]
        assert (move["from_team_id"], move["from_status"], move["to_team_id"], move["to_status"]) == (NAVI, before, NAVI, after), kind


def test_every_row_parses_to_a_known_kind(navi, rooster):
    assert {m["kind"] for m in navi["moves"] + rooster["moves"]} - {"other"} == {m["kind"] for m in navi["moves"] + rooster["moves"]}


def test_the_current_roster_is_a_dated_snapshot_of_players_and_coach(rooster):
    snapshot = sorted((r["person_name"], r["role"], r["status"]) for r in rooster["roster"])
    assert snapshot == [("ADK", "player", "STARTER"), ("JD", "coach", "COACH"), ("SkulL", "player", "STARTER"),
                        ("rekonz", "player", "STARTER")]
    assert {r["snapshot_date"] for r in rooster["roster"]} == {FETCHED} and {r["team_id"] for r in rooster["roster"]} == {ROOSTER}


def timeline_days(soup: BeautifulSoup) -> dict[str, collections.Counter]:
    """Days in team per person and status, from the page's all-time roster timeline."""
    tl = soup.select_one('[data-toggle="players-timeline"][data-toggle-target="all-time"]')
    out: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for period in tl.select(".timeline-player-period"):
        status = next(v for c, v in TIMELINE_STATUS.items() if c in period["class"])
        tip = soup.select_one(f'#{period["data-tooltip-id"]}')
        name = re.search(r'"(.+?)"', tip.select_one(".player-name").get_text(strip=True)).group(1)
        days = {r.select_one(".stat-name").get_text(strip=True): r.select_one(".stat-value").get_text(strip=True)
                for r in tip.select(".stat-row")}["Days in team"]
        out[name][status] += int(days.replace(",", ""))
    return out


def rebuilt_days(moves: list[dict], team_id: int) -> dict[str, collections.Counter]:
    """Days per person and status on team_id, replaying the moves: each sets the person's status there."""
    out: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    by_person = collections.defaultdict(list)
    for m in moves:
        by_person[m["person_name"]].append(m)
    for name, ms in by_person.items():
        status, since = None, None
        for m in sorted(ms, key=lambda m: m["event_date"]):
            if status is not None:
                out[name][status] += (m["event_date"] - since).days
            status = m["to_status"] if m["to_team_id"] == team_id else None
            since = m["event_date"]
        if status is not None:
            out[name][status] += (FETCHED - since).days
    return out


@pytest.mark.parametrize("fixture, team_id", [("hltv_team_rooster", ROOSTER), ("hltv_team_navi", NAVI)])
def test_the_moves_rebuild_the_pages_roster_timeline(fixture, team_id):
    soup = page(fixture)
    timeline = timeline_days(soup)
    rebuilt = rebuilt_days(parse_team_page_html(soup, team_id, FETCHED)["moves"], team_id)

    assert set(rebuilt) == set(timeline)
    for name, statuses in timeline.items():
        for status, days in statuses.items():
            assert abs(rebuilt[name][status] - days) <= 2, (name, status, rebuilt[name], dict(statuses))


def test_the_lineup_box_marks_stand_ins_and_names_each_boxs_team():
    soup = page("hltv_match_standins")
    lineups = parse_match_detail_html(soup, 2399544)["lineups"]

    rooster = {r["player_name"]: r for r in lineups if r["team_id"] == ROOSTER}
    assert sorted(n for n, r in rooster.items() if r["is_standin"]) == ["chelleos", "cookie", "ju1ces"]
    assert [r["slot"] for r in rooster.values()] == [0, 1, 2, 3, 4]
    assert not any(r["is_standin"] for r in lineups if r["team_id"] != ROOSTER)
    assert {r["team_name"] for r in lineups} == {"FlyQuest", "Rooster"}


def test_the_lineup_box_gives_every_players_nationality_and_the_header_each_teams_flag():
    parsed = parse_match_detail_html(page("hltv_match_standins"), 2399544)
    countries = {r["player_name"]: r["country"] for r in parsed["lineups"]}
    assert all(countries.values())
    assert countries["nettik"] == countries["chelleos"] == "New Zealand"
    assert countries["jks"] == "Australia"
    row = parsed["rows"][0]
    assert (row["team_a_country"], row["team_b_country"]) == ("Australia", "Australia")


def test_the_transfers_feed_is_newest_first_and_links_the_next_page():
    parsed = parse_transfers_page_html(page("hltv_transfers"), 0)

    dates = [m["event_date"] for m in parsed["moves"]]
    assert parsed["has_next"] and len(parsed["moves"]) == 50
    assert dates == sorted(dates, reverse=True)
    assert not parse_transfers_page_html(page("hltv_transfers"), 5)["has_next"]
