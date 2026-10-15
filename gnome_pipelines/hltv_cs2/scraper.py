"""
HLTV page parsers for CS2 match history and team rankings, and the ZenRows fetch.

Parses three page types:
  - /results?startDate=...&endDate=...  — paginated match result listings
  - /matches/{id}/...                   — per-match detail (lineups, veto, stats)
  - /ranking/teams/{year}/{month}/{day} — weekly team rankings

collect.py walks them; fetch.py decides where pages come from. The ZenRows fetch needs
ZENROWS_API_KEY, or gnome/pipelines/zenrows in Secrets Manager, and uses js_render +
premium_proxy (25 credits/page) to get past Cloudflare on HLTV.
"""
from __future__ import annotations

import datetime
import logging
import re
import time

import pandas as pd
from bs4 import BeautifulSoup
from zenrows import ZenRowsClient

from gnomepy_research.models.cs2.hltv.config import MAP_POOL
from gnome_pipelines.credentials import secret
from gnome_pipelines.hltv_cs2.html_cache import is_complete

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.hltv.org"
_ZENROWS_PARAMS = {"js_render": True, "premium_proxy": True}


class _Session:
    """ZenRows session — residential proxy + JS render bypasses Cloudflare on HLTV."""

    def __init__(self) -> None:
        self._client = ZenRowsClient(secret("zenrows", "ZENROWS_API_KEY"))

    def get_html(self, url: str) -> str | None:
        try:
            resp = self._client.get(url, params=_ZENROWS_PARAMS)
            if resp.status_code == 200:
                return resp.text
            logger.warning("ZenRows HTTP %s for %s", resp.status_code, url)
            return None
        except Exception as exc:
            logger.warning("ZenRows error for %s: %s", url, exc)
            return None


def _match_url(match_id: int) -> str:
    # HLTV ignores the slug but needs one: /matches/<id>/ alone serves the home page
    return f"{_BASE_URL}/matches/{match_id}/match"


def _make_session() -> _Session:
    return _Session()


def _get_html(session: _Session, url: str, retries: int = 3) -> str | None:
    """
    Fetch a page through ZenRows, retrying on failure, rate limits and truncation.

    A page without its footer is treated as a failed fetch: the local fetcher once
    cached listings cut off mid-render and silently lost 406 series, so a partial
    page is never parsed here either.
    """
    for attempt in range(retries):
        html = session.get_html(url)
        if html is None:
            time.sleep(5 * (attempt + 1))
            continue
        title = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
        if title and "429" in title.group(1):
            wait = 60 * (attempt + 1)
            logger.warning("Rate limited — sleeping %ds", wait)
            time.sleep(wait)
            continue
        if not is_complete(html):
            logger.warning("incomplete page for %s (attempt %d)", url, attempt + 1)
            time.sleep(5 * (attempt + 1))
            continue
        return html
    return None


def _get(session: _Session, url: str, retries: int = 3) -> BeautifulSoup | None:
    html = _get_html(session, url, retries)
    return BeautifulSoup(html, "lxml") if html is not None else None


# ---------------------------------------------------------------------------
# Match result listings
# ---------------------------------------------------------------------------

def parse_match_ids_from_html(html: str) -> tuple[list[tuple[int, str, int]], bool]:
    """Parse match IDs, URLs, and star ratings from a results page HTML. Returns ([(id, url, stars)], has_more_pages)."""
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select(".results-all a.a-reset[href*='/matches/']")
    matches = []
    for link in links:
        href = link.get("href", "")
        parts = href.split("/")
        if len(parts) >= 3:
            try:
                stars_el = link.select_one(".stars")
                stars = len(stars_el.select(".star")) if stars_el else 0
                matches.append((int(parts[2]), href, stars))
            except ValueError:
                pass
    return matches, len(links) >= 100


# ---------------------------------------------------------------------------
# Match detail pages
# ---------------------------------------------------------------------------

def _ranking_value(ranking_div, selector: str, prefix: str) -> int | None:
    """Pull an integer rank out of a '#12'-style ranking link, or None if absent."""
    link = ranking_div.select_one(selector)
    if not link:
        return None
    text = link.get_text(strip=True).replace(prefix, "").strip().lstrip("#")
    try:
        return int(text)
    except ValueError:
        return None


def _avg_rating(players: list[dict]) -> float:
    rated = [p["rating"] for p in players if p["rating"] is not None]
    return sum(rated) / len(rated) if rated else float("nan")


def _picked_map_value(picked_maps: dict, map_name: str, team_a_name: str) -> float:
    picker = picked_maps.get(map_name)
    if picker is None:
        return float("nan")
    return 1.0 if picker == team_a_name else 0.0


def _parse_bo_type(fmt: str) -> int:
    m = re.search(r"Best of (\d+)", fmt)
    return int(m.group(1)) if m else 1


_VETO_ACTION = re.compile(
    r"^(?P<order>\d+)\.\s+(?P<team>.+?)\s+(?P<action>removed|picked)\s+(?P<map>[A-Za-z0-9]+)\s*$", re.I)
_VETO_LEFTOVER = re.compile(
    r"^(?P<order>\d+)\.\s+(?P<map>[A-Za-z0-9]+)\s+was left over\s*$", re.I)


def _parse_veto(soup: BeautifulSoup, team_names: list[str], team_ids: list, context: str = "") -> list[dict]:
    """
    The full pick/ban sequence, in order.

    Previously only lines containing "picked" survived, so every ban and the
    ordering were discarded. The picker was also identified by testing whether a
    team name appeared *as a substring* of the line — so a team called "G2"
    matched inside "G2 Ares", and "Spirit" inside "Team Spirit". That silently
    mis-attributed or dropped picks, which is the likely source of much of the
    ~19.6% team_a_picked_map NaN rate.
    """
    veto_boxes = soup.select(".veto-box")
    veto_box = veto_boxes[-1] if veto_boxes else None
    if not veto_box:
        return []

    short_to_map = {m.replace("de_", "").lower(): m for m in MAP_POOL}
    by_name = {n.strip().lower(): (n, i) for i, n in enumerate(team_names[:2]) if n}

    steps = []
    for item in veto_box.select(".padding > div"):
        text = item.get_text(" ", strip=True)
        if not text:
            continue
        m = _VETO_ACTION.match(text)
        if m:
            raw_team = m.group("team").strip()
            resolved = by_name.get(raw_team.lower())
            if resolved is None:
                logger.warning("%s: veto line %r names a team that is neither %r nor %r",
                               context, text, team_names[0], team_names[1])
            map_name = short_to_map.get(m.group("map").lower())
            steps.append({
                "order": int(m.group("order")),
                "team_name": resolved[0] if resolved else raw_team,
                "team_id": team_ids[resolved[1]] if resolved else None,
                "action": m.group("action").lower(),
                "map_name": map_name,
            })
            continue
        m = _VETO_LEFTOVER.match(text)
        if m:
            steps.append({
                "order": int(m.group("order")),
                "team_name": None,
                "team_id": None,
                "action": "left_over",
                "map_name": short_to_map.get(m.group("map").lower()),
            })
    return steps


def _stat_float(cell) -> float | None:
    try:
        return float(cell.get_text(strip=True).replace("%", "").replace("+", ""))
    except (ValueError, AttributeError):
        return None


def _kd_kills(cell) -> float | None:
    # K-D format: "23-18" — take kills only
    try:
        return float(cell.get_text(strip=True).split("-")[0])
    except (ValueError, AttributeError, IndexError):
        return None


def _kd_deaths(cell) -> float | None:
    # K-D format: "23-18" — take deaths only
    try:
        return float(cell.get_text(strip=True).split("-")[1])
    except (ValueError, AttributeError, IndexError):
        return None


_STAT_SIDES = {"all": ".totalstats", "ct": ".ctstats", "t": ".tstats"}


def _cell(row, *classes):
    """First cell carrying all of `classes`. Positional indexing is what let four columns go unread."""
    for td in row.find_all("td"):
        got = set(td.get("class") or [])
        if all(c in got for c in classes):
            return td
    return None


def _parse_player_stats(container, context: str = "", has_sub: bool = False, side: str = "all") -> list[dict]:
    """
    Per-player stats for one side of the map.

    HLTV ships three tables per team in every {mapstatsid}-content div — overall,
    CT-only and T-only — with the latter two merely CSS-hidden. Only the overall
    table was ever read, and of its nine columns only five, so the side split, the
    economy-adjusted variants and roundSwing were all discarded at parse time.

    roundSwing is the notable one: it is HLTV's own per-player change in round-win
    probability, already denominated in the units the model predicts.

    Cells are located by class rather than position, so a column order change
    surfaces as missing data instead of silently mis-read data.
    """
    selector = _STAT_SIDES.get(side, ".totalstats")
    players = []
    seen_teams: set[int | str] = set()
    for table in container.select(selector):
        all_rows = table.select("tbody tr")
        if not all_rows:
            continue
        header_cols = all_rows[0].find_all("td")
        team_name = header_cols[0].get_text(strip=True) if header_cols else ""
        team_link = all_rows[0].select_one("a[href*='/team/']")
        team_id = _href_id(team_link)
        if len(header_cols) != 9:
            logger.warning("Unexpected stats table column count %d for team %r [%s]", len(header_cols), team_name, context)
        key = team_id if team_id is not None else team_name
        if key in seen_teams:
            continue
        seen_teams.add(key)
        players_before = len(players)
        for row in all_rows[1:]:
            player_link = row.select_one("a[href*='/player/']")
            if not player_link:
                continue
            nick_el = player_link.select_one(".player-nick")
            player_name = nick_el.get_text(strip=True) if nick_el else player_link.get_text(strip=True)
            kd = _cell(row, "kd", "traditional-data") or _cell(row, "kd")
            ekd = _cell(row, "kd", "eco-adjusted-data")
            rating_cell = _cell(row, "rating")
            if rating_cell is None:
                logger.warning("Could not locate rating cell for player %r team %r [%s]", player_name, team_name, context)
            players.append({
                "team_name": team_name,
                "team_id": team_id,
                "player_id": _href_id(player_link),
                "player_name": player_name,
                "side": side,
                "kills": _kd_kills(kd),
                "deaths": _kd_deaths(kd),
                "ek": _kd_kills(ekd),
                "ed": _kd_deaths(ekd),
                "round_swing_pct": _stat_float(_cell(row, "roundSwing")),
                "adr": _stat_float(_cell(row, "adr", "traditional-data") or _cell(row, "adr")),
                "eadr": _stat_float(_cell(row, "adr", "eco-adjusted-data")),
                "kast": _stat_float(_cell(row, "kast", "traditional-data") or _cell(row, "kast")),
                "ekast": _stat_float(_cell(row, "kast", "eco-adjusted-data")),
                "rating": _stat_float(rating_cell),
            })
        team_player_count = len(players) - players_before
        if team_player_count == 0:
            logger.warning("Parsed 0 players for team %r on side %r — possible selector change [%s]", team_name, side, context)
        elif team_player_count != 5 and side == "all":
            if has_sub:
                logger.info("Parsed %d players for team %r (substitute in lineup) [%s]", team_player_count, team_name, context)
            else:
                logger.warning("Parsed %d players for team %r (expected 5) [%s]", team_player_count, team_name, context)
    return players


def _same_team(player: dict, team_id, team_name: str) -> bool:
    """Prefer the stable id; fall back to the display name only when the id is missing."""
    if player.get("team_id") is not None and team_id is not None:
        return player["team_id"] == team_id
    return player.get("team_name") == team_name


_STAGE_MARKERS = {
    "is_elimination": ("elimination", "lower bracket", "decider"),
    "is_qualifier": ("qualifier", "open qualifier", "closed qualifier"),
    "is_playoff": ("bracket", "final", "semi-final", "quarter-final", "playoff"),
    "is_group": ("group", "swiss"),
}


def _parse_event_context(fmt: str) -> dict:
    """
    Stage and stakes out of the format blurb, e.g.
    "Best of 3 (LAN)\n\n* Swiss round 2 (teams with a 1-0 record)".

    Only bo_type and is_lan were ever read off this string. Whether a match is an
    elimination game, a qualifier or a dead rubber changes how hard teams try, and
    it costs nothing to extract.
    """
    text = (fmt or "").lower()
    stage = ""
    for line in (fmt or "").splitlines():
        line = line.strip()
        if line.startswith("*") and not line.startswith("**"):
            stage = line.lstrip("*").strip()
            break
    flags = {k: float(any(m in text for m in markers)) for k, markers in _STAGE_MARKERS.items()}
    return {"stage_text": stage, **flags}


def _parse_recent_form(soup: BeautifulSoup, match_id: int, match_date) -> list[dict]:
    """
    Each team's recent results as shown on the match page.

    HLTV renders four tables but 2-3 are byte-identical duplicates of 0-1, so only
    the first two are read. The "16 weeks ago" label is relative to when the page
    was *fetched*, not when the match was played, so it is kept only as a fallback —
    the row's href carries a real match id, which is both exact and doubles as a
    crawl frontier into matches older than our nine-month history.
    """
    rows = []
    for team_index, table in enumerate(soup.select("table.past-matches-table")[:2]):
        for position, tr in enumerate(table.select("tr")):
            opponent = tr.select_one("td.past-matches-team")
            link = tr.select_one("td.past-matches-map a[href*='/matches/']")
            score = tr.select_one("td.past-matches-score")
            if opponent is None:
                continue
            text = opponent.get_text(" ", strip=True)
            weeks = re.search(r"(\d+)\s+weeks?\s+ago", text)
            scores = re.findall(r"(\d+)", score.get_text(" ", strip=True)) if score else []
            rows.append({
                "match_id": match_id,
                "match_date": match_date,
                "team_index": team_index,          # 0 = team_a, 1 = team_b
                "position": position,
                "opponent_team_id": _href_id(opponent.select_one("a[href*='/team/']")),
                "opponent_name": re.sub(r"\s*\d+\s+weeks?\s+ago\s*$", "", text).strip(),
                "ref_match_id": _href_id(link),
                "weeks_ago": int(weeks.group(1)) if weeks else None,
                "score_for": int(scores[0]) if len(scores) >= 2 else None,
                "score_against": int(scores[1]) if len(scores) >= 2 else None,
            })
    return rows


def _parse_h2h(soup: BeautifulSoup, match_id: int, match_date) -> tuple[dict, list[dict]]:
    """
    Head-to-head, both the lifetime aggregate and the individual meetings.

    Our own h2h_win_rate is NaN on ~72% of rows because the match history only
    spans nine months. HLTV's box reaches back over the teams' whole shared
    history, and the listing carries real timestamps rather than relative labels.

    Returns (scalars_for_the_match_row, meeting_rows).
    """
    scalars = {"h2h_team_a_wins": None, "h2h_team_b_wins": None, "h2h_overtimes": None}
    box = soup.select_one(".head-to-head")
    if box:
        numbers = []
        for col in box.select(".flexbox-column"):
            m = re.search(r"(\d+)", col.get_text(" ", strip=True))
            numbers.append(int(m.group(1)) if m else None)
        if len(numbers) >= 3:
            scalars["h2h_team_a_wins"], scalars["h2h_overtimes"], scalars["h2h_team_b_wins"] = numbers[:3]

    meetings = []
    listing = soup.select_one(".head-to-head-listing")
    for row in (listing.select("tr") if listing else []):
        date_td = row.select_one("td.date")
        stamp = date_td.select_one("[data-unix]") if date_td else None
        unix = (stamp or date_td or {}).get("data-unix") if (stamp or date_td) else None
        result = row.select_one("td.result")
        scores = re.findall(r"(\d+)", result.get_text(" ", strip=True)) if result else []
        map_td = row.select_one("td.map")
        map_text = map_td.get_text(" ", strip=True).split() if map_td else []
        map_name = f"de_{map_text[-1].lower()}" if map_text else None
        t1, t2 = row.select_one("td.team1"), row.select_one("td.team2")
        meetings.append({
            "match_id": match_id,
            "match_date": match_date,
            "h2h_date": datetime.datetime.fromtimestamp(int(unix) / 1000, tz=datetime.timezone.utc) if unix else None,
            "team1_name": t1.get_text(" ", strip=True) if t1 else None,
            "team2_name": t2.get_text(" ", strip=True) if t2 else None,
            "team1_won": ("winner" in (t1.get("class") or [])) if t1 else None,
            "event_name": (row.select_one("td.event").get_text(" ", strip=True) if row.select_one("td.event") else None),
            "map_name": map_name if map_name in MAP_POOL else None,
            "team1_score": int(scores[0]) if len(scores) >= 2 else None,
            "team2_score": int(scores[1]) if len(scores) >= 2 else None,
        })
    return scalars, meetings


def _parse_lineups(soup: BeautifulSoup) -> list[list[dict]]:
    """
    The announced starting five per team, with AWP/IGL roles where HLTV marks them.

    This is the honest source for roster features. Rosters were previously derived
    from the per-map stats tables, which only exist once the match is over — so a
    roster feature built from them could never have been available at prediction
    time. The lineup box is rendered before the match starts.
    """
    out: list[list[dict]] = []
    for box in soup.select(".lineups .lineup")[:2]:
        players = []
        for cell in box.select(".player"):
            # completed pages link each photo to the player; upcoming and live pages
            # render a compare widget with only a data attribute instead
            link = cell.select_one("a[href*='/player/']")
            compare = cell.select_one("[data-player-id]")
            player_id = _href_id(link) if link else _int_or_none(compare.get("data-player-id")) if compare else None
            if player_id is None:
                continue
            # the lineup box holds only a photo, so the nick comes from the image
            # title ("Danil 'molodoy' Golubenko") or, failing that, the href slug
            name = ""
            img = cell.select_one("img[title], img[alt]")
            if img:
                blurb = img.get("title") or img.get("alt") or ""
                quoted = re.search(r"['\u2018\u2019\"](.+?)['\u2018\u2019\"]", blurb)
                if quoted:
                    name = quoted.group(1)
            if not name and link:
                parts = (link.get("href") or "").rstrip("/").split("/")
                name = parts[-1] if parts else ""
            # roles live in a sibling .role-pills container, not on the link's parent
            players.append({
                "player_id": player_id,
                "player_name": name,
                "is_awp": cell.select_one(".role-pill--awp") is not None,
                "is_igl": cell.select_one(".role-pill--igl") is not None,
            })
        seen, unique = set(), []
        for pl in players:
            if pl["player_id"] in seen:
                continue
            seen.add(pl["player_id"])
            unique.append(pl)
        out.append(unique)
    while len(out) < 2:
        out.append([])
    return out


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _href_id(link) -> int | None:
    """The numeric id out of an HLTV /team/{id}/slug or /player/{id}/slug href."""
    if not link:
        return None
    parts = (link.get("href") or "").split("/")
    try:
        return int(parts[2]) if len(parts) >= 3 else None
    except ValueError:
        return None


def _parse_half_scores(mapholder_el, context: str = "") -> dict:
    hs = mapholder_el.select_one(".results-center-half-score")
    if not hs:
        return {"team_a_h1_score": None, "team_b_h1_score": None, "team_a_started_ct": None}
    # Spans with a class are the score values; classless spans are separators (: and ;)
    spans = [s for s in hs.select("span") if s.get("class")]
    if len(spans) < 4:
        logger.warning("Half-score element found but only %d scored spans (expected >=4) [%s]", len(spans), context)
        return {"team_a_h1_score": None, "team_b_h1_score": None, "team_a_started_ct": None}
    try:
        team_a_h1 = int(spans[0].get_text(strip=True))
        team_b_h1 = int(spans[1].get_text(strip=True))
        team_a_side_h1 = (spans[0].get("class") or [""])[0]
        if team_a_side_h1 not in ("ct", "t"):
            logger.warning("Unexpected starting-side class %r (expected 'ct' or 't') [%s]", team_a_side_h1, context)
        if team_a_h1 + team_b_h1 > 15:
            logger.warning("H1 scores sum to %d > 15 (impossible in regulation) [%s]", team_a_h1 + team_b_h1, context)
        return {
            "team_a_h1_score": team_a_h1,
            "team_b_h1_score": team_b_h1,
            "team_a_started_ct": int(team_a_side_h1 == "ct"),
        }
    except (ValueError, IndexError):
        logger.warning("Failed to parse half-score values [%s]", context)
        return {"team_a_h1_score": None, "team_b_h1_score": None, "team_a_started_ct": None}


def _parse_match_header(soup: BeautifulSoup, match_id: int) -> dict | None:
    """
    Everything on a match page that is known before the first map starts.

    Shared by completed and upcoming pages so that the features served live are
    parsed by exactly the code that built the training data.
    """
    team_boxes = soup.select(".team")
    team_names = []
    team_ids = []
    for box in team_boxes[:2]:
        link = box.select_one("a[href*='/team/']")
        if link:
            parts = link.get("href", "").split("/")
            try:
                team_ids.append(int(parts[2]))
            except (ValueError, IndexError):
                team_ids.append(None)
            name_el = box.select_one(".teamName")
            team_names.append(name_el.get_text(strip=True) if name_el else link.get_text(strip=True))
        else:
            name_el = box.select_one(".teamName")
            team_names.append(name_el.get_text(strip=True) if name_el else "")
            team_ids.append(None)

    if len(team_names) < 2 or not all(team_names):
        logger.warning("match %d: found %d team name(s), expected 2 — skipping", match_id, len([n for n in team_names if n]))
        return None

    # Every .teamRanking div carries both an HLTV rank and a Valve Regional
    # Standings rank. Only HLTV was read, and it is absent for lower-tier teams —
    # which is why rank_diff was NaN on ~19% of rows, concentrated exactly where
    # the model is weakest. VRS is present on those rows.
    ranks, vrs_ranks = [], []
    ranking_divs = soup.select(".teamRanking")
    if len(ranking_divs) < 2:
        logger.warning("match %d: found %d .teamRanking div(s), expected 2", match_id, len(ranking_divs))
    for ranking_div in ranking_divs[:2]:
        ranks.append(_ranking_value(ranking_div, "a.hltv-ranking", "HLTV:"))
        vrs_ranks.append(_ranking_value(ranking_div, "a.vrs-ranking", "VRS:"))
    while len(ranks) < 2:
        ranks.append(None)
    while len(vrs_ranks) < 2:
        vrs_ranks.append(None)

    event_el = soup.select_one(".event a")
    event_name = event_el.get_text(strip=True) if event_el else ""


    format_el = soup.select_one(".preformatted-text")
    fmt = format_el.get_text(strip=True) if format_el else ""

    date_el = soup.select_one(".date")
    match_date = None
    match_time = None
    if date_el:
        ts = date_el.get("data-unix")
        if ts:
            # The page carries the real kick-off time; truncating to a date is
            # what forced Elo into same-day batch updates and left market-timing
            # analysis guessing at when a match actually started.
            match_time = datetime.datetime.fromtimestamp(int(ts) / 1000, tz=datetime.timezone.utc)
            match_date = match_time.date()
    if match_date is None:
        logger.warning("match %d: could not parse match_date", match_id)

    if not fmt:
        logger.warning("match %d: empty format string — bo_type and is_lan will be wrong", match_id)

    veto = _parse_veto(soup, team_names, team_ids, context=f"match {match_id}")
    picked_maps = {
        v["map_name"]: v["team_name"]
        for v in veto
        if v["action"] == "picked" and v["map_name"] and v["team_name"]
    }

    lineups = _parse_lineups(soup)
    h2h_scalars, h2h_rows = _parse_h2h(soup, match_id, match_date)
    form_rows = _parse_recent_form(soup, match_id, match_date)
    event_context = _parse_event_context(fmt)
    veto_rows = [
        {"match_id": match_id, "match_date": match_date, **v}
        for v in veto
    ]

    return {
        "team_names": team_names, "team_ids": team_ids, "ranks": ranks, "vrs_ranks": vrs_ranks,
        "event_name": event_name, "fmt": fmt, "match_time": match_time, "match_date": match_date,
        "veto": veto, "picked_maps": picked_maps, "lineups": lineups, "h2h_scalars": h2h_scalars,
        "h2h_rows": h2h_rows, "form_rows": form_rows, "event_context": event_context, "veto_rows": veto_rows,
    }


def _series_fields(h: dict, match_id: int, stars: int) -> dict:
    """The series-level columns of a cs2_match_history row."""
    lineups, fmt = h["lineups"], h["fmt"]
    return {
        "match_id": match_id,
        "match_date": h["match_date"],
        "event_name": h["event_name"],
        "event_tier": stars,
        "format": fmt,
        "bo_type": _parse_bo_type(fmt),
        "is_lan": int("(LAN)" in fmt),
        "team_a_name": h["team_names"][0],
        "team_a_id": h["team_ids"][0],
        "team_b_name": h["team_names"][1],
        "team_b_id": h["team_ids"][1],
        "team_a_rank": h["ranks"][0],
        "team_b_rank": h["ranks"][1],
        "team_a_vrs_rank": h["vrs_ranks"][0],
        "team_b_vrs_rank": h["vrs_ranks"][1],
        "match_time": h["match_time"],
        "team_a_lineup_ids": [pl["player_id"] for pl in lineups[0]],
        "team_b_lineup_ids": [pl["player_id"] for pl in lineups[1]],
        **h["h2h_scalars"],
        **h["event_context"],
        "team_a_awp_id": next((pl["player_id"] for pl in lineups[0] if pl["is_awp"]), None),
        "team_b_awp_id": next((pl["player_id"] for pl in lineups[1] if pl["is_awp"]), None),
    }


def parse_match_detail_html(soup: BeautifulSoup, match_id: int, stars: int = 0) -> dict | None:
    """
    Parse a single HLTV match detail page from a BeautifulSoup object.
    Returns a dict with "rows" (list of per-map dicts) and "demo_url" (str or None).
    """
    try:
        h = _parse_match_header(soup, match_id)
        if h is None:
            return None
        team_names, team_ids, fmt, match_date = h["team_names"], h["team_ids"], h["fmt"], h["match_date"]
        picked_maps = h["picked_maps"]
        series = _series_fields(h, match_id, stars)

        raw_map_names = [
            el.select_one(".mapname").get_text(strip=True)
            for el in soup.select(".mapholder")
            if el.select_one(".mapname")
        ]
        if any(n.lower() == "default" for n in raw_map_names):
            logger.info("match %d: contains a forfeit/default map — skipping", match_id)
            return None

        maps = []
        for map_el in soup.select(".mapholder"):
            name_el = map_el.select_one(".mapname")
            score_els = map_el.select(".results-team-score")
            if name_el and len(score_els) >= 2:
                try:
                    stats_link = map_el.select_one("a.results-stats[href*='mapstatsid']")
                    msid_match = re.search(r"mapstatsid/(\d+)/", stats_link["href"]) if stats_link else None
                    raw_map = name_el.get_text(strip=True)
                    ctx = f"match {match_id} map {raw_map}"
                    maps.append({
                        "map": raw_map,
                        "score_a": int(score_els[0].get_text(strip=True)),
                        "score_b": int(score_els[1].get_text(strip=True)),
                        "mapstatsid": msid_match.group(1) if msid_match else None,
                        **_parse_half_scores(map_el, context=ctx),
                    })
                    if not msid_match:
                        logger.warning("match %d map %r: no mapstatsid found — per-map player stats unavailable", match_id, raw_map)
                except ValueError:
                    pass

        if not maps:
            logger.warning("match %d: no playable maps found — page may not be fully rendered", match_id)

        # Demo download link
        demo_link_el = soup.select_one("a[href*='/download/demo/']")
        demo_url = f"{_BASE_URL}{demo_link_el['href']}" if demo_link_el else None

        has_sub = "substitut" in fmt.lower()
        rows = []
        player_rows: list[dict] = []
        series_a, series_b = 0, 0
        for idx, m in enumerate(maps):
            map_name = f"de_{m['map'].lower()}" if not m["map"].startswith("de_") else m["map"].lower()
            ctx = f"match {match_id} map {map_name}"
            team_a_won = m["score_a"] > m["score_b"]

            if map_name not in MAP_POOL:
                logger.warning("match %d: map %r not in MAP_POOL — new map added to pool?", match_id, map_name)
                continue

            if m.get("team_a_h1_score") is None:
                logger.warning("%s: half scores missing", ctx)

            per_map_sc = soup.find(id=f"{m['mapstatsid']}-content") if m.get("mapstatsid") else None
            per_map_players = []
            if per_map_sc is not None:
                for side in _STAT_SIDES:
                    per_map_players.extend(
                        _parse_player_stats(per_map_sc, context=ctx, has_sub=has_sub, side=side)
                    )

            # Match on team_id, not display name. The name comparison dropped a whole
            # team's players whenever the stats table spelled the name differently
            # from the match header.
            overall = [p for p in per_map_players if p["side"] == "all"]
            team_a_players = [p for p in overall if _same_team(p, team_ids[0], team_names[0])]
            team_b_players = [p for p in overall if _same_team(p, team_ids[1], team_names[1])]

            if overall and not team_a_players:
                logger.warning("%s: team_a %r (id=%s) not found in stats (found %s)", ctx, team_names[0], team_ids[0],
                               list({(p["team_id"], p["team_name"]) for p in overall}))
            if overall and not team_b_players:
                logger.warning("%s: team_b %r (id=%s) not found in stats (found %s)", ctx, team_names[1], team_ids[1],
                               list({(p["team_id"], p["team_name"]) for p in overall}))

            for p in per_map_players:
                is_a = _same_team(p, team_ids[0], team_names[0])
                player_rows.append({
                    "match_id": match_id,
                    "match_date": match_date,
                    "map_name": map_name,
                    "mapstatsid": m.get("mapstatsid"),
                    "is_team_a": is_a,
                    **p,
                })

            rows.append({
                **series,
                "mapstatsid": m.get("mapstatsid"),
                "map_name": map_name,
                "map_position_in_series": idx + 1,
                "is_decider": float(map_name not in picked_maps),
                "team_a_series_score": series_a,
                "team_b_series_score": series_b,
                "team_a_score": m["score_a"],
                "team_b_score": m["score_b"],
                "team_a_won": int(team_a_won),
                "team_a_picked_map": _picked_map_value(picked_maps, map_name, team_names[0]),
                "team_a_h1_score": m.get("team_a_h1_score"),
                "team_b_h1_score": m.get("team_b_h1_score"),
                "team_a_started_ct": m.get("team_a_started_ct"),
                "team_a_player_ids": [p["player_id"] for p in team_a_players],
                "team_a_player_names": [p["player_name"] for p in team_a_players],
                "team_b_player_ids": [p["player_id"] for p in team_b_players],
                "team_b_player_names": [p["player_name"] for p in team_b_players],
                "team_a_avg_rating": _avg_rating(team_a_players),
                "team_b_avg_rating": _avg_rating(team_b_players),
            })
            if team_a_won:
                series_a += 1
            else:
                series_b += 1

        return {"rows": rows, "players": player_rows, "veto": h["veto_rows"],
                "h2h": h["h2h_rows"], "recent_form": h["form_rows"], "demo_url": demo_url}

    except Exception as exc:
        logger.warning("Failed to parse match %d: %s", match_id, exc)
        return None


# ---------------------------------------------------------------------------
# Upcoming matches
# ---------------------------------------------------------------------------

def parse_upcoming_listing_html(html: str) -> list[dict]:
    """
    Every match on HLTV's /matches page: id, scheduled time, stars, format and team names.

    Teams are None while still TBD (later Swiss rounds, playoff brackets); such
    matches cannot be priced yet and are left for a later run.
    """
    soup = BeautifulSoup(html, "lxml")
    out, seen = [], set()
    for wrap in soup.select("div.match-wrapper[data-match-id]"):
        match_id = _int_or_none(wrap.get("data-match-id"))
        if match_id is None or match_id in seen:
            continue
        seen.add(match_id)
        time_el = wrap.select_one(".match-time[data-unix]")
        unix = _int_or_none(time_el.get("data-unix")) if time_el else None
        meta = wrap.select_one(".match-meta")
        names = [el.get_text(strip=True) or None for el in wrap.select(".match-teamname")][:2]
        while len(names) < 2:
            names.append(None)
        out.append({
            "match_id": match_id,
            "match_time": datetime.datetime.fromtimestamp(unix / 1000, tz=datetime.timezone.utc) if unix else None,
            "stars": _int_or_none(wrap.get("data-stars")) or 0,
            "event_id": _int_or_none(wrap.get("data-event-id")),
            "bo_type": _int_or_none(bo.group(1)) if (bo := re.search(r"bo(\d)", meta.get_text(strip=True) if meta else "")) else None,
            "is_live": "live-match-container" in (wrap.get("class") or []),
            "team_a_name": names[0],
            "team_b_name": names[1],
        })
    return out


def parse_upcoming_match_html(soup: BeautifulSoup, match_id: int, stars: int = 0) -> dict | None:
    """
    The pre-match state of an upcoming match page.

    "match" carries the same series-level columns as a cs2_match_history row, so
    the feature builders treat it like any other series. None while either team
    is TBD. The veto is only posted once the match goes live, so veto_known is
    normally False before kickoff.
    """
    try:
        h = _parse_match_header(soup, match_id)
    except Exception as exc:
        logger.warning("Failed to parse upcoming match %d: %s", match_id, exc)
        return None
    if h is None or None in h["team_ids"]:
        return None
    return {
        "match": _series_fields(h, match_id, stars),
        "veto": h["veto_rows"],
        "veto_known": bool(h["veto_rows"]),
        "h2h": h["h2h_rows"],
        "recent_form": h["form_rows"],
    }


def scrape_upcoming_matches(session: _Session, hours_ahead: float, bo_types: tuple[int, ...] = (3,)) -> list[dict]:
    """Upcoming matches with known teams kicking off within hours_ahead, pages parsed."""
    html = _get_html(session, f"{_BASE_URL}/matches")
    if html is None:
        logger.warning("could not fetch the upcoming matches listing")
        return []
    now = datetime.datetime.now(datetime.timezone.utc)
    horizon = now + datetime.timedelta(hours=hours_ahead)
    wanted = [
        m for m in parse_upcoming_listing_html(html)
        if m["match_time"] and now < m["match_time"] <= horizon and m["bo_type"] in bo_types
        and m["team_a_name"] and m["team_b_name"] and not m["is_live"]
    ]
    out = []
    for m in wanted:
        page = _get_html(session, _match_url(m["match_id"]))
        parsed = parse_upcoming_match_html(BeautifulSoup(page, "lxml"), m["match_id"], m["stars"]) if page else None
        if parsed is not None:
            out.append(parsed)
    logger.info("%d upcoming matches within %.0fh, %d parsed", len(wanted), hours_ahead, len(out))
    return out


# ---------------------------------------------------------------------------
# Weekly team rankings
# ---------------------------------------------------------------------------

def _all_mondays(start_date: datetime.date, end_date: datetime.date) -> list[datetime.date]:
    days_ahead = (7 - start_date.weekday()) % 7
    current = start_date + datetime.timedelta(days=days_ahead)
    mondays = []
    while current <= end_date:
        mondays.append(current)
        current += datetime.timedelta(weeks=1)
    return mondays


def parse_team_ranking_html(soup: BeautifulSoup, date: datetime.date) -> list[dict] | None:
    """Parse HLTV team rankings from a BeautifulSoup object."""
    ranked_teams = soup.select(".ranked-team")
    rows = []
    for team_el in ranked_teams:
        pos_el = team_el.select_one(".position")
        name_el = team_el.select_one(".teamLine .name")
        pts_el = team_el.select_one(".teamLine .points")
        change_el = team_el.select_one(".change")

        team_link = team_el.select_one("a[href*='/team/']")
        team_id = None
        if team_link:
            parts = team_link.get("href", "").split("/")
            try:
                team_id = int(parts[2])
            except (ValueError, IndexError):
                pass

        if not (pos_el and name_el):
            continue

        try:
            rank = int(pos_el.get_text(strip=True).lstrip("#"))
        except ValueError:
            continue

        pts_text = pts_el.get_text(strip=True) if pts_el else ""
        pts_match = re.search(r"[\d,]+", pts_text)
        try:
            points = int(pts_match.group(0).replace(",", "")) if pts_match else 0
        except ValueError:
            points = 0

        change_text = change_el.get_text(strip=True) if change_el else "0"
        try:
            change = int(change_text.replace("+", ""))
        except ValueError:
            change = 0

        rows.append({
            "date": pd.Timestamp(date),
            "team_name": name_el.get_text(strip=True),
            "team_id": team_id,
            "rank": rank,
            "points": points,
            "change": change,
        })

    return rows if rows else None
