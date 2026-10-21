"""News RSS: Google News search per team, read through an extractor into pending-review evidence."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import pandas as pd

from gnome_pipelines.cfb.qb_status.evidence import Evidence, evidence_id
from gnome_pipelines.cfb.qb_status.extract import Extractor

PARAPHRASE_LEN = 140


def rss_url(team: str) -> str:
    return (f"https://news.google.com/rss/search?q={quote(chr(34) + team + chr(34))}%20quarterback"
            f"&hl=en-US&gl=US&ceid=US:en")


def parse_rss(xml: str) -> list[dict]:
    root = ET.fromstring(xml)
    items = []
    for it in root.iter("item"):
        pub = it.findtext("pubDate")
        try:
            published = parsedate_to_datetime(pub) if pub else None
        except (TypeError, ValueError):
            published = None
        if published is not None and published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
        items.append({"title": (it.findtext("title") or "").strip(), "link": (it.findtext("link") or "").strip(),
                      "guid": (it.findtext("guid") or it.findtext("link") or "").strip(), "published": published})
    return items


def _publisher(title: str) -> str:
    return title.rsplit(" - ", 1)[1].strip() if " - " in title else "news"


def news_evidence(items: list[dict], team: str, team_id: int, qbs: pd.DataFrame, extractor: Extractor,
                  seen_at: datetime, game_id: int | None = None) -> list[Evidence]:
    names = dict(zip(qbs["player_id"].astype(str), qbs["name"]))
    season = seen_at.year if seen_at.month >= 7 else seen_at.year - 1
    out = []
    for item in items:
        for player_id, status, hedge, confidence in extractor.extract(item["title"], team_id, qbs):
            out.append(Evidence(
                evidence_id=evidence_id("news", item["guid"], player_id, status), source_type="news",
                source_id=item["guid"], source_org=_publisher(item["title"]), first_seen_at=seen_at, team=team,
                status=status, season=season, published_at=item["published"], team_id=team_id,
                player_raw=names.get(player_id), player_id=player_id, match_confidence=1.0, game_id=game_id,
                hedge=hedge, paraphrase=item["title"][:PARAPHRASE_LEN], url=item["link"], extractor="rules",
                extraction_confidence=confidence, review_state="pending"))
    return out
