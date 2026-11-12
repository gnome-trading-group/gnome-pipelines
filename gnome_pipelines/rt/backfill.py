"""rt_backfill: the research run's films and outlet reviews (Jan-Nov 2026) loaded into the rt_reviews source's tables,
read by the live code, so research and live read one dataset.

The research run (~/gnome-research-work/rt/outlets) left, under s3://<research bucket>/rt_reviews/research/:
films.json (the settled KXRT films), reviews.json (each matched URL with its outlet) and pages.json (URL -> the page's
key in the page cache, rt_reviews/pages/..., where its HTML was uploaded). Each page is read with page.py and
classified with classify.py exactly as live, and written with backfill=True and first_seen_at = article_seen_at =
the page's own publish time: the research run saw them later, from archive sitemaps, so live-style discovery times
don't exist for them (a backtest adds the live lags measured in poll_health). A page with no publish time is skipped,
since a row needs a first_seen_at. Films the live source already recorded keep their own rows. The source leaves
backfill rows out of discovery-lag figures.
"""
from __future__ import annotations

import gzip
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import pandas as pd
from gnomepy.data import CatalogConfig
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources
from gnome_pipelines.datasets.rt import RT_ARTICLES, RT_FILMS, RT_REVIEWS
from gnome_pipelines.polling.claude import ClaudeReader, Unavailable
from gnome_pipelines.rt import classify as claude_read
from gnome_pipelines.rt.match import film_id
from gnome_pipelines.rt.page import facts
from gnome_pipelines.stores import dataset_store

PREFIX = "rt_reviews/research"


class RtBackfillParams(PipelineParams):
    claude: bool = Field(True, description="classify each page with Claude (off: rows are written without a verdict)")
    limit: int | None = Field(None, ge=1, description="only the first N reviews (a trial run)")


def _bucket() -> str:
    return CatalogConfig.from_env().artifact_root.removeprefix("s3://").split("/", 1)[0]


def _json(s3, bucket: str, name: str):
    return json.loads(s3.get_object(Bucket=bucket, Key=f"{PREFIX}/{name}")["Body"].read())


class RtBackfillPipeline(Pipeline[RtBackfillParams]):
    name = "rt_backfill"
    description = "Load the research run's RT films and outlet reviews into rt_films, rt_articles and rt_reviews"
    resources = Resources(cpu=1024, memory=4096)

    def run(self, params: RtBackfillParams) -> PipelineResult:
        s3, bucket = boto3.client("s3"), _bucket()
        films, reviews, pages = _json(s3, bucket, "films.json"), _json(s3, bucket, "reviews.json"), \
            _json(s3, bucket, "pages.json")
        reader = ClaudeReader(claude_read.MODEL, 10**6) if params.claude else None
        titles = {f["event"]: re.sub(r'^"|"$', "", f["name"]) for f in films}
        film_rows = [{"film_id": film_id(titles[f["event"]]), "title": titles[f["event"]], "rt_slug": f.get("slug"),
                      "close": datetime.fromtimestamp(f["close"], timezone.utc), "open": False,
                      "kalshi_event": f["event"],
                      "kalshi_strikes": sorted({int(m["strike"]) for m in f["markets"] if m.get("strike") is not None}),
                      "polymarket_event": None, "polymarket_us_event": None,
                      "first_seen_at": datetime.fromtimestamp(f["close"], timezone.utc)} for f in films]
        # A URL the research matcher gave two films (a short title inside a longer one) keeps the first, as live does.
        todo, taken = [], set()
        for ev, rs in reviews.items():
            for r in rs:
                if r["url"] in pages and r["url"] not in taken:
                    taken.add(r["url"])
                    todo.append((ev, r))
        todo = todo[:params.limit]

        def one(item):
            ev, r = item
            body = gzip.decompress(s3.get_object(Bucket=bucket, Key=pages[r["url"]])["Body"].read())
            parsed = facts(body.decode("utf-8", "replace"), r["outlet"])
            try:
                verdict = claude_read.classify(reader, titles[ev], r["outlet"], r["url"], parsed) if reader else None
            except Unavailable:
                verdict = None
            return ev, r, parsed, verdict

        with ThreadPoolExecutor(8) as ex:
            done = list(ex.map(one, todo))
        articles, rows, undated = [], [], 0
        for ev, r, parsed, v in done:
            fid, seen = film_id(titles[ev]), parsed.published_at
            if seen is None:
                # No time to stand for when it was seen; every row must have one.
                undated += 1
                continue
            articles.append({"url": r["url"], "film_id": fid, "outlet": r["outlet"], "feed": "research",
                             "feed_published_at": seen, "first_seen_at": seen})
            rows.append({"url": r["url"], "film_id": fid, "outlet": r["outlet"], "published_at": seen,
                         "article_seen_at": seen, "first_seen_at": seen, "headline": parsed.headline,
                         "rating_kind": parsed.rating_kind, "rating_value": parsed.rating_value,
                         "rating_grade": parsed.rating_grade, "is_review": v.is_review if v else None,
                         "fresh": v.fresh if v else None, "confidence": v.confidence if v else None,
                         "model": claude_read.MODEL if v else None, "archive_key": pages[r["url"]], "backfill": True})
        store = dataset_store()
        try:
            live = set(store.load(RT_FILMS.name)["film_id"])
        except NoSuchTableError:
            live = set()
        # A film the live source already knows keeps its own rows: a backfill row ingested later would replace them.
        film_rows = [f for f in film_rows if f["film_id"] not in live]
        store.publish(RT_FILMS, _frame(RT_FILMS, film_rows))
        store.publish(RT_ARTICLES, _frame(RT_ARTICLES, articles))
        store.publish(RT_REVIEWS, _frame(RT_REVIEWS, rows))
        return PipelineResult(status="succeeded", outputs={
            "films": len(film_rows), "reviews": len(rows), "undated_skipped": undated,
            "classified": sum(r["is_review"] is not None for r in rows),
            "claude_usd": round(reader.usd(), 3) if reader else 0.0})


def _frame(spec, rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=spec.schema.names)
