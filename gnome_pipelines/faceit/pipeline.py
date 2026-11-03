"""
FACEIT data for the CS2 e12 model.

faceit_daily walks the last few days of FACEIT history of every player in cs2_hltv_faceit_map (about 3.5k calls), then
fetches details and stats for the organised series in it that aren't stored yet, plus the rest of those competitions'
series (matches between two unlinked teams keep the strength chain through a league connected). It then relinks the
recent FACEIT series to HLTV series and rederives their exclusions. It runs after hltv_cs2 so the HLTV side of a link
is there; a series HLTV publishes later is linked on one of the following days, inside link_days.

faceit_backfill loads a research build of the same tables (the local FACEIT folder's out/ directory) once, instead of
refetching the roughly 600k API calls it took to build.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pydantic import Field

from gnome_pipelines import Pipeline, PipelineParams, PipelineResult, Resources, Schedule
from gnome_pipelines.credentials import secret
from gnome_pipelines.datasets import cs2, faceit
from gnome_pipelines.faceit.client import FaceitClient
from gnome_pipelines.faceit.link import LINK_COLUMNS, exclusions, series_links
from gnome_pipelines.faceit.normalize import history_rows, match_rows, organised_championship, team_of_quarter
from gnome_pipelines.stores import dataset_store

logger = logging.getLogger(__name__)

HLTV_COLUMNS = ["match_id", "match_date", "match_time", "team_a_id", "team_b_id", "map_name", "team_a_score", "team_b_score",
                "team_a_lineup_ids", "team_b_lineup_ids"]


def _frame(spec, rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=spec.schema.names)


class FaceitDailyParams(PipelineParams):
    lookback_days: int = Field(3, ge=1, le=30, description="Days of each linked player's FACEIT history to walk")
    link_days: int = Field(7, ge=1, le=60, description="Relink and rederive exclusions for series finished in this many days")
    workers: int = Field(32, ge=1, le=128, description="Concurrent API requests at most (the limiter adapts below it)")


class FaceitDailyPipeline(Pipeline[FaceitDailyParams]):
    name = "faceit_daily"
    description = "FACEIT history, organised series, HLTV links and exclusions for the e12 model"
    resources = Resources(cpu=1024, memory=4096)
    schedules = (Schedule("daily", "cron(30 7 * * ? *)", {}, "07:30 UTC daily, after hltv_cs2"),)

    def run(self, params: FaceitDailyParams) -> PipelineResult:
        ds = dataset_store()
        client = FaceitClient(secret("faceit", "FACEIT_API_KEY"))
        now = pd.Timestamp.now(tz="UTC")
        start, end = int((now - pd.Timedelta(days=params.lookback_days)).timestamp()), int(now.timestamp())
        f2h = {f: int(h) for f, h in ds.load(faceit.HLTV_FACEIT_MAP.name).set_index("faceit_id").hltv_id.items()}

        with ThreadPoolExecutor(params.workers) as pool:
            items = [i for page in pool.map(lambda f: client.player_history(f, start, end), f2h) for i in page]
        m_rows, p_rows = history_rows(items)
        hm = _frame(faceit.HISTORY_MATCHES, m_rows).drop_duplicates("match_id")
        hp = _frame(faceit.HISTORY_PLAYERS, p_rows).drop_duplicates(["match_id", "faceit_id"])
        hm_result = ds.publish(faceit.HISTORY_MATCHES, hm)
        hp_result = ds.publish(faceit.HISTORY_PLAYERS, hp)

        organised = hm[[organised_championship(r) for r in hm.to_dict("records")]]
        with ThreadPoolExecutor(params.workers) as pool:
            listed = [m for ms in pool.map(client.championship_matches, organised.competition_id.dropna().unique()) for m in ms]
        listed_ids = {m["match_id"] for m in listed if (m.get("status") or "").upper() == "FINISHED"
                      and (m.get("finished_at") or end) >= start}
        known = set(ds.load(faceit.MATCHES.name, columns=["match_id"]).match_id)
        new_ids = sorted((set(organised.match_id) | listed_ids) - known)
        team_of = team_of_quarter(ds.load(cs2.MATCH_LINEUPS.name, columns=["player_id", "team_id", "match_date"]))

        def fetch(mid: str):
            details = client.get(f"/matches/{mid}")
            if not details or details.get("status") != "FINISHED":
                return None
            return match_rows(mid, details, client.get(f"/matches/{mid}/stats"), f2h, team_of)

        with ThreadPoolExecutor(params.workers) as pool:
            fetched = [r for r in pool.map(fetch, new_ids) if r]
        published = {}
        for spec, part in ((faceit.MATCHES, "matches"), (faceit.ROSTERS, "rosters"), (faceit.MATCH_HISTORY, "history")):
            rows = [row for r in fetched for row in r[part]]
            if rows:
                published[spec.name] = ds.publish(spec, _frame(spec, rows)).rows_written

        since = now - pd.Timedelta(days=params.link_days)
        fm = ds.load(faceit.MATCHES.name)
        fm = fm[fm.finished_at >= int(since.timestamp())]
        fh = ds.load(faceit.MATCH_HISTORY.name)
        fh = fh[fh.match_id.isin(fm.match_id)]
        hltv = ds.load(cs2.MATCH_HISTORY.name, columns=HLTV_COLUMNS)
        hltv = hltv[pd.to_datetime(hltv.match_date) >= (since - pd.Timedelta(days=3)).tz_convert(None)]
        old = ds.load(faceit.SERIES_LINK.name)
        taken = set(old.hltv_match_id[~old.faceit_match_id.isin(fm.match_id)].dropna().astype(int))
        links = series_links(fm, fh, hltv, taken)
        if len(links):
            published[faceit.SERIES_LINK.name] = ds.publish(faceit.SERIES_LINK, links).rows_written
        linked = set(old.faceit_match_id) | set(links.faceit_match_id)
        ex = exclusions(fm, fh, linked)
        if len(ex):
            published[faceit.EXCLUSIONS.name] = ds.publish(faceit.EXCLUSIONS, ex).rows_written

        outputs = {"players": len(f2h), "history_matches": hm_result.rows_written, "history_players": hp_result.rows_written,
                   "new_series": len(new_ids), "normalised_series": len(fetched), "published": published,
                   "api_calls": client.calls, "throttled": client.limiter.throttles}
        logger.info("faceit_daily: %s", outputs)
        return PipelineResult("SUCCEEDED", outputs)


class FaceitBackfillParams(PipelineParams):
    source: str = Field(..., description="A research FACEIT build: a directory with hltv_faceit_map.parquet, "
                                         "history_matches.parquet, history_players.parquet and norm/")
    chunk_rows: int = Field(2_000_000, ge=100_000, description="Rows per publish of the history tables")


class FaceitBackfillPipeline(Pipeline[FaceitBackfillParams]):
    name = "faceit_backfill"
    description = "Load a research FACEIT build into the faceit_* datasets (one-time)"
    resources = Resources(cpu=4096, memory=30720)

    def run(self, params: FaceitBackfillParams) -> PipelineResult:
        ds = dataset_store()
        src = Path(params.source)
        written = {}

        def load(spec, path: Path, rename: dict | None = None) -> pd.DataFrame:
            df = pd.read_parquet(path).rename(columns=rename or {})
            return df[list(spec.schema.names)]

        mp = load(faceit.HLTV_FACEIT_MAP, src / "hltv_faceit_map.parquet")
        fm = load(faceit.MATCHES, src / "norm/faceit_matches.parquet")
        fh = load(faceit.MATCH_HISTORY, src / "norm/faceit_match_history.parquet")
        ro = load(faceit.ROSTERS, src / "norm/faceit_rosters.parquet").drop_duplicates(["match_id", "faceit_id"])
        links = pd.read_parquet(src / "norm/faceit_hltv_series_link.parquet")[LINK_COLUMNS]
        for spec, df in ((faceit.HLTV_FACEIT_MAP, mp), (faceit.MATCHES, fm), (faceit.MATCH_HISTORY, fh), (faceit.ROSTERS, ro),
                         (faceit.SERIES_LINK, links), (faceit.EXCLUSIONS, exclusions(fm, fh, set(links.faceit_match_id)))):
            written[spec.name] = ds.publish(spec, df).rows_written
            logger.info("%s: %d rows", spec.name, written[spec.name])
        for spec, name in ((faceit.HISTORY_MATCHES, "history_matches.parquet"), (faceit.HISTORY_PLAYERS, "history_players.parquet")):
            written[spec.name] = 0
            for batch in pq.ParquetFile(src / name).iter_batches(batch_size=params.chunk_rows, columns=list(spec.schema.names)):
                written[spec.name] += ds.publish(spec, batch.to_pandas().drop_duplicates(list(spec.key))).rows_written
                logger.info("%s: %d rows so far", spec.name, written[spec.name])
        return PipelineResult("SUCCEEDED", {"written": written})
