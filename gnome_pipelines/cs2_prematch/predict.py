"""
Every 30 min: price upcoming BO3 series, find their Polymarket markets, and ask
gnome-launcher for one paper session per match.

1. Upcoming BO3s with both teams known, kicking off within hours_ahead (HLTV via ZenRows).
2. Features with build_priors_for - the code that built the training data - and
   pre-veto prices for the series and map 1.
3. Append to cs2_prematch_predictions (the strategy reads the latest row as of
   now) and the feature rows to cs2_prematch_prediction_inputs, for skew audits.
4. Resolve each match's markets by HLTV team id + kickoff (identity.py), then the
   outcome tokens to registry listings.
5. Send each match's launch trigger once, and its shutdown trigger once after
   kickoff. Once, not every run: the launcher dedups only within a window and
   only against running sessions, so a repeat could schedule a second session
   for a match still waiting to start. What was sent is kept in cs2_prematch_matches.

Params:
  hours_ahead (24), lead_hours (12): session starts this long before kickoff,
  shutdown_after_minutes (30), launch_queue_url, shutdown_queue_url,
  dry_run (false): compute everything, publish and send nothing.
  model_path: artifact URI or local path to price with instead of the latest
    published pre-veto model (e.g. a candidate under evaluation).
"""
from __future__ import annotations

import json
import logging

import boto3
import pandas as pd

from gnomepy.data import DatasetStore, TableSpec, resolve_artifact_path
from gnomepy.registry import RegistryClient
from pyiceberg.exceptions import NoSuchTableError

from gnome_pipelines import Pipeline, PipelineResult, register_pipeline
from gnome_pipelines.cs2_prematch.model import price
from gnome_pipelines.cs2_prematch.train import ARTIFACT_TYPE, MODEL_NAME
from gnome_pipelines.datasets import cs2
from gnome_pipelines.hltv_cs2.publish import publish
from gnome_pipelines.hltv_cs2.scraper import _make_session, scrape_upcoming_matches
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cs2.hltv.build_priors import build_priors_for
from gnomepy_research.models.cs2.prematch import identity, polymarket
from gnomepy_research.models.cs2.prematch.predictions import validate
from gnomepy_research.models.cs2.win_probability.pre_map_model import CS2PreMapModel

logger = logging.getLogger(__name__)

RULE_TYPE = "cs2_prematch"
MARKET_LOOKBACK = pd.Timedelta(days=21)

STATE_COLUMNS = ["match_id", "team_a_id", "team_b_id", "kickoff", "series_slug", "team_a_outcome", "learned", "markets",
                 "launch_at", "launch_sent_at", "shutdown_sent_at", "alias_settled", "first_seen", "updated_at"]


def empty_state() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in STATE_COLUMNS})


def build_targets(upcoming: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One feature row per series (first map, unknown), plus the head-to-head rows from their pages."""
    rows = [{**u["match"], "map_name": "", "map_position_in_series": 1, "team_a_series_score": 0,
             "team_b_series_score": 0, "is_decider": 0.0} for u in upcoming]
    targets = pd.DataFrame(rows)
    if not targets.empty:
        targets["match_date"] = pd.to_datetime(targets.match_date)
    return targets, pd.DataFrame([r for u in upcoming for r in u["h2h"]])


def prediction_rows(prices: pd.DataFrame, targets: pd.DataFrame, priors: pd.DataFrame,
                    model_version: str, now: pd.Timestamp) -> pd.DataFrame:
    kickoff = targets.set_index("match_id").match_time
    rank = priors.set_index("match_id").rank_known_both
    long = prices.melt(id_vars="match_id", value_vars=["series", "game1"], var_name="market", value_name="p_team_a")
    long["rank_known_both"] = long.match_id.map(rank)
    long["kickoff"] = long.match_id.map(kickoff)
    long["model_version"] = model_version
    long["generated_at"] = now
    return validate(long)


def _orientation(market_row, team_a_name_norm: str) -> int | None:
    if market_row.o0 == team_a_name_norm:
        return 0
    if market_row.o1 == team_a_name_norm:
        return 1
    return None


def resolve_legs(match: dict, pm: pd.DataFrame, aliases: pd.DataFrame,
                 resolve) -> tuple[identity.Resolution, list[dict], str | None]:
    """
    (series resolution, legs, reason if no legs). Map 1's market may list its outcomes in
    a different order from the series market, so each market is oriented by the
    name team A has in the series market, never by position.
    """
    r = identity.resolve_match(match, pm, aliases)
    if r.market_slug is None:
        return r, [], r.reason
    by_slug = pm.set_index("market_slug")
    series = by_slug.loc[r.market_slug]
    a_name = series.o0 if r.team_a_outcome == 0 else series.o1
    legs = []
    for market, slug in (("series", r.market_slug), ("game1", identity.sibling_market(pm, r.market_slug, "game1"))):
        if slug is None:
            continue
        mk = by_slug.loc[slug]
        side = _orientation(mk, a_name)
        if side is None:
            continue
        tok_a, tok_b = (mk.tokens[0], mk.tokens[1]) if side == 0 else (mk.tokens[1], mk.tokens[0])
        listing_a, listing_b = resolve(mk.condition_id, str(tok_a)), resolve(mk.condition_id, str(tok_b))
        if listing_a is not None and listing_b is not None:
            legs.append({"market": market, "listing_team_a": int(listing_a), "listing_team_b": int(listing_b)})
    return r, legs, None if legs else "markets not in registry"


def upsert_state(state: pd.DataFrame, match: dict, resolution: identity.Resolution, legs: list[dict],
                 now: pd.Timestamp, lead: pd.Timedelta) -> pd.DataFrame:
    """
    Kickoff follows HLTV every run. The launch time and markets are fixed once the
    launch trigger has gone, so the session launched is the one the trigger described.
    """
    mid = int(match["match_id"])
    kickoff = pd.Timestamp(match["match_time"])
    hit = state.match_id == mid
    if hit.any():
        row = state[hit].iloc[0].to_dict()
    else:
        row = {"match_id": mid, "team_a_id": int(match["team_a_id"]), "team_b_id": int(match["team_b_id"]),
               "series_slug": None, "team_a_outcome": None, "learned": "[]", "markets": "[]", "launch_at": None, "launch_sent_at": None,
               "shutdown_sent_at": None, "alias_settled": False, "first_seen": now}
    row["kickoff"], row["updated_at"] = kickoff, now
    if pd.isna(row["launch_sent_at"]):
        row["launch_at"] = max(now, kickoff - lead)
        if legs:
            row["series_slug"] = resolution.market_slug
            row["team_a_outcome"] = resolution.team_a_outcome
            row["markets"] = json.dumps(legs)
            row["learned"] = json.dumps([list(x) for x in resolution.learned])
    rest = state[~hit]
    new = pd.DataFrame([row])[STATE_COLUMNS]
    return pd.concat([rest, new], ignore_index=True) if len(rest) else new


def plan_triggers(state: pd.DataFrame, now: pd.Timestamp,
                  shutdown_after: pd.Timedelta) -> tuple[list[dict], list[dict], pd.DataFrame]:
    """(launch messages, shutdown messages, state with what was sent marked)."""
    state = state.copy()
    launches, shutdowns = [], []
    for i, r in state.iterrows():
        markets = json.loads(r.markets)
        data = {"match_id": int(r.match_id), "launch_at": pd.Timestamp(r.launch_at).isoformat(), "markets": markets}
        if markets and pd.isna(r.launch_sent_at):
            launches.append({"rule_type": RULE_TYPE, "data": data})
            state.at[i, "launch_sent_at"] = now
        elif not pd.isna(r.launch_sent_at) and pd.isna(r.shutdown_sent_at) \
                and pd.Timestamp(r.kickoff) + shutdown_after <= now:
            shutdowns.append({"rule_type": RULE_TYPE, "data": data})
            state.at[i, "shutdown_sent_at"] = now
    return launches, shutdowns, state


def settle_aliases(state: pd.DataFrame, pm: pd.DataFrame, series_won: pd.Series,
                   aliases: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fold settled series markets into the aliases each match taught us."""
    state = state.copy()
    by_slug = pm.set_index("market_slug")
    for i, r in state.iterrows():
        learned = json.loads(r.learned)
        if r.alias_settled or not learned or r.series_slug not in by_slug.index or r.match_id not in series_won.index:
            continue
        mk = by_slug.loc[r.series_slug]
        final = [float(x) for x in mk.final] if len(mk.final) == 2 else []
        if not final or max(final) < 0.9:
            continue
        agrees = int(final[int(r.team_a_outcome)] > 0.5) == int(series_won[r.match_id])
        for name, team_id in learned:
            aliases = identity.settle(aliases, "polymarket", name, int(team_id), agrees)
        state.at[i, "alias_settled"] = True
    return state, aliases


def _load(ds: DatasetStore, spec: TableSpec, empty: pd.DataFrame) -> pd.DataFrame:
    try:
        return ds.load(spec.name)
    except NoSuchTableError:
        return empty


def _series_won(history: pd.DataFrame) -> pd.Series:
    decided = history[history.team_a_score != history.team_b_score]
    return decided.groupby("match_id").team_a_won.agg(lambda w: int(w.sum() * 2 > len(w)))


@register_pipeline
class CS2PrematchPredictPipeline(Pipeline):
    name = "cs2_prematch_predict"

    def run(self, params: dict) -> PipelineResult:
        now = pd.Timestamp.now(tz="UTC").floor("s")
        dry_run = bool(params.get("dry_run", False))
        lead = pd.Timedelta(hours=params.get("lead_hours", 12))
        shutdown_after = pd.Timedelta(minutes=params.get("shutdown_after_minutes", 30))
        ds = dataset_store()

        upcoming = scrape_upcoming_matches(_make_session(), params.get("hours_ahead", 24))
        history = ds.load("cs2_match_history")
        targets, page_h2h = build_targets(upcoming)

        preds = pd.DataFrame()
        priors = pd.DataFrame()
        if not targets.empty:
            priors = build_priors_for(
                history, ds.load("cs2_team_rankings"), targets,
                player_stats=ds.load("cs2_player_map_stats"), veto=ds.load("cs2_match_veto"),
                h2h=pd.concat([ds.load("cs2_h2h_history"), page_h2h], ignore_index=True),
            )
            model_path, version = params.get("model_path"), params.get("model_path")
            if not model_path:
                ref = artifact_store().latest(ARTIFACT_TYPE, MODEL_NAME)
                model_path, version = str(ref), f"{MODEL_NAME}:{ref.version}"
            model = CS2PreMapModel(resolve_artifact_path(model_path))
            preds = prediction_rows(price(model, priors), targets, priors, version, now)

        pm = polymarket.fetch_markets(now.tz_localize(None) - MARKET_LOOKBACK, now.tz_localize(None) + pd.Timedelta(days=2))
        aliases = _load(ds, cs2.TEAM_ALIASES, identity.empty_aliases())
        state = _load(ds, cs2.PREMATCH_MATCHES, empty_state())
        registry = RegistryClient()

        skipped: dict[str, int] = {}
        for match in targets.to_dict("records"):
            resolution, legs, reason = resolve_legs(
                match, pm, aliases, lambda cond, tok: polymarket.resolve_listing(cond, tok, registry))
            aliases = identity.learn(aliases, resolution.learned, now)
            if reason:
                skipped[reason] = skipped.get(reason, 0) + 1
            state = upsert_state(state, match, resolution, legs, now, lead)

        state, aliases = settle_aliases(state, pm, _series_won(history), aliases)
        launches, shutdowns, state = plan_triggers(state, now, shutdown_after)

        if not dry_run:
            if not preds.empty:
                publish(cs2.PREMATCH_PREDICTIONS, preds)
                publish(cs2.PREMATCH_PREDICTION_INPUTS, priors.assign(generated_at=now))
            publish(cs2.TEAM_ALIASES, aliases)
            publish(cs2.PREMATCH_MATCHES, state)
            sqs = boto3.client("sqs")
            for queue, messages in ((params.get("launch_queue_url"), launches),
                                    (params.get("shutdown_queue_url"), shutdowns)):
                if messages and not queue:
                    raise ValueError("launch_queue_url and shutdown_queue_url are required unless dry_run")
                for message in messages:
                    sqs.send_message(QueueUrl=queue, MessageBody=json.dumps(message))

        logger.info("launch triggers: %s", launches)
        logger.info("shutdown triggers: %s", shutdowns)
        return PipelineResult(status="succeeded", outputs={
            "upcoming": len(upcoming), "predicted": int(preds.match_id.nunique()) if len(preds) else 0,
            "launches": len(launches), "shutdowns": len(shutdowns), "skipped": skipped,
            "aliases": len(aliases), "dry_run": dry_run,
        })
