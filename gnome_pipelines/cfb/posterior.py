"""Fit and publish the CFB team-week posteriors and each season's joint draws.

    poetry run python -m gnome_pipelines.cfb.posterior 2020 2026 [--likelihood studentt]
"""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from gnome_pipelines.datasets import cfb
from gnome_pipelines.stores import artifact_store, dataset_store
from gnomepy_research.models.cfb.efficiency.inference import Inputs
from gnomepy_research.models.cfb.efficiency.posterior_snapshots import (
    MODEL_VERSION,
    N_DRAWS_KEPT,
    hyper_name,
    snapshot_seasons,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("first", type=int)
    ap.add_argument("last", type=int)
    ap.add_argument("--likelihood", default="studentt")
    args = ap.parse_args()
    ds = dataset_store()
    store = artifact_store()
    with tempfile.TemporaryDirectory() as tmp:
        table, paths = snapshot_seasons(Inputs.load(ds), args.first, args.last, args.likelihood, out_dir=Path(tmp))
        print(ds.publish(cfb.TEAM_WEEK_POSTERIOR, table))
        for season, path in paths:
            print(store.publish(
                path, "posterior_draws", f"cfb_team_week_draws_{season}", inputs=ds.loaded,
                description=f"{MODEL_VERSION} joint draws (alpha, O, D) per weekly cutoff, {season}",
                params={"likelihood": args.likelihood, "hyperparameters": hyper_name(args.likelihood, season),
                        "draws": N_DRAWS_KEPT},
            ))


if __name__ == "__main__":
    main()
