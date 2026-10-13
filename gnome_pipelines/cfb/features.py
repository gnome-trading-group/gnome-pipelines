"""Build and publish the CFB point-in-time feature tables for every historical cutoff.

    poetry run python -m gnome_pipelines.cfb.features 2018 2026
"""
from __future__ import annotations

import argparse

from gnome_pipelines.datasets import cfb
from gnome_pipelines.stores import dataset_store
from gnomepy_research.sessions.cfb_win_probability.features.snapshots import build_all
from gnomepy_research.sessions.cfb_win_probability.features.source import SourceData

SPECS = {s.name: s for s in (cfb.TEAM_PRESEASON_FEATURES, cfb.TEAM_WEEK_FEATURES, cfb.PREGAME_FEATURES)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("first", type=int)
    ap.add_argument("last", type=int)
    args = ap.parse_args()
    ds = dataset_store()
    tables = build_all(SourceData.load(ds), list(range(args.first, args.last + 1)))
    for name, df in tables.items():
        print(name, ds.publish(SPECS[name], df))


if __name__ == "__main__":
    main()
