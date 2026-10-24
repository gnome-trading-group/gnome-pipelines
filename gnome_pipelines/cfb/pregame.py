"""Publish a refresh of B0's pregame predictions into cfb_pregame_predictions.

    GNOME_CODE_VERSION=gnome-pipelines@$(git rev-parse --short HEAD) \\
    AWS_PROFILE=AWSAdministratorAccess-241533121172 poetry run python -m gnome_pipelines.cfb.pregame PREDICTIONS.parquet

The file comes from gnomepy-research's cfb_win_probability live predictor, run on a laptop because B0's research code
is not in models/ yet (posterior.py and features.py are laptop publishers the same way). Each refresh appends a row per
game at its generated_at; the CFB pregame paper sessions trade on the newest one at or before each moment. A file that
is missing a column, holds a probability outside (0, 1) or a row without its key publishes nothing.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from gnomepy.data import DatasetStore, PublishResult

from gnome_pipelines.datasets import cfb
from gnome_pipelines.stores import dataset_store

SPEC = cfb.PREGAME_PREDICTIONS


def publish(path: Path, store: DatasetStore) -> PublishResult:
    df = pd.read_parquet(path)
    missing = [c for c in SPEC.schema.names if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")
    for key in SPEC.key:
        if df[key].isna().any():
            raise ValueError(f"{path}: {int(df[key].isna().sum())} rows without {key}")
    bad = ~df["p_home"].between(0, 1, inclusive="neither")
    if bad.any():
        raise ValueError(f"{path}: p_home outside (0, 1) for games {sorted(df.loc[bad, 'game_id'].tolist())}")
    return store.publish(SPEC, df[SPEC.schema.names])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", type=Path)
    args = ap.parse_args()
    print(publish(args.path, dataset_store()))


if __name__ == "__main__":
    main()
