"""One-off: copy the old DynamoDB + Parquet research store into Iceberg.

Copies the newest version of every dataset that has a spec (cs2_round_features is
re-backfilled instead) and the newest version of every artifact type listed in
ARTIFACT_TYPES. Each dataset's rows get ingested_at = the old version's created_at,
the time that data was known. Re-running is safe: unchanged rows are skipped and
an artifact name that already has a version is left alone.

    AWS_PROFILE=prod GNOME_TABLE_BUCKET_ARN=... poetry run python scripts/migrate_research_store.py
    GNOME_LOCAL_WAREHOUSE=/tmp/wh GNOME_ARTIFACT_ROOT=/tmp/wh/files AWS_PROFILE=prod poetry run python scripts/migrate_research_store.py

Delete this script once prod is migrated.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
from boto3.dynamodb.conditions import Attr, Key
from gnomepy.data import ArtifactStore, CatalogConfig, DatasetStore, TableSpec

from gnome_pipelines.datasets import cfb, cs2

OLD_TABLE = "gnome-research-sessions"
OLD_DATASETS_PK = "__datasets__"
SPECS = {s.name: s for s in (*cs2.ALL, *cfb.ALL) if s is not cs2.ROUND_FEATURES}
ARTIFACT_TYPES = ("xgboost_model",)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="*", help="dataset names to migrate (default: all with a spec)")
    parser.add_argument("--skip-artifacts", action="store_true")
    args = parser.parse_args()

    code_version = f"migration@{_git_sha()}"
    config = CatalogConfig.from_env()
    old = boto3.resource("dynamodb").Table(OLD_TABLE)
    s3 = boto3.client("s3")

    failures = []
    store = DatasetStore(config, code_version=code_version)
    for record in _newest_datasets(old):
        name = record["dataset_name"]
        if name not in SPECS or (args.only and name not in args.only):
            print(f"skip {name}")
            continue
        try:
            _migrate_dataset(store, s3, SPECS[name], record)
        except Exception as e:
            failures.append(name)
            print(f"FAIL {name}: {e}")

    if not args.skip_artifacts:
        artifacts = ArtifactStore(config, code_version=code_version)
        for record in _newest_artifacts(old):
            _migrate_artifact(artifacts, s3, record)

    if failures:
        sys.exit(f"failed: {failures}")


def _migrate_dataset(store: DatasetStore, s3, spec: TableSpec, record: dict) -> None:
    source = _fit(spec, _read_parquet(s3, record["s3_uri"]))
    ingested_at = datetime.fromisoformat(record["created_at"])
    result = store.publish(spec, source, ingested_at=ingested_at)
    _verify(spec, source, store.load(spec.name))
    print(f"ok   {spec.name} v{record['version']}: {len(source)} rows, {result.rows_written} written, "
          f"{result.rows_skipped} skipped, ingested_at={ingested_at.isoformat()}")


def _fit(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    if spec is cs2.H2H_HISTORY:
        df = df.assign(map_name=df["map_name"].fillna(""))
    return df


def _verify(spec: TableSpec, source: pd.DataFrame, loaded: pd.DataFrame) -> None:
    key = list(spec.key)
    # astype(str) because list columns are unhashable; publish collapses exact duplicates.
    unique = source[~source.astype(str).duplicated()]
    expected = _canonical(spec, unique).sort_values(key).reset_index(drop=True)
    actual = _canonical(spec, loaded).sort_values(key).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)


def _canonical(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    rows = pa.Table.from_pandas(df[spec.schema.names], preserve_index=False)
    return rows.cast(spec.schema, safe=False).to_pandas()


def _migrate_artifact(store: ArtifactStore, s3, record: dict) -> None:
    artifact_type, name = record["artifact_type"], record["artifact_name"]
    if store.list(artifact_type, name):
        print(f"skip artifact {artifact_type}/{name}: already migrated")
        return
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(record["s3_uri"]).name
        bucket, key = record["s3_uri"].removeprefix("s3://").split("/", 1)
        s3.download_file(bucket, key, str(local))
        description = (f"{record.get('description') or ''} migrated from artifact://{artifact_type}/{name}:"
                       f"{record['version']} (git {record.get('git_commit', 'unknown')}, {record['created_at']})").strip()
        ref = store.publish(local, artifact_type, name, description=description)
    print(f"ok   artifact {ref}")


def _newest_datasets(table) -> list[dict]:
    newest: dict[str, dict] = {}
    for item in _query(table, KeyConditionExpression=Key("session_name").eq(OLD_DATASETS_PK)):
        name = item["dataset_name"]
        if name not in newest or int(item["version"]) > int(newest[name]["version"]):
            newest[name] = item
    return [newest[n] for n in sorted(newest)]


def _newest_artifacts(table) -> list[dict]:
    newest: dict[tuple[str, str], dict] = {}
    for item in _scan(table, FilterExpression=Attr("sk").begins_with("ARTIFACT#")):
        ident = (item["artifact_type"], item["artifact_name"])
        if item["artifact_type"] not in ARTIFACT_TYPES:
            continue
        if ident not in newest or int(item["version"]) > int(newest[ident]["version"]):
            newest[ident] = item
    return [newest[i] for i in sorted(newest)]


def _query(table, **kwargs) -> list[dict]:
    return _paginate(table.query, kwargs)


def _scan(table, **kwargs) -> list[dict]:
    return _paginate(table.scan, kwargs)


def _paginate(call, kwargs: dict) -> list[dict]:
    items = []
    while True:
        page = call(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs = {**kwargs, "ExclusiveStartKey": page["LastEvaluatedKey"]}


def _read_parquet(s3, uri: str) -> pd.DataFrame:
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        s3.download_file(bucket, key, f.name)
        return pd.read_parquet(f.name)


def _git_sha() -> str:
    return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                          cwd=Path(__file__).parent, check=True).stdout.strip()


if __name__ == "__main__":
    main()
