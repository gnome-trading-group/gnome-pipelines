"""Bringing a stored table in line with a spec that added and dropped columns, against a local catalog."""
import pandas as pd
import pyarrow as pa
import pytest
from gnomepy.data import CatalogConfig, DatasetStore, TableSpec
from gnomepy.data.catalog import open_catalog

from gnome_pipelines import schema_sync

KEY = pa.field("id", pa.int64(), nullable=False)
OLD = TableSpec("probe", pa.schema([KEY, pa.field("kept", pa.string()), pa.field("dropped", pa.float64())]), key=("id",))
NEW = TableSpec("probe", pa.schema([KEY, pa.field("kept", pa.string()), pa.field("added", pa.float64())]), key=("id",))


@pytest.fixture
def stores(tmp_path):
    config = CatalogConfig(local_warehouse=tmp_path)
    store = DatasetStore(config)
    store.publish(OLD, pd.DataFrame({"id": [1, 2], "kept": ["a", "b"], "dropped": [0.1, 0.2]}))
    return store, open_catalog(config), config.namespace


def test_the_plan_names_what_the_spec_added_and_dropped(stores):
    _, catalog, ns = stores
    p = schema_sync.plan(catalog, ns, NEW)
    assert (p.missing, p.added, p.removed) == (False, ("added",), ("dropped",)) and not p.in_sync
    assert schema_sync.plan(catalog, ns, OLD).in_sync
    assert schema_sync.plan(catalog, ns, TableSpec("absent", pa.schema([KEY]), key=("id",))).missing


def test_dropped_columns_are_kept_unless_asked_and_rows_survive_their_deletion(stores):
    store, catalog, ns = stores
    with pytest.raises(ValueError, match="dropped"):
        schema_sync.apply(store, catalog, ns, NEW, schema_sync.plan(catalog, ns, NEW))
    schema_sync.apply(store, catalog, ns, NEW, schema_sync.plan(catalog, ns, NEW), drop_removed=True)
    assert schema_sync.plan(catalog, ns, NEW).in_sync
    # Reads are pinned to the newest snapshot, whose schema they use: the change shows from the next write on.
    store.publish(NEW, pd.DataFrame({"id": [3], "kept": ["c"], "added": [1.5]}))
    rows = store.load("probe").sort_values("id")
    assert list(rows.columns) == ["id", "kept", "added"]
    assert rows.kept.tolist() == ["a", "b", "c"] and rows.added.isna().tolist() == [True, True, False]


def test_a_derived_table_is_recreated_empty(stores):
    store, catalog, ns = stores
    assert schema_sync.apply(store, catalog, ns, NEW, schema_sync.plan(catalog, ns, NEW), recreate=True) == "recreated empty"
    assert store.load("probe").empty and schema_sync.plan(catalog, ns, NEW).in_sync
