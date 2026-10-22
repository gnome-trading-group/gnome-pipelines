"""Every table spec accepts a row and reads it back, against a local catalog."""
from datetime import datetime, timezone

import pyarrow as pa
import pytest
from gnomepy.data import CatalogConfig, DatasetStore

from gnome_pipelines.datasets import cfb, cs2, qb_status

SPECS = (*cs2.ALL, *cfb.ALL, *qb_status.ALL)


def sample_value(field: pa.Field):
    t = field.type
    if pa.types.is_timestamp(t):
        return datetime(2026, 1, 1, tzinfo=timezone.utc if t.tz else None)
    if pa.types.is_list(t):
        return [sample_value(pa.field("element", t.value_type))]
    if pa.types.is_boolean(t):
        return True
    if pa.types.is_integer(t):
        return 1
    if pa.types.is_floating(t):
        return 1.0
    return "x"


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_spec_accepts_a_row_and_reads_it_back(spec, tmp_path):
    row = pa.Table.from_pylist([{f.name: sample_value(f) for f in spec.schema}], schema=spec.schema).to_pandas()
    store = DatasetStore(CatalogConfig(local_warehouse=tmp_path))

    written = store.publish(spec, row).rows_written
    loaded = store.load(spec.name)

    assert written == 1
    assert list(loaded.columns) == spec.schema.names
    assert len(loaded) == 1


def test_spec_names_are_unique():
    names = [s.name for s in SPECS]
    assert len(names) == len(set(names))
