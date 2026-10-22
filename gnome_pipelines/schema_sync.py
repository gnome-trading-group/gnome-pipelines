"""
Bring existing tables in line with their specs (gnome_pipelines/datasets/).

DatasetStore.evolve only adds columns, and a publish refuses a table whose columns differ from its spec, so a spec
that drops columns needs this first. Columns the spec added are added (rows already stored read them as null). Columns
it dropped are deleted only with drop_removed, which keeps every row. A table that is wholly derived from others, such
as cs2_match_priors, can instead be recreated empty (its data files deleted) and republished by its writer.

A schema change makes no snapshot, and reads are pinned to the newest snapshot's schema, so readers see the new
columns from the table's next write on.
"""
from __future__ import annotations

from dataclasses import dataclass

from gnomepy.data import DatasetStore, TableSpec
from gnomepy.data.datasets import MANAGED_COLUMNS
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError


@dataclass(frozen=True)
class SchemaPlan:
    name: str
    missing: bool
    added: tuple[str, ...]
    removed: tuple[str, ...]

    @property
    def in_sync(self) -> bool:
        return not (self.missing or self.added or self.removed)


def plan(catalog: Catalog, namespace: str, spec: TableSpec) -> SchemaPlan:
    try:
        table = catalog.load_table(f"{namespace}.{spec.name}")
    except NoSuchTableError:
        return SchemaPlan(spec.name, True, (), ())
    stored = [f.name for f in table.schema().fields if f.name not in MANAGED_COLUMNS]
    declared = list(spec.schema.names)
    return SchemaPlan(spec.name, False, tuple(c for c in declared if c not in stored),
                      tuple(c for c in stored if c not in declared))


def apply(store: DatasetStore, catalog: Catalog, namespace: str, spec: TableSpec, schema_plan: SchemaPlan, *,
          drop_removed: bool = False, recreate: bool = False) -> str:
    """Carry out the plan; returns what was done. Raises if columns would be left that the spec no longer has."""
    ident = f"{namespace}.{spec.name}"
    if schema_plan.missing:
        store.create(spec)
        return "created"
    if recreate:
        # S3 Tables only drops a table together with its data files.
        catalog.purge_table(ident)
        store.create(spec)
        return "recreated empty"
    if schema_plan.removed and not drop_removed:
        raise ValueError(f"{spec.name} has columns its spec dropped ({', '.join(schema_plan.removed)}): "
                         "pass drop_removed to delete them, or recreate a derived table")
    if schema_plan.removed:
        with catalog.load_table(ident).update_schema() as update:
            for column in schema_plan.removed:
                update.delete_column(column)
    if schema_plan.added:
        store.evolve(spec)
    return f"added {len(schema_plan.added)}, deleted {len(schema_plan.removed)} columns"
