---
name: iceberg-ops-migrations
description: Write, review and run versioned schema migrations for Apache Iceberg tables with the iceberg-ops CLI and Python API. Use when a project has an iceberg-ops.yaml file or a migrations/ directory of NNNN_*.py files built on iceberg_ops.migration, or when asked to create an Iceberg table, add, rename, retype or drop columns, change partitioning, sort order or table properties, autogenerate a migration from a target schema, or apply, roll back, stamp or inspect Iceberg migrations.
license: MIT
compatibility: Python 3.12+, iceberg-ops 0.1.x (PyIceberg >=0.12.0,<0.13). The CLI command is iceberg-ops.
---

# iceberg-ops schema migrations

iceberg-ops applies Alembic-style migration files (`migrations/NNNN_*.py`)
to Apache Iceberg tables through PyIceberg. Applied versions are recorded
in an Iceberg history table inside the same catalog, one set of rows per
namespace.

## Install and set up a project

```bash
pip install "iceberg-ops[sqlite]"   # SQLite catalog for local work and tests
iceberg-ops init                     # creates migrations/, iceberg-ops.yaml, a .gitignore block
```

Other catalog extras exist (`glue`, `hive`, `s3tables`, `rest`), plus
`transforms` for writing to tables partitioned by bucket, truncate or time
transforms.

Edit `iceberg-ops.yaml`. CLI flags override it; relative paths in the file
resolve against the directory that holds it:

```yaml
catalog: sqlite          # catalog kind
namespace: sales         # namespace the migrations target
db_path: catalog.db      # SQLite catalog file
warehouse: warehouse     # data and metadata files
# migrations_dir: ./migrations
```

Global options go **before** the subcommand:
`iceberg-ops --catalog sqlite --namespace sales --db-path catalog.db --warehouse warehouse migrate`.
`--config PATH` points at another config file. Catalog options are listed
by `iceberg-ops --help`; check it before assuming a catalog can be reached
from the CLI.

Catalog configuration rules:

- For SQLite, always set `db_path` and `warehouse` explicitly (config file
  or flags) so every command opens the same catalog.
- `memory` does not survive between CLI invocations; use it only from
  Python (tests).
- For production catalogs that the CLI cannot configure, use the Python
  API with any PyIceberg catalog:

```python
from pyiceberg.catalog import load_catalog

from iceberg_ops.runner import MigrationRunner

catalog = load_catalog("prod")  # reads ~/.pyiceberg.yaml or PYICEBERG_CATALOG__PROD__* env vars
runner = MigrationRunner(catalog, namespace="sales", migrations_dir="migrations")
runner.plan()
runner.migrate()
```

`MigrationRunner` also has `status()`, `history()`, `rollback(steps=1,
target=None)` and `stamp(target=None)`.

## Write a migration

```bash
iceberg-ops revision -m "create orders"   # writes migrations/0001_create_orders.py
```

The skeleton raises `NotImplementedError` in both methods until you fill
them in. If `migrations/<namespace>/` exists, files go there and the runner
for that namespace reads only that directory.

```python
# migrations/0001_create_orders.py
from __future__ import annotations

import pyarrow as pa
from pyiceberg.types import StringType

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = "0001"
    description = "create orders"

    def upgrade(self, ctx: MigrationContext) -> None:
        ctx.create_table_if_not_exists(
            "orders",
            pa.schema(
                [
                    pa.field("order_id", pa.int64(), nullable=False),
                    pa.field("amount", pa.float64()),
                ]
            ),
        )
        ctx.enforce_column("orders", "status", StringType())

    def downgrade(self, ctx: MigrationContext) -> None:
        ctx.drop_table_if_exists("orders")
```

Rules for migration files:

- Pass bare table names (`"orders"`); the context qualifies them with the
  runner's namespace, so one file works for every namespace.
- `version` matches the file's `NNNN` prefix.
- `upgrade()` must be idempotent: prefer `create_table_if_not_exists`,
  `drop_table_if_exists`, `enforce_column` and `enforce_rename`.
- `create_table_if_not_exists` takes a PyArrow schema (or a PyIceberg
  `Schema`). `partition_spec` / `sort_order` need a PyIceberg `Schema`;
  with a `pa.Schema`, call `update_partition_spec` / `replace_sort_order`
  after creating. `add_column` and `enforce_column` take a PyIceberg type
  (`from pyiceberg.types import StringType`, then `StringType()`) or a
  primitive PyArrow type such as `pa.string()`; nested types (struct,
  list, map) must be PyIceberg types.
- `downgrade()` must undo `upgrade()` exactly, or raise
  `NotImplementedError` when it cannot. Never leave it as a silent no-op.

## MigrationContext helpers

| Helper | What it does |
|---|---|
| `create_table_if_not_exists(name, pa_schema)` | Create the table (and namespace) unless it exists |
| `drop_table_if_exists(name)` | Drop the table if present |
| `add_column(name, column, iceberg_type, doc=None)` | Add a column; raises if it already exists |
| `enforce_column(name, column, iceberg_type, doc=None, legacy_archival=False)` | Idempotent: adds the column, or updates it in place to a compatible type; an incompatible type raises unless `legacy_archival=True`, which archives the old column as `<column>__legacy_N` and adds a fresh one |
| `enforce_rename(name, old, new)` | Idempotent rename; no-op if already renamed |
| `delete_column(name, column)` | Metadata-only drop; re-adding the name later creates a new, empty column |
| `update_partition_spec(name, add_fields=[(column, transform, partition_name)], remove_fields=[partition_name])` | Evolve partitioning; transforms come from `pyiceberg.transforms`, e.g. `DayTransform()` |
| `replace_sort_order(name, sort_fields=[(column, "asc" or "desc")])` | Replace the sort order; empty clears it |
| `set_table_properties(name, {key: value})` / `unset_table_properties(name, [key])` | Change table properties in one commit |
| `set_compaction_target_size(name, target_mb)` | Set `write.target-file-size-bytes` |
| `load_table(name)`, `qualified(name)` | Escape hatches to the PyIceberg `Table` and the full identifier |

Column names may be dotted paths (`"address.zip"`) for nested struct
fields. `BaseMigration` also offers optional `pre_upgrade`, `post_upgrade`,
`pre_downgrade` and `post_downgrade` hooks; an exception in `pre_upgrade`
stops the migration and records it as failed.

## Autogenerate a migration

Diff a live table against a target schema and write the matching file:

```bash
iceberg-ops revision --autogenerate -m "add status" \
  --target-table orders --target-schema-yaml contracts/orders.yaml
```

- Target: exactly one of `--target-schema-yaml PATH` (a contract with a
  `columns:` list of `name`, `type`, optional `required` and `doc`; types
  such as `long`, `double`, `string`, `timestamptz`) or
  `--target-schema-arrow module:attr` (an importable `pyarrow.Schema`).
  With a `pyarrow.Schema` target, existing struct, list and map columns
  are not compared and are treated as unchanged; write changes to them
  by hand.
- Without hints a renamed column appears as a drop plus an add; pass
  `--rename-hint old:new` (repeatable) to emit a rename.
- From the CLI only columns are diffed. Partition spec, sort order and
  property changes are written by hand.
- Needs the catalog and namespace, and an existing migrations directory.

## Run and inspect

| Command | Effect |
|---|---|
| `iceberg-ops plan [--target V]` | Preview pending migrations; writes nothing to the catalog |
| `iceberg-ops migrate [--target V]` | Apply pending migrations in version order |
| `iceberg-ops status` (alias `current`) | Applied / pending / failed state per version |
| `iceberg-ops history` | Recorded history rows for the namespace |
| `iceberg-ops rollback [--steps N] [--target V]` | Run `downgrade()` newest first; default one step; `--target V` undoes everything after V |
| `iceberg-ops stamp [--target V]` | Record migrations as applied without running them |

- A failing `upgrade()` is recorded as failed, the run stops, and
  `migrate` exits 1. Fix the file and run `migrate` again.
- A `downgrade()` that raises stops `rollback` with exit 1 and leaves that
  version applied.
- `status`, `current`, `history` and `plan` print and exit 0 even when a
  migration is recorded as failed; read their output rather than relying
  on the exit code.
- `stamp` adopts tables created outside iceberg-ops. It does not check that
  the live schema matches the stamped migrations.

## Safety rules

1. **Review every generated file before running it**, especially the
   downgrade: confirm it really restores the previous state and names the
   intended table. A type widening cannot be narrowed back, so its
   downgrade must raise. Every run-and-inspect command except `history`
   (`plan` and `status` included) imports and executes the migration files
   with the caller's catalog credentials, so never run them on unreviewed
   files with production credentials.
2. **Irreversible steps raise.** If a change cannot be undone (a dropped
   column's data, a widened type), `downgrade()` raises
   `NotImplementedError` with a short reason.
3. **Run one migration job per namespace.** Concurrent runs against the
   same namespace are not guarded; serialise them (one CI job, or an
   orchestrator concurrency limit).
4. **Never edit a migration that has been applied anywhere.** Add a new
   migration instead.
5. **Rehearse first.** Run `plan`, then `migrate` and `rollback` against a
   scratch SQLite catalog (a copy, or `tmp_path` in a test) before touching
   a shared catalog.
6. **Dry-run maintenance and data moves first.** Call the maintenance
   helpers (`remove_orphan_files`, `expire_snapshots`,
   `compact_small_files_streaming`) with `dry_run=True` explicitly before a
   real run, and use `--dry-run` with `iceberg-ops migrate-data` and
   `iceberg-ops run-policies`. Compaction and orphan removal support local
   filesystems only, and `run-policies` is experimental.
7. Keep credentials out of migration files, config files committed to git,
   and command lines that end up in shell history or CI logs.
