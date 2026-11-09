# iceberg-ops

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/namitkewat/iceberg-ops/main/docs/_static/logo-dark.svg">
  <img src="https://raw.githubusercontent.com/namitkewat/iceberg-ops/main/docs/_static/logo-light.svg" alt="iceberg-ops logo" width="96" height="96" align="right">
</picture>

**Alembic-style schema migrations for Apache Iceberg.**
Versioned, reviewable table changes: numbered `upgrade()` / `downgrade()`
revisions, autogenerate, `stamp`, and a migration history stored as an
Iceberg table in your own catalog. Pure Python on PyIceberg: no Spark, no
JVM.

[![CI](https://github.com/namitkewat/iceberg-ops/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/namitkewat/iceberg-ops/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/namitkewat/iceberg-ops?display_name=tag&sort=semver)](https://github.com/namitkewat/iceberg-ops/releases/latest)
[![PyPI](https://img.shields.io/pypi/v/iceberg-ops)](https://pypi.org/project/iceberg-ops/)
[![Python](https://img.shields.io/pypi/pyversions/iceberg-ops)](https://pypi.org/project/iceberg-ops/)
[![Docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://namitkewat.github.io/iceberg-ops/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/namitkewat/iceberg-ops/blob/main/LICENSE)

`iceberg-ops` brings the migration workflow that application teams use for
relational databases to Apache Iceberg tables. Each schema change is a
numbered Python file that you review in a pull request, apply with
`migrate` and undo with `rollback`. The status of each migration is
recorded in a history table that lives next to your data.

## What you get

- **Versioned revisions.** `migrations/NNNN_*.py` files with `upgrade()`
  and `downgrade()`, driven by `migrate`, `rollback`, `plan`, `status`,
  `history` and `stamp` from the CLI or the Python API. A `downgrade()`
  that cannot undo a step should raise `NotImplementedError`.
- **Autogenerate.** `revision --autogenerate` diffs a live table against a
  YAML schema contract or a `pyarrow.Schema` and writes a migration file
  for you to review. Renames come from explicit `--rename-hint old:new`
  pairs. A step that cannot run (an incompatible type change, or a nested
  type change in a Python `pyiceberg.schema.Schema` target) makes the
  generated `upgrade()` raise, and a step that cannot be undone (a column
  drop, a type widening) makes the generated `downgrade()` raise, before
  any step runs. With a `pyarrow.Schema` target, existing struct, list
  and map columns are not compared and are treated as unchanged; write
  changes to them by hand. The CLI diffs columns; the Python API also
  diffs partition spec, sort order and table properties.
- **History in your catalog.** `_iceberg_ops.migration_history` is an
  Iceberg table in the same catalog, holding the latest status of each
  version per namespace. There is no extra database to run.
- **Adopt existing tables.** `stamp` records migrations as applied
  without running them, so tables created by hand can join the workflow.
- **Namespace-agnostic files.** Migration helpers take bare table names
  and the runner adds the namespace, so one set of files can be applied
  to `sales`, `staging` or `finance`.
- **Catalog-agnostic.** The Python API takes any PyIceberg `Catalog`. The
  CLI opens a local SQLite catalog, or any catalog defined in
  `.pyiceberg.yaml` or `PYICEBERG_CATALOG__*` environment variables.
  Integration tests cover SQLite and in-memory catalogs so far.
- **No cluster.** Runs on a laptop, in CI or from your orchestrator.

## Try it in 60 seconds

```bash
pip install "iceberg-ops[sqlite]"
mkdir demo && cd demo
iceberg-ops init                          # migrations/, iceberg-ops.yaml, a .gitignore entry
iceberg-ops revision -m "create orders"   # writes migrations/0001_create_orders.py
```

Replace the contents of `migrations/0001_create_orders.py` with:

```python
# migrations/0001_create_orders.py
import pyarrow as pa

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = "0001"
    description = "Create orders table"

    def upgrade(self, ctx: MigrationContext) -> None:
        ctx.create_table_if_not_exists(
            "orders",
            pa.schema(
                [
                    pa.field("order_id", pa.int64(), nullable=False),
                    pa.field("order_total", pa.float64()),
                    pa.field("created_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )

    def downgrade(self, ctx: MigrationContext) -> None:
        ctx.drop_table_if_exists("orders")
```

Apply it to a local SQLite catalog (`demo.db`, data files under
`warehouse/`) in the `sales` namespace:

```bash
iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales migrate
```

Describe the schema you want next in `contracts/orders.yaml`:

```yaml
# contracts/orders.yaml
columns:
  - {name: order_id, type: long, required: true}
  - {name: order_amount, type: double}  # renamed from order_total
  - {name: created_at, type: timestamptz}
  - {name: status, type: string}  # new column
```

Generate the migration and review it, then apply it, check the status and
undo it:

```bash
iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales \
  revision -m "rename total, add status" --autogenerate \
  --target-table orders --target-schema-yaml contracts/orders.yaml \
  --rename-hint order_total:order_amount

iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales plan
iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales migrate
iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales status
iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales rollback
```

`rollback` undoes one migration; pass `--steps N` or `--target 0001` to go
further back. Global options such as `--namespace` go before the
subcommand. To drop the flags, set `catalog`, `namespace`, `db_path` and
`warehouse` in `iceberg-ops.yaml`; without `db_path` and `warehouse` the
SQLite catalog lives under `local_data/` next to that file. In
PowerShell, end continued lines with a backtick instead of `\`.

## Install

```bash
pip install iceberg-ops                 # library + CLI; bring your own catalog
pip install "iceberg-ops[sqlite]"       # local SQLite and in-memory catalogs
pip install "iceberg-ops[glue]"         # AWS Glue
pip install "iceberg-ops[hive]"         # Hive Metastore
pip install "iceberg-ops[s3tables]"     # AWS S3 Tables (REST with SigV4 signing)
pip install "iceberg-ops[transforms]"   # writing to bucket / truncate / time-partitioned tables
pip install "iceberg-ops[polaris]"      # experimental Apache Polaris policy executor
```

REST catalogs (Apache Polaris, Nessie, Lakekeeper and others) need no
extra: PyIceberg's REST client is part of the core install. The `[rest]`
and `[contracts]` extras are accepted as aliases but add nothing to the
core install.

Requires Python 3.12 or later (CI tests 3.12, 3.13 and 3.14), PyIceberg
`>=0.12.0,<0.13` and PyArrow `>=18`. iceberg-ops does not use NumPy, but
PyArrow 26 and later refuse to import next to NumPy 1.x: if your
environment pins NumPy 1.x, also pin `pyarrow<26`.

## Use your own catalog

From the CLI, `--catalog-name` (or `catalog_name:` in `iceberg-ops.yaml`)
loads a catalog with `pyiceberg.catalog.load_catalog`, so its settings
come from `.pyiceberg.yaml` or `PYICEBERG_CATALOG__<NAME>__*` environment
variables:

```bash
iceberg-ops --catalog-name prod --namespace sales plan
iceberg-ops --catalog-name prod --namespace sales migrate
```

`--catalog glue`, `rest`, `hive` or `s3tables` loads the catalog named
after the kind in the same way, so `PYICEBERG_CATALOG__REST__URI`
configures `--catalog rest`. From Python, pass any PyIceberg catalog:

```python
from pyiceberg.catalog import load_catalog

from iceberg_ops.runner import MigrationRunner

catalog = load_catalog("prod")  # or any other PyIceberg Catalog instance
runner = MigrationRunner(catalog, namespace="sales", migrations_dir="migrations")
runner.plan()
runner.migrate()
```

The runner methods print their report. Integration tests cover SQLite and
in-memory catalogs; Glue, REST, Hive and S3 Tables catalogs go through
PyIceberg and have not yet been tested against live services.
[Catalog compatibility reports](https://github.com/namitkewat/iceberg-ops/issues)
are welcome.

## What a migration can do

`MigrationContext` methods take bare table names; the runner adds the
namespace.

- Tables: `create_table_if_not_exists` (with optional `partition_spec`,
  `sort_order`, `properties` and `location`, applied on create;
  `partition_spec` / `sort_order` need a `pyiceberg.schema.Schema`, so
  with a `pa.Schema` call `update_partition_spec` / `replace_sort_order`
  after creating), `drop_table_if_exists`, `load_table`
- Columns: `add_column`, `enforce_column` (idempotent, with opt-in
  `legacy_archival` for type changes Iceberg cannot promote),
  `enforce_rename`, `delete_column`
- Layout: `update_partition_spec`, `replace_sort_order`,
  `set_clustering_keys`
- Properties: `set_table_properties`, `unset_table_properties`,
  `set_snapshot_retention`, `set_compaction_target_size`
- Hooks: `pre_upgrade`, `post_upgrade`, `pre_downgrade`, `post_downgrade`
  (for example a data-quality gate before a change)

Column types for `add_column` and `enforce_column` can be PyIceberg types
such as `StringType()` or primitive PyArrow types such as `pa.string()`.

## Also included

These ship in the same package but are secondary to the migration
workflow.

**Cross-table data migration (experimental).** `iceberg-ops migrate-data`
(Python: `iceberg_ops.data_migration.DataMigrator`) streams rows from one
Iceberg table to another, in the same catalog or across catalogs, with
column renames, type coercions, missing-column policies, checkpoint
resume and `row_count` / `partition_hash` / `sample` verification. The
destination table must already exist. A resume refuses changed settings
(for example `--batch-rows` or `--row-filter`) and a checkpoint that
records a completed run. `--write-mode overwrite_partition` is refused
unless the destination is identity-partitioned and the migration fits in
one commit. `--batch-rows` bounds the rows per commit, not peak memory.

**Maintenance helpers (basic).** `table_health_report`,
`expire_snapshots`, `compact_small_files_streaming` and
`remove_orphan_files` for self-managed tables, from the Python API.
Compaction (identity partitions only) and orphan removal support the
local filesystem only. `remove_orphan_files` defaults to a dry run and
refuses tables with `gc.enabled=false`. The scope is frozen: these
helpers get bug fixes only.

**Apache Polaris policy executor (experimental).** `iceberg_ops.polaris`
and `iceberg-ops run-policies` dispatch Polaris policies to the
maintenance helpers: `system.snapshot-expiry` runs through
`expire_snapshots`, and other policy types are reported as skipped. The
client's requests do not yet match the released Polaris policy API, so
it cannot fetch policies from a released Polaris server yet, and it has
not been validated against a live one.

## What iceberg-ops is, and isn't

**It is** a migration framework (files, history, rollback, `stamp`,
autogenerate) for Iceberg schemas, partition specs, sort orders and table
properties, shipped as a Python library and CLI on top of PyIceberg.

**It isn't:**

- a query engine or a managed maintenance service;
- a merge-on-read writer: the rewrites it performs are copy-on-write, and
  it does not write delete files or deletion vectors. Compaction does not
  support tables that already contain them and does not check for them,
  so do not run it on such tables;
- a lock service: concurrent runs for one namespace are not guarded. A
  second concurrent run is detected when it records history
  (`ConcurrentMigrationError`), but its `upgrade()` may already have run.
  Run migrations for a namespace from a single job;
- a partition-rewrite engine: `migrate-data --write-mode
  overwrite_partition` is refused unless the whole migration fits in one
  commit.

## How it compares

As of November 2026:

| If you want to... | Consider |
|---|---|
| Ship ordered, reviewable schema changes with up/down revisions, a history table and `stamp`, without a cluster | **iceberg-ops** |
| Declare tables as infrastructure and let `terraform plan` / `apply` converge them | The Apache Iceberg Terraform provider |
| Declare the desired end state and have a tool plan and apply the difference, with approvals | A desired-state planner for Iceberg metadata |
| Run one-off DDL from an engine you already operate | Spark, Trino or Flink `ALTER TABLE` |
| Version the schema of a relational database | Alembic (the model iceberg-ops follows), Flyway and similar tools |
| Compact and clean up tables at object-storage scale | Your catalog's managed maintenance, Iceberg's Spark procedures, or a dedicated optimizer |

Declarative tools describe where a table should end up. iceberg-ops
records which changes were applied, in what order and in which namespace,
and keeps the code that undoes them next to the code that applied them.

## Compatibility

| | |
|---|---|
| Python | 3.12 or later; CI tests 3.12, 3.13 and 3.14 |
| PyIceberg | `>=0.12.0,<0.13` |
| PyArrow | `>=18` |
| Catalogs, CLI | `--catalog sqlite`; any catalog PyIceberg can load, through `--catalog-name` or `--catalog glue/rest/hive/s3tables` |
| Catalogs, Python | any PyIceberg `Catalog`; `CatalogFactory` builds sqlite, memory, glue, rest, hive and s3tables |
| Integration-tested | SQLite and in-memory catalogs |
| Local filesystem only | `compact_small_files_streaming`, `remove_orphan_files` |

The full matrix, the delete-file and v3 boundary, and the upstream
PyIceberg issues that affect iceberg-ops are on the
[compatibility page](https://namitkewat.github.io/iceberg-ops/compatibility.html).

## Stability

iceberg-ops is Alpha software (Development Status 3). Until 1.0, any
minor release (0.2, 0.3, ...) may change the Python API, CLI options and
the history-table or checkpoint layouts; breaking changes are listed in
the changelog.

- Experimental: the Apache Polaris policy executor (`iceberg_ops.polaris`,
  `run-policies`) and cross-table data migration
  (`iceberg_ops.data_migration`, `migrate-data`).
- Basic and frozen in scope: the maintenance helpers
  (`iceberg_ops.maintenance`).
- The test doubles in `iceberg_ops.testing` may change before 1.0.

The migration workflow (`migration`, `runner`, `history`, `autogen`,
`scaffolding` and the CLI's migration commands) is where development
focuses.

## For coding agents

- [AGENTS.md](https://github.com/namitkewat/iceberg-ops/blob/main/AGENTS.md):
  for agents that change this repository (layout, layering rules, CI
  gates, test conventions).
- [agent-skills/iceberg-ops-migrations/SKILL.md](https://github.com/namitkewat/iceberg-ops/blob/main/agent-skills/iceberg-ops-migrations/SKILL.md):
  a skill in the agentskills.io format that teaches a coding agent to
  write, review and run iceberg-ops migrations in your project. Copy the
  `iceberg-ops-migrations` directory into your agent's skills directory.
- [llms.txt](https://namitkewat.github.io/iceberg-ops/llms.txt) and
  [llms-full.txt](https://namitkewat.github.io/iceberg-ops/llms-full.txt):
  a plain-text index of the documentation and the full text of every page.

## Project status

Alpha. The test suite (more than 1,000 tests) runs against real SQLite and
in-memory PyIceberg catalogs, without mocking PyIceberg, on Linux
(Python 3.12 to 3.14) and Windows (Python 3.12).

Next up, from the [roadmap](https://github.com/namitkewat/iceberg-ops/blob/main/ROADMAP.md):

- an `iceberg-ops check` command that fails CI when migrations are
  pending or failed
- claim-before-apply and an append-only history for concurrent runs
- CLI autogenerate for partition spec, sort order and table properties
- a baseline migration generated from a live table
- an integration test against a REST catalog

## Links

- Documentation: https://namitkewat.github.io/iceberg-ops/
- Changelog: https://github.com/namitkewat/iceberg-ops/blob/main/CHANGELOG.md
- Roadmap: https://github.com/namitkewat/iceberg-ops/blob/main/ROADMAP.md
- Contributing: https://github.com/namitkewat/iceberg-ops/blob/main/CONTRIBUTING.md
- Code of conduct: https://github.com/namitkewat/iceberg-ops/blob/main/CODE_OF_CONDUCT.md
- Security policy: https://github.com/namitkewat/iceberg-ops/blob/main/SECURITY.md
- Issues: https://github.com/namitkewat/iceberg-ops/issues
- Discussions: https://github.com/namitkewat/iceberg-ops/discussions

## License

MIT. See [LICENSE](https://github.com/namitkewat/iceberg-ops/blob/main/LICENSE).

Apache Iceberg, Iceberg, Apache Polaris and Apache are trademarks of the
Apache Software Foundation. iceberg-ops is not affiliated with or endorsed
by the ASF.
