Architecture
============

Where iceberg-ops fits
----------------------

Many tools can change or maintain Apache Iceberg tables without a JVM.
They take different approaches (as of November 2026):

* **Declarative tools**, such as the Apache Iceberg Terraform provider
  and desired-state planners for Iceberg metadata, describe where a
  table should end up and compute the change for you.
* **Engines** such as Spark, Trino and Flink run ``ALTER TABLE`` DDL,
  but they do not version or track it.
* **iceberg-ops** follows the model of Alembic for relational databases.
  Each change is a numbered migration file with an ``upgrade()`` and a
  ``downgrade()``. The runner records which changes were applied, in
  what order and in which namespace, in an Iceberg history table inside
  your catalog. Autogenerate writes the downgrade where a step can be
  undone and makes it raise where it cannot.

The maintenance helpers and the cross-table data mover ship in the same
package but are secondary to the migration workflow.

Capability surface
------------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Module
     - Responsibility
   * - ``iceberg_ops.migration``
     - ``BaseMigration`` + ``MigrationContext`` -- the per-migration API
       surface (create, drop, schema evolution, partition spec, sort
       order, table properties).
   * - ``iceberg_ops.runner``
     - Migration runner: discover, apply, roll back, stamp, plan,
       status and history for one namespace at a time.
   * - ``iceberg_ops.history``
     - Iceberg-backed migration history table inside an
       ``_iceberg_ops`` meta-namespace, and ``ConcurrentMigrationError``.
   * - ``iceberg_ops.autogen``
     - Schema, partition-spec, sort-order and property diffs turned into
       migration ops, and a renderer that writes a migration file for
       review.
   * - ``iceberg_ops.scaffolding``
     - ``init_project`` + ``create_revision`` -- writes the
       ``iceberg-ops.yaml`` stub and ``NNNN_*.py`` migration skeletons.
   * - ``iceberg_ops.cli``
     - Click-based ``iceberg-ops`` entry point.
   * - ``iceberg_ops.core``
     - Catalog factory, schema diff / contract / evolution helpers,
       Arrow <-> Iceberg type translation, snapshot / upsert / time-
       travel utilities.
   * - ``iceberg_ops.data_migration``
     - Cross-table streaming migrator with schema reconciliation,
       checkpoint-based resume, and three verify methods
       (experimental).
   * - ``iceberg_ops.maintenance``
     - Basic maintenance helpers for self-managed tables:
       ``compact_small_files_streaming``, ``expire_snapshots``,
       ``remove_orphan_files``, ``table_health_report``.
   * - ``iceberg_ops.polaris``
     - Apache Polaris policy client and dispatcher (experimental;
       snapshot expiry only).
   * - ``iceberg_ops.testing``
     - Public ``FakeSession`` / ``FakeResponse`` for downstream
       integrators driving the Polaris client without HTTP.

Catalog backends
----------------

Every API that takes a catalog accepts any PyIceberg ``Catalog``.
``CatalogFactory.build`` is a convenience for six kinds:

.. list-table::
   :header-rows: 1
   :widths: 18 30 52

   * - Kind
     - Install
     - Notes
   * - ``"sqlite"``
     - ``pip install "iceberg-ops[sqlite]"``
     - Local file-backed catalog for development and CI. Without
       ``db_path`` / ``warehouse_dir`` it uses
       ``./local_data/iceberg_catalog.db`` and
       ``./local_data/iceberg_warehouse`` in the current working
       directory at call time. The CLI's ``--catalog sqlite`` uses
       ``local_data/`` next to the config file, or ``--db-path`` /
       ``--warehouse``.
   * - ``"memory"``
     - ``pip install "iceberg-ops[sqlite]"``
     - In-memory SQLite (``sqlite://``) for tests and throw-away jobs.
       Data files go to ``warehouse_dir`` or a fresh temporary
       directory. Python API only: the CLI rejects it because its
       state is gone when the command exits.
   * - ``"glue"``
     - ``pip install "iceberg-ops[glue]"``
     - AWS Glue Data Catalog.
   * - ``"rest"``
     - ``pip install iceberg-ops``
     - REST catalogs such as Apache Polaris, Nessie and Lakekeeper.
       PyIceberg's REST client is part of the core install.
   * - ``"hive"``
     - ``pip install "iceberg-ops[hive]"``
     - Hive Metastore.
   * - ``"s3tables"``
     - ``pip install "iceberg-ops[s3tables]"``
     - AWS S3 Tables (REST + SigV4, pre-set); pass ``uri``,
       ``warehouse`` (the table bucket ARN) and ``region``.

The remote kinds (``glue``, ``rest``, ``hive``, ``s3tables``) are
loaded through :func:`pyiceberg.catalog.load_catalog` under a catalog
name that defaults to the kind, so ``.pyiceberg.yaml`` and
``PYICEBERG_CATALOG__<NAME>__*`` environment variables apply; explicit
keyword arguments win. From the CLI, ``--catalog-name NAME`` opens any
catalog PyIceberg can load. Only the SQLite and in-memory kinds are
integration-tested; see :doc:`compatibility`.

Backends are *lazy-imported*: every backend lives behind a
``_lazy_import`` helper so ``import iceberg_ops`` succeeds even when none
of the optional extras are installed. Calling ``CatalogFactory.build``
for a backend whose extra is missing raises an ``ImportError`` that
names the ``pip install 'iceberg-ops[...]'`` line to run.

Self-managed metadata namespace
-------------------------------

The runner stores migration history as a real Iceberg table inside the
user's catalog under a meta-namespace (default ``_iceberg_ops``):

.. code-block:: text

   _iceberg_ops.migration_history
     version, namespace, description, applied_at, status, error

The table holds one row per ``(version, namespace)`` with the latest
status (``applied``, ``rolled_back`` or ``failed``). Earlier states are
readable by time travel on the history table until its snapshots
expire. The meta-namespace and the table are created by the first
write; ``plan``, ``status`` and ``history`` do not create them.

Keeping history in the catalog means it moves with the data and needs
no separate database to back up. The Python API can use another
meta-namespace through ``MigrationRunner(meta_namespace=...)``; the CLI
always uses ``_iceberg_ops``.

Concurrent runs against one namespace are not guarded: run one
migration job per namespace. If two ``migrate`` runs overlap, both can
execute ``upgrade()``; the second one to record history raises
``ConcurrentMigrationError`` and the history keeps one row per version
and namespace. See :doc:`compatibility`.

Namespace-scoped runner
-----------------------

.. code-block:: python

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.runner import MigrationRunner

   catalog = CatalogFactory.build("sqlite", db_path="catalog.db", warehouse_dir="warehouse")

   sales_runner = MigrationRunner(catalog, namespace="sales")
   finance_runner = MigrationRunner(catalog, namespace="finance")

Each namespace has its own history rows (one shared history table keyed
by namespace) and its own pending / applied view. Migration files are
namespace-agnostic: use bare table names so the same files apply
against ``sales`` and ``finance`` without edits. Files written by
``revision --autogenerate`` keep a bare ``--target-table`` bare, so they
follow the same rule. When ``migrations/<namespace>/`` exists, the
runner for that namespace uses it instead of the flat layout.

Local-filesystem-only operations
--------------------------------

Two of the maintenance helpers (``compact_small_files_streaming`` and
``remove_orphan_files``) support local ``file://`` table locations
only. Tables on ``s3://`` / ``gs://`` / ``abfs://`` raise
``NotImplementedError`` with the scheme named in the error message.
``expire_snapshots`` and ``table_health_report`` only touch metadata, so
they are expected to work on any filesystem PyIceberg supports, but they
are tested on local tables only. The maintenance helpers get bug fixes
only; the roadmap is to hand these jobs to PyIceberg's own maintenance
APIs as they land.
