Compatibility and limits
========================

This page collects what iceberg-ops 0.1 supports, what it has been
tested with, and where its boundaries are. The guides link here instead
of repeating the details.

Python and dependencies
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 18 22 60

   * - Requirement
     - Range
     - Notes
   * - Python
     - 3.12 or later
     - CI tests 3.12, 3.13 and 3.14 on Linux and 3.12 on Windows, plus
       one run at the lowest declared dependency versions. Python 3.15
       is not tested; at release time PyIceberg 0.12.0 published no
       Python 3.15 wheels.
   * - PyIceberg
     - ``>=0.12.0,<0.13``
     - Every catalog extra uses the same range.
   * - PyArrow
     - ``>=18``
     - iceberg-ops does not use NumPy, but PyArrow 26 and later refuse
       to import next to NumPy 1.x. In an environment pinned to NumPy
       1.x, also pin ``pyarrow<26``.
   * - Click
     - ``>=8.2``
     - The CLI.
   * - PyYAML
     - ``>=6.0.1``
     - Reads ``iceberg-ops.yaml`` and YAML schema contracts.
   * - tzdata
     - Windows only
     - Windows ships no time-zone database; ``timestamptz`` columns
       need one.

Extras
------

.. list-table::
   :header-rows: 1
   :widths: 22 33 45

   * - Extra
     - Installs
     - Needed for
   * - ``sqlite``
     - ``pyiceberg[sql-sqlite]``
     - The local SQLite catalog and the in-memory catalog.
   * - ``glue``
     - ``pyiceberg[glue]``
     - AWS Glue catalogs.
   * - ``hive``
     - ``pyiceberg[hive]``
     - Hive Metastore catalogs.
   * - ``s3tables``
     - ``pyiceberg[rest-sigv4]``
     - AWS S3 Tables (REST with SigV4 signing; brings boto3).
   * - ``transforms``
     - ``pyiceberg[pyiceberg-core]``
     - Writing data to tables partitioned by bucket, truncate, year,
       month, day or hour transforms (for example with
       ``migrate-data``). Schema migrations do not need it.
   * - ``polaris``
     - ``requests``, ``urllib3``
     - The experimental Apache Polaris policy client.
   * - ``rest``, ``contracts``
     - nothing beyond the core install
     - Accepted as aliases. PyIceberg's REST client and PyYAML are part
       of the core install.

``docs`` and ``dev`` are for contributors; see
`CONTRIBUTING.md <https://github.com/namitkewat/iceberg-ops/blob/main/CONTRIBUTING.md>`__.

Catalogs
--------

The Python API accepts any PyIceberg ``Catalog`` instance, and
``CatalogFactory.build`` is a convenience for the kinds below. The CLI
selects a catalog with ``--catalog`` or ``--catalog-name`` (see
:doc:`cli`).

.. list-table::
   :header-rows: 1
   :widths: 17 29 29 25

   * - Catalog
     - Python API
     - CLI
     - Tested
   * - SQLite (local file)
     - ``CatalogFactory.build("sqlite", db_path=..., warehouse_dir=...)``
     - ``--catalog sqlite``, with ``--db-path`` / ``--warehouse`` or the
       ``local_data/`` default
     - Integration-tested
   * - In-memory
     - ``CatalogFactory.build("memory")``
     - Not accepted: its state is gone when the command exits
     - Integration-tested (the test suite's default catalog)
   * - REST (Apache Polaris, Nessie, Lakekeeper, ...)
     - ``load_catalog(name)`` or ``CatalogFactory.build("rest", ...)``
     - ``--catalog-name NAME`` or ``--catalog rest``
     - Through PyIceberg; not tested against a live server
   * - AWS Glue
     - ``load_catalog(name)`` or ``CatalogFactory.build("glue", ...)``
     - ``--catalog-name NAME`` or ``--catalog glue``
     - Through PyIceberg; not tested against the live service
   * - Hive Metastore
     - ``load_catalog(name)`` or ``CatalogFactory.build("hive", ...)``
     - ``--catalog-name NAME`` or ``--catalog hive``
     - Through PyIceberg; not tested against a live metastore
   * - AWS S3 Tables
     - ``CatalogFactory.build("s3tables", uri=..., warehouse=..., region=...)``
     - ``--catalog-name NAME`` or ``--catalog s3tables``
     - Through PyIceberg; not tested against the live service
   * - Other PyIceberg catalog types
     - ``load_catalog(name)`` or your own instance
     - ``--catalog-name NAME``
     - ``--catalog-name`` is tested with SQL catalogs declared in
       ``.pyiceberg.yaml``

How the CLI finds a catalog:

* ``--catalog-name NAME`` (or ``catalog_name:`` in ``iceberg-ops.yaml``)
  calls ``pyiceberg.catalog.load_catalog(NAME)``. Its properties come
  from ``.pyiceberg.yaml`` (PyIceberg looks in ``$PYICEBERG_HOME``, then
  the home directory, then the current directory, and uses the first
  file it finds) and from ``PYICEBERG_CATALOG__<NAME>__<KEY>``
  environment variables. In variable names ``_`` stands for ``-``, so the
  catalog ``my-cat`` reads ``PYICEBERG_CATALOG__MY_CAT__URI``.
* ``--catalog glue``, ``rest``, ``hive`` or ``s3tables`` loads the
  catalog named after the kind the same way, so
  ``PYICEBERG_CATALOG__REST__URI`` configures ``--catalog rest``.
  ``--warehouse`` sets the ``warehouse`` property; ``--db-path`` applies
  to ``sqlite`` only.
* ``migrate-data`` and ``run-policies`` take their own catalog options;
  the catalog settings in ``iceberg-ops.yaml`` do not apply to them.

Migration history is a table in the ``_iceberg_ops`` namespace of the
managed catalog, created on the first write. The Python API can use
another namespace through ``MigrationRunner(meta_namespace=...)``; the
CLI always uses ``_iceberg_ops``, so a catalog that rejects that
namespace name needs the Python API.

Filesystems
-----------

Schema migrations are metadata operations and go through PyIceberg's
file IO, so they work wherever PyIceberg can read and write the table's
metadata. Two maintenance helpers walk or rewrite files themselves and
support the local filesystem only:

.. list-table::
   :header-rows: 1
   :widths: 40 20 40

   * - Helper
     - ``file://``
     - ``s3://``, ``gs://``, ``abfs://``
   * - ``table_health_report``
     - yes
     - not tested (reads metadata only)
   * - ``expire_snapshots``
     - yes
     - not tested (metadata only; data files stay on storage)
   * - ``compact_small_files_streaming``
     - yes
     - no (``NotImplementedError``)
   * - ``remove_orphan_files``
     - yes
     - no (``NotImplementedError``)

See :doc:`maintenance` for each helper's behaviour.

Format versions, delete files and deletion vectors
--------------------------------------------------

* Every write iceberg-ops performs (history rows, compaction, data
  migration, upserts) goes through PyIceberg and is copy-on-write.
  iceberg-ops never writes position-delete files, equality-delete files
  or deletion vectors.
* ``compact_small_files_streaming`` does not support tables that already
  contain delete files or deletion vectors, and it does not check for
  them. Do not run it on such tables.
* ``remove_orphan_files`` keeps every file the table metadata
  references, including delete files, deletion-vector Puffin files and
  statistics files.
* ``table_health_report`` does not account for delete files.
* Format version 3 features are handled only as far as PyIceberg 0.12
  supports them. iceberg-ops adds no v3-specific handling and has no v3
  tests.

Concurrency
-----------

Concurrent runs against one namespace are not guarded: run one migration
job per namespace at a time, for example one CI job or a concurrency
group in your orchestrator. If two ``migrate`` runs overlap, both can
execute ``upgrade()``; the second one to record history raises
``ConcurrentMigrationError``, and the history keeps one row per version
and namespace. Because that run's ``upgrade()`` may already have run,
keep ``upgrade()`` idempotent. Runners for different namespaces can
share the history table. See :doc:`schema_migrations` for the details.

A data-migration checkpoint file belongs to one job; do not share it
between concurrent runs. A concurrent append to a partition being
compacted fails the run with ``ValidationException`` and the staged
files are removed. PyIceberg retries the commit after appends to other
partitions; those appends are preserved and counted in
``files_after``.

Experimental and frozen modules
-------------------------------

iceberg-ops is Alpha (0.x). Any minor release before 1.0 may change the
Python API, CLI options and the history-table or checkpoint layouts;
the changelog lists breaking changes.

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Module
     - Status
     - Scope
   * - Migrations (``migration``, ``runner``, ``history``, ``autogen``,
       ``scaffolding``, the CLI migration commands)
     - Alpha
     - The focus of development.
   * - ``iceberg_ops.data_migration``, ``migrate-data``
     - Experimental
     - Documented fences in :doc:`data_migration`:
       ``overwrite_partition`` only in one commit on identity-partitioned
       destinations, resume refuses changed settings, the destination
       table must exist, and memory is not bounded by ``batch_rows``.
   * - ``iceberg_ops.maintenance``
     - Basic, frozen
     - Helpers for self-managed tables on the local filesystem; bug
       fixes only. See :doc:`maintenance`.
   * - ``iceberg_ops.polaris``, ``run-policies``
     - Experimental
     - Executes ``system.snapshot-expiry`` only. Its requests do not yet
       match the released Polaris policy API, so fetching policies from
       a released Polaris server fails. See :doc:`polaris`.
   * - ``iceberg_ops.testing``
     - May change
     - Test doubles for the Polaris client.

Upstream PyIceberg issues
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Issue
     - Effect and what iceberg-ops does
   * - `apache/iceberg-python#4194
       <https://github.com/apache/iceberg-python/issues/4194>`__
     - On PyIceberg 0.12 a concurrent ``Table.upsert`` can be retried
       into a duplicate row. iceberg-ops writes history rows with an
       overwrite scoped to ``(version, namespace)`` instead, so a
       conflicting write raises ``ConcurrentMigrationError``. Duplicate
       rows left by earlier runs are read as their latest status and
       removed by the next write for that version. Upserts in your own
       code are not covered by this guard.
   * - `apache/iceberg-python#3508
       <https://github.com/apache/iceberg-python/issues/3508>`__
     - A composite-key upsert with a few thousand keys can crash the
       interpreter. iceberg-ops splits larger composite-key batches into
       sub-batches of 1,000 keys inside one commit.
   * - `apache/iceberg-python#2467
       <https://github.com/apache/iceberg-python/issues/2467>`__
     - The first upsert after a schema-only change fails. The data
       migrator and ``upsert_streaming`` commit one empty snapshot
       first.
   * - `apache/iceberg-python#2407
       <https://github.com/apache/iceberg-python/issues/2407>`__
     - PyIceberg's batch reader decodes whole files, so peak memory in
       compaction and data migration grows with the number and size of
       source files. iceberg-ops does not work around this.

Windows
-------

* ``tzdata`` is installed automatically.
* Write a local warehouse in ``.pyiceberg.yaml`` as
  ``file://C:/path/to/warehouse``; PyArrow rejects the
  ``file:///C:/...`` form. The built-in ``sqlite`` kind builds its
  warehouse URI in the ``file://C:...`` form for you. Other engines may
  expect a different spelling of a local Windows path, so a table
  written this way may not open elsewhere.
