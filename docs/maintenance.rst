Storage Maintenance
===================

The maintenance package wraps PyIceberg's lower-level primitives in
four operations. They are basic helpers for self-managed tables on the
local filesystem; delete files and v3 deletion vectors are out of
scope. Catalog services that run table maintenance themselves (for
example AWS S3 Tables) do not need them. These APIs may change before
1.0.

The four helpers
----------------

* :py:func:`iceberg_ops.maintenance.compact_small_files_streaming` --
  per-partition compaction. Reads each qualifying partition via
  ``Table.scan().to_arrow_batch_reader``, writes pre-staged Parquet
  files of roughly ``target_size_mb``, then commits one PyIceberg
  transaction (overwrite per touched partition + ``add_files``).
  Identity-transformed partitions (and unpartitioned tables) only;
  other partitions are skipped with a warning. Peak memory is **not**
  bounded by ``target_size_mb``: PyIceberg's batch reader decodes
  whole source files (`apache/iceberg-python#2407
  <https://github.com/apache/iceberg-python/issues/2407>`_,
  `apache/iceberg-python#4028
  <https://github.com/apache/iceberg-python/pull/4028>`_), so memory
  grows with the number and size of files per partition.

* :py:func:`iceberg_ops.maintenance.expire_snapshots` -- max-age +
  min-count snapshot expiry via PyIceberg's
  ``Table.maintenance.expire_snapshots()``. All arguments after
  ``table`` are keyword-only. Branch and tag heads (including the
  current snapshot) are never expired; those old enough to expire are
  reported in ``protected_ids``. Unset arguments fall back to the table's
  ``history.expire.max-snapshot-age-ms`` and
  ``history.expire.min-snapshots-to-keep`` properties (as written by
  ``MigrationContext.set_snapshot_retention``), then to the Iceberg
  defaults of 5 days and 1 snapshot. ``min_snapshots`` below 1 or a
  negative age raise ``ValueError``. Expiry is metadata-only: data
  files of expired snapshots stay on storage until orphan cleanup
  reclaims them (``file://`` only).

* :py:func:`iceberg_ops.maintenance.remove_orphan_files` -- recursive
  ``os.walk`` over a configurable sweep list (defaults to ``data/`` +
  ``metadata/``). **Defaults to** ``dry_run=True``: it reports and
  deletes nothing unless you pass ``dry_run=False``. A file is kept if
  any table metadata references it: manifest lists, manifests, every
  live manifest entry of every snapshot (data files, position- and
  equality-delete files, deletion-vector Puffin files), table and
  partition statistics files, and every metadata JSON. Matching is by
  basename and is conservative (a file is kept if any referenced path
  shares its name). Names are compared case-insensitively after
  Unicode NFC normalisation, because Windows and default macOS
  filesystems resolve them that way. The walk does not follow
  directory symlinks or Windows junctions, and a file whose real path
  lies outside the table location is never deleted. Only files with
  known Iceberg suffixes
  (``.parquet`` / ``.avro`` / ``.metadata.json`` / ``.puffin`` /
  ``.stats``) whose mtime predates the cutoff are candidates. Tables
  with ``gc.enabled=false`` are refused with ``ValueError`` (in
  dry-run too), a negative ``older_than_hours`` is rejected, sweep
  entries must be relative and stay inside the table location, and
  deletions that fail are reported in ``failed_paths``. Never run it
  against a location shared with another table.

* :py:func:`iceberg_ops.maintenance.table_health_report` -- read-only
  diagnostic. File sizes are aggregated with ``pyarrow.compute``, but
  the input comes from ``inspect.files()``, which builds one Python
  dict per file, so cost grows with the file count. Returns a
  ``TableHealthReport`` with file counts and sizes, snapshot count,
  oldest-snapshot age, small-file count, and a list of human-readable
  recommendations.

The examples below each build a small local table first, so they run
as written; with your own table, skip that part.

Compaction example
------------------

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.maintenance import compact_small_files_streaming

   # A local table with six small data files (one per append).
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   for i in range(6):
       table.append(pa.table({"order_id": [i]}))

   result = compact_small_files_streaming(
       table,
       target_size_mb=512,
       min_file_count=5,
       sort_by_iceberg_order=True,
       dry_run=False,
   )
   print(
       f"compacted {result.files_rewritten} files; "
       f"{result.bytes_before:,} -> {result.bytes_after:,} bytes"
   )

If the commit fails with ``CommitFailedException`` or
``ValidationException``, nothing was committed and the staged files are
removed. Any other commit error (for example
``CommitStateUnknownException``) means the commit may have landed, so
the staged files are kept and the error is re-raised: refresh the table
and check. If the commit did not land, ``remove_orphan_files`` reclaims
the staged files once they are older than ``older_than_hours``.

Snapshot expiry example
-----------------------

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.maintenance import expire_snapshots

   # A local table with three snapshots.
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   for i in range(3):
       table.append(pa.table({"order_id": [i]}))

   result = expire_snapshots(
       table,
       max_age_hours=120,
       min_snapshots=2,
       dry_run=False,
   )
   print(f"expired {result.expired_count}/{result.total_snapshots} snapshots")
   print(f"kept because a branch or tag points at them: {result.protected_ids}")

Omit ``max_age_hours`` / ``min_snapshots`` to use the table's
``history.expire.*`` properties. ``max_age_hours`` accepts fractional
hours.

Orphan cleanup example
----------------------

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.maintenance import remove_orphan_files

   # A local table to sweep.
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   table.append(pa.table({"order_id": [1, 2, 3]}))

   # Report first (the default is dry_run=True) ...
   preview = remove_orphan_files(table, older_than_hours=72)
   print(f"would delete {preview.orphan_count} orphan files: {preview.orphan_paths[:10]}")

   # ... then delete explicitly.
   result = remove_orphan_files(
       table,
       older_than_hours=72,
       sweep=("data", "metadata"),
       dry_run=False,
   )
   print(f"found {result.orphan_count} orphan files; failed: {result.failed_paths}")

``older_than_hours`` below 24 logs a warning: files written by an
in-flight writer that has not committed yet look like orphans.

Health report example
---------------------

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.maintenance import table_health_report

   # A local table with a few small files.
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   for i in range(3):
       table.append(pa.table({"order_id": [i]}))

   report = table_health_report(table, small_file_threshold_mb=100)
   print(f"snapshots: {report.snapshot_count}")
   print(f"small files: {report.small_file_count}/{report.total_files}")
   for rec in report.recommendations:
       print(f"  - {rec}")

Known limitations
-----------------

* A compaction run writes ``2N + 1`` snapshots for ``N`` touched
  partitions (a delete and an empty append per partition, then one
  append). Incremental readers see the rewritten rows as new data.
* Qualifying partitions are rewritten whole, including files that are
  already large.
* Compacted files use PyArrow's default codec (Snappy), not the
  table's configured ``write.parquet.compression-codec``.
* A concurrent append to a partition being compacted fails the run
  with ``ValidationException`` and the staged files are removed.
  PyIceberg retries the commit after appends to other partitions;
  those appends are preserved and counted in ``files_after``.
* Compaction supports Identity-transformed partitions (and
  unpartitioned tables) only; other partitions are skipped.
* Snapshot expiry is metadata-only. It also commits without asserting
  the table's refs (a PyIceberg limitation), so do not create tags
  while an expiry runs.
* Delete files and v3 deletion vectors are out of scope. Compaction
  and the health report read ``inspect.files()``, which lists delete
  files next to data files, and neither checks for them: do not run
  compaction on a table that has position or equality delete files or
  deletion vectors. Orphan cleanup keeps every delete file the
  metadata references.

Filesystem support matrix
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 35 22 22 21

   * - Helper
     - ``file://``
     - ``s3://``
     - ``gs://`` / ``abfs://``
   * - ``table_health_report``
     - yes
     - not tested
     - not tested
   * - ``expire_snapshots``
     - yes
     - not tested (metadata only)
     - not tested (metadata only)
   * - ``compact_small_files_streaming``
     - yes
     - no
     - no
   * - ``remove_orphan_files``
     - yes
     - no
     - no

The metadata-only helpers (``table_health_report`` and
``expire_snapshots``) only go through PyIceberg's metadata APIs, never
listing or deleting objects themselves, so they are expected to work
wherever PyIceberg can read the table; the test suite runs them on
local tables only. The two helpers that *do* touch storage raise
``NotImplementedError`` with the scheme named in the message when the
table location is not local. Plain paths, including Windows
drive-letter paths such as a ``.pyiceberg.yaml`` warehouse of
``C:\warehouse``, count as local.

See :doc:`api/maintenance` for the full reference.
