Cross-Table Data Migration
==========================

The ``iceberg_ops.data_migration`` subpackage moves rows between two
existing Iceberg tables. It handles schema reconciliation (renames,
type coercions, missing-column policies), checkpoint-based resume, and
three post-migration verify methods. Source and destination can live
in different catalogs: pass any two PyIceberg tables.

The destination table must already exist (create it with a schema
migration first); the migrator never creates tables.

.. note::

   Data migration is a companion to the schema-migration workflow and
   is early-stage. Read the write-mode, resume and verify limits on
   this page before you point it at data you care about. Its API may
   change before 1.0.

Three composable pieces
-----------------------

``SchemaReconciler`` builds a ``ReconciliationPlan`` from a (source,
dest) schema pair and projects each ``pyarrow.RecordBatch`` against it.
Handles column renames via ``column_mapping`` (every key must be a
source column), per-column ``type_coercions`` (for a column the
destination already has, the override must equal its type or be a
legal Iceberg promotion of it), and two missing-column policies:

* ``on_missing_in_source`` -- ``"error"`` (default), ``"null"``,
  or ``"skip"``.
* ``on_missing_in_dest`` -- ``"error"`` (default) or ``"auto_add"``.

Static lossy-cast detection (narrowing integer / float casts, float to
integer, ``timestamp`` -> ``date``) surfaces warnings without
blocking; data-dependent failures still raise inside
``project_batch``.

``CheckpointStore`` persists per-batch progress to a JSON file with
atomic writes (``tmp`` file, ``fsync``, ``os.replace``). Disk I/O is
lazy. A corrupt, unreadable or unsupported-version file raises
``CheckpointError`` (a ``RuntimeError``) with a remediation message.
Single-writer model; one checkpoint file per migration job.

``DataMigrator`` wires the two together with a streaming loop: it
reads the source at one snapshot, accumulates up to ``batch_rows``
source rows, projects them, and writes them in one destination
commit. Pre-flight runs every check before it writes anything to the
destination (auto-added columns included).

Write modes
-----------

* ``"append"`` -- one ``Table.append`` per batch.
* ``"upsert"`` -- requires ``upsert_keys``; uses PyIceberg's native
  ``Table.upsert`` (see `Upserts`_).
* ``"overwrite_partition"`` -- ``Table.dynamic_partition_overwrite``.
  In this release it needs a destination partitioned by identity
  transforms only, and the whole migration must fit in **one commit**
  (at most ``batch_rows`` source rows after the row filter); a second
  commit would delete the rows the first one wrote to the same
  partitions. Pre-flight (dry runs included) rejects anything else
  with ``ValueError`` before any write: raise ``batch_rows`` or use
  ``append`` / ``upsert``.

Memory
~~~~~~

``batch_rows`` bounds the rows in each commit, not peak memory.
PyIceberg's batch reader decodes whole source data files at a time
(`apache/iceberg-python#2407
<https://github.com/apache/iceberg-python/issues/2407>`_), so peak
memory grows with the number and size of the source data files. An
upsert also loads the destination rows that match each batch's keys.

Checkpoints and resume
----------------------

Attach a ``CheckpointStore`` to make a run resumable. A crashed run
resumes from the last committed batch on the next invocation, reading
the source at the snapshot stored in the checkpoint (not the source's
current snapshot), so rows added to the source after the crash are
neither skipped nor duplicated.

The checkpoint records the job's settings: source and destination
table UUIDs, row filter, ``batch_rows``, write mode, upsert keys and
reconciler settings. A resume raises ``CheckpointError`` before writing
when:

* the checkpoint already records a **completed** migration (a
  completed checkpoint is terminal: call ``CheckpointStore.reset()``
  or delete the file to run again);
* any recorded setting differs (for example a changed ``batch_rows``
  or row filter, or a recreated destination table), or the checkpoint
  has no recorded settings;
* the recorded source snapshot no longer exists (expired), or an
  explicit ``source_snapshot`` differs from it;
* the destination's current history no longer holds a checkpointed
  batch's snapshots (it was rolled back): resuming would skip those
  rows.

``CheckpointStore.reset()`` forgets which batches are already in the
destination. When batches were committed, every refusal names the
job's ``dest_snapshot_before``. Roll the destination back to it with
``rollback_to_snapshot`` (or delete the job's rows) before you reset
and re-run, or an append writes those rows again.

Every destination snapshot a batch creates carries
``iceberg-ops.migration-job`` and ``iceberg-ops.migration-batch``
summary properties; the empty schema-stamp snapshot an upsert may
commit first carries only ``iceberg-ops.migration-job``. On resume, a
batch that was committed just before a crash but not yet checkpointed
is found through them and recorded instead of being written twice.

Upserts
-------

* Two rows with the same key inside one batch raise ``ValueError``
  (earlier batches stay committed). Across batches the later row
  wins, so the outcome for duplicate keys depends on ``batch_rows``.
* ``on_missing_in_source="skip"`` is rejected for upserts (an upsert
  writes every destination column). With ``"null"``, matched keys get
  those columns overwritten with null; the plan carries a warning.
* ``upsert_keys`` must be non-blank, unique, present in the
  projected destination schema, and mapped from a source column (a
  key the plan would fill with null is rejected before any write);
  they are rejected with any other write mode.
* Composite-key batches of more than 1,000 rows are split into
  sub-batches inside one transaction, so each batch is still one
  catalog commit (works around
  `apache/iceberg-python#3508
  <https://github.com/apache/iceberg-python/issues/3508>`_).
* When the destination's current snapshot predates its schema (for
  example right after ``auto_add``), one empty snapshot is committed
  first so the upsert can run (workaround for
  `apache/iceberg-python#2467
  <https://github.com/apache/iceberg-python/issues/2467>`_).
* One batch is one catalog commit but can create up to three
  destination snapshots (three per 1,000-key sub-batch when a
  composite-key batch is split), plus the optional empty snapshot
  above. ``MigrationResult.new_snapshots`` lists all of them, and
  ``rows_inserted`` / ``rows_updated`` count the upsert's effect. To
  undo the run, roll back to ``MigrationResult.dest_snapshot_before``
  (see `Undoing a migration`_), not to the snapshot before the last
  batch.

Partitioned destinations
------------------------

Writing to a destination partitioned by a bucket, truncate, year,
month, day or hour transform needs ``pyiceberg-core``:

.. code-block:: bash

   pip install "iceberg-ops[transforms]"

Without it, pre-flight raises ``ImportError`` before any write (a dry
run reports it as a plan warning). Identity-partitioned and
unpartitioned destinations do not need the extra.

Verify methods
--------------

After the migration completes, ``DataMigrator.verify(method=...)``
runs one of three consistency checks. The source is read at the
snapshot the run migrated (an explicit ``source_snapshot``, else the
snapshot the last ``run()`` read), so rows appended to the source
afterwards do not count as a mismatch. The destination is read in
full, so the result is meaningful for an append into an empty
destination; account for pre-existing rows or upserts yourself.

* ``"row_count"`` (default) -- compares row counts from manifest
  metadata; reads data files only where the row filter cannot be
  decided per file or delete files apply.
* ``"partition_hash"`` -- per-partition SHA256 of sorted rows.
  Supports unpartitioned tables and identity-transform partition
  specs; non-identity partitions raise ``NotImplementedError`` (the
  CLI prints ``Verify error:`` and exits 1). Reads both tables in
  full.
* ``"sample"`` -- compares the first ``sample_size`` rows of each side
  after sorting. Reliable only when both tables fit in the sample or
  the read order is the same on both sides; otherwise it can report a
  false mismatch.

Each method returns a ``VerificationResult`` with ``method``,
``matches``, ``source_count`` / ``dest_count`` (``row_count`` and
``sample``), ``sample_size``, ``mismatched_partitions``
(``partition_hash``) and ``details`` (list of strings).

Undoing a migration
-------------------

``MigrationResult.dest_snapshot_before`` is the destination's current
snapshot before the run wrote anything (for a resumed run, the one
recorded when the job first started); it is ``None`` when the
destination had no snapshot. Pass it to
``iceberg_ops.core.utils.rollback_to_snapshot(table, snapshot_id)`` to
put the destination back. Columns that ``auto_add`` added stay in the
schema. After a rollback, call ``CheckpointStore.reset()`` before you
run the job again: a resume after a rollback raises
``CheckpointError``.

CLI usage
---------

.. code-block:: bash

   iceberg-ops migrate-data \
     --src-catalog sqlite --src-db-path src.db --src-warehouse src_wh \
     --src-namespace sales --src-table orders \
     --dst-catalog sqlite --dst-db-path dst.db --dst-warehouse dst_wh \
     --dst-namespace warehouse --dst-table orders \
     --batch-rows 50000 \
     --column-mapping order_total:total_amount \
     --on-missing-in-source null \
     --on-missing-in-dest auto_add \
     --checkpoint orders.checkpoint.json \
     --verify row_count

To write into another catalog, name it with ``--dst-catalog-name``. The
name is resolved through PyIceberg's ``load_catalog``, so it is read
from ``.pyiceberg.yaml`` or ``PYICEBERG_CATALOG__<NAME>__*``
environment variables:

.. code-block:: yaml

   # .pyiceberg.yaml
   catalog:
     dest:
       type: rest
       uri: https://nessie.example.com/iceberg

.. code-block:: bash

   iceberg-ops migrate-data \
     --src-catalog sqlite --src-db-path src.db --src-warehouse src_wh \
     --src-namespace sales --src-table orders \
     --dst-catalog-name dest --dst-namespace warehouse --dst-table orders

Key flags:

* ``--src-catalog KIND`` or ``--src-catalog-name NAME`` -- one of the
  two is required. ``--src-db-path`` / ``--src-warehouse`` configure a
  sqlite source.
* ``--dst-catalog`` / ``--dst-catalog-name`` -- the destination
  catalog. When neither is given, the destination is the source
  catalog (useful for cross-namespace migration in one catalog), and
  the source's ``--src-db-path`` / ``--src-warehouse`` carry over only
  to that same catalog.
* ``--dst-table`` -- defaults to ``--src-table``. The table must
  already exist.
* ``--row-filter`` -- PyIceberg row filter expression on the source
  scan.
* ``--source-snapshot`` -- pin the read to a specific source snapshot
  ID.
* ``--batch-rows`` -- source rows per commit (default 50000, at least
  1). Bounds rows per commit, not peak memory.
* ``--write-mode`` -- ``append`` (default) / ``upsert`` /
  ``overwrite_partition`` (identity-partitioned destination, one
  commit of at most ``--batch-rows`` rows; see `Write modes`_).
* ``--upsert-keys`` -- comma-separated; required when
  ``--write-mode=upsert``.
* ``--checkpoint`` -- path to the JSON checkpoint file. A second run
  with the checkpoint of a completed migration exits 1.
* ``--column-mapping SRC:DST`` -- repeatable.
* ``--dry-run`` -- pre-flight only: runs the checks and counts rows +
  bytes, does not write.
* ``--verify {row_count|partition_hash|sample}`` -- run a
  post-migration check (skipped on a dry run). A mismatch exits 2.

Errors from the migrator (a refused resume, a missing extra, a missing
destination table, ...) print one ``Error:`` line and exit 1; set
``ICEBERG_OPS_TRACEBACK=1`` to see the traceback.

Python API
----------

.. code-block:: python

   import os
   import tempfile

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.data_migration import CheckpointStore, DataMigrator, SchemaReconciler

   work = tempfile.mkdtemp()

   # Source: a SQLite catalog with a populated sales.orders table.
   src_catalog = CatalogFactory.build(
       "sqlite",
       db_path=os.path.join(work, "src.db"),
       warehouse_dir=os.path.join(work, "src_wh"),
   )
   src_catalog.create_namespace("sales")
   src = src_catalog.create_table(
       "sales.orders",
       pa.schema([("order_id", pa.int64()), ("order_total", pa.float64()), ("region", pa.string())]),
   )
   src.append(
       pa.table(
           {"order_id": [1, 2, 3], "order_total": [9.5, 20.0, 7.25], "region": ["eu", "us", "eu"]}
       )
   )

   # Destination: a second catalog. The table must already exist.
   dst_catalog = CatalogFactory.build(
       "sqlite",
       db_path=os.path.join(work, "dst.db"),
       warehouse_dir=os.path.join(work, "dst_wh"),
   )
   dst_catalog.create_namespace("warehouse")
   dst = dst_catalog.create_table(
       "warehouse.orders",
       pa.schema([("order_id", pa.int64()), ("total_amount", pa.float64()), ("channel", pa.string())]),
   )

   reconciler = SchemaReconciler(
       column_mapping={"order_total": "total_amount"},
       on_missing_in_source="null",  # 'channel' has no source column
       on_missing_in_dest="auto_add",  # 'region' is added to the destination
   )

   migrator = DataMigrator(
       source=src,
       dest=dst,
       schema_reconciler=reconciler,
       batch_rows=50_000,
       write_mode="append",
       checkpoint=CheckpointStore(os.path.join(work, "orders.checkpoint.json")),
   )

   result = migrator.run()
   print(f"moved {result.rows_moved:,} rows in {len(result.new_snapshots)} snapshots")
   print(f"columns added to the destination: {result.schema_changes}")

   verification = migrator.verify(method="row_count")
   assert verification.matches, verification.details

Any PyIceberg catalog works on either side. For example, to load the
destination from a catalog defined in ``.pyiceberg.yaml``:

.. code-block:: python

   # illustrative: needs a reachable catalog named "dest"
   from pyiceberg.catalog import load_catalog

   dst_catalog = load_catalog("dest")  # e.g. Nessie at https://nessie.example.com/iceberg
   dst = dst_catalog.load_table("warehouse.orders")

Testing seam
------------

The ``iceberg_ops.testing`` module ships ``FakeSession`` / ``FakeResponse``
for integrators driving the Polaris client without HTTP. They are not
needed for ``DataMigrator`` itself -- the data-migration tests run
against real PyIceberg catalogs.

See :doc:`api/data_migration` for the full reference.
