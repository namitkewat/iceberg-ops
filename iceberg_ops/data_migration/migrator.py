"""
iceberg_ops.data_migration.migrator
===================================
Streaming, schema-reconciling, resumable Iceberg-to-Iceberg data migration.

A :class:`DataMigrator` reads from a source Iceberg table, projects
each batch through a
:class:`~iceberg_ops.data_migration.SchemaReconciler`, and writes to
a destination Iceberg table - appending, upserting, or partition-
overwriting per ``write_mode``. A
:class:`~iceberg_ops.data_migration.CheckpointStore` makes the loop
resumable: a crashed migration restarts from the last successfully
committed batch.

Algorithm
---------
1. **Pre-flight**. Every check runs before the destination is
   touched:

   - Refresh both tables and resolve the source snapshot: the
     checkpoint's snapshot when resuming, else ``source_snapshot``,
     else the source's current snapshot.
   - When resuming, validate the checkpoint (not completed, same job
     settings, source snapshot still present).
   - Build a :class:`~iceberg_ops.data_migration.ReconciliationPlan`
     from the source schema *at that snapshot*.
   - Validate write-mode prerequisites (``upsert_keys`` present in
     the projection, an upsert-compatible missing-column policy, the
     ``overwrite_partition`` limits below) and that
     ``pyiceberg-core`` is installed when the destination's
     partition spec needs it.

   Only then does pre-flight write, in this order:

   - add any ``auto_add_columns`` to the destination schema (one
     schema commit, no snapshot);
   - for ``write_mode="upsert"`` only, commit one *empty* append when
     the destination's current snapshot predates its schema - a
     workaround for apache/iceberg-python#2467, without which the
     first upsert after a schema-only change fails;
   - start the checkpoint file (fresh runs).

   A dry run performs the checks but none of the writes.

2. **Streaming loop**:

   - Open
     ``source.scan(row_filter, snapshot_id).to_arrow_batch_reader()``.
   - Accumulate raw source batches up to ``batch_rows`` rows.
   - Project each accumulated chunk through the reconciler.
   - Write to dest via :meth:`Table.append` / :meth:`Table.upsert` /
     :meth:`Table.dynamic_partition_overwrite` - one catalog commit
     per chunk.
   - Mark the checkpoint after each write.

3. **Post-flight**: mark the checkpoint completed.

Memory
------
Each destination commit is bounded by ``batch_rows`` rows, but peak
memory is not: PyIceberg's batch reader decodes whole source data
files at a time (apache/iceberg-python#2407), so peak memory grows
with the number and size of the source data files. An upsert also
loads the destination rows that match each chunk's keys.

Write modes
-----------
``append``
    One :meth:`Table.append` per chunk.
``upsert``
    One upsert keyed by ``upsert_keys`` per chunk. Two rows with the
    same key inside one chunk raise :class:`ValueError` (earlier
    chunks stay committed); across chunks the later row wins, so the
    outcome for duplicate keys depends on ``batch_rows``.
``overwrite_partition``
    :meth:`Table.dynamic_partition_overwrite`. In this release it
    needs a destination partitioned by identity transforms only, and
    the whole migration must fit in **one commit** (at most
    ``batch_rows`` source rows after the row filter): a second commit
    would delete the rows the first one wrote to the same
    partitions. Pre-flight rejects anything else - raise
    ``batch_rows`` or use ``append`` / ``upsert``.

Resume
------
The checkpoint records the job's identity (source and destination
table UUIDs, row filter, ``batch_rows``, write mode, upsert keys,
reconciler settings) and the source snapshot. A resumed run reads at
that snapshot and raises
:class:`~iceberg_ops.data_migration.CheckpointError` before writing
if any setting changed, if the snapshot no longer exists, or if the
checkpoint already records a completed migration (call
:meth:`~iceberg_ops.data_migration.CheckpointStore.reset` to run
again).

Every destination snapshot a batch creates carries the job ID and
batch index in its summary (``iceberg-ops.migration-job`` /
``iceberg-ops.migration-batch``); the empty schema-stamp snapshot an
upsert may commit first carries only ``iceberg-ops.migration-job``.
On resume, a batch whose write
landed just before a crash but was not yet checkpointed is found by
these properties and recorded instead of being written twice. A
resume also raises ``CheckpointError`` when a checkpointed batch's
snapshot is no longer in the destination's current history (the
destination was rolled back), because skipping that batch would lose
its rows.

:meth:`~iceberg_ops.data_migration.CheckpointStore.reset` forgets
which batches are already in the destination. When batches were
committed, every refusal says so and names the rollback target
(the job's ``dest_snapshot_before``): roll the destination back
before resetting, or an append re-run writes those rows again.

Verification
------------
After :meth:`DataMigrator.run`, call :meth:`DataMigrator.verify` to
confirm the destination matches the source. Three methods are
supported - ``row_count`` (manifest record counts), ``partition_hash``
(thorough but materialises both sides), and ``sample`` (first-N rows;
reliable only for small tables or an identical read order).

Out of scope (today)
--------------------
- ``PartitionStrategy.REPARTITION_BY_DEST_SPEC`` and ``FLATTEN`` -
  the only strategy supported today is ``PRESERVE``: the
  destination's own partition spec dictates the layout (PyIceberg's
  writer applies it).
- ``target_file_size_mb`` - controlled by the destination table's
  ``write.target-file-size-bytes`` property; the migrator does not
  override it.
- Non-identity partition transforms in
  :meth:`~DataMigrator.verify` with ``method="partition_hash"``
  (it raises :class:`NotImplementedError`).
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.expressions.parser import parse as parse_filter
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot, ancestors_of
from pyiceberg.transforms import IdentityTransform

from iceberg_ops import __version__
from iceberg_ops.core.utils import (
    _ensure_schema_snapshot,
    _require_transforms_extra,
    _upsert_chunk,
)
from iceberg_ops.data_migration.checkpoint import CheckpointError, CheckpointStore
from iceberg_ops.data_migration.reconciler import (
    ReconciliationPlan,
    SchemaReconciler,
)

logger = logging.getLogger(__name__)

# Snapshot-summary properties stamped on every destination snapshot
# the migrator creates. Resume uses them to find a batch that landed
# but was not checkpointed; result bookkeeping uses them to skip
# snapshots committed by concurrent writers.
_JOB_PROPERTY = "iceberg-ops.migration-job"
_BATCH_PROPERTY = "iceberg-ops.migration-batch"

# Checkpoint job keys that must match for a resume to be allowed.
# The job dict also carries ``job_id``, ``iceberg_ops_version`` and
# ``dest_snapshot_before``, which are informational and not compared.
_FINGERPRINT_KEYS = (
    "source_table_uuid",
    "dest_table_uuid",
    "row_filter",
    "batch_rows",
    "write_mode",
    "upsert_keys",
    "reconciler",
)


# ===========================================================================
# Public types
# ===========================================================================


WriteMode = Literal["append", "upsert", "overwrite_partition"]


class PartitionStrategy(StrEnum):
    """How the migrator should treat the destination partition spec.

    Only ``PRESERVE`` is implemented today: the destination's own
    spec dictates the layout. ``REPARTITION_BY_DEST_SPEC`` and
    ``FLATTEN`` are reserved for a future release.
    """

    PRESERVE = "preserve"


# Progress callback signature: (rows_done, rows_total_or_none).
# ``rows_total`` is None during a real run because the migrator does
# not pre-count; it is set during a dry-run after the reader is
# fully drained.
ProgressFn = Callable[[int, "int | None"], None]


@dataclass
class MigrationResult:
    """Outcome of :meth:`DataMigrator.run`.

    Attributes
    ----------
    rows_moved : int
        Number of rows actually written to the destination - i.e.
        excludes batches skipped because the checkpoint already had
        them. For dry-runs this is always 0; see ``estimated_rows``.
        For upserts it counts incoming rows, not distinct keys; see
        ``rows_inserted`` / ``rows_updated``.
    bytes_moved : int
        Approximate bytes written, computed as
        ``sum(chunk.nbytes)`` across every chunk passed to the
        destination's writer. The on-disk Parquet size will differ
        (compression, encoding) - this is an in-memory measure
        useful for progress reporting.
    new_snapshots : list of int
        Every destination snapshot this run created, in commit
        order: the empty schema-stamp snapshot (upsert after a
        schema-only change, when needed), then every snapshot each
        batch's commit created. One batch is one catalog commit but
        can create several snapshots - up to three for an upsert
        (three per sub-batch when a composite-key batch of more than
        1,000 rows is split), two for ``overwrite_partition``.
        Snapshots committed by other writers in the meantime are not
        included.
    schema_changes : list of str
        Names of columns the migrator added to the destination via
        ``auto_add`` reconciliation.
    duration_seconds : float
        Wall-clock elapsed time across pre-flight + streaming +
        post-flight.
    checkpoint_path : pathlib.Path or None
        Filesystem path of the active
        :class:`~iceberg_ops.data_migration.CheckpointStore`, or
        ``None`` if no checkpoint was used.
    dry_run : bool
        ``True`` when this result describes a dry-run.
    estimated_rows : int or None
        Total rows scanned (post-filter) during a dry-run. ``None``
        for real runs.
    estimated_bytes : int or None
        Total Arrow bytes scanned during a dry-run. ``None`` for
        real runs.
    plan : ReconciliationPlan
        The plan the migrator used. Useful for post-mortem
        introspection (auto-add list, warnings).
    dest_snapshot_before : int or None
        The destination's current snapshot before the migration
        wrote anything - the target for
        :func:`~iceberg_ops.core.utils.rollback_to_snapshot` to undo
        it. For a resumed run it is the snapshot recorded when the
        job first started. ``None`` when the destination had no
        snapshot then, or for a dry-run.
    rows_inserted : int or None
        Rows the upsert inserted, summed over this run's batches.
        ``None`` unless ``write_mode="upsert"`` (and not a dry-run).
    rows_updated : int or None
        Rows the upsert rewrote because a non-key value changed,
        summed over this run's batches. ``None`` unless
        ``write_mode="upsert"`` (and not a dry-run).
    """

    rows_moved: int = 0
    bytes_moved: int = 0
    new_snapshots: list[int] = field(default_factory=list)
    schema_changes: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    checkpoint_path: Path | None = None
    dry_run: bool = False
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    plan: ReconciliationPlan | None = None
    dest_snapshot_before: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None


# Method options for :meth:`DataMigrator.verify`.
VerifyMethod = Literal["row_count", "partition_hash", "sample"]


@dataclass
class VerificationResult:
    """Outcome of :meth:`DataMigrator.verify`.

    A small structured report indicating whether the destination
    matches the source under the chosen verification method.

    Attributes
    ----------
    method : ``"row_count"`` | ``"partition_hash"`` | ``"sample"``
        Method used to compute this result.
    matches : bool
        ``True`` when source and destination agreed under the chosen
        method. For ``row_count`` this means equal counts; for
        ``partition_hash`` it means every partition's content hash
        matched; for ``sample`` it means the sampled rows matched.
    source_count : int or None
        Total source rows seen (after row filter + snapshot pin +
        reconciler projection). Populated for ``row_count`` and
        ``sample``.
    dest_count : int or None
        Total destination rows seen. Populated for ``row_count`` and
        ``sample``.
    sample_size : int or None
        For ``method="sample"``, the number of rows actually
        compared (capped at the smaller of source rows / dest rows /
        requested ``sample_size``).
    mismatched_partitions : list of str
        For ``method="partition_hash"``, the partition keys whose
        hashes differed (or that exist on only one side).
    details : list of str
        Human-readable explanations for any mismatch. Empty when
        ``matches`` is ``True``.

    Caveat
    ------
    All three methods compare the *current* state of ``source``
    (filtered + snapshot-pinned per the migrator's config) against
    the *current* state of ``dest`` (no filter). When
    ``write_mode != "append"`` or the destination had pre-existing
    rows, the source-vs-dest counts will not naturally agree -
    interpret the result accordingly.
    """

    method: VerifyMethod
    matches: bool
    source_count: int | None = None
    dest_count: int | None = None
    sample_size: int | None = None
    mismatched_partitions: list[str] = field(default_factory=list)
    details: list[str] = field(default_factory=list)


# ===========================================================================
# DataMigrator
# ===========================================================================


class DataMigrator:
    """Streaming, schema-reconciling, resumable cross-table migrator.

    See module docstring for the algorithm. The migrator is
    constructed with all configuration up-front; :meth:`run`
    performs the migration.

    Parameters
    ----------
    source : pyiceberg.table.Table
        Source Iceberg table.
    dest : pyiceberg.table.Table
        Destination Iceberg table. May be in a different catalog
        than ``source``.
    schema_reconciler : SchemaReconciler, optional
        Declarative source -> dest schema mapping. Defaults to a
        strict reconciler (errors on any mismatch); passing one
        explicitly lets the user enable rename / null-fill /
        auto-add behaviour.
    partition_strategy : PartitionStrategy, optional
        Currently only ``PRESERVE`` is supported.
    row_filter : str or BooleanExpression, optional
        PyIceberg row filter applied to the source scan. Defaults to
        :class:`AlwaysTrue` (no filter).
    source_snapshot : int, optional
        Pin the source read to a specific snapshot ID. Defaults to
        the source's current snapshot when :meth:`run` starts. The
        snapshot is recorded in the checkpoint; a resumed run reads
        at the recorded snapshot and refuses an explicit
        ``source_snapshot`` that differs from it.
    batch_rows : int, optional
        Target rows per destination commit. Higher -> fewer commits;
        lower -> more, smaller commits. Each batch is one catalog
        commit, which can create several snapshots (see
        :attr:`MigrationResult.new_snapshots`). It does not bound
        peak memory - see the module docstring. Default ``50_000``.
    write_mode : ``"append"`` | ``"upsert"`` | ``"overwrite_partition"``
        How to write each chunk to the destination. ``upsert``
        requires ``upsert_keys``. ``overwrite_partition`` needs a
        destination partitioned by identity transforms only and a
        migration that fits in one commit (at most ``batch_rows``
        source rows after the row filter); pre-flight rejects
        anything else.
    upsert_keys : list of str, optional
        Required when ``write_mode="upsert"`` and rejected otherwise.
        Forwarded to :meth:`Table.upsert` as ``join_cols``; every key
        must be a column of the projected destination schema.
    checkpoint : CheckpointStore, optional
        Per-batch checkpoint for resumability. When ``None`` the
        migration is non-resumable (a crash means re-running from
        scratch). An existing checkpoint file is resumed only when
        it matches this migrator's settings and is not completed;
        otherwise :meth:`run` raises
        :class:`~iceberg_ops.data_migration.CheckpointError`.
    progress : callable, optional
        ``progress(rows_done, rows_total_or_none)`` called once
        after each successful commit. ``rows_total`` is ``None``
        during live migration (not pre-counted) and the post-scan
        total during a dry-run.
    dry_run : bool, optional
        When ``True``, scan the source through the reconciler but
        skip every destination write. Returns a
        :class:`MigrationResult` populated with ``estimated_rows`` /
        ``estimated_bytes`` and an empty ``new_snapshots`` list. A
        dry run ignores the checkpoint.

    Raises
    ------
    ValueError
        If ``write_mode="upsert"`` without ``upsert_keys``, if
        ``upsert_keys`` is given for another write mode or holds a
        blank or repeated name, if ``batch_rows <= 0``, if the
        partition strategy is not ``PRESERVE``, or if ``source`` and
        ``dest`` are the same table (same table UUID, even when loaded
        through different catalogs).
    """

    def __init__(
        self,
        source: Table,
        dest: Table,
        *,
        schema_reconciler: SchemaReconciler | None = None,
        partition_strategy: PartitionStrategy = PartitionStrategy.PRESERVE,
        row_filter: str | BooleanExpression | None = None,
        source_snapshot: int | None = None,
        batch_rows: int = 50_000,
        write_mode: WriteMode = "append",
        upsert_keys: list[str] | None = None,
        checkpoint: CheckpointStore | None = None,
        progress: ProgressFn | None = None,
        dry_run: bool = False,
    ) -> None:
        # Validate eagerly so a typo blows up at construction, not
        # mid-stream after the reader has opened.
        if batch_rows <= 0:
            raise ValueError(f"batch_rows must be positive, got {batch_rows!r}")
        if write_mode not in ("append", "upsert", "overwrite_partition"):
            raise ValueError(
                f"write_mode must be 'append', 'upsert', or "
                f"'overwrite_partition'; got {write_mode!r}"
            )
        if write_mode == "upsert" and not upsert_keys:
            raise ValueError("write_mode='upsert' requires non-empty upsert_keys.")
        if upsert_keys and write_mode != "upsert":
            raise ValueError(
                f"upsert_keys is only used with write_mode='upsert'; got "
                f"write_mode={write_mode!r}. Drop upsert_keys or switch to upsert."
            )
        if upsert_keys:
            if any(not k.strip() for k in upsert_keys):
                raise ValueError(f"upsert_keys must not contain blank names; got {upsert_keys!r}.")
            if len(set(upsert_keys)) != len(upsert_keys):
                raise ValueError(f"upsert_keys must not repeat a column; got {upsert_keys!r}.")
        if partition_strategy != PartitionStrategy.PRESERVE:
            raise ValueError(
                f"partition_strategy={partition_strategy!r} is not yet "
                "supported; only PartitionStrategy.PRESERVE is implemented."
            )
        if str(source.metadata.table_uuid) == str(dest.metadata.table_uuid):
            raise ValueError(
                "source and dest are the same table "
                f"(table-uuid {source.metadata.table_uuid}); migrate into a different "
                "destination table."
            )

        self.source = source
        self.dest = dest
        self.schema_reconciler = schema_reconciler or SchemaReconciler()
        self.partition_strategy = partition_strategy
        self.row_filter: str | BooleanExpression = (
            row_filter if row_filter is not None else AlwaysTrue()
        )
        self.source_snapshot = source_snapshot
        self.batch_rows = batch_rows
        self.write_mode = write_mode
        self.upsert_keys = list(upsert_keys) if upsert_keys else None
        self.checkpoint = checkpoint
        self.progress = progress
        self.dry_run = dry_run
        # Set by run(): the source snapshot the run read, and the job
        # ID stamped on every destination snapshot it creates.
        self._run_source_snapshot: int | None = None
        self._job_id: str = ""

    # -----------------------------------------------------------------------
    # Entry point
    # -----------------------------------------------------------------------

    def run(self) -> MigrationResult:
        """Execute the migration.

        Returns
        -------
        MigrationResult

        Raises
        ------
        CheckpointError
            If an existing checkpoint records a completed migration,
            was started with different settings (or without a job
            fingerprint), reads a source snapshot that no longer
            exists or differs from ``source_snapshot``, or records
            batches whose snapshots are no longer in the destination's
            current history (the destination was rolled back). Raised
            before any destination write.
        ValueError
            If the reconciler cannot map the schemas, ``upsert_keys``
            are not in the projection or have no source column (they
            would be filled with null), an upsert would drop
            destination columns (``on_missing_in_source="skip"``), or
            ``overwrite_partition`` limits are not met - all raised
            before any destination write. During streaming, an upsert
            batch that holds duplicate keys also raises it; earlier
            batches stay committed.
        ImportError
            If the destination's partition spec needs
            ``pyiceberg-core`` and it is not installed (real runs
            only; a dry run reports it in ``plan.warnings``).
        """
        started = time.monotonic()

        result = MigrationResult(
            checkpoint_path=self.checkpoint.path if self.checkpoint else None,
            dry_run=self.dry_run,
        )
        plan, resolved_snap = self._preflight(result)
        result.plan = plan

        if self.dry_run:
            self._run_dry_run(plan, resolved_snap, result)
        else:
            if self.write_mode == "upsert":
                result.rows_inserted = 0
                result.rows_updated = 0
            self._run_streaming(plan, resolved_snap, result)

        # Mark the checkpoint complete on success - the file remains
        # for audit; the user calls reset() to delete it.
        if self.checkpoint and not self.dry_run:
            self.checkpoint.mark_completed()

        result.duration_seconds = time.monotonic() - started
        return result

    # -----------------------------------------------------------------------
    # Pre-flight
    # -----------------------------------------------------------------------

    def _preflight(self, result: MigrationResult) -> tuple[ReconciliationPlan, int | None]:
        """Validate everything, then apply the pre-flight writes.

        Fills ``result.schema_changes``, ``result.dest_snapshot_before``
        and (for the schema stamp) ``result.new_snapshots``.

        Returns
        -------
        (plan, resolved_source_snapshot)
        """
        # 1. Refresh both ends so we're working with the latest
        # metadata. Without this, a freshly-evolved source / dest
        # may report a stale schema.
        self.source.refresh()
        self.dest.refresh()

        # 2. Resolve the source snapshot; validate a resumed checkpoint.
        fingerprint = self._job_fingerprint()
        resuming = self.checkpoint is not None and not self.dry_run and self.checkpoint.exists()
        if resuming:
            assert self.checkpoint is not None
            job = self._validate_resume(fingerprint)
            # Read exactly what the original run read, so the
            # positional batch skip lines up with committed batches.
            resolved_snap = self.checkpoint.source_snapshot_id
            # Only the checkpoint file is updated here, not the dest.
            self._recover_uncheckpointed_batches(job)
        else:
            resolved_snap = self.source_snapshot
            current = self.source.current_snapshot()
            if resolved_snap is None and current is not None:
                resolved_snap = current.snapshot_id
            job = {
                **fingerprint,
                "job_id": uuid.uuid4().hex,
                "iceberg_ops_version": __version__,
            }
        self._run_source_snapshot = resolved_snap
        self._job_id = str(job.get("job_id") or uuid.uuid4().hex)

        # 3. Plan from the source schema at the snapshot being read.
        plan = self.schema_reconciler.compute_plan(
            self._source_schema_at(resolved_snap), self.dest.schema()
        )

        # 4-6. Write-mode and environment checks.
        if self.write_mode == "upsert":
            self._check_upsert(plan)
        elif self.write_mode == "overwrite_partition":
            self._check_overwrite_partition(resolved_snap)
        try:
            _require_transforms_extra(self.dest)
        except ImportError as exc:
            if not self.dry_run:
                raise
            plan.warnings.append(str(exc))

        if self.dry_run:
            # Record what would have been added without mutating.
            result.schema_changes = [name for name, _ in plan.auto_add_columns]
            return plan, resolved_snap

        # 7. Destination writes - only after every check passed.
        if plan.auto_add_columns:
            with self.dest.update_schema() as upd:
                for name, ice_type in plan.auto_add_columns:
                    # A tuple path keeps a top-level "a.b" from being
                    # parsed as a nested field.
                    upd.add_column((name,), ice_type)
            self.dest.refresh()
            result.schema_changes = [name for name, _ in plan.auto_add_columns]
            logger.info(
                "DataMigrator pre-flight: added %d column(s) to dest: %s",
                len(result.schema_changes),
                ", ".join(result.schema_changes),
            )

        current_dest = self.dest.current_snapshot()
        if resuming:
            before = job.get("dest_snapshot_before")
            result.dest_snapshot_before = None if before is None else int(before)
        else:
            result.dest_snapshot_before = (
                current_dest.snapshot_id if current_dest is not None else None
            )
            job["dest_snapshot_before"] = result.dest_snapshot_before

        if self.write_mode == "upsert" and _ensure_schema_snapshot(
            self.dest, {_JOB_PROPERTY: self._job_id}
        ):
            self.dest.refresh()
            stamp = self.dest.current_snapshot()
            if stamp is not None:
                result.new_snapshots.append(stamp.snapshot_id)

        if self.checkpoint is not None and not resuming:
            self.checkpoint.start(source_snapshot_id=resolved_snap, job=job)

        return plan, resolved_snap

    def _job_fingerprint(self) -> dict[str, Any]:
        """Return the settings a resumed run must share with the original.

        Every value is JSON-serialisable so it round-trips through the
        checkpoint file unchanged.
        """
        rec = self.schema_reconciler
        rf = self.row_filter
        return {
            "source_table_uuid": str(self.source.metadata.table_uuid),
            "dest_table_uuid": str(self.dest.metadata.table_uuid),
            "row_filter": repr(parse_filter(rf)) if isinstance(rf, str) else repr(rf),
            "batch_rows": self.batch_rows,
            "write_mode": str(self.write_mode),
            "upsert_keys": list(self.upsert_keys or []),
            "reconciler": {
                "column_mapping": dict(sorted((rec.column_mapping or {}).items())),
                "type_coercions": {
                    k: str(v) for k, v in sorted((rec.type_coercions or {}).items())
                },
                "on_missing_in_source": rec.on_missing_in_source,
                "on_missing_in_dest": rec.on_missing_in_dest,
            },
        }

    def _validate_resume(self, fingerprint: dict[str, Any]) -> dict[str, Any]:
        """Check that the existing checkpoint may be resumed by this run.

        Returns
        -------
        dict
            The checkpoint's job record.

        Raises
        ------
        CheckpointError
            On a completed checkpoint, a missing or different job
            fingerprint, or an unusable source snapshot.
        """
        cp = self.checkpoint
        assert cp is not None
        job = cp.job
        if cp.completed:
            raise CheckpointError(
                f"Checkpoint {cp.path} records a completed migration. {self._reset_hint(job)}"
            )
        if job is None:
            raise CheckpointError(
                f"Checkpoint {cp.path} has no job fingerprint, so this run cannot "
                f"be checked against the one that started it. {self._reset_hint(job)}"
            )
        diffs = [k for k in _FINGERPRINT_KEYS if job.get(k) != fingerprint[k]]
        if diffs:
            raise CheckpointError(
                f"Checkpoint {cp.path} was started with different settings: "
                f"{', '.join(diffs)} changed. Resume with the original settings, "
                f"or start over. {self._reset_hint(job)}"
            )

        stored = cp.source_snapshot_id
        if self.source_snapshot is not None and self.source_snapshot != stored:
            raise CheckpointError(
                f"source_snapshot={self.source_snapshot} differs from snapshot "
                f"{stored} recorded in checkpoint {cp.path}. Resume without "
                f"source_snapshot, or start over. {self._reset_hint(job)}"
            )
        if stored is None:
            if self.source.current_snapshot() is not None:
                raise CheckpointError(
                    f"Checkpoint {cp.path}: the source had no snapshot when the "
                    f"checkpoint started, but it has data now. {self._reset_hint(job)}"
                )
        elif self.source.snapshot_by_id(stored) is None:
            raise CheckpointError(
                f"Checkpoint {cp.path} reads source snapshot {stored}, which no "
                f"longer exists (expired?). {self._reset_hint(job)}"
            )
        return job

    def _reset_hint(self, job: dict[str, Any] | None) -> str:
        """Return the remedy a resume refusal gives for starting over.

        :meth:`CheckpointStore.reset` forgets which batches are
        already in the destination, so a re-run after it writes them
        again (an append duplicates them). When batches of this job
        were committed, the hint says so and names the rollback
        target, ``job["dest_snapshot_before"]``, which lives only in
        the checkpoint file that ``reset()`` deletes.
        """
        cp = self.checkpoint
        assert cp is not None
        plain = "Call CheckpointStore.reset() (or delete the file) to start over."
        last = cp.last_committed()
        if job is None:
            if last is None:
                return plain
            return (
                f"{last + 1} batch(es) of the earlier run were committed to the "
                f"destination (snapshots {cp.dest_snapshot_ids}). Delete those rows "
                "before you reset() the checkpoint and re-run, or the re-run writes "
                "them a second time."
            )
        if last is None:
            # A batch can land before its checkpoint entry is written.
            job_id = str(job.get("job_id") or "")
            landed = bool(job_id) and any(
                _snapshot_stamp(s)[0] == job_id and _snapshot_stamp(s)[1] is not None
                for s in self.dest.metadata.snapshots
            )
            if not landed:
                return plain
            committed = "A batch of this job was committed to the destination."
        else:
            committed = f"{last + 1} batch(es) of this job were committed to the destination."
        before = job.get("dest_snapshot_before")
        if before is None:
            undo = (
                "delete this job's rows from the destination (it had no snapshot "
                "when the job started, so no snapshot rollback can undo the job)"
            )
        else:
            undo = (
                f"roll the destination back with rollback_to_snapshot(dest, {before}) "
                "(or delete this job's rows)"
            )
        return (
            f"{committed} Before you reset() the checkpoint and re-run, {undo}; "
            "otherwise the re-run writes them a second time."
        )

    def _recover_uncheckpointed_batches(self, job: dict[str, Any]) -> None:
        """Reconcile the checkpoint with the destination's current history.

        Walks the destination history from its current snapshot back
        to the job's starting snapshot and makes two checks:

        - Every checkpointed batch must still be in that history. A
          rollback of the destination (to the job's starting snapshot,
          or to any snapshot before a checkpointed batch) removes rows
          the checkpoint still counts as written; resuming would skip
          those batches and lose them. Expiry can cut the walk short
          (PyIceberg clears the parent of a snapshot whose parent it
          expires), so when the walk does not reach the job's starting
          snapshot, only checkpointed snapshots committed after the
          oldest walked one count as rolled back. An expired
          checkpointed snapshot is never reported.
        - A crash between a destination commit and
          :meth:`CheckpointStore.mark_committed` leaves a batch
          written but not recorded; replaying it would duplicate an
          append. Snapshots newer than the last checkpointed one that
          carry this job's ID are recorded as their batches.

        Raises
        ------
        CheckpointError
            If a checkpointed batch's snapshot is no longer in the
            destination's current history, or the stamped batches do
            not directly follow the last checkpointed batch.
        """
        cp = self.checkpoint
        assert cp is not None
        job_id = str(job.get("job_id", ""))
        stop_at = job.get("dest_snapshot_before")
        lineage: list[Snapshot] = []  # newest first, excluding stop_at
        reached_start = False
        for snap in ancestors_of(self.dest.current_snapshot(), self.dest.metadata):
            if snap.snapshot_id == stop_at:
                reached_start = True
                break
            lineage.append(snap)

        # Ancestors older than the walk's oldest snapshot may be hidden
        # behind an expired parent; anything newer that is not in the
        # walk is not an ancestor of the current snapshot.
        oldest = None if reached_start or not lineage else _commit_order(lineage[-1])
        in_lineage = {s.snapshot_id for s in lineage}
        gone: list[int] = []
        for sid in cp.dest_snapshot_ids:
            recorded = self.dest.metadata.snapshot_by_id(sid)
            if sid in in_lineage or recorded is None:
                continue
            if oldest is None or _commit_order(recorded) > oldest:
                gone.append(sid)
        if gone:
            raise CheckpointError(
                f"Checkpoint {cp.path}: the destination no longer contains "
                f"snapshot(s) {gone} of batches up to {cp.last_committed()} of this "
                "job (rolled back?). Resuming would skip those batches and lose "
                f"their rows. {self._reset_hint(job)}"
            )

        known = set(cp.dest_snapshot_ids)
        found: dict[int, list[int]] = {}
        for snap in lineage:
            if snap.snapshot_id in known:
                break
            stamp_job, stamp_batch = _snapshot_stamp(snap)
            if stamp_job == job_id and stamp_batch is not None:
                found.setdefault(stamp_batch, []).append(snap.snapshot_id)

        last = cp.last_committed()
        first = 0 if last is None else last + 1
        pending = sorted(b for b in found if b >= first)
        if pending != list(range(first, first + len(pending))):
            raise CheckpointError(
                f"Checkpoint {cp.path} is at batch {last}, but the destination holds "
                f"batches {pending} of this job; they do not follow on. Inspect the "
                f"destination before you start over. {self._reset_hint(job)}"
            )
        for batch_idx in pending:
            # ancestors_of walks newest-first; record in commit order.
            cp.mark_committed(batch_idx, list(reversed(found[batch_idx])))
            logger.warning(
                "DataMigrator resume: batch %d was committed to the destination "
                "but not checkpointed before the previous run stopped; recorded it.",
                batch_idx,
            )

    def _source_schema_at(self, snapshot_id: int | None) -> IcebergSchema:
        """Return the source schema the scan at *snapshot_id* projects."""
        if snapshot_id is None:
            return self.source.schema()
        return self.source.scan(snapshot_id=snapshot_id).projection()

    def _check_upsert(self, plan: ReconciliationPlan) -> None:
        """Validate ``upsert_keys`` and the missing-column policy against *plan*.

        Raises
        ------
        ValueError
            If a key is not in the projection, a key has no source
            column (the plan would fill it with null), or the policy
            would leave a destination column out of the projection.
        """
        assert self.upsert_keys is not None
        projected = list(plan.dest_arrow_schema.names)
        missing = [k for k in self.upsert_keys if k not in projected]
        if missing:
            raise ValueError(
                f"upsert_keys not in the projected destination schema: {missing}. "
                f"Projected columns: {projected}."
            )
        null_keys = [
            p.dest_name
            for p in plan.projections
            if p.fill_with_null and p.dest_name in self.upsert_keys
        ]
        if null_keys:
            raise ValueError(
                f"upsert_keys {null_keys} have no source column and would be filled "
                "with null; map them with column_mapping or choose other keys."
            )
        dest_columns = [f.name for f in self.dest.schema().fields]
        dest_columns += [name for name, _ in plan.auto_add_columns]
        skipped = [c for c in dest_columns if c not in projected]
        if skipped:
            raise ValueError(
                "write_mode=upsert needs every destination column in the projection; "
                f"skipped: {skipped}. Map them with column_mapping, or use "
                "on_missing_in_source='null'."
            )
        nulled = [p.dest_name for p in plan.projections if p.fill_with_null]
        if nulled:
            plan.warnings.append(
                f"upsert overwrites existing values of {nulled} with null for matched keys."
            )

    def _check_overwrite_partition(self, snapshot_id: int | None) -> None:
        """Reject ``overwrite_partition`` migrations this release cannot do safely.

        Raises
        ------
        ValueError
            If the destination is not partitioned by identity
            transforms only, or the source holds more than
            ``batch_rows`` rows after the row filter (the migration
            would need a second commit, which would delete the rows
            the first one wrote to the same partitions).
        """
        spec = self.dest.spec()
        if spec.is_unpartitioned() or any(
            not isinstance(f.transform, IdentityTransform) for f in spec.fields
        ):
            raise ValueError(
                "write_mode=overwrite_partition needs a destination partitioned by "
                "identity transforms only."
            )
        if snapshot_id is None:
            return
        scan = self.source.scan(row_filter=self.row_filter, snapshot_id=snapshot_id)
        # Fast path: file statistics bound the row count from above.
        if sum(task.file.record_count for task in scan.plan_files()) <= self.batch_rows:
            return
        rows = 0
        for batch in scan.to_arrow_batch_reader():
            rows += batch.num_rows
            if rows > self.batch_rows:
                raise ValueError(
                    "write_mode=overwrite_partition supports only migrations that fit "
                    f"in one commit in this release: the source has more than "
                    f"batch_rows={self.batch_rows} rows after the row filter. Raise "
                    "batch_rows (--batch-rows) above the source row count, or use "
                    "write_mode append or upsert."
                )

    # -----------------------------------------------------------------------
    # Dry-run
    # -----------------------------------------------------------------------

    def _run_dry_run(
        self,
        plan: ReconciliationPlan,
        source_snapshot: int | None,
        result: MigrationResult,
    ) -> None:
        """Iterate the source through the reconciler without writing.

        Counts rows + bytes so the user gets a useful estimate. Does
        not call any destination mutator (no append, upsert,
        overwrite, or schema update).
        """
        reader = self._open_reader(source_snapshot)
        total_rows = 0
        total_bytes = 0
        for src_batch in reader:
            projected = self.schema_reconciler.project_batch(src_batch, plan)
            total_rows += projected.num_rows
            total_bytes += projected.nbytes
        result.estimated_rows = total_rows
        result.estimated_bytes = total_bytes
        if self.progress is not None:
            # One terminal callback so dry-run users can populate UI.
            self.progress(total_rows, total_rows)

    # -----------------------------------------------------------------------
    # Streaming run loop
    # -----------------------------------------------------------------------

    def _run_streaming(
        self,
        plan: ReconciliationPlan,
        source_snapshot: int | None,
        result: MigrationResult,
    ) -> None:
        """The main streaming + write loop.

        Source batches that exceed ``batch_rows`` are sliced
        (zero-copy via :meth:`pa.RecordBatch.slice`) so each commit
        is bounded by ``batch_rows``. Smaller source batches are
        accumulated until the threshold is reached.
        """
        reader = self._open_reader(source_snapshot)

        acc: list[pa.RecordBatch] = []
        acc_rows = 0
        batch_idx = 0

        for src_batch in reader:
            # Slice the source batch into batch_rows-sized chunks
            # (and whatever remainder is left for the accumulator).
            offset = 0
            while offset < src_batch.num_rows:
                take = min(self.batch_rows - acc_rows, src_batch.num_rows - offset)
                acc.append(src_batch.slice(offset, take))
                acc_rows += take
                offset += take
                if acc_rows >= self.batch_rows:
                    self._commit_chunk(acc, batch_idx, plan, result)
                    batch_idx += 1
                    acc.clear()
                    acc_rows = 0

        # Flush remainder: anything left in the accumulator is also
        # a commit-worthy batch.
        if acc:
            self._commit_chunk(acc, batch_idx, plan, result)

    def _commit_chunk(
        self,
        acc: list[pa.RecordBatch],
        batch_idx: int,
        plan: ReconciliationPlan,
        result: MigrationResult,
    ) -> None:
        """Project, write, checkpoint, and report progress for one chunk."""
        if self.checkpoint and self.checkpoint.is_committed(batch_idx):
            # Already-committed batch - discard. Note: we still paid
            # for the source-side I/O to read it. A future
            # optimisation could try to skip-read at the scan level,
            # but that's PyIceberg-internal and not exposed today.
            return
        if self.write_mode == "overwrite_partition" and batch_idx > 0:
            # Pre-flight bounds overwrite_partition to one commit; a
            # second dynamic overwrite would delete the first one's
            # rows in the same partitions.
            raise RuntimeError(
                "write_mode=overwrite_partition reached a second commit, which "
                "would delete rows the first commit wrote. Aborting before writing it."
            )

        projected_chunks = [self.schema_reconciler.project_batch(b, plan) for b in acc]
        chunk_table = pa.Table.from_batches(projected_chunks, schema=plan.dest_arrow_schema)

        # Refresh so the "before" snapshot is current, write, then
        # refresh again: PyIceberg's writer updates the table object
        # in place but a concurrent writer may have committed too.
        self.dest.refresh()
        before = self.dest.current_snapshot()
        before_id = before.snapshot_id if before is not None else None
        counts = self._write(chunk_table, batch_idx)
        self.dest.refresh()
        new_ids = self._batch_snapshots_since(before_id, batch_idx)

        if self.checkpoint:
            self.checkpoint.mark_committed(batch_idx, new_ids)

        result.rows_moved += chunk_table.num_rows
        result.bytes_moved += int(chunk_table.nbytes)
        result.new_snapshots.extend(new_ids)
        if counts is not None:
            updated, inserted = counts
            result.rows_updated = (result.rows_updated or 0) + updated
            result.rows_inserted = (result.rows_inserted or 0) + inserted

        if self.progress is not None:
            self.progress(result.rows_moved, None)

    def _batch_snapshots_since(self, before_id: int | None, batch_idx: int) -> list[int]:
        """Return this job's snapshots for *batch_idx* newer than *before_id*.

        Walks the destination's current lineage back to *before_id*
        and keeps only snapshots stamped with this job and batch, so
        snapshots committed concurrently by other writers are
        excluded. The result is in commit order.
        """
        ids: list[int] = []
        for snap in ancestors_of(self.dest.current_snapshot(), self.dest.metadata):
            if snap.snapshot_id == before_id:
                break
            if _snapshot_stamp(snap) == (self._job_id, batch_idx):
                ids.append(snap.snapshot_id)
        ids.reverse()
        return ids

    # -----------------------------------------------------------------------
    # Internal: reader + writer
    # -----------------------------------------------------------------------

    def _open_reader(self, source_snapshot: int | None) -> pa.RecordBatchReader:
        """Open the source scan as a PyArrow batch reader."""
        scan = self.source.scan(
            row_filter=self.row_filter,
            snapshot_id=source_snapshot,
        )
        return scan.to_arrow_batch_reader()

    def _write(self, chunk: pa.Table, batch_idx: int) -> tuple[int, int] | None:
        """Dispatch to the right destination writer per ``write_mode``.

        Every snapshot the write creates is stamped with this job's
        ID and *batch_idx*.

        Returns
        -------
        tuple of (int, int) or None
            ``(rows_updated, rows_inserted)`` for an upsert, else ``None``.
        """
        props = {_JOB_PROPERTY: self._job_id, _BATCH_PROPERTY: str(batch_idx)}
        if self.write_mode == "append":
            self.dest.append(chunk, snapshot_properties=props)
            return None
        if self.write_mode == "upsert":
            # upsert_keys is non-None per __init__ validation.
            assert self.upsert_keys is not None
            return _upsert_chunk(self.dest, chunk, self.upsert_keys, snapshot_properties=props)
        # overwrite_partition
        self.dest.dynamic_partition_overwrite(chunk, snapshot_properties=props)
        return None

    # -----------------------------------------------------------------------
    # Verification
    # -----------------------------------------------------------------------

    def verify(
        self,
        *,
        method: VerifyMethod = "row_count",
        sample_size: int = 100,
    ) -> VerificationResult:
        """Post-migration consistency check.

        Three methods, ordered cheapest -> most thorough:

        ``row_count`` (default)
            Count source rows (after the migrator's row filter and
            snapshot pin) and destination rows. Compare. Uses the
            manifests' per-file record counts; reads data files only
            where the row filter cannot be decided per file or delete
            files apply.

        ``partition_hash``
            Group rows by destination partition value, sort each
            group's rows by all columns, SHA256-hash, compare.
            Currently supports only identity-transform partition
            specs and unpartitioned tables; other specs raise
            :class:`NotImplementedError`. Memory-intensive -
            materialises both sides in full.

        ``sample``
            Take the first ``sample_size`` rows on each side, sort
            within the sample by all columns, hash, compare. Fast
            but only reliable when both tables fit within
            ``sample_size`` (degenerates to a full check) or when
            the read order is deterministic across both sides.

        Parameters
        ----------
        method : ``"row_count"`` | ``"partition_hash"`` | ``"sample"``
            Which verification to run.
        sample_size : int, optional
            For ``method="sample"`` only. Default ``100``. Ignored
            for other methods.

        Returns
        -------
        VerificationResult

        Raises
        ------
        ValueError
            If *method* is unknown, or ``sample_size <= 0`` for
            ``method="sample"``.
        NotImplementedError
            For ``method="partition_hash"`` when the destination's
            partition spec has a non-identity transform. It is a
            :class:`RuntimeError`, so it is not reported as a
            mismatch.

        Notes
        -----
        The source is read at the explicit ``source_snapshot``, else
        at the snapshot the last :meth:`run` on this migrator read
        (including the checkpoint's snapshot on a resumed run), else
        at the source's current snapshot. Data appended to the source
        after the run therefore does not count as a mismatch.

        Caveat
        ------
        :meth:`verify` compares ``source`` (filtered + snapshot-
        pinned per the migrator's config) against ``dest`` (current
        state, no filter). Meaningful only when
        ``write_mode="append"`` into an empty destination, or when
        the user accounts for any pre-existing rows in their
        interpretation of the result.
        """
        if method not in ("row_count", "partition_hash", "sample"):
            raise ValueError(
                f"method must be 'row_count', 'partition_hash', or 'sample'; got {method!r}"
            )
        # Refresh both ends so a verify call right after run sees
        # the latest snapshot.
        self.source.refresh()
        self.dest.refresh()
        resolved_snap = self._resolve_source_snapshot()

        if method == "row_count":
            return self._verify_row_count(resolved_snap)
        if method == "partition_hash":
            return self._verify_partition_hash(resolved_snap)
        return self._verify_sample(resolved_snap, sample_size)

    def _resolve_source_snapshot(self) -> int | None:
        """Pick the source snapshot to read at.

        The explicit pin, else the snapshot the last run read, else
        the source's current snapshot.
        """
        if self.source_snapshot is not None:
            return self.source_snapshot
        if self._run_source_snapshot is not None:
            return self._run_source_snapshot
        snap = self.source.current_snapshot()
        return snap.snapshot_id if snap is not None else None

    def _verify_row_count(self, source_snapshot: int | None) -> VerificationResult:
        # DataScan.count() sums manifest record counts and reads a data
        # file only for a residual filter or delete files. A table
        # without a snapshot plans no files and counts 0.
        src_count = self.source.scan(
            row_filter=self.row_filter, snapshot_id=source_snapshot
        ).count()
        dst_count = self.dest.scan().count()
        matches = src_count == dst_count
        details: list[str] = []
        if not matches:
            details.append(
                f"row count mismatch: source={src_count}, "
                f"dest={dst_count}, delta={dst_count - src_count}"
            )
        return VerificationResult(
            method="row_count",
            matches=matches,
            source_count=src_count,
            dest_count=dst_count,
            details=details,
        )

    def _verify_partition_hash(self, source_snapshot: int | None) -> VerificationResult:
        spec = self.dest.spec()
        # Only identity transforms are supported. Non-identity
        # transforms (bucket, day, etc.) require computing the
        # partition value from the row, which is out of scope here.
        # Raise rather than report a mismatch: the data may be fine.
        if not spec.is_unpartitioned():
            for f in spec.fields:
                if not isinstance(f.transform, IdentityTransform):
                    raise NotImplementedError(
                        "partition_hash supports only unpartitioned and "
                        f"identity-transform partition specs; field {f.name!r} uses "
                        f"{type(f.transform).__name__}. Use method='row_count' or "
                        "'sample'."
                    )

        plan = self.schema_reconciler.compute_plan(
            self._source_schema_at(source_snapshot), self.dest.schema()
        )

        src_table = self._read_source_projected(source_snapshot, plan)
        dst_table = self.dest.scan().to_arrow()

        column_names = list(plan.dest_arrow_schema.names)

        if spec.is_unpartitioned():
            src_hash = _hash_table_content(src_table, column_names)
            dst_hash = _hash_table_content(dst_table, column_names)
            matches = src_hash == dst_hash
            details: list[str] = []
            mismatched: list[str] = []
            if not matches:
                mismatched.append("(unpartitioned)")
                details.append(
                    f"content hash mismatch: source={src_hash[:12]}..., dest={dst_hash[:12]}..."
                )
            return VerificationResult(
                method="partition_hash",
                matches=matches,
                mismatched_partitions=mismatched,
                details=details,
            )

        # Partitioned: group by the values of the columns the
        # partition fields are derived from. A partition field's own
        # name (e.g. "region_part") need not be a column, so resolve
        # each field's source column. The source's projected schema
        # (post-reconciler) uses the dest column names.
        dest_schema = self.dest.schema()
        partition_cols = [str(dest_schema.find_column_name(f.source_id)) for f in spec.fields]
        src_groups = _group_hash_by_columns(src_table, partition_cols, column_names)
        dst_groups = _group_hash_by_columns(dst_table, partition_cols, column_names)

        all_keys = set(src_groups) | set(dst_groups)
        mismatched = []
        details = []
        for key in sorted(all_keys, key=lambda k: tuple(str(v) for v in k)):
            sh = src_groups.get(key)
            dh = dst_groups.get(key)
            if sh != dh:
                mismatched.append(_format_partition_key(partition_cols, key))
                if sh is None:
                    details.append(
                        f"partition {_format_partition_key(partition_cols, key)}: "
                        "missing in source"
                    )
                elif dh is None:
                    details.append(
                        f"partition {_format_partition_key(partition_cols, key)}: missing in dest"
                    )
                else:
                    details.append(
                        f"partition {_format_partition_key(partition_cols, key)}: "
                        f"hash mismatch (source={sh[:12]}..., dest={dh[:12]}...)"
                    )
        return VerificationResult(
            method="partition_hash",
            matches=not mismatched,
            mismatched_partitions=mismatched,
            details=details,
        )

    def _verify_sample(self, source_snapshot: int | None, sample_size: int) -> VerificationResult:
        if sample_size <= 0:
            raise ValueError(f"sample_size must be positive, got {sample_size!r}")

        plan = self.schema_reconciler.compute_plan(
            self._source_schema_at(source_snapshot), self.dest.schema()
        )

        # Take first sample_size rows from each side.
        src_table = self._take_first_n_source(source_snapshot, plan, sample_size)
        dst_table = self._take_first_n_dest(sample_size)

        column_names = list(plan.dest_arrow_schema.names)
        src_hash = _hash_table_content(src_table, column_names)
        dst_hash = _hash_table_content(dst_table, column_names)

        compared = min(src_table.num_rows, dst_table.num_rows)
        matches = src_hash == dst_hash
        details: list[str] = []
        if not matches:
            details.append(
                f"sample-of-{compared} hash mismatch "
                f"(source={src_hash[:12]}..., dest={dst_hash[:12]}...)"
            )
        return VerificationResult(
            method="sample",
            matches=matches,
            source_count=src_table.num_rows,
            dest_count=dst_table.num_rows,
            sample_size=compared,
            details=details,
        )

    # -----------------------------------------------------------------------
    # Verification helpers (private)
    # -----------------------------------------------------------------------

    def _read_source_projected(
        self,
        source_snapshot: int | None,
        plan: ReconciliationPlan,
    ) -> pa.Table:
        """Read the full source through the reconciler's projection."""
        batches: list[pa.RecordBatch] = []
        for src_batch in self._open_reader(source_snapshot):
            batches.append(self.schema_reconciler.project_batch(src_batch, plan))
        if not batches:
            # Build an empty table with the right schema so
            # downstream code can rely on column membership.
            return plan.dest_arrow_schema.empty_table()
        return pa.Table.from_batches(batches, schema=plan.dest_arrow_schema)

    def _take_first_n_source(
        self,
        source_snapshot: int | None,
        plan: ReconciliationPlan,
        n: int,
    ) -> pa.Table:
        """Read up to *n* rows from source (post-projection)."""
        batches: list[pa.RecordBatch] = []
        rows = 0
        for src_batch in self._open_reader(source_snapshot):
            if rows >= n:
                break
            proj = self.schema_reconciler.project_batch(src_batch, plan)
            if rows + proj.num_rows > n:
                proj = proj.slice(0, n - rows)
            batches.append(proj)
            rows += proj.num_rows
        if not batches:
            return plan.dest_arrow_schema.empty_table()
        return pa.Table.from_batches(batches, schema=plan.dest_arrow_schema)

    def _take_first_n_dest(self, n: int) -> pa.Table:
        """Read up to *n* rows from dest."""
        batches: list[pa.RecordBatch] = []
        rows = 0
        for batch in self.dest.scan().to_arrow_batch_reader():
            if rows >= n:
                break
            if rows + batch.num_rows > n:
                batch = batch.slice(0, n - rows)
            batches.append(batch)
            rows += batch.num_rows
        if not batches:
            return self.dest.scan().to_arrow().slice(0, 0)
        return pa.Table.from_batches(batches)


# ===========================================================================
# Hashing helpers (module-level so they can be unit-tested)
# ===========================================================================


def _hash_table_content(table: pa.Table, columns: list[str]) -> str:
    """SHA256 of *table*'s rows after sorting by *columns*.

    The hash is row-order-independent (because we sort first) and
    column-order-independent within each row (we sort dict items
    when serialising). Empty tables produce the SHA256 of the empty
    string for a stable sentinel value.
    """
    if table.num_rows == 0:
        return hashlib.sha256(b"").hexdigest()
    # Sort by every column so two tables with the same multiset of
    # rows hash the same.
    sort_keys = [(c, "ascending") for c in columns]
    ordered = table.sort_by(sort_keys)
    h = hashlib.sha256()
    for row in ordered.to_pylist():
        # Sort dict items so column-order changes in the schema
        # don't perturb the hash.
        h.update(repr(sorted(row.items())).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _group_hash_by_columns(
    table: pa.Table,
    partition_cols: list[str],
    all_cols: list[str],
) -> dict[tuple[Any, ...], str]:
    """For every distinct combination of values in *partition_cols*,
    return the content hash of the rows that match.

    The returned dict's keys are tuples in *partition_cols* order;
    the values are SHA256 hex digests produced by
    :func:`_hash_table_content`.
    """
    if table.num_rows == 0:
        return {}
    # Distinct partition-value tuples.
    pc_table = table.select(partition_cols)
    seen: set[tuple[Any, ...]] = set()
    distinct: list[tuple[Any, ...]] = []
    for row in pc_table.to_pylist():
        key = tuple(row[c] for c in partition_cols)
        if key not in seen:
            seen.add(key)
            distinct.append(key)

    result: dict[tuple[Any, ...], str] = {}
    # PyArrow's stubs don't expose `is_null` / `equal` / `and_` on
    # the public ``pyarrow.compute`` surface in the version we pin;
    # cast once at function scope to avoid per-line ignores.
    import pyarrow.compute as _pc

    pc: Any = _pc

    for key in distinct:
        # Build mask: col1 == val1 AND col2 == val2 AND ...
        masks = []
        for col, val in zip(partition_cols, key, strict=True):
            if val is None:
                masks.append(pc.is_null(table[col]))
            else:
                masks.append(pc.equal(table[col], val))
        mask = masks[0]
        for m in masks[1:]:
            mask = pc.and_(mask, m)
        subset = table.filter(mask)
        result[key] = _hash_table_content(subset, all_cols)
    return result


def _format_partition_key(cols: list[str], key: tuple[Any, ...]) -> str:
    """Pretty-print a partition key tuple as ``col1=v1/col2=v2``."""
    return "/".join(f"{c}={v}" for c, v in zip(cols, key, strict=True))


def _commit_order(snap: Snapshot) -> tuple[int, int]:
    """Sort key that orders *snap* among a table's commits.

    The sequence number rises with every commit in format v2 and
    later; format v1 leaves it at 0, so the timestamp breaks the tie.
    """
    return (snap.sequence_number or 0, snap.timestamp_ms)


def _snapshot_stamp(snap: Snapshot) -> tuple[str | None, int | None]:
    """Return the ``(job ID, batch index)`` the migrator stamped on *snap*.

    Either element is ``None`` when the property is absent (for
    example a snapshot written by another tool, or the schema-stamp
    snapshot, which carries a job ID but no batch).
    """
    summary = snap.summary
    if summary is None:
        return None, None
    job_id = summary.get(_JOB_PROPERTY)
    batch = summary.get(_BATCH_PROPERTY)
    batch_idx = int(batch) if batch is not None and batch.isdigit() else None
    return job_id, batch_idx


# ===========================================================================
# Re-exports for the package's __init__.py
# ===========================================================================


__all__ = [
    "DataMigrator",
    "MigrationResult",
    "PartitionStrategy",
    "VerificationResult",
    "VerifyMethod",
    "WriteMode",
]
