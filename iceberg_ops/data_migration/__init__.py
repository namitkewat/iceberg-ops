"""
iceberg_ops.data_migration
==========================
Cross-table, schema-reconciling, streaming Iceberg-to-Iceberg data migration.

This subpackage is the *logical* data migrator - moving rows between
Iceberg tables with optional schema reconciliation, type coercion, and
column renames - distinct from the schema migrations driven by
:mod:`iceberg_ops.runner`.

Public API
----------
:class:`SchemaReconciler`
    Declarative description of how to map source columns to dest
    columns, including renames, type coercions, and missing-column
    policies.

:class:`ReconciliationPlan`
    The structured plan returned by
    :meth:`SchemaReconciler.compute_plan`. Lists per-column
    projections plus any auto-add columns and warnings.

:class:`ColumnProjection`
    One entry of a plan: how to derive a single dest column from the
    source batch (rename + cast, or null-fill).

:class:`CheckpointStore`
    JSON-file-backed checkpoint that records per-batch progress so a
    crashed migration can resume from the last committed batch.

:class:`CheckpointError`
    A :class:`RuntimeError` raised when a checkpoint file is corrupt,
    unreadable, or has an unsupported version, and when
    :class:`DataMigrator` refuses to resume one (completed, started
    with different settings, its source snapshot is gone, or the
    destination no longer holds its checkpointed batches).

:class:`DataMigrator`
    Streaming, schema-reconciling, resumable migrator. Reads from a
    source Iceberg table, projects through ``SchemaReconciler``, and
    writes to a destination Iceberg table - appending, upserting, or
    partition-overwriting per the configured ``write_mode``.

:class:`MigrationResult`
    Outcome of :meth:`DataMigrator.run`: rows moved, bytes moved,
    snapshot list, schema changes, duration, dry-run estimates.

:class:`PartitionStrategy`
    How the migrator treats the destination partition spec. Only
    ``PRESERVE`` is implemented today.

:class:`VerificationResult`
    Outcome of :meth:`DataMigrator.verify`: structured report of
    whether the destination matches the source under the chosen
    method (``row_count`` / ``partition_hash`` / ``sample``).

CLI surface
-----------
:command:`iceberg-ops migrate-data` exposes the migrator from the shell.
"""

from __future__ import annotations

from iceberg_ops.data_migration.checkpoint import (
    CheckpointError,
    CheckpointStore,
)
from iceberg_ops.data_migration.migrator import (
    DataMigrator,
    MigrationResult,
    PartitionStrategy,
    VerificationResult,
    VerifyMethod,
    WriteMode,
)
from iceberg_ops.data_migration.reconciler import (
    ColumnProjection,
    ReconciliationPlan,
    SchemaReconciler,
)

__all__ = [
    "CheckpointError",
    "CheckpointStore",
    "ColumnProjection",
    "DataMigrator",
    "MigrationResult",
    "PartitionStrategy",
    "ReconciliationPlan",
    "SchemaReconciler",
    "VerificationResult",
    "VerifyMethod",
    "WriteMode",
]
