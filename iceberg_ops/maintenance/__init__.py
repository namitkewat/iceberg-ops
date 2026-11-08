"""
iceberg_ops.maintenance
=======================
Storage maintenance for Apache Iceberg tables.

These are basic helpers for self-managed tables on the local
filesystem. Delete files and v3 deletion vectors are out of scope.
They are only needed when your catalog does not provide managed
maintenance; AWS S3 Tables, Snowflake-managed Iceberg, and Databricks
Unity Catalog handle compaction / snapshot expiry / orphan cleanup
automatically.

For self-hosted deployments, schedule these helpers on a cadence
appropriate for your write rate (e.g. cron, or a runner like the one
in :mod:`iceberg_ops.runner`).

Public API
----------
:func:`compact_small_files_streaming(table, *, target_size_mb=512, ...)`
    Partition-aware compaction for Identity-partitioned (or
    unpartitioned) tables. Rewrites qualifying partitions into
    Parquet files of roughly ``target_size_mb`` and swaps them in via
    one PyIceberg transaction. Peak memory grows with the files per
    partition; it is not bounded by ``target_size_mb``.

:func:`expire_snapshots(table, *, max_age_hours=None, min_snapshots=None, dry_run=False)`
    Expire snapshots older than *max_age_hours* while keeping at
    least *min_snapshots* and every branch / tag head. Unset
    arguments fall back to the table's ``history.expire.*``
    properties. Metadata-only.

:func:`remove_orphan_files(table, *, older_than_hours=72, sweep=("data", "metadata"), dry_run=True)`
    Recursively walk the table location and report (or, with
    ``dry_run=False``, delete) files that no table metadata
    references and whose mtime predates the cutoff. Refuses tables
    with ``gc.enabled=false``.

:func:`table_health_report(table, small_file_threshold_mb=100)`
    Read-only diagnostic of file layout, snapshot count, and
    partition surface, with actionable recommendations. Reads
    ``inspect.files()``, which builds one Python dict per file, so
    cost grows with the file count.

Local filesystem only
---------------------
The compaction and orphan-cleanup helpers support ``file://``
locations only. Tables on ``s3://`` / ``gs://`` / ``abfs://``
locations raise :class:`NotImplementedError` with a clear message.
:func:`expire_snapshots` and :func:`table_health_report` only touch
metadata and work on every backend.

The APIs may change before 1.0.
"""

from iceberg_ops.maintenance.advisor import TableHealthReport, table_health_report
from iceberg_ops.maintenance.compaction import (
    CompactionResult,
    compact_small_files_streaming,
)
from iceberg_ops.maintenance.orphans import OrphanCleanupResult, remove_orphan_files
from iceberg_ops.maintenance.snapshots import SnapshotExpiryResult, expire_snapshots

__all__ = [
    "CompactionResult",
    "OrphanCleanupResult",
    "SnapshotExpiryResult",
    "TableHealthReport",
    "compact_small_files_streaming",
    "expire_snapshots",
    "remove_orphan_files",
    "table_health_report",
]
