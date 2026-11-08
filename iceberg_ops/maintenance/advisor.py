"""
iceberg_ops.maintenance.advisor
===============================
Read-only diagnostic of table file layout and health.

Public API
----------
``table_health_report(table, small_file_threshold_mb=100)``
    Generate a health report with file statistics and recommendations.
    Never modifies the table - safe to run from any context.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, cast

import pyarrow.compute as _pc
from pyiceberg.table import Table

logger = logging.getLogger(__name__)


@dataclass
class TableHealthReport:
    """
    Read-only diagnostic of table file layout and health.

    Attributes
    ----------
    total_files : int
        Number of data files in the current snapshot.
    total_size_bytes : int
        Sum of all data file sizes.
    avg_file_size_bytes : float
        Average file size in bytes.
    min_file_size_bytes : int
        Smallest data file size.
    max_file_size_bytes : int
        Largest data file size.
    small_file_count : int
        Number of files below ``small_file_threshold_mb``.
    small_file_percentage : float
        Percentage of files that are "small".
    snapshot_count : int
        Total number of snapshots in metadata.
    oldest_snapshot_age_hours : float
        Age of the oldest snapshot in hours.
    partition_field_count : int
        Number of active partition fields in the current spec.
    recommendations : list[str]
        Human-readable maintenance suggestions.
    """

    total_files: int
    total_size_bytes: int
    avg_file_size_bytes: float
    min_file_size_bytes: int
    max_file_size_bytes: int
    small_file_count: int
    small_file_percentage: float
    snapshot_count: int
    oldest_snapshot_age_hours: float
    partition_field_count: int
    recommendations: list[str] = field(default_factory=list)


def table_health_report(
    table: Table,
    small_file_threshold_mb: int = 100,
) -> TableHealthReport:
    """
    Generate a read-only health report for an Iceberg table.

    Aggregates file sizes with :mod:`pyarrow.compute`. The input comes
    from ``table.inspect.files()``, which builds one Python dict per
    file, so time and memory grow with the number of files. Also
    computes snapshot count and oldest-snapshot age for retention
    recommendations.

    This function never modifies the table. Safe to run from any
    context including read-only Lambda functions.

    Parameters
    ----------
    table : Table
        The Iceberg table to inspect.
    small_file_threshold_mb : int, optional
        Files smaller than this are counted as "small". Default ``100``.

    Returns
    -------
    TableHealthReport
        Diagnostic data with actionable recommendations.
    """
    # PyArrow's stubs don't declare ``sum`` / ``min`` / ``max`` / ``less``
    # on the public ``pyarrow.compute`` surface in the version we pin;
    # cast to Any once at function scope to avoid per-line ignores.
    pc: Any = _pc

    table.refresh()
    threshold_bytes = small_file_threshold_mb * 1024 * 1024

    # --- File statistics --------------------------------------------------
    sizes = None
    total_files = 0
    try:
        files_arrow = table.inspect.files()
        sizes = files_arrow.column("file_size_in_bytes")  # ChunkedArray
        total_files = sizes.length()
    except Exception:
        # Some catalog backends raise on inspect.files() for empty
        # tables; we recover with zero stats below.
        pass

    if sizes is not None and total_files:
        total_size_bytes = cast(int, pc.sum(sizes).as_py())
        min_size = cast(int, pc.min(sizes).as_py())
        max_size = cast(int, pc.max(sizes).as_py())
        avg_size = total_size_bytes / total_files
        small_count = cast(int, pc.sum(pc.less(sizes, threshold_bytes)).as_py())
        small_pct = (small_count / total_files) * 100.0
    else:
        total_size_bytes = 0
        min_size = 0
        max_size = 0
        small_count = 0
        avg_size = 0.0
        small_pct = 0.0

    # --- Snapshot statistics ----------------------------------------------
    snapshots = table.metadata.snapshots
    snapshot_count = len(snapshots)

    oldest_age_hours = 0.0
    if snapshots:
        now = datetime.datetime.now(datetime.UTC)
        oldest_ts = min(s.timestamp_ms for s in snapshots)
        oldest_dt = datetime.datetime.fromtimestamp(
            oldest_ts / 1000.0,
            tz=datetime.UTC,
        )
        oldest_age_hours = (now - oldest_dt).total_seconds() / 3600.0

    # --- Partition info ---------------------------------------------------
    spec_fields = table.spec().fields
    partition_field_count = len(spec_fields)

    # --- Recommendations --------------------------------------------------
    recommendations: list[str] = []

    if total_files > 0 and small_pct > 20.0:
        recommendations.append(
            f"Consider running compaction - {small_pct:.0f}% of files "
            f"({small_count}/{total_files}) are under {small_file_threshold_mb} MB."
        )

    if snapshot_count > 100:
        recommendations.append(
            f"Consider expiring old snapshots - {snapshot_count} snapshots found."
        )

    if oldest_age_hours > 720.0:
        days = oldest_age_hours / 24.0
        recommendations.append(
            f"Oldest snapshot is {days:.0f} days old - consider a retention policy."
        )

    if total_files == 0:
        recommendations.append("Table has no data files. Append data before running maintenance.")

    logger.info(
        "Health report for '%s': %d files, %d bytes, %d snapshots.",
        table.name(),
        total_files,
        total_size_bytes,
        snapshot_count,
    )

    return TableHealthReport(
        total_files=total_files,
        total_size_bytes=total_size_bytes,
        avg_file_size_bytes=avg_size,
        min_file_size_bytes=min_size,
        max_file_size_bytes=max_size,
        small_file_count=small_count,
        small_file_percentage=small_pct,
        snapshot_count=snapshot_count,
        oldest_snapshot_age_hours=oldest_age_hours,
        partition_field_count=partition_field_count,
        recommendations=recommendations,
    )
