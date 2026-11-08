"""
iceberg_ops.maintenance.snapshots
=================================
Expire old snapshots based on age and minimum count policy.

Public API
----------
``expire_snapshots(table, *, max_age_hours=None, min_snapshots=None, dry_run=False)``
    Identify and expire snapshots older than *max_age_hours*, while
    always keeping at least *min_snapshots* and never expiring a branch
    or tag head. Unset arguments fall back to the table's
    ``history.expire.*`` properties, then to the Iceberg defaults
    (5 days, 1 snapshot).

Expiry is metadata-only: the snapshots are removed from table metadata
but their data files stay on storage until an orphan cleanup (see
:func:`iceberg_ops.maintenance.remove_orphan_files`, ``file://`` only)
reclaims them.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field

from pyiceberg.table import Table
from pyiceberg.utils.properties import property_as_int

logger = logging.getLogger(__name__)


# Iceberg's table-property names and defaults for snapshot retention.
_MAX_AGE_PROPERTY = "history.expire.max-snapshot-age-ms"
_MIN_SNAPSHOTS_PROPERTY = "history.expire.min-snapshots-to-keep"
_DEFAULT_MAX_AGE_MS = 432_000_000  # 5 days
_DEFAULT_MIN_SNAPSHOTS = 1
_MS_PER_HOUR = 3_600_000


def _expire_snapshot_ids(table: Table, snapshot_ids: list[int]) -> None:
    """Expire the given *snapshot_ids* via ``Table.maintenance``.

    The caller must have filtered out branch and tag heads already;
    PyIceberg refuses to expire them.

    Parameters
    ----------
    table : Table
        The Iceberg table whose snapshots should be expired.
    snapshot_ids : list of int
        Snapshot IDs to expire. An empty list is a no-op.
    """
    if snapshot_ids:
        table.maintenance.expire_snapshots().by_ids(snapshot_ids).commit()


@dataclass
class SnapshotExpiryResult:
    """
    Result of a snapshot expiry operation.

    Attributes
    ----------
    total_snapshots : int
        Number of snapshots before the operation.
    expired_count : int
        Number of snapshots expired (or identified for expiry in dry-run).
    retained_count : int
        Number of snapshots retained after the operation.
    expired_ids : list[int]
        Snapshot IDs that were (or would be) expired.
    protected_ids : list[int]
        Snapshot IDs that were old enough to expire but were kept
        because a branch or tag points at them.
    """

    total_snapshots: int
    expired_count: int
    retained_count: int
    expired_ids: list[int] = field(default_factory=list)
    protected_ids: list[int] = field(default_factory=list)


def expire_snapshots(
    table: Table,
    *,
    max_age_hours: float | None = None,
    min_snapshots: int | None = None,
    dry_run: bool = False,
) -> SnapshotExpiryResult:
    """
    Expire old snapshots based on age and minimum count policy.

    Snapshots are sorted newest-first. The function expires snapshots
    older than *max_age_hours* but always retains the *min_snapshots*
    most recent ones. Branch and tag heads (including the current
    snapshot after a rollback) are never expired; they count towards
    *min_snapshots*. Dry-run and real runs select the same snapshots.

    Expiry is metadata-only: data files of expired snapshots stay on
    storage until an orphan cleanup reclaims them.

    Parameters
    ----------
    table : Table
        The Iceberg table to manage.
    max_age_hours : float, optional
        Expire snapshots older than this many hours. When ``None``
        (the default), the table property
        ``history.expire.max-snapshot-age-ms`` is used, falling back to
        the Iceberg default of 5 days (120 hours). An age that reaches
        back before the Unix epoch (for example the property set to
        ``9223372036854775807``, Java's ``Long.MAX_VALUE``) expires
        nothing by age.
    min_snapshots : int, optional
        Always keep at least this many snapshots. When ``None`` (the
        default), the table property
        ``history.expire.min-snapshots-to-keep`` is used, falling back
        to the Iceberg default of ``1``.
    dry_run : bool, optional
        If True, report which snapshots would be expired without acting.
        Default ``False``.

    Returns
    -------
    SnapshotExpiryResult
        Statistics about expired snapshots.

    Raises
    ------
    ValueError
        If the resolved ``min_snapshots`` is below 1 or the resolved
        ``max_age_hours`` is negative, or if a ``history.expire.*``
        property is not an integer.

    Notes
    -----
    PyIceberg commits the expiry without asserting the table's refs,
    so a tag created on an expiring snapshot between this function's
    refresh and its commit is dropped with the snapshot. Avoid
    creating tags while an expiry runs.
    """
    table.refresh()

    if max_age_hours is None:
        max_age_ms = property_as_int(table.properties, _MAX_AGE_PROPERTY, _DEFAULT_MAX_AGE_MS)
        max_age_hours = (_DEFAULT_MAX_AGE_MS if max_age_ms is None else max_age_ms) / _MS_PER_HOUR
    if min_snapshots is None:
        min_keep = property_as_int(
            table.properties, _MIN_SNAPSHOTS_PROPERTY, _DEFAULT_MIN_SNAPSHOTS
        )
        min_snapshots = _DEFAULT_MIN_SNAPSHOTS if min_keep is None else min_keep

    if min_snapshots < 1:
        raise ValueError(f"min_snapshots must be >= 1, got {min_snapshots!r}")
    if max_age_hours < 0:
        raise ValueError(f"max_age_hours must be >= 0, got {max_age_hours!r}")

    # Integer milliseconds, not timedelta: a "never expire by age" value
    # such as Long.MAX_VALUE ms would overflow timedelta / datetime.
    # ``None`` means the age reaches back before the epoch, so no
    # snapshot is old enough.
    now_ms = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
    age_ms = max_age_hours * _MS_PER_HOUR
    cutoff_ms: int | None = now_ms - int(age_ms) if age_ms < now_ms else None
    cutoff_label = (
        "none"
        if cutoff_ms is None
        else datetime.datetime.fromtimestamp(cutoff_ms / 1000, tz=datetime.UTC).isoformat()
    )

    snapshots = sorted(
        table.metadata.snapshots,
        key=lambda s: s.timestamp_ms,
        reverse=True,
    )
    total = len(snapshots)

    # Branch and tag heads can never be expired.
    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}

    # Always keep the newest min_snapshots; of the rest, expire the old
    # ones that no ref points at.
    old_enough = (
        []
        if cutoff_ms is None
        else [s for s in snapshots[min_snapshots:] if s.timestamp_ms < cutoff_ms]
    )
    expire_ids = [s.snapshot_id for s in old_enough if s.snapshot_id not in protected]
    protected_ids = [s.snapshot_id for s in old_enough if s.snapshot_id in protected]

    if not expire_ids:
        logger.info(
            "No snapshots to expire for '%s' (%d total, cutoff=%s).",
            table.name(),
            total,
            cutoff_label,
        )
        return SnapshotExpiryResult(
            total_snapshots=total,
            expired_count=0,
            retained_count=total,
            expired_ids=[],
            protected_ids=protected_ids,
        )

    if dry_run:
        logger.info(
            "Expire dry-run for '%s': would expire %d of %d snapshots.",
            table.name(),
            len(expire_ids),
            total,
        )
        return SnapshotExpiryResult(
            total_snapshots=total,
            expired_count=len(expire_ids),
            retained_count=total - len(expire_ids),
            expired_ids=expire_ids,
            protected_ids=protected_ids,
        )

    _expire_snapshot_ids(table, expire_ids)

    table.refresh()
    retained = len(table.metadata.snapshots)

    logger.info(
        "Expired %d snapshots from '%s' (%d -> %d).",
        len(expire_ids),
        table.name(),
        total,
        retained,
    )

    return SnapshotExpiryResult(
        total_snapshots=total,
        expired_count=len(expire_ids),
        retained_count=retained,
        expired_ids=expire_ids,
        protected_ids=protected_ids,
    )
