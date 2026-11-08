"""
iceberg_ops.maintenance.compaction
==================================
Rewrite small data files into larger ones to improve query performance.

Public API
----------
``compact_small_files_streaming(table, *, target_size_mb=512, ...)``
    Partition-aware compaction for Identity-partitioned (or
    unpartitioned) tables. Reads each qualifying partition via
    :meth:`Table.scan().to_arrow_batch_reader`, writes pre-staged
    Parquet files of roughly ``target_size_mb``, then swaps them in
    with a single PyIceberg transaction.

Memory
------
PyIceberg's batch reader decodes whole files before yielding batches
(apache/iceberg-python#2407, apache/iceberg-python#4028), so peak
memory grows with the number and size of files per partition. It is
not bounded by ``target_size_mb``.

Scope
-----
A basic helper for self-managed tables on the local filesystem
(``file://`` URIs and plain paths, including Windows drive-letter
paths; other schemes raise :class:`NotImplementedError`).
Delete files and v3 deletion vectors are out of scope.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import CommitFailedException, ValidationException
from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, EqualTo, IsNull
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

logger = logging.getLogger(__name__)


@dataclass
class CompactionResult:
    """
    Result of a compaction operation.

    Attributes
    ----------
    files_before : int
        Total data files before compaction.
    files_after : int
        Total data files after compaction.
    bytes_before : int
        Total bytes across all data files before compaction.
    bytes_after : int
        Total bytes across all data files after compaction.
    files_rewritten : int
        Number of small files that were (or would be) rewritten.
    skipped : bool
        True if compaction was skipped (no partition met the
        ``min_file_count`` threshold).
    """

    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    files_rewritten: int
    skipped: bool = False


def _iter_file_records(
    table: Table,
    batch_size: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """
    Stream ``{path, size_bytes, record_count, partition, spec_id}``
    records for each data file.

    Iterates ``table.inspect.files()`` via
    ``to_batches(max_chunksize=batch_size)``. ``inspect.files()``
    itself builds one Python dict per file, so memory still grows with
    the file count; this iterator only avoids a second full copy.

    The caller is responsible for ``table.refresh()`` before calling
    this helper - the iterator is otherwise pure.

    Parameters
    ----------
    table : Table
        The Iceberg table to inspect.
    batch_size : int, optional
        Maximum number of file rows per Arrow ``RecordBatch``. Default
        ``10_000``.

    Yields
    ------
    dict
        ``{"path": str, "size_bytes": int, "record_count": int,
        "partition": dict, "spec_id": int}`` for each data file in the
        current snapshot. ``partition`` is the struct of partition
        values for the file (empty dict for unpartitioned tables).
    """
    files_arrow = table.inspect.files()
    for batch in files_arrow.to_batches(max_chunksize=batch_size):
        paths = batch.column("file_path")
        sizes = batch.column("file_size_in_bytes")
        counts = batch.column("record_count")
        partitions = batch.column("partition")
        spec_ids = batch.column("spec_id")
        for i in range(batch.num_rows):
            yield {
                "path": paths[i].as_py(),
                "size_bytes": sizes[i].as_py(),
                "record_count": counts[i].as_py(),
                "partition": partitions[i].as_py() or {},
                "spec_id": spec_ids[i].as_py(),
            }


def _is_drive_path(location: str) -> bool:
    """True if *location* is a local path with a drive (Windows only).

    ``urlparse`` reads the drive letter of a path like ``C:/wh/t`` as
    the scheme ``c``; PyIceberg treats such locations as local files.
    """
    return bool(os.path.splitdrive(location)[0])


def _file_uri_to_local_path(uri: str) -> str:
    """Convert a ``file://`` URI or a local path to a local filesystem path.

    Mirrors the helper in :mod:`iceberg_ops.maintenance.orphans` - kept
    private here to avoid cross-module imports inside ``maintenance/``.
    """
    if _is_drive_path(uri):
        return os.path.normpath(uri)
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        path = (parsed.netloc + parsed.path) if parsed.netloc else parsed.path
        if os.name == "nt" and path.startswith("/") and len(path) > 2 and path[2] == ":":
            path = path[1:]
        return os.path.normpath(path)
    return uri


def _partition_key(partition: dict) -> tuple[tuple[str, object], ...]:
    """Hashable key for a partition value dict. Empty for unpartitioned."""
    return tuple(sorted(partition.items()))


def _partition_predicate(
    partition: dict,
    spec_fields,
    schema,
) -> BooleanExpression:
    """Build a row filter that matches exactly one partition.

    Only Identity-transformed partition fields are supported - for
    those, the partition struct value equals the source column value.
    Non-identity transforms (Bucket / Truncate / Year / etc.) cannot be
    inverted to a row predicate without scanning all data, so partitions
    using those transforms are skipped by the caller.

    Parameters
    ----------
    partition : dict
        Partition struct value, e.g. ``{"region": "eu", "year": 2024}``.
    spec_fields : list[PartitionField]
        ``table.spec().fields`` - used to look up source column names.
    schema : Schema
        ``table.schema()`` - used to resolve ``source_id`` to column
        name.

    Returns
    -------
    BooleanExpression
        A predicate over source columns. ``AlwaysTrue`` for empty
        partition (unpartitioned table).
    """
    if not partition:
        return AlwaysTrue()

    # Map partition spec field name -> source column name (Identity only).
    source_by_spec_name: dict[str, str] = {}
    for f in spec_fields:
        if isinstance(f.transform, IdentityTransform):
            source_by_spec_name[f.name] = schema.find_field(f.source_id).name

    clauses: list[BooleanExpression] = []
    for spec_name, value in partition.items():
        if spec_name not in source_by_spec_name:
            # Caller should have filtered this out. Raise rather than
            # silently dropping to a broader predicate that might delete
            # more than intended.
            raise ValueError(
                f"Cannot build row filter for partition field {spec_name!r}: "
                "non-Identity transforms not yet supported in streaming compaction."
            )
        col = source_by_spec_name[spec_name]
        # PyIceberg's stub for IsNull / EqualTo is narrower than the
        # runtime signature - both accept ``str`` for the term at runtime.
        if value is None:
            clauses.append(IsNull(col))  # type: ignore[arg-type, misc]
        else:
            clauses.append(EqualTo(col, value))  # type: ignore[arg-type, misc, call-arg]

    expr = clauses[0]
    for c in clauses[1:]:
        expr = And(expr, c)
    return expr


def _partition_dir_segment(partition: dict, spec_fields) -> str:
    """Derive the on-disk subdirectory for a partition value.

    For an Identity-transformed partition ``{"region": "eu"}`` this
    returns ``"region=eu"``. Multiple Identity fields produce
    ``"a=1/b=2"``. Returns ``""`` for unpartitioned.

    Names and values are escaped with ``quote_plus(..., safe="")`` the
    way PyIceberg's ``PartitionSpec.partition_to_path`` escapes them,
    so a value such as ``"../x"`` or ``"a#b"`` stays one literal path
    segment (``region=..%2Fx``, ``region=a%23b``). ``None`` is written
    as ``null``.
    """
    if not partition:
        return ""
    parts: list[str] = []
    for f in spec_fields:
        if not isinstance(f.transform, IdentityTransform):
            continue
        if f.name in partition:
            v = partition[f.name]
            value = "null" if v is None else str(v)
            parts.append(f"{quote_plus(f.name, safe='')}={quote_plus(value, safe='')}")
    return "/".join(parts)


def _has_only_identity_transforms(partition: dict, spec_fields) -> bool:
    """True iff every spec field referenced by *partition* is Identity."""
    spec_by_name = {f.name: f for f in spec_fields}
    for name in partition:
        spec_field = spec_by_name.get(name)
        if spec_field is None or not isinstance(spec_field.transform, IdentityTransform):
            return False
    return True


def _flush_buffer(
    buffer: list[pa.RecordBatch],
    *,
    output_path: str,
    sort_keys: list[tuple[str, str]] | None,
) -> int:
    """Write *buffer* to *output_path* as a single Parquet file.

    Returns the number of rows written. Resetting the caller's buffer
    via ``buffer.clear()`` is the caller's responsibility.
    """
    table = pa.Table.from_batches(buffer)
    if sort_keys:
        table = table.sort_by(sort_keys)
    pq.write_table(table, output_path)
    return int(table.num_rows)


def _remove_staged_files(paths: list[str]) -> None:
    """Best-effort removal of staged Parquet files that were never committed."""
    for p in paths:
        try:
            os.remove(p)
        except OSError as exc:
            logger.debug("Failed to clean up staged file %s: %s", p, exc)


def compact_small_files_streaming(
    table: Table,
    *,
    target_size_mb: int = 512,
    min_file_count: int = 5,
    max_table_size_bytes: int | None = None,
    sort_by_iceberg_order: bool = True,
    dry_run: bool = False,
) -> CompactionResult:
    """
    Partition-aware compaction of small data files.

    Reads each qualifying partition through a PyArrow
    ``RecordBatchReader``, writes pre-staged Parquet files of roughly
    ``target_size_mb``, then swaps them in with a single PyIceberg
    transaction.

    Algorithm
    ---------
    1. Read ``inspect.files()`` to collect, per partition, the list
       of small files (size < ``target_size_mb``) and total bytes.
    2. For each partition with at least ``min_file_count`` small files:

       a. Open ``table.scan(row_filter=<partition predicate>)
          .to_arrow_batch_reader()``.
       b. Buffer batches until ``buffer.nbytes >= target_size_mb``.
       c. (Optional) Pre-sort the buffer via ``table.sort_order()`` if
          ``sort_by_iceberg_order=True`` and the table has a sort
          order.
       d. Flush each full buffer to a NEW Parquet file at
          ``<table.location>/data/<partition_dir>/_iceberg_ops_compacted_<uuid>_<seq>.parquet``.
          ``<partition_dir>`` escapes names and values the way
          PyIceberg does (``region=a%2Fb``), so every staged file
          stays under ``<table.location>/data``.

    3. **Swap** - build one PyIceberg transaction: for each touched
       partition, ``tx.overwrite(empty_df, overwrite_filter=<partition
       predicate>)`` removes the old files; ``tx.add_files(<staged
       URIs>)`` registers the new ones; then commit. The commit writes
       ``2N + 1`` snapshots for ``N`` touched partitions (a delete and
       an empty append per partition, then one append), which
       incremental readers see as new data.

    Failure outcomes
    ----------------
    * Staging or transaction building fails: nothing was committed;
      the staged files are deleted and the error is re-raised.
    * The catalog rejects the commit (``CommitFailedException`` or
      ``ValidationException``): nothing was committed; the staged
      files are deleted and the error is re-raised.
    * Any other commit error (for example ``CommitStateUnknownException``
      or a timeout): the commit may have landed, so the staged files
      are kept, an error is logged, and the error is re-raised. Refresh
      the table and verify; if the commit did not land,
      :func:`~iceberg_ops.maintenance.remove_orphan_files` reclaims the
      files once they are older than its ``older_than_hours``.

    Parameters
    ----------
    table : Table
        The Iceberg table to compact.
    target_size_mb : int, optional
        Files smaller than this are considered "small". Each compacted
        Parquet buffer also caps at this size in memory. Default
        ``512``.
    min_file_count : int, optional
        A partition is only compacted when it has at least this many
        small files. Default ``5``.
    max_table_size_bytes : int or None, optional
        Refuse to run when the total data-file size exceeds this value.
        Default ``None`` (no limit). Useful as a guard in scheduled jobs
        that touch unfamiliar tables.
    sort_by_iceberg_order : bool, optional
        If True (default), pre-sort each output buffer by the table's
        sort order before writing - matches the behaviour S3 Tables
        and Snowflake apply automatically.
    dry_run : bool, optional
        If True, report what would happen without writing or
        committing. Default ``False``.

    Returns
    -------
    CompactionResult
        Statistics about the compaction.

    Raises
    ------
    ValueError
        If ``target_size_mb`` is not positive, or a staging directory
        would resolve outside ``<table.location>/data`` (staged files
        are removed).
    RuntimeError
        If ``max_table_size_bytes`` is set and exceeded.
    NotImplementedError
        If the table's location uses a non-local scheme (``s3://``
        etc). Plain paths, including Windows drive-letter paths, are
        local.
    pyiceberg.exceptions.CommitFailedException
        If the catalog rejects the commit (staged files are removed).
    pyiceberg.exceptions.ValidationException
        If a concurrent commit added files to a partition being
        compacted (staged files are removed).

    Notes
    -----
    Only Identity-transformed partition fields are supported.
    Partitions using Bucket / Truncate / Year / Month / Day / Hour
    transforms are skipped (with a warning log) because their partition
    struct values cannot be inverted to a row predicate without
    scanning all data. Qualifying partitions are rewritten whole, and
    output files use PyArrow's default codec rather than the table's
    configured codec. A concurrent append to a partition being
    compacted fails the run with ``ValidationException`` and the staged
    files are removed. PyIceberg retries the commit after appends to
    other partitions; those appends are preserved and counted in
    ``files_after``. Delete files and v3 deletion vectors are out of
    scope.

    Peak memory is not bounded by ``target_size_mb``: PyIceberg's batch
    reader decodes whole source files (apache/iceberg-python#2407), so
    memory grows with the number and size of files per partition.
    """
    if target_size_mb <= 0:
        raise ValueError(f"target_size_mb must be positive, got {target_size_mb!r}")

    table.refresh()

    location = table.location()
    parsed = urlparse(location)
    if not _is_drive_path(location) and parsed.scheme not in ("", "file"):
        raise NotImplementedError(
            f"Streaming compaction for '{parsed.scheme}://' is not yet supported. "
            f"Only local filesystems (file://) are currently implemented."
        )

    threshold_bytes = target_size_mb * 1024 * 1024
    spec_fields = list(table.spec().fields)
    schema = table.schema()
    sort_order_fields = list(table.sort_order().fields)

    sort_keys: list[tuple[str, str]] | None = None
    if sort_by_iceberg_order and sort_order_fields:
        sort_keys = []
        for f in sort_order_fields:
            col_name = schema.find_field(f.source_id).name
            direction = "ascending" if f.direction.name == "ASC" else "descending"
            sort_keys.append((col_name, direction))

    # Pass 1: stream the inspect tables, group small files by partition.
    total_files = 0
    total_bytes = 0
    small_files_by_partition: dict[tuple[tuple[str, object], ...], list[dict[str, object]]] = {}

    for rec in _iter_file_records(table):
        total_files += 1
        size = int(rec["size_bytes"])  # type: ignore[arg-type]
        total_bytes += size
        if size < threshold_bytes:
            partition = rec["partition"] or {}
            key = _partition_key(partition)  # type: ignore[arg-type]
            small_files_by_partition.setdefault(key, []).append(rec)

    qualifying: list[tuple[tuple[tuple[str, object], ...], list[dict[str, object]]]] = []
    for key, files in small_files_by_partition.items():
        if len(files) < min_file_count:
            continue
        # All Identity? (skip non-identity for now.)
        sample_partition = files[0]["partition"] or {}
        if not _has_only_identity_transforms(sample_partition, spec_fields):  # type: ignore[arg-type]
            logger.warning(
                "Skipping partition %s in '%s': non-Identity transforms not yet "
                "supported in streaming compaction.",
                sample_partition,
                table.name(),
            )
            continue
        qualifying.append((key, files))

    files_rewritten = sum(len(f) for _, f in qualifying)

    if not qualifying:
        logger.info(
            "Streaming compaction skipped for '%s': no partition met min_file_count=%d.",
            table.name(),
            min_file_count,
        )
        return CompactionResult(
            files_before=total_files,
            files_after=total_files,
            bytes_before=total_bytes,
            bytes_after=total_bytes,
            files_rewritten=0,
            skipped=True,
        )

    if dry_run:
        logger.info(
            "Streaming compaction dry-run for '%s': would rewrite %d small files "
            "across %d partition(s).",
            table.name(),
            files_rewritten,
            len(qualifying),
        )
        return CompactionResult(
            files_before=total_files,
            files_after=total_files,
            bytes_before=total_bytes,
            bytes_after=total_bytes,
            files_rewritten=files_rewritten,
            skipped=False,
        )

    if max_table_size_bytes is not None and total_bytes > max_table_size_bytes:
        raise RuntimeError(
            f"compact_small_files_streaming refuses to run on a "
            f"{total_bytes / 1024**3:.1f} GB table "
            f"(limit: {max_table_size_bytes / 1024**3:.1f} GB). "
            "Pass max_table_size_bytes=None to disable the guard."
        )

    # Stage Parquet files. Track them so we can clean up on failure.
    job_uuid = uuid.uuid4().hex[:12]
    table_root_local = _file_uri_to_local_path(location)
    data_root_real = os.path.realpath(os.path.join(table_root_local, "data"))
    base_uri = location.rstrip("/")
    staged_local_paths: list[str] = []
    staged_uris: list[str] = []
    overwrite_filters: list[BooleanExpression] = []

    try:
        for part_idx, (_key, _files) in enumerate(qualifying):
            sample_partition = _files[0]["partition"] or {}
            partition_filter = _partition_predicate(
                sample_partition,  # type: ignore[arg-type]
                spec_fields,
                schema,
            )
            overwrite_filters.append(partition_filter)
            partition_dir = _partition_dir_segment(sample_partition, spec_fields)  # type: ignore[arg-type]

            local_dir = (
                os.path.join(table_root_local, "data", *partition_dir.split("/"))
                if partition_dir
                else os.path.join(table_root_local, "data")
            )
            try:
                inside = (
                    os.path.commonpath([data_root_real, os.path.realpath(local_dir)])
                    == data_root_real
                )
            except ValueError:  # different drives on Windows
                inside = False
            if not inside:
                raise ValueError(
                    f"Refusing to stage compacted files for partition {sample_partition!r}: "
                    "the staging directory resolves outside the table's data directory."
                )
            os.makedirs(local_dir, exist_ok=True)
            uri_dir = f"{base_uri}/data/{partition_dir}" if partition_dir else f"{base_uri}/data"

            reader = table.scan(row_filter=partition_filter).to_arrow_batch_reader()

            buffer: list[pa.RecordBatch] = []
            buffer_bytes = 0
            seq = 0

            for batch in reader:
                buffer.append(batch)
                buffer_bytes += batch.nbytes
                if buffer_bytes >= threshold_bytes:
                    fname = f"_iceberg_ops_compacted_{job_uuid}_{part_idx}_{seq}.parquet"
                    local_path = os.path.join(local_dir, fname)
                    _flush_buffer(buffer, output_path=local_path, sort_keys=sort_keys)
                    staged_local_paths.append(local_path)
                    staged_uris.append(f"{uri_dir}/{fname}")
                    buffer.clear()
                    buffer_bytes = 0
                    seq += 1

            if buffer:
                fname = f"_iceberg_ops_compacted_{job_uuid}_{part_idx}_{seq}.parquet"
                local_path = os.path.join(local_dir, fname)
                _flush_buffer(buffer, output_path=local_path, sort_keys=sort_keys)
                staged_local_paths.append(local_path)
                staged_uris.append(f"{uri_dir}/{fname}")

        if not staged_uris:
            # Edge case: every qualifying partition was empty after the
            # row filter (shouldn't happen if inspect.files() shows
            # them, but be safe). Treat as no-op skip.
            logger.info(
                "Streaming compaction for '%s': qualifying partitions yielded "
                "no rows; nothing to commit.",
                table.name(),
            )
            return CompactionResult(
                files_before=total_files,
                files_after=total_files,
                bytes_before=total_bytes,
                bytes_after=total_bytes,
                files_rewritten=0,
                skipped=True,
            )

        # Build the swap transaction. Nothing is committed until
        # commit_transaction() below.
        empty_df = schema.as_arrow().empty_table()
        tx = table.transaction()
        for predicate in overwrite_filters:
            tx.overwrite(empty_df, overwrite_filter=predicate)
        tx.add_files(staged_uris)

    except Exception:
        # Nothing was committed, so the staged files are unreferenced.
        _remove_staged_files(staged_local_paths)
        raise

    try:
        tx.commit_transaction()
    except (CommitFailedException, ValidationException):
        # The catalog definitively rejected the commit.
        _remove_staged_files(staged_local_paths)
        raise
    except Exception:
        # Unknown outcome (e.g. CommitStateUnknownException, timeout):
        # the commit may have landed and may reference the staged files,
        # so deleting them could make the table unreadable.
        logger.error(
            "Commit state unknown for '%s'; kept %d staged file(s) because the "
            "commit may have landed. Refresh and verify; if it did not land, "
            "remove_orphan_files reclaims them after older_than_hours.",
            table.name(),
            len(staged_local_paths),
        )
        raise

    table.refresh()

    new_total_files = 0
    new_total_bytes = 0
    for rec in _iter_file_records(table):
        new_total_files += 1
        new_total_bytes += int(rec["size_bytes"])  # type: ignore[arg-type]

    logger.info(
        "Streaming compaction for '%s': %d files (%d bytes) -> %d files (%d bytes); "
        "rewrote %d small file(s) across %d partition(s).",
        table.name(),
        total_files,
        total_bytes,
        new_total_files,
        new_total_bytes,
        files_rewritten,
        len(qualifying),
    )

    return CompactionResult(
        files_before=total_files,
        files_after=new_total_files,
        bytes_before=total_bytes,
        bytes_after=new_total_bytes,
        files_rewritten=files_rewritten,
    )
