"""
iceberg_ops.core.utils
======================
General-purpose data utilities for PyIceberg.

Functions
---------
``create_arrow_table(schema, rows)``
    Build a PyArrow Table from a list of dicts using an explicit schema.

``upsert_streaming(table, source, *, join_cols, batch_rows=50_000)``
    Apply ``table.upsert()`` one bounded chunk at a time over a streaming
    source of PyArrow record batches.

``rollback_to_snapshot(table, snapshot_id)``
    Restore a table to an earlier Iceberg snapshot.

``get_snapshots(table)``
    Return all snapshot metadata as a list of dicts.

``print_time_travel(table)``
    Print every historical snapshot with its full data.

``trace_field_history(table, field_id)``
    Show how a single field evolved across all schema versions.
"""

from __future__ import annotations

import datetime
import importlib.util
import logging
from collections.abc import Iterable
from enum import Enum
from typing import Any

import pyarrow as pa
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform, VoidTransform


def _coerce_operation(raw: Any) -> str:
    """Return the snapshot operation as a plain string.

    PyIceberg stores ``snapshot.summary["operation"]`` as an
    ``Operation`` enum on read, but JSON-serialised manifests carry
    the bare string. Callers downstream (CLI tables, JSON exports)
    expect the string form, so we coerce here.
    """
    if isinstance(raw, Enum):
        return str(raw.value)
    return str(raw)


logger = logging.getLogger(__name__)


# ===========================================================================
# Write-path guards and upstream workarounds (private)
# ===========================================================================


# A composite-key upsert builds one ``Or(And(EqualTo, ...), ...)`` term
# per key; a few thousand keys overflow the native stack and kill the
# interpreter without a traceback (apache/iceberg-python#3508). Larger
# chunks are split into sub-batches of this many rows inside one
# transaction. Remove once apache/iceberg-python#3508 / #4027 ships.
_COMPOSITE_UPSERT_MAX_KEYS = 1_000


def _has_pyiceberg_core() -> bool:
    """Return ``True`` if the optional ``pyiceberg_core`` module is importable."""
    return importlib.util.find_spec("pyiceberg_core") is not None


def _require_transforms_extra(table: Table) -> None:
    """Fail early when writing *table* needs ``pyiceberg-core`` and it is missing.

    PyIceberg computes bucket, truncate, year, month, day and hour
    partition values through ``pyiceberg_core``. Without it the first
    write raises deep inside PyIceberg, possibly after earlier
    commits. Only the table's current default spec is checked.

    Parameters
    ----------
    table : Table
        Destination table about to be written.

    Raises
    ------
    ImportError
        If the spec has a non-identity, non-void transform and
        ``pyiceberg_core`` is not importable. The message names the
        ``iceberg-ops[transforms]`` extra.
    """
    needs = [
        f
        for f in table.spec().fields
        if not isinstance(f.transform, IdentityTransform | VoidTransform)
    ]
    if not needs or _has_pyiceberg_core():
        return
    names = ", ".join(f"{f.name}: {f.transform}" for f in needs)
    raise ImportError(
        f"Writing to {'.'.join(table.name())} needs pyiceberg-core: its partition "
        f"spec uses non-identity transforms ({names}). Install it with: "
        "pip install 'iceberg-ops[transforms]'"
    )


def _ensure_schema_snapshot(
    table: Table,
    snapshot_properties: dict[str, str] | None = None,
) -> bool:
    """Commit an empty append when the current snapshot predates the schema.

    Workaround for apache/iceberg-python#2467 / #3816: the first
    ``upsert`` after a schema-only change (for example
    ``add_column``) on a table that already has data fails with
    "Target schema's field names are not matching". An empty append
    creates a snapshot that carries the current schema ID, after
    which upserts work. Remove once the upstream fix ships.

    Parameters
    ----------
    table : Table
        Table about to be upserted.
    snapshot_properties : dict of str to str, optional
        Extra properties for the empty snapshot's summary.

    Returns
    -------
    bool
        ``True`` if an empty snapshot was committed; ``False`` if the
        table has no snapshot or its snapshot already uses the
        current schema.
    """
    snap = table.current_snapshot()
    if snap is None or snap.schema_id == table.metadata.current_schema_id:
        return False
    table.append(
        table.schema().as_arrow().empty_table(),
        snapshot_properties=snapshot_properties or {},
    )
    return True


def _upsert_chunk(
    table: Table,
    chunk: pa.Table,
    join_cols: list[str],
    snapshot_properties: dict[str, str] | None = None,
) -> tuple[int, int]:
    """Upsert one in-memory *chunk* as a single catalog commit.

    Composite-key chunks larger than ``_COMPOSITE_UPSERT_MAX_KEYS``
    rows are split into sub-batches inside one transaction, so the
    catalog still sees one commit (see apache/iceberg-python#3508).
    Duplicate keys are checked on the whole chunk first so the
    behaviour matches a single ``Table.upsert`` call.

    Parameters
    ----------
    table : Table
        Destination table.
    chunk : pa.Table
        Rows to upsert. Its schema must match the table.
    join_cols : list of str
        Key columns.
    snapshot_properties : dict of str to str, optional
        Extra properties for every snapshot the upsert creates.

    Returns
    -------
    tuple of (int, int)
        ``(rows_updated, rows_inserted)`` as reported by PyIceberg.

    Raises
    ------
    ValueError
        If *chunk* holds two rows with the same key (nothing is
        written), or for any error ``Table.upsert`` raises.
    """
    props = snapshot_properties or {}
    if len(join_cols) < 2 or chunk.num_rows <= _COMPOSITE_UPSERT_MAX_KEYS:
        res = table.upsert(chunk, join_cols=join_cols, snapshot_properties=props)
        return res.rows_updated, res.rows_inserted
    key_counts = chunk.select(join_cols).group_by(join_cols).aggregate([([], "count_all")])
    if key_counts.num_rows != chunk.num_rows:
        raise ValueError(
            "Duplicate rows found in source dataset based on the key columns. No upsert executed"
        )
    updated = inserted = 0
    with table.transaction() as tx:
        for offset in range(0, chunk.num_rows, _COMPOSITE_UPSERT_MAX_KEYS):
            res = tx.upsert(
                chunk.slice(offset, _COMPOSITE_UPSERT_MAX_KEYS),
                join_cols=join_cols,
                snapshot_properties=props,
            )
            updated += res.rows_updated
            inserted += res.rows_inserted
    return updated, inserted


# ===========================================================================
# Arrow / data helpers
# ===========================================================================


def create_arrow_table(schema: pa.Schema, rows: list[dict]) -> pa.Table:
    """
    Build a PyArrow Table from a list of row dicts using an explicit schema.

    Missing keys in a row dict are filled with ``None`` (null), so partial
    rows match the full table schema without raising ``KeyError``.

    Parameters
    ----------
    schema : pa.Schema
        The Arrow schema defining column names and types.
    rows : list[dict]
        Each dict represents one row. Unknown keys are ignored.

    Returns
    -------
    pa.Table

    Example
    -------
    ::

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.float64())])
        tbl    = create_arrow_table(schema, [{"id": 1, "v": 3.14}, {"id": 2}])
    """
    arrays = []
    for field in schema:
        col_data = [row.get(field.name, None) for row in rows]
        arrays.append(pa.array(col_data, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def upsert_streaming(
    table: Table,
    source: Iterable[pa.RecordBatch],
    *,
    join_cols: list[str],
    batch_rows: int = 50_000,
) -> int:
    """
    Apply ``table.upsert()`` one bounded chunk at a time.

    PyIceberg's :meth:`pyiceberg.table.Table.upsert` requires a fully
    materialised :class:`pyarrow.Table`. For pipelines whose incoming
    rowset is too large to hold in memory, this helper streams the source
    in record batches and flushes a chunk to ``upsert`` once at least
    *batch_rows* rows have been buffered.

    Memory: *batch_rows* bounds only the incoming-row buffer (roughly
    ``batch_rows x column count x column width``; any leftover after
    the loop is flushed once at the end). Each upsert also loads the
    destination rows that match the chunk's keys, so peak memory
    grows with the matched destination data as well.

    Each chunk is **one catalog commit**, which creates up to three
    Iceberg snapshots (delete or overwrite of the matched rows, the
    rewritten rows, the new rows). A composite-key chunk of more than
    1,000 rows is upserted in sub-batches inside that one commit
    (apache/iceberg-python#3508), with up to three snapshots per
    sub-batch. Pick a larger *batch_rows* for fewer commits, smaller
    for a smaller buffer.

    Duplicate keys: two rows with the same key in one chunk raise
    :class:`ValueError` (earlier chunks stay committed); across
    chunks the later row wins.

    If the table's current snapshot was written under an older schema
    (for example right after ``add_column``), one empty append is
    committed before the first chunk so the upsert can run - a
    workaround for apache/iceberg-python#2467.

    Parameters
    ----------
    table : Table
        Destination Iceberg table.
    source : iterable of pa.RecordBatch
        Streaming source of incoming rows. Accepts either a
        :class:`pyarrow.RecordBatchReader` or any Python iterator /
        generator that yields :class:`pyarrow.RecordBatch`.
    join_cols : list of str
        Equality columns for the upsert. Forwarded verbatim to
        ``table.upsert(join_cols=...)``.
    batch_rows : int, optional
        Buffer at least this many rows before each upsert call. Default
        50 000. Must be a positive integer.

    Returns
    -------
    int
        Total rows passed to ``table.upsert()``. Equals
        ``sum(b.num_rows for b in source)`` — note this counts incoming
        rows, not the number of distinct keys actually inserted vs
        updated.

    Raises
    ------
    ValueError
        If *batch_rows* is not a positive integer, or a chunk holds
        duplicate keys.
    ImportError
        If the table's partition spec uses a non-identity transform
        and ``pyiceberg-core`` is not installed (``pip install
        'iceberg-ops[transforms]'``). Raised before the source is read.

    Example
    -------
    ::

        import pyarrow.parquet as pq

        pf = pq.ParquetFile("incoming.parquet")
        n  = upsert_streaming(
            table,
            pf.iter_batches(batch_size=10_000),
            join_cols=["id"],
            batch_rows=50_000,
        )

    Notes
    -----
    All batches yielded by *source* must share a schema compatible with
    the destination table; :func:`pyarrow.Table.from_batches` enforces
    this and raises on mismatch.
    """
    if batch_rows <= 0:
        raise ValueError(f"batch_rows must be positive, got {batch_rows!r}")
    _require_transforms_extra(table)

    accum: list[pa.RecordBatch] = []
    accum_rows = 0
    total_rows = 0
    schema_stamped = False

    def _flush() -> None:
        nonlocal schema_stamped
        if not schema_stamped:
            _ensure_schema_snapshot(table)
            schema_stamped = True
        _upsert_chunk(table, pa.Table.from_batches(accum), join_cols)

    for batch in source:
        accum.append(batch)
        accum_rows += batch.num_rows
        if accum_rows >= batch_rows:
            _flush()
            total_rows += accum_rows
            accum.clear()
            accum_rows = 0

    if accum:
        _flush()
        total_rows += accum_rows

    logger.info(
        "upsert_streaming: %d rows applied to '%s' over chunks of >= %d rows",
        total_rows,
        table.name(),
        batch_rows,
    )
    return total_rows


# ===========================================================================
# Snapshot management
# ===========================================================================


def rollback_to_snapshot(table: Table, snapshot_id: int) -> None:
    """
    Roll back *table* to the state it was in at *snapshot_id*.

    Creates a new snapshot that points to the data files of the target
    historical snapshot. All snapshots created *after* the rollback point
    remain in the metadata log (for audit) but are no longer the current
    state.

    Parameters
    ----------
    table : Table
        The Iceberg table to roll back.
    snapshot_id : int
        The snapshot ID to restore. Obtain valid IDs via
        :func:`get_snapshots`.

    Raises
    ------
    ValueError
        If *snapshot_id* does not exist in this table's snapshot history.

    Example
    -------
    ::

        snaps = get_snapshots(table)
        rollback_to_snapshot(table, snaps[0]["snapshot_id"])
    """
    table.refresh()
    valid_ids = {s.snapshot_id for s in table.metadata.snapshots}
    if snapshot_id not in valid_ids:
        raise ValueError(
            f"Snapshot {snapshot_id} not found in table '{table.name()}'. "
            f"Valid IDs: {sorted(valid_ids)}"
        )

    with table.manage_snapshots() as mgr:
        mgr.rollback_to_snapshot(snapshot_id)

    logger.info("Rolled back table '%s' to snapshot %d.", table.name(), snapshot_id)


def get_snapshots(table: Table) -> list[dict[str, Any]]:
    """
    Return all snapshot metadata for *table* as a list of dicts.

    Each dict contains:

    ``snapshot_id`` : int
        Unique snapshot identifier.
    ``timestamp`` : str
        UTC datetime string in ISO 8601 form.
    ``schema_id`` : int
        Iceberg schema ID active at this snapshot.
    ``operation`` : str
        e.g. ``"append"``, ``"overwrite"``, ``"delete"``.

    Example
    -------
    ::

        for snap in get_snapshots(table):
            print(snap["snapshot_id"], snap["timestamp"], snap["operation"])
    """
    table.refresh()
    result = []
    for snap in table.metadata.snapshots:
        ts = datetime.datetime.fromtimestamp(snap.timestamp_ms / 1000.0, tz=datetime.UTC)
        result.append(
            {
                "snapshot_id": snap.snapshot_id,
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "schema_id": snap.schema_id,
                "operation": _coerce_operation(
                    (snap.summary or {}).get("operation", "unknown")  # type: ignore[call-overload]
                ),
            }
        )
    return result


# ===========================================================================
# Time-travel display
# ===========================================================================


def _print_arrow_table(tbl: pa.Table, indent: str = "  ") -> None:
    """Print a PyArrow Table in simple fixed-width columns."""
    if tbl.num_rows == 0:
        print(f"{indent}(empty)")
        return
    rows = tbl.to_pylist()
    cols = tbl.column_names
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "  ".join(f"{c:{widths[c]}}" for c in cols)
    print(f"{indent}{header}")
    print(f"{indent}" + "-" * len(header))
    for row in rows:
        print(f"{indent}" + "  ".join(f"{row.get(c, '')!s:{widths[c]}}" for c in cols))


def print_time_travel(table: Table) -> None:
    """
    Print every historical snapshot of *table* with its full data.

    Iterates all snapshots in chronological order, querying each one
    independently via PyIceberg's snapshot scan. Demonstrates that data
    from every past schema version is still accessible even after schema
    evolution.

    Parameters
    ----------
    table : Table
        The Iceberg table to inspect.
    """
    table.refresh()
    print("\n" + "=" * 64)
    print("TIME TRAVEL - All historical snapshots")
    print("=" * 64)

    for snap in table.metadata.snapshots:
        ts = datetime.datetime.fromtimestamp(snap.timestamp_ms / 1000.0)
        op = _coerce_operation(
            (snap.summary or {}).get("operation", "unknown")  # type: ignore[call-overload]
        )
        print(
            f"\n  Snapshot {snap.snapshot_id}"
            f"  |  {ts:%Y-%m-%d %H:%M:%S}"
            f"  |  schema_id={snap.schema_id}"
            f"  |  op={op}"
        )
        print("  " + "-" * 60)
        try:
            _print_arrow_table(table.scan(snapshot_id=snap.snapshot_id).to_arrow())
        except Exception as exc:
            print(f"  [ERROR] Cannot load snapshot: {exc}")

    print()


def trace_field_history(table: Table, field_id: int) -> None:
    """
    Trace how a single field evolved across all recorded schema versions.

    Iceberg assigns every field a **stable integer ID** that persists
    through renames and type changes. This function leverages that ID to
    show the complete lifecycle of a column — from its creation, through
    any renames, to its archival as a legacy column.

    Parameters
    ----------
    table : Table
        The Iceberg table whose schema history to inspect.
    field_id : int
        The stable Iceberg field ID to trace. Use
        ``table.schema().fields`` to find field IDs in the current
        schema.

    Example
    -------
    ::

        for f in table.schema().fields:
            print(f.field_id, f.name)

        trace_field_history(table, field_id=3)
    """
    table.refresh()
    print(f"\n{'=' * 64}")
    print(f"FIELD HISTORY - Iceberg field ID {field_id}")
    print("=" * 64)

    for schema in table.metadata.schemas:
        print(f"\n  Schema ID: {schema.schema_id}")
        try:
            f = schema.find_field(field_id)
            print(f"    name : {f.name}")
            print(f"    type : {f.field_type}")
            print(f"    doc  : {f.doc or '(none)'}")
        except ValueError:
            print(f"    -> Field ID {field_id} did not exist in this schema version.")

    print()
