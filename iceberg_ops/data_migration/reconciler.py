"""
iceberg_ops.data_migration.reconciler
=====================================
Schema reconciliation primitive for the cross-table data migrator.

A :class:`SchemaReconciler` declares *how* to map a source Iceberg
schema to a destination Iceberg schema - column renames, type
coercions, and policies for columns missing from one side. It produces
a :class:`ReconciliationPlan` with a per-dest-column
:class:`ColumnProjection`, and projects individual
:class:`pyarrow.RecordBatch` instances against that plan in
``O(rows x columns)`` memory.

The reconciler is intentionally *just* schema-level: it does not read
from the source table or write to the destination table. The
streaming run loop and the snapshot-pinned read live in
:mod:`iceberg_ops.data_migration.migrator`.

Workflow
--------
1. Caller resolves source and destination schemas (Arrow or Iceberg).
2. ``plan = reconciler.compute_plan(source_schema, dest_schema)`` -
   once.
3. For every batch off ``source.scan(...).to_arrow_batch_reader()``:
   ``out = reconciler.project_batch(batch, plan)``.
4. Caller writes ``out`` to the destination table via
   :meth:`Table.append` / :meth:`Table.upsert` /
   :meth:`Table.dynamic_partition_overwrite`.

Memory bound: ``project_batch`` allocates
``O(batch.num_rows x plan_columns)`` via
:func:`pyarrow.compute.cast` and :func:`pyarrow.nulls` - independent
of total source / destination size.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg import types as ice_types
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema

from iceberg_ops.core.types import arrow_type_to_iceberg, iceberg_type_to_arrow

logger = logging.getLogger(__name__)


# ===========================================================================
# Data classes
# ===========================================================================


@dataclass(frozen=True)
class ColumnProjection:
    """One column in the reconciled output.

    Each ``ColumnProjection`` describes how
    :meth:`SchemaReconciler.project_batch` derives a single destination
    column from a source :class:`pyarrow.RecordBatch`.

    Attributes
    ----------
    dest_name : str
        Column name in the destination schema.
    source_name : str or None
        Column name in the source schema. ``None`` when
        ``fill_with_null`` is ``True`` (i.e., the source has no
        matching column and the reconciler's policy is ``"null"``).
    dest_arrow_type : pyarrow.DataType
        Cast target. The source column is cast to this type via
        :func:`pyarrow.compute.cast`; null-fill columns are allocated
        directly in this type.
    fill_with_null : bool
        ``True`` when the column is missing from the source and the
        ``on_missing_in_source`` policy is ``"null"``. The projector
        emits ``pa.nulls(num_rows, type=dest_arrow_type)``.
    """

    dest_name: str
    source_name: str | None
    dest_arrow_type: pa.DataType
    fill_with_null: bool = False


@dataclass
class ReconciliationPlan:
    """Plan returned by :meth:`SchemaReconciler.compute_plan`.

    The plan is the entire static description of how to project
    source batches into destination-shape batches. It is computed
    once during pre-flight and then used by every batch in the
    streaming loop.

    Attributes
    ----------
    projections : list of ColumnProjection
        One entry per output column, in destination order. Source
        columns covered by no projection are dropped from the output
        (subject to the ``on_missing_in_dest`` policy at plan time).
    auto_add_columns : list of (str, IcebergType)
        Columns the source has that the destination does not, when
        ``on_missing_in_dest="auto_add"``. The data migrator is
        responsible for adding these to the destination table before
        invoking :meth:`SchemaReconciler.project_batch`.
    warnings : list of str
        Non-fatal issues surfaced during planning - e.g. doc-only
        differences, columns covered only by null-fill, type
        coercions that widen lossily.
    dest_arrow_schema : pyarrow.Schema
        The full destination Arrow schema (after auto-adds). Useful
        for asserting that the projected batches match what the
        writer expects.
    """

    projections: list[ColumnProjection] = field(default_factory=list)
    auto_add_columns: list[tuple[str, ice_types.IcebergType]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    dest_arrow_schema: pa.Schema = field(default_factory=lambda: pa.schema([]))


# ===========================================================================
# Schema-shape helpers
# ===========================================================================


def _normalise_to_arrow(
    schema: pa.Schema | IcebergSchema,
) -> tuple[pa.Schema, IcebergSchema | None]:
    """Return ``(arrow_schema, iceberg_schema_or_none)``.

    When the caller passes an :class:`IcebergSchema` we keep the
    original alongside the Arrow projection so that ``compute_plan``
    can reach back for the native Iceberg type during ``auto_add``.
    """
    if isinstance(schema, IcebergSchema):
        return schema_to_pyarrow(schema), schema
    if isinstance(schema, pa.Schema):
        return schema, None
    raise TypeError(
        f"Unsupported schema type {type(schema).__name__}: "
        "expected pa.Schema or pyiceberg.schema.Schema."
    )


def _iceberg_field_for(
    iceberg_schema: IcebergSchema | None,
    name: str,
) -> ice_types.IcebergType | None:
    """Return the Iceberg type of ``name`` if available, else ``None``."""
    if iceberg_schema is None:
        return None
    for f in iceberg_schema.fields:
        if f.name == name:
            return f.field_type
    return None


# ===========================================================================
# SchemaReconciler
# ===========================================================================


@dataclass
class SchemaReconciler:
    """Declarative source -> destination schema reconciliation policy.

    A reconciler is a *plan factory*: it does not hold per-batch
    state. Construct one, call :meth:`compute_plan` once, then reuse
    the resulting :class:`ReconciliationPlan` for every batch in the
    streaming migration loop.

    Parameters
    ----------
    column_mapping : dict[str, str], optional
        Source-name to destination-name renames. ``{"order_total":
        "total_amount"}`` means the source column ``order_total``
        lands in the destination as ``total_amount``. Columns not in
        this mapping are matched by exact name. The mapping is
        one-to-one; many-to-one or one-to-many fan-out belongs in a
        custom transformation layer above the reconciler. Every key
        must name a source column; ``compute_plan`` reports any other
        key as an error.
    type_coercions : dict[str, IcebergType], optional
        Per-destination-column override of the cast target. Keys are
        *destination* column names. Set this when the source's
        natural type differs from what you want at rest (e.g.
        ``"qty": LongType()`` widens an ``int32`` source to ``int64``
        at the destination). The override applies whether the
        destination already has the column or it is being auto-added.
        For a column the destination already has, the override must
        match that column's type or be a legal Iceberg promotion of
        it; otherwise the write fails at the first batch, not at plan
        time.
    on_missing_in_source : ``"error"`` | ``"skip"`` | ``"null"``
        Policy when a destination column has no matching source
        column (after mapping). ``"error"`` raises during
        ``compute_plan``; ``"null"`` emits a column of nulls in the
        destination type; ``"skip"`` drops the column from the
        projected output (the downstream writer must accept a partial
        schema, or the destination column must allow nulls and the
        writer must fill them). With ``DataMigrator`` upserts,
        ``"skip"`` is rejected at pre-flight (an upsert must write
        every destination column), and ``"null"`` overwrites the
        existing values of those columns with null for every matched
        key - the plan carries a warning saying so.
    on_missing_in_dest : ``"error"`` | ``"auto_add"``
        Policy when a source column has no matching destination
        column (after mapping). ``"error"`` raises during
        ``compute_plan``; ``"auto_add"`` records the column in
        :attr:`ReconciliationPlan.auto_add_columns`. The data
        migrator is then expected to extend the destination table
        before writing.

    Examples
    --------
    Same-schema migration (the trivial case):

    >>> reconciler = SchemaReconciler()
    >>> plan = reconciler.compute_plan(src.schema(), dst.schema())
    >>> for batch in src.scan().to_arrow_batch_reader():
    ...     out = reconciler.project_batch(batch, plan)
    ...     dst.append(pa.Table.from_batches([out]))

    Cross-schema migration with rename, type widening, and a missing
    column filled with nulls:

    >>> reconciler = SchemaReconciler(
    ...     column_mapping={"order_total": "total_amount"},
    ...     type_coercions={"qty": ice_types.LongType()},
    ...     on_missing_in_source="null",
    ...     on_missing_in_dest="error",
    ... )

    Notes
    -----
    The reconciler does not validate semantic compatibility beyond
    what :func:`pyarrow.compute.cast` enforces at projection time.
    Lossy casts (e.g. ``string`` -> ``int64`` with non-numeric values)
    raise inside ``project_batch`` rather than during
    ``compute_plan``; this is by design - many casts are
    data-dependent and cannot be decided statically.
    """

    column_mapping: dict[str, str] | None = None
    type_coercions: dict[str, ice_types.IcebergType] | None = None
    on_missing_in_source: Literal["error", "skip", "null"] = "error"
    on_missing_in_dest: Literal["error", "auto_add"] = "error"

    def __post_init__(self) -> None:
        # Validate policy literals up front so a user typo fails at
        # construction time, not on the first batch.
        if self.on_missing_in_source not in ("error", "skip", "null"):
            raise ValueError(
                f"on_missing_in_source must be 'error', 'skip', or 'null'; "
                f"got {self.on_missing_in_source!r}"
            )
        if self.on_missing_in_dest not in ("error", "auto_add"):
            raise ValueError(
                f"on_missing_in_dest must be 'error' or 'auto_add'; "
                f"got {self.on_missing_in_dest!r}"
            )

    # -----------------------------------------------------------------------
    # Plan computation
    # -----------------------------------------------------------------------

    def compute_plan(
        self,
        source_schema: pa.Schema | IcebergSchema,
        dest_schema: pa.Schema | IcebergSchema,
    ) -> ReconciliationPlan:
        """Compute the plan that turns ``source_schema`` into ``dest_schema``.

        The plan is computed once per migration. It validates the
        reconciler's policies eagerly and lists every per-column
        decision in :attr:`ReconciliationPlan.projections`.

        Parameters
        ----------
        source_schema : pyarrow.Schema or pyiceberg.schema.Schema
            Schema of the source Iceberg table.
        dest_schema : pyarrow.Schema or pyiceberg.schema.Schema
            Schema of the destination Iceberg table - *before* any
            ``auto_add`` columns are applied.

        Returns
        -------
        ReconciliationPlan

        Raises
        ------
        ValueError
            If ``on_missing_in_source="error"`` and at least one
            destination column has no source counterpart, or if
            ``on_missing_in_dest="error"`` and at least one source
            column has no destination counterpart. All such issues
            are collected and reported in a single error message.
        TypeError
            For unsupported schema kinds (neither :class:`pa.Schema`
            nor :class:`IcebergSchema`).
        """
        column_mapping = self.column_mapping or {}
        type_coercions = self.type_coercions or {}

        src_arrow, src_iceberg = _normalise_to_arrow(source_schema)
        dest_arrow, dest_iceberg = _normalise_to_arrow(dest_schema)

        # Build look-ups once.
        src_types: dict[str, pa.DataType] = {f.name: f.type for f in src_arrow}
        dest_types: dict[str, pa.DataType] = {f.name: f.type for f in dest_arrow}
        errors: list[str] = []
        # A mapping key that names no source column is a typo; it
        # would otherwise be ignored silently. Reported with the other
        # unresolved differences below.
        for key in column_mapping:
            if key not in src_types:
                errors.append(f"column_mapping key {key!r} is not a source column.")
        # Forward map: source column name -> destination column name.
        # Names not in ``column_mapping`` map to themselves.
        src_to_dest: dict[str, str] = {n: column_mapping.get(n, n) for n in src_types}
        # Reverse: destination column name -> source column that
        # feeds it. Built explicitly so a many-to-one mapping (two
        # source columns landing on one destination column) is
        # rejected; a target missing from the destination is handled
        # by the on_missing_in_dest policy below.
        dest_to_src: dict[str, str] = {}
        for src_name, dest_name in src_to_dest.items():
            if dest_name in dest_to_src:
                raise ValueError(
                    f"column_mapping is not one-to-one: both "
                    f"{dest_to_src[dest_name]!r} and {src_name!r} map "
                    f"to dest {dest_name!r}."
                )
            dest_to_src[dest_name] = src_name

        # Capture per-column nullability from the *destination* side -
        # the Arrow types alone don't carry this, but PyIceberg's
        # ``Table.append`` rejects a non-nullable field that arrives
        # as nullable. We propagate this into ``plan.dest_arrow_schema``
        # below so the writer accepts the projected batch as-is.
        dest_nullable: dict[str, bool] = {}
        if dest_iceberg is not None:
            for f in dest_iceberg.fields:
                dest_nullable[f.name] = not f.required
        else:
            for f in dest_arrow:
                dest_nullable[f.name] = f.nullable

        plan = ReconciliationPlan()

        # ------------------------------------------------------------------
        # 1. Determine auto-adds (source columns the dest doesn't have).
        # ------------------------------------------------------------------
        for src_name, src_type in src_types.items():
            mapped_dest_name = src_to_dest[src_name]
            if mapped_dest_name in dest_types:
                continue
            # Source column has no destination counterpart.
            if self.on_missing_in_dest == "error":
                if src_name in column_mapping:
                    errors.append(
                        f"source column {src_name!r} is mapped to "
                        f"{mapped_dest_name!r}, which does not exist "
                        "in the destination schema."
                    )
                else:
                    errors.append(
                        f"source column {src_name!r} has no destination "
                        "counterpart; pass on_missing_in_dest='auto_add' "
                        "or add a column_mapping entry."
                    )
                continue
            # auto_add path: choose the iceberg type for the new dest column.
            if mapped_dest_name in type_coercions:
                iceberg_type = type_coercions[mapped_dest_name]
            elif (
                src_iceberg is not None
                and (native := _iceberg_field_for(src_iceberg, src_name)) is not None
            ):
                iceberg_type = native
            else:
                iceberg_type = arrow_type_to_iceberg(src_type)
            plan.auto_add_columns.append((mapped_dest_name, iceberg_type))

        # ------------------------------------------------------------------
        # 2. Build projections for every dest column (incl. auto-adds).
        # ------------------------------------------------------------------
        # The "effective" dest schema is the original plus auto-adds, in
        # original-then-added order. Auto-adds get their Arrow type from
        # iceberg_type_to_arrow so the cast target round-trips through
        # PyIceberg's writer.
        effective_dest: list[tuple[str, pa.DataType]] = [(f.name, f.type) for f in dest_arrow]
        for name, iceberg_type in plan.auto_add_columns:
            effective_dest.append((name, iceberg_type_to_arrow(iceberg_type)))

        for dest_name, dest_arrow_type in effective_dest:
            # Apply type_coercions override even when the dest already
            # has the column - this is the explicit-type-override path.
            if dest_name in type_coercions:
                dest_arrow_type = iceberg_type_to_arrow(type_coercions[dest_name])

            mapped_src: str | None = dest_to_src.get(dest_name)
            if mapped_src is not None and mapped_src in src_types:
                plan.projections.append(
                    ColumnProjection(
                        dest_name=dest_name,
                        source_name=mapped_src,
                        dest_arrow_type=dest_arrow_type,
                        fill_with_null=False,
                    )
                )
                continue

            # Destination column has no source counterpart.
            policy = self.on_missing_in_source
            if policy == "error":
                errors.append(
                    f"destination column {dest_name!r} has no source "
                    "counterpart; pass on_missing_in_source='null' or "
                    "'skip', or add a column_mapping entry."
                )
            elif policy == "null":
                plan.projections.append(
                    ColumnProjection(
                        dest_name=dest_name,
                        source_name=None,
                        dest_arrow_type=dest_arrow_type,
                        fill_with_null=True,
                    )
                )
                plan.warnings.append(
                    f"destination column {dest_name!r} will be filled "
                    "with nulls (no source counterpart)."
                )
            else:  # "skip"
                plan.warnings.append(
                    f"destination column {dest_name!r} skipped (no source "
                    "counterpart, on_missing_in_source='skip')."
                )

        if errors:
            bullet = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"SchemaReconciler.compute_plan found {len(errors)} "
                f"unresolved difference(s):\n{bullet}"
            )

        # Newly auto-added Iceberg columns are optional by default -
        # the data migrator's auto-add path uses ``add_column(...)``
        # which always lands required=False.
        for name, _ in plan.auto_add_columns:
            dest_nullable.setdefault(name, True)

        # Build the published dest schema. Faithful nullability is
        # essential - PyIceberg's append-time schema check rejects a
        # required Iceberg field that arrives as nullable Arrow.
        plan.dest_arrow_schema = pa.schema(
            [
                pa.field(
                    p.dest_name,
                    p.dest_arrow_type,
                    nullable=dest_nullable.get(p.dest_name, True),
                )
                for p in plan.projections
            ]
        )

        # Type-promotion / lossy-cast warnings - surface only what we
        # can detect statically; data-dependent failures still raise
        # inside project_batch.
        for proj in plan.projections:
            if proj.fill_with_null or proj.source_name is None:
                continue
            src_type = src_types[proj.source_name]
            if src_type == proj.dest_arrow_type:
                continue
            if _is_lossy_cast(src_type, proj.dest_arrow_type):
                plan.warnings.append(
                    f"column {proj.dest_name!r}: cast "
                    f"{src_type} -> {proj.dest_arrow_type} may lose "
                    "precision."
                )

        # Avoid a guaranteed-cache-busting log on every plan build -
        # debug level keeps the line out of the default INFO stream.
        logger.debug(
            "SchemaReconciler.compute_plan: %d projection(s), %d auto-add(s), %d warning(s).",
            len(plan.projections),
            len(plan.auto_add_columns),
            len(plan.warnings),
        )
        return plan

    # -----------------------------------------------------------------------
    # Per-batch projection
    # -----------------------------------------------------------------------

    def project_batch(
        self,
        batch: pa.RecordBatch,
        plan: ReconciliationPlan,
    ) -> pa.RecordBatch:
        """Project a single source ``batch`` according to ``plan``.

        Each column in :attr:`ReconciliationPlan.projections` becomes
        one column in the returned batch. Source columns referenced
        by the plan are pulled by name and cast to the target type
        via :func:`pyarrow.compute.cast`. ``fill_with_null=True``
        projections are realised as ``pa.nulls(num_rows,
        type=dest_arrow_type)``.

        Parameters
        ----------
        batch : pyarrow.RecordBatch
            One batch from the source's
            ``scan(...).to_arrow_batch_reader()``.
        plan : ReconciliationPlan
            The plan from :meth:`compute_plan`. Must be produced by a
            ``compute_plan`` call whose ``source_schema`` was a
            superset of ``batch.schema``; otherwise the column lookup
            below raises :class:`KeyError`.

        Returns
        -------
        pyarrow.RecordBatch
            A batch whose schema matches ``plan.dest_arrow_schema``.
            ``num_rows`` equals ``batch.num_rows``.

        Raises
        ------
        KeyError
            If ``batch`` is missing a source column the plan
            references - typically a sign that ``compute_plan`` was
            called against a stale ``source_schema``.
        pyarrow.lib.ArrowInvalid
            If a cast fails on the actual data (e.g. string -> int
            with non-numeric values). This bubbles up unchanged.
        """
        arrays: list[pa.Array] = []
        for proj in plan.projections:
            if proj.fill_with_null:
                arr: pa.Array = pa.nulls(batch.num_rows, type=proj.dest_arrow_type)
            else:
                # source_name is non-None whenever fill_with_null is
                # False - guaranteed by compute_plan; assert
                # documents the contract.
                assert proj.source_name is not None
                src_arr = batch.column(proj.source_name)
                if src_arr.type == proj.dest_arrow_type:
                    arr = src_arr
                else:
                    arr = pc.cast(src_arr, proj.dest_arrow_type)
            arrays.append(arr)
        # Build the batch against ``plan.dest_arrow_schema`` so the
        # output carries the destination's nullable flags - required
        # for PyIceberg's append-time schema check on tables that
        # have any ``required`` columns.
        return pa.RecordBatch.from_arrays(arrays, schema=plan.dest_arrow_schema)


# ===========================================================================
# Cast-loss heuristic (private)
# ===========================================================================


def _is_lossy_cast(src: pa.DataType, dst: pa.DataType) -> bool:
    """Return ``True`` if casting ``src`` -> ``dst`` is *statically*
    lossy.

    Only the obvious cases - narrowing integer / float casts and
    timestamp -> date - are flagged. Everything else (e.g. string ->
    int) is data-dependent and surfaces at projection time.
    """
    int_ranks = {
        pa.int8(): 1,
        pa.int16(): 2,
        pa.int32(): 3,
        pa.int64(): 4,
    }
    float_ranks = {pa.float32(): 1, pa.float64(): 2}
    if src in int_ranks and dst in int_ranks:
        return int_ranks[src] > int_ranks[dst]
    if src in float_ranks and dst in float_ranks:
        return float_ranks[src] > float_ranks[dst]
    if pa.types.is_timestamp(src) and (pa.types.is_date32(dst) or pa.types.is_date64(dst)):
        return True
    return bool(
        pa.types.is_floating(src) and (pa.types.is_integer(dst) or pa.types.is_signed_integer(dst))
    )
