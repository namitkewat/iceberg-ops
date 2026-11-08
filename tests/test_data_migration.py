"""
tests/test_data_migration.py
============================
Tests for :mod:`iceberg_ops.data_migration`.

Covers:

- :class:`SchemaReconciler` (compute_plan + project_batch + plan
  accessors)
- :class:`CheckpointStore` (lifecycle, persistence, atomicity,
  corruption handling)
- :class:`DataMigrator` (basic run, cross-catalog, schema
  reconciliation, write modes, dry-run, checkpoint resume, memory
  bound)
- :meth:`DataMigrator.verify` (row_count, partition_hash, sample)

All tests use real PyIceberg catalogs (memory_catalog / sqlite_catalog
fixtures); no mocking of Iceberg internals.
"""

from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg import types as ice_types
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField

from iceberg_ops.core.utils import rollback_to_snapshot
from iceberg_ops.data_migration import (
    CheckpointError,
    CheckpointStore,
    ColumnProjection,
    DataMigrator,
    MigrationResult,
    PartitionStrategy,
    ReconciliationPlan,
    SchemaReconciler,
    VerificationResult,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_arrow_src() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.float64()),
        ]
    )


@pytest.fixture
def simple_arrow_dest() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.large_string()),
            pa.field("amount", pa.float64()),
        ]
    )


@pytest.fixture
def simple_iceberg_src() -> IcebergSchema:
    return IcebergSchema(
        NestedField(1, "id", ice_types.LongType(), required=True),
        NestedField(2, "name", ice_types.StringType()),
        NestedField(3, "amount", ice_types.DoubleType()),
    )


@pytest.fixture
def simple_iceberg_dest() -> IcebergSchema:
    return IcebergSchema(
        NestedField(1, "id", ice_types.LongType(), required=True),
        NestedField(2, "name", ice_types.StringType()),
        NestedField(3, "amount", ice_types.DoubleType()),
    )


# ===========================================================================
# SchemaReconciler construction / validation
# ===========================================================================


class TestSchemaReconcilerInit:
    def test_defaults(self) -> None:
        r = SchemaReconciler()
        assert r.column_mapping is None
        assert r.type_coercions is None
        assert r.on_missing_in_source == "error"
        assert r.on_missing_in_dest == "error"

    def test_custom_values(self) -> None:
        r = SchemaReconciler(
            column_mapping={"a": "b"},
            type_coercions={"x": ice_types.LongType()},
            on_missing_in_source="null",
            on_missing_in_dest="auto_add",
        )
        assert r.column_mapping == {"a": "b"}
        assert r.on_missing_in_source == "null"
        assert r.on_missing_in_dest == "auto_add"

    def test_invalid_missing_in_source(self) -> None:
        with pytest.raises(ValueError, match="on_missing_in_source"):
            SchemaReconciler(on_missing_in_source="ignore")  # type: ignore[arg-type]

    def test_invalid_missing_in_dest(self) -> None:
        with pytest.raises(ValueError, match="on_missing_in_dest"):
            SchemaReconciler(on_missing_in_dest="silent")  # type: ignore[arg-type]


class TestComputePlanIdentity:
    def test_arrow_arrow_identity(self, simple_arrow_src: pa.Schema) -> None:
        r = SchemaReconciler()
        plan = r.compute_plan(simple_arrow_src, simple_arrow_src)
        assert len(plan.projections) == 3
        assert [p.dest_name for p in plan.projections] == ["id", "name", "amount"]
        assert all(p.source_name == p.dest_name for p in plan.projections)
        assert all(not p.fill_with_null for p in plan.projections)
        assert plan.auto_add_columns == []
        assert plan.warnings == []

    def test_iceberg_iceberg_identity(self, simple_iceberg_src: IcebergSchema) -> None:
        r = SchemaReconciler()
        plan = r.compute_plan(simple_iceberg_src, simple_iceberg_src)
        assert len(plan.projections) == 3
        assert plan.auto_add_columns == []
        assert plan.warnings == []

    def test_mixed_arrow_iceberg_inputs(
        self,
        simple_arrow_src: pa.Schema,
        simple_iceberg_dest: IcebergSchema,
    ) -> None:
        # arrow string maps onto iceberg StringType (which is large_string in arrow)
        r = SchemaReconciler()
        plan = r.compute_plan(simple_arrow_src, simple_iceberg_dest)
        assert len(plan.projections) == 3
        # dest's name column is iceberg StringType -> arrow large_string,
        # so the cast target should be large_string.
        name_proj = next(p for p in plan.projections if p.dest_name == "name")
        assert pa.types.is_large_string(name_proj.dest_arrow_type)

    def test_unsupported_schema_type_raises(self) -> None:
        r = SchemaReconciler()
        with pytest.raises(TypeError, match="Unsupported schema type"):
            r.compute_plan({"id": "long"}, {"id": "long"})  # type: ignore[arg-type]


class TestComputePlanRename:
    def test_simple_rename(self) -> None:
        src = pa.schema([pa.field("order_total", pa.float64())])
        dst = pa.schema([pa.field("total_amount", pa.float64())])
        r = SchemaReconciler(column_mapping={"order_total": "total_amount"})
        plan = r.compute_plan(src, dst)
        assert len(plan.projections) == 1
        proj = plan.projections[0]
        assert proj.dest_name == "total_amount"
        assert proj.source_name == "order_total"

    def test_rename_with_passthrough_columns(self) -> None:
        src = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("order_total", pa.float64()),
                pa.field("name", pa.string()),
            ]
        )
        dst = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("total_amount", pa.float64()),
                pa.field("name", pa.string()),
            ]
        )
        r = SchemaReconciler(column_mapping={"order_total": "total_amount"})
        plan = r.compute_plan(src, dst)
        sources = {p.dest_name: p.source_name for p in plan.projections}
        assert sources == {"id": "id", "total_amount": "order_total", "name": "name"}

    def test_one_to_many_mapping_raises(self) -> None:
        src = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.int64())])
        dst = pa.schema([pa.field("c", pa.int64())])
        r = SchemaReconciler(column_mapping={"a": "c", "b": "c"})
        with pytest.raises(ValueError, match="not one-to-one"):
            r.compute_plan(src, dst)

    def test_mapping_to_nonexistent_dest_errors(self) -> None:
        src = pa.schema([pa.field("x", pa.int64())])
        dst = pa.schema([pa.field("y", pa.int64())])
        r = SchemaReconciler(column_mapping={"x": "z"}, on_missing_in_source="null")
        # x is mapped to z but z doesn't exist in dest; on_missing_in_dest
        # defaults to error so this should raise.
        with pytest.raises(ValueError, match="z"):
            r.compute_plan(src, dst)


class TestComputePlanTypeCoercion:
    def test_widening_cast(self) -> None:
        src = pa.schema([pa.field("qty", pa.int32())])
        dst = pa.schema([pa.field("qty", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        proj = plan.projections[0]
        assert proj.dest_arrow_type == pa.int64()
        assert plan.warnings == []

    def test_explicit_coercion_overrides_dest_type(self) -> None:
        # dest already says int64 - but user wants string for some reason.
        src = pa.schema([pa.field("qty", pa.int32())])
        dst = pa.schema([pa.field("qty", pa.int64())])
        r = SchemaReconciler(type_coercions={"qty": ice_types.StringType()})
        plan = r.compute_plan(src, dst)
        # Cast target should be the override (large_string per pyiceberg).
        proj = plan.projections[0]
        assert pa.types.is_large_string(proj.dest_arrow_type)

    def test_decimal_precision_change(self) -> None:
        src = pa.schema([pa.field("price", pa.decimal128(8, 2))])
        dst = pa.schema([pa.field("price", pa.decimal128(10, 2))])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        assert plan.projections[0].dest_arrow_type == pa.decimal128(10, 2)


class TestComputePlanMissingInSource:
    def test_error_lists_all_missing(self) -> None:
        src = pa.schema([pa.field("a", pa.int64())])
        dst = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("missing1", pa.string()),
                pa.field("missing2", pa.float64()),
            ]
        )
        r = SchemaReconciler()
        with pytest.raises(ValueError) as excinfo:
            r.compute_plan(src, dst)
        msg = str(excinfo.value)
        assert "missing1" in msg
        assert "missing2" in msg
        # Should report 2 unresolved differences.
        assert "2 unresolved difference(s)" in msg

    def test_null_emits_fill_projection(self) -> None:
        src = pa.schema([pa.field("a", pa.int64())])
        dst = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("notes", pa.string()),
            ]
        )
        r = SchemaReconciler(on_missing_in_source="null")
        plan = r.compute_plan(src, dst)
        notes_proj = next(p for p in plan.projections if p.dest_name == "notes")
        assert notes_proj.fill_with_null is True
        assert notes_proj.source_name is None
        assert any("notes" in w for w in plan.warnings)

    def test_skip_omits_dest_column_from_projections(self) -> None:
        src = pa.schema([pa.field("a", pa.int64())])
        dst = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("dropped", pa.string()),
            ]
        )
        r = SchemaReconciler(on_missing_in_source="skip")
        plan = r.compute_plan(src, dst)
        dest_names = [p.dest_name for p in plan.projections]
        assert dest_names == ["a"]
        assert any("dropped" in w for w in plan.warnings)


class TestComputePlanMissingInDest:
    def test_error_lists_all_extras(self) -> None:
        src = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("extra1", pa.string()),
                pa.field("extra2", pa.float64()),
            ]
        )
        dst = pa.schema([pa.field("a", pa.int64())])
        r = SchemaReconciler()
        with pytest.raises(ValueError) as excinfo:
            r.compute_plan(src, dst)
        msg = str(excinfo.value)
        assert "extra1" in msg
        assert "extra2" in msg

    def test_auto_add_records_columns_and_projects(self) -> None:
        src = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("new_col", pa.string()),
            ]
        )
        dst = pa.schema([pa.field("a", pa.int64())])
        r = SchemaReconciler(on_missing_in_dest="auto_add")
        plan = r.compute_plan(src, dst)
        assert len(plan.auto_add_columns) == 1
        name, ice_type = plan.auto_add_columns[0]
        assert name == "new_col"
        assert isinstance(ice_type, ice_types.StringType)
        # Projection for the auto-added column lives at the end.
        assert plan.projections[-1].dest_name == "new_col"
        assert plan.projections[-1].source_name == "new_col"

    def test_auto_add_uses_iceberg_native_type_when_available(
        self,
        simple_iceberg_src: IcebergSchema,
    ) -> None:
        # source as iceberg schema -> auto_add should use the native type
        # (LongType, not the int64 -> LongType inference).
        dst = pa.schema([pa.field("name", pa.string())])
        r = SchemaReconciler(on_missing_in_dest="auto_add", on_missing_in_source="null")
        plan = r.compute_plan(simple_iceberg_src, dst)
        added = dict(plan.auto_add_columns)
        assert isinstance(added["id"], ice_types.LongType)
        assert isinstance(added["amount"], ice_types.DoubleType)

    def test_auto_add_with_coercion_uses_override(self) -> None:
        src = pa.schema([pa.field("qty", pa.int32())])
        dst = pa.schema([])
        r = SchemaReconciler(
            type_coercions={"qty": ice_types.LongType()},
            on_missing_in_dest="auto_add",
        )
        plan = r.compute_plan(src, dst)
        assert plan.auto_add_columns == [("qty", ice_types.LongType())]
        # Cast target is int64 per the LongType coercion.
        assert plan.projections[0].dest_arrow_type == pa.int64()

    def test_mapping_with_auto_add_renames(self) -> None:
        src = pa.schema([pa.field("legacy_col", pa.int64())])
        dst = pa.schema([])
        r = SchemaReconciler(
            column_mapping={"legacy_col": "renamed"},
            on_missing_in_dest="auto_add",
        )
        plan = r.compute_plan(src, dst)
        assert plan.auto_add_columns[0][0] == "renamed"
        proj = plan.projections[0]
        assert proj.dest_name == "renamed"
        assert proj.source_name == "legacy_col"


class TestComputePlanWarnings:
    def test_narrowing_int_warns(self) -> None:
        src = pa.schema([pa.field("x", pa.int64())])
        dst = pa.schema([pa.field("x", pa.int32())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        assert any("may lose precision" in w for w in plan.warnings)

    def test_widening_int_no_warn(self) -> None:
        src = pa.schema([pa.field("x", pa.int32())])
        dst = pa.schema([pa.field("x", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        assert not any("may lose precision" in w for w in plan.warnings)

    def test_float_to_int_warns(self) -> None:
        src = pa.schema([pa.field("x", pa.float64())])
        dst = pa.schema([pa.field("x", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        assert any("may lose precision" in w for w in plan.warnings)


class TestProjectBatchBasic:
    def test_identity_round_trip(self) -> None:
        src = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, src)
        batch = pa.record_batch([pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"])
        out = r.project_batch(batch, plan)
        assert out.schema == src
        assert out.num_rows == 2
        assert out.column("id").to_pylist() == [1, 2]
        assert out.column("name").to_pylist() == ["a", "b"]

    def test_rename_and_widen(self) -> None:
        src = pa.schema(
            [
                pa.field("order_total", pa.float32()),
                pa.field("qty", pa.int32()),
            ]
        )
        dst = pa.schema(
            [
                pa.field("total_amount", pa.float64()),
                pa.field("qty", pa.int64()),
            ]
        )
        r = SchemaReconciler(column_mapping={"order_total": "total_amount"})
        plan = r.compute_plan(src, dst)
        batch = pa.record_batch(
            [
                pa.array([1.5, 2.5], type=pa.float32()),
                pa.array([10, 20], type=pa.int32()),
            ],
            names=["order_total", "qty"],
        )
        out = r.project_batch(batch, plan)
        assert out.column_names == ["total_amount", "qty"]
        assert out.column("total_amount").type == pa.float64()
        assert out.column("qty").type == pa.int64()
        assert out.column("total_amount").to_pylist() == [1.5, 2.5]
        assert out.column("qty").to_pylist() == [10, 20]

    def test_null_fill(self) -> None:
        src = pa.schema([pa.field("a", pa.int64())])
        dst = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("notes", pa.string()),
            ]
        )
        r = SchemaReconciler(on_missing_in_source="null")
        plan = r.compute_plan(src, dst)
        batch = pa.record_batch([pa.array([1, 2, 3])], names=["a"])
        out = r.project_batch(batch, plan)
        assert out.num_rows == 3
        assert out.column("notes").to_pylist() == [None, None, None]
        assert out.column("a").to_pylist() == [1, 2, 3]

    def test_skip_drops_dest_column(self) -> None:
        src = pa.schema([pa.field("a", pa.int64())])
        dst = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("dropped", pa.string()),
            ]
        )
        r = SchemaReconciler(on_missing_in_source="skip")
        plan = r.compute_plan(src, dst)
        batch = pa.record_batch([pa.array([1, 2])], names=["a"])
        out = r.project_batch(batch, plan)
        assert out.column_names == ["a"]
        assert "dropped" not in out.column_names

    def test_empty_batch(self) -> None:
        src = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        dst = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        # 0-row batch
        batch = pa.record_batch(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            names=["a", "b"],
        )
        out = r.project_batch(batch, plan)
        assert out.num_rows == 0
        assert out.column_names == ["a", "b"]


class TestProjectBatchEdge:
    def test_missing_source_column_raises(self) -> None:
        src = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        dst = src
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        # Hand-craft a batch missing column "b".
        partial = pa.record_batch([pa.array([1, 2])], names=["a"])
        with pytest.raises(KeyError):
            r.project_batch(partial, plan)

    def test_lossy_cast_data_dependent_raises(self) -> None:
        src = pa.schema([pa.field("x", pa.string())])
        dst = pa.schema([pa.field("x", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        batch = pa.record_batch([pa.array(["abc"])], names=["x"])
        with pytest.raises((pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError)):
            r.project_batch(batch, plan)

    def test_multi_batch_consistency(self) -> None:
        # Two consecutive batches with the same plan must produce
        # batches with identical schemas.
        src = pa.schema([pa.field("a", pa.int32())])
        dst = pa.schema([pa.field("a", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        b1 = pa.record_batch([pa.array([1, 2], type=pa.int32())], names=["a"])
        b2 = pa.record_batch([pa.array([3], type=pa.int32())], names=["a"])
        out1 = r.project_batch(b1, plan)
        out2 = r.project_batch(b2, plan)
        assert out1.schema == out2.schema
        assert out1.num_rows == 2
        assert out2.num_rows == 1


class TestPlanAccessors:
    def test_dest_arrow_schema_matches_projections(self) -> None:
        src = pa.schema([pa.field("a", pa.int32())])
        dst = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        r = SchemaReconciler(on_missing_in_source="null")
        plan = r.compute_plan(src, dst)
        # dest_arrow_schema reflects projections (a:int64, b:string)
        assert [f.name for f in plan.dest_arrow_schema] == ["a", "b"]
        assert plan.dest_arrow_schema.field("a").type == pa.int64()

    def test_column_projection_is_frozen(self) -> None:
        from dataclasses import FrozenInstanceError

        proj = ColumnProjection(
            dest_name="x",
            source_name="x",
            dest_arrow_type=pa.int64(),
        )
        with pytest.raises(FrozenInstanceError):
            proj.dest_name = "y"  # type: ignore[misc]

    def test_reconciliation_plan_is_dataclass(self) -> None:
        plan = ReconciliationPlan()
        assert plan.projections == []
        assert plan.auto_add_columns == []
        assert plan.warnings == []


# ===========================================================================
# End-to-end with a real Iceberg catalog
# ===========================================================================


class TestEndToEndWithCatalog:
    """Round-trip through real PyIceberg tables - proves the projection
    output is accepted by ``Table.append`` without further coercion."""

    def test_round_trip_same_schema(self, memory_catalog) -> None:
        memory_catalog.create_namespace_if_not_exists("dm")
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "name", ice_types.StringType()),
        )
        src = memory_catalog.create_table("dm.src", src_schema)
        dst = memory_catalog.create_table("dm.dst", src_schema)

        src.append(
            pa.Table.from_pydict(
                {"id": [1, 2, 3], "name": ["a", "b", "c"]},
                schema=src.schema().as_arrow(),
            )
        )

        r = SchemaReconciler()
        plan = r.compute_plan(src.schema(), dst.schema())
        for batch in src.scan().to_arrow_batch_reader():
            out = r.project_batch(batch, plan)
            dst.append(pa.Table.from_batches([out], schema=plan.dest_arrow_schema))

        dst_rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert dst_rows == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]

    def test_round_trip_rename_and_widen(self, memory_catalog) -> None:
        memory_catalog.create_namespace_if_not_exists("dm")
        src_schema = IcebergSchema(
            NestedField(1, "order_total", ice_types.FloatType()),
            NestedField(2, "qty", ice_types.IntegerType()),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "total_amount", ice_types.DoubleType()),
            NestedField(2, "qty", ice_types.LongType()),
        )
        src = memory_catalog.create_table("dm.src2", src_schema)
        dst = memory_catalog.create_table("dm.dst2", dst_schema)
        src.append(
            pa.Table.from_pydict(
                {"order_total": [1.5, 2.5], "qty": [10, 20]},
                schema=src.schema().as_arrow(),
            )
        )

        r = SchemaReconciler(
            column_mapping={"order_total": "total_amount"},
            type_coercions={"qty": ice_types.LongType()},
        )
        plan = r.compute_plan(src.schema(), dst.schema())
        for batch in src.scan().to_arrow_batch_reader():
            out = r.project_batch(batch, plan)
            dst.append(pa.Table.from_batches([out], schema=plan.dest_arrow_schema))

        dst_rows = dst.scan().to_arrow().sort_by("qty").to_pylist()
        # float32 -> float64 casts may pick up float32 representation noise;
        # compare with tolerance.
        assert len(dst_rows) == 2
        assert dst_rows[0]["qty"] == 10
        assert dst_rows[1]["qty"] == 20
        assert dst_rows[0]["total_amount"] == pytest.approx(1.5, rel=1e-6)
        assert dst_rows[1]["total_amount"] == pytest.approx(2.5, rel=1e-6)

    def test_round_trip_null_fill(self, memory_catalog) -> None:
        memory_catalog.create_namespace_if_not_exists("dm")
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "notes", ice_types.StringType()),
        )
        src = memory_catalog.create_table("dm.src3", src_schema)
        dst = memory_catalog.create_table("dm.dst3", dst_schema)
        src.append(pa.Table.from_pydict({"id": [1, 2]}, schema=src.schema().as_arrow()))

        r = SchemaReconciler(on_missing_in_source="null")
        plan = r.compute_plan(src.schema(), dst.schema())
        for batch in src.scan().to_arrow_batch_reader():
            out = r.project_batch(batch, plan)
            dst.append(pa.Table.from_batches([out], schema=plan.dest_arrow_schema))

        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert rows == [
            {"id": 1, "notes": None},
            {"id": 2, "notes": None},
        ]

    def test_streaming_batches_consistent(self, memory_catalog) -> None:
        # Drives multi-batch reading - proves project_batch's output
        # schema is stable across reader iterations.
        memory_catalog.create_namespace_if_not_exists("dm")
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
        )
        src = memory_catalog.create_table("dm.src4", src_schema)
        dst = memory_catalog.create_table("dm.dst4", src_schema)
        # Three appends -> typically three batches when scanned.
        for chunk in ([1, 2], [3, 4], [5]):
            src.append(pa.Table.from_pydict({"id": chunk}, schema=src.schema().as_arrow()))

        r = SchemaReconciler()
        plan = r.compute_plan(src.schema(), dst.schema())
        seen_schemas: set[bytes] = set()
        for batch in src.scan().to_arrow_batch_reader():
            out = r.project_batch(batch, plan)
            seen_schemas.add(out.schema.serialize().to_pybytes())
            dst.append(pa.Table.from_batches([out], schema=plan.dest_arrow_schema))

        # Every batch produced the same schema.
        assert len(seen_schemas) == 1
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]


class TestProjectBatchPyArrowSemantics:
    """Cross-checks that the projector's casts agree with bare
    ``pyarrow.compute`` output. Catches regressions if we ever
    short-circuit casts incorrectly."""

    def test_cast_matches_compute_cast(self) -> None:
        src = pa.schema([pa.field("x", pa.int32())])
        dst = pa.schema([pa.field("x", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        batch = pa.record_batch([pa.array([1, 2, 3, 4, 5], type=pa.int32())], names=["x"])
        out = r.project_batch(batch, plan)
        expected = pc.cast(batch.column("x"), pa.int64())
        assert out.column("x").equals(expected)

    def test_no_cast_returns_same_array(self) -> None:
        # When source type matches dest type exactly, no cast is needed.
        src = pa.schema([pa.field("x", pa.int64())])
        dst = pa.schema([pa.field("x", pa.int64())])
        r = SchemaReconciler()
        plan = r.compute_plan(src, dst)
        arr = pa.array([1, 2, 3], type=pa.int64())
        batch = pa.record_batch([arr], names=["x"])
        out = r.project_batch(batch, plan)
        # Same buffer references => no copy happened.
        assert out.column("x").equals(arr)


# ===========================================================================
# CheckpointStore
# ===========================================================================


class TestCheckpointStoreInit:
    def test_construction_does_not_touch_disk(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        # Just constructing the store must not create any file.
        CheckpointStore(cp_path)
        assert not cp_path.exists()
        assert list(tmp_path.iterdir()) == []

    def test_exists_false_before_start(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        assert store.exists() is False

    def test_last_committed_none_before_start(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        assert store.last_committed() is None

    def test_is_committed_false_before_start(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        assert store.is_committed(0) is False
        assert store.is_committed(99999) is False

    def test_path_property(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        assert store.path == cp_path

    def test_accepts_string_or_path(self, tmp_path: Path) -> None:
        as_str = CheckpointStore(str(tmp_path / "a.json"))
        as_path = CheckpointStore(tmp_path / "b.json")
        assert isinstance(as_str.path, Path)
        assert isinstance(as_path.path, Path)


class TestCheckpointStoreStart:
    def test_start_creates_file(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        assert cp_path.exists()
        assert store.exists() is True

    def test_start_records_source_snapshot_id(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start(source_snapshot_id=12345)
        assert store.source_snapshot_id == 12345

    def test_start_without_snapshot_id_is_none(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        assert store.source_snapshot_id is None

    def test_start_records_started_at_iso(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        ts = store.started_at
        assert ts is not None
        # ISO 8601 UTC: YYYY-MM-DDTHH:MM:SSZ
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts)

    def test_start_overwrites_previous_state(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start(source_snapshot_id=1)
        store.mark_committed(0, 100)
        store.mark_committed(1, 200)
        # Re-start should clear progress.
        store.start(source_snapshot_id=2)
        assert store.last_committed() is None
        assert store.dest_snapshot_ids == []
        assert store.source_snapshot_id == 2
        assert store.completed is False

    def test_start_creates_parent_directory(self, tmp_path: Path) -> None:
        # Nested dir that doesn't yet exist.
        cp_path = tmp_path / "deep" / "nested" / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        assert cp_path.exists()


class TestCheckpointStoreCommit:
    def test_mark_and_read_back(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(0, 100)
        assert store.last_committed() == 0
        assert store.dest_snapshot_ids == [100]

    def test_multiple_commits_append(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        for idx, snap in enumerate([100, 200, 300, 400]):
            store.mark_committed(idx, snap)
        assert store.last_committed() == 3
        assert store.dest_snapshot_ids == [100, 200, 300, 400]

    def test_is_committed_at_and_below(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(0, 100)
        store.mark_committed(1, 200)
        store.mark_committed(2, 300)
        assert store.is_committed(0) is True
        assert store.is_committed(1) is True
        assert store.is_committed(2) is True
        assert store.is_committed(3) is False
        assert store.is_committed(99) is False

    def test_out_of_order_commit_raises(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(5, 500)
        # Out-of-order - must advance.
        with pytest.raises(ValueError, match="strictly greater"):
            store.mark_committed(3, 300)
        # Same idx - must advance.
        with pytest.raises(ValueError, match="strictly greater"):
            store.mark_committed(5, 501)

    def test_mark_committed_lazy_creates_file(self, tmp_path: Path) -> None:
        # No start() - mark_committed should still work and create the file.
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.mark_committed(0, 100)
        assert cp_path.exists()
        assert store.last_committed() == 0
        assert store.dest_snapshot_ids == [100]

    def test_non_zero_first_batch_idx(self, tmp_path: Path) -> None:
        # Streaming loops are zero-based but allow skipping
        # (e.g. resuming partway). First commit can be any non-negative int.
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(7, 700)
        assert store.last_committed() == 7
        assert store.is_committed(0) is True  # everything <= 7 considered committed
        assert store.is_committed(7) is True
        assert store.is_committed(8) is False


class TestCheckpointStorePersistence:
    def test_state_round_trips_across_instances(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        s1 = CheckpointStore(cp_path)
        s1.start(source_snapshot_id=999)
        s1.mark_committed(0, 100)
        s1.mark_committed(1, 200)
        # Brand new instance pointing at the same file.
        s2 = CheckpointStore(cp_path)
        assert s2.last_committed() == 1
        assert s2.dest_snapshot_ids == [100, 200]
        assert s2.source_snapshot_id == 999
        assert s2.is_committed(1) is True

    def test_resume_continues_from_last_committed(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        s1 = CheckpointStore(cp_path)
        s1.start()
        s1.mark_committed(0, 100)
        s1.mark_committed(1, 200)
        # Simulated crash -> new run.
        s2 = CheckpointStore(cp_path)
        # mark_committed(2) must advance past last_committed=1.
        s2.mark_committed(2, 300)
        assert s2.last_committed() == 2
        assert s2.dest_snapshot_ids == [100, 200, 300]

    def test_each_commit_is_persisted_immediately(self, tmp_path: Path) -> None:
        """Crash between batches -> on-disk state has the most recent commit."""
        cp_path = tmp_path / "cp.json"
        s = CheckpointStore(cp_path)
        s.start()
        s.mark_committed(0, 100)
        # Read raw JSON without going through CheckpointStore - proves
        # persistence is immediate, not buffered.
        raw = json.loads(cp_path.read_text())
        assert raw["last_committed_batch"] == 0
        assert raw["dest_snapshot_ids"] == [100]


class TestCheckpointStoreCompleted:
    def test_mark_completed_flips_flag(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(0, 100)
        assert store.completed is False
        store.mark_completed()
        assert store.completed is True

    def test_completed_persists_across_instances(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        s1 = CheckpointStore(cp_path)
        s1.start()
        s1.mark_committed(0, 100)
        s1.mark_completed()
        s2 = CheckpointStore(cp_path)
        assert s2.completed is True

    def test_file_remains_after_completion(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        store.mark_completed()
        # Plan: file remains so the user can audit dest_snapshot_ids.
        assert cp_path.exists()


class TestCheckpointStoreReset:
    def test_reset_removes_file(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        store.mark_committed(0, 100)
        assert cp_path.exists()
        store.reset()
        assert not cp_path.exists()

    def test_reset_clears_in_memory_state(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(0, 100)
        store.reset()
        assert store.last_committed() is None
        assert store.dest_snapshot_ids == []

    def test_reset_idempotent_when_no_file(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        # Should not raise even though no file exists.
        store.reset()
        store.reset()
        assert store.exists() is False

    def test_can_restart_after_reset(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start(source_snapshot_id=1)
        store.mark_committed(0, 100)
        store.reset()
        # Fresh start.
        store.start(source_snapshot_id=2)
        store.mark_committed(0, 200)
        assert store.last_committed() == 0
        assert store.dest_snapshot_ids == [200]
        assert store.source_snapshot_id == 2

    def test_reset_removes_leftover_tmp(self, tmp_path: Path) -> None:
        # Simulate a crash that left a stale .tmp file behind.
        cp_path = tmp_path / "cp.json"
        tmp_file = cp_path.with_suffix(".json.tmp")
        tmp_file.write_text("partial garbage")
        assert tmp_file.exists()
        store = CheckpointStore(cp_path)
        store.reset()
        assert not tmp_file.exists()


class TestCheckpointStoreCorruption:
    def test_invalid_json_raises_clear_error(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        cp_path.write_text("{not valid json")
        store = CheckpointStore(cp_path)
        with pytest.raises(CheckpointError, match="not valid JSON"):
            store.last_committed()

    def test_top_level_array_raises(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        cp_path.write_text("[1, 2, 3]")
        store = CheckpointStore(cp_path)
        with pytest.raises(CheckpointError, match="expected object"):
            store.last_committed()

    def test_unsupported_version_raises(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        cp_path.write_text(
            json.dumps(
                {
                    "version": 999,
                    "source_snapshot_id": None,
                    "last_committed_batch": None,
                    "dest_snapshot_ids": [],
                    "completed": False,
                    "started_at": "2026-01-01T00:00:00Z",
                }
            )
        )
        store = CheckpointStore(cp_path)
        with pytest.raises(CheckpointError, match="version"):
            store.last_committed()

    def test_missing_required_key_raises(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        # Has correct version but missing dest_snapshot_ids.
        cp_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source_snapshot_id": None,
                    "last_committed_batch": 0,
                    "completed": False,
                    "started_at": "2026-01-01T00:00:00Z",
                }
            )
        )
        store = CheckpointStore(cp_path)
        with pytest.raises(CheckpointError, match="missing required key"):
            store.last_committed()

    def test_corrupt_file_can_be_reset_and_reused(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        cp_path.write_text("garbage")
        store = CheckpointStore(cp_path)
        # Corrupt -> reset wipes it without trying to load.
        store.reset()
        assert not cp_path.exists()
        # Now usable.
        store.start()
        store.mark_committed(0, 100)
        assert store.last_committed() == 0


class TestCheckpointStoreAtomicity:
    def test_no_lingering_tmp_file_after_write(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        store.mark_committed(0, 100)
        # No <name>.tmp left behind on a successful write.
        tmp_file = cp_path.with_suffix(".json.tmp")
        assert not tmp_file.exists()

    def test_file_layout_matches_documented_format(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start(source_snapshot_id=42)
        store.mark_committed(0, 100)
        store.mark_committed(1, 200)
        raw = json.loads(cp_path.read_text())
        assert raw["version"] == 1
        assert raw["source_snapshot_id"] == 42
        assert raw["last_committed_batch"] == 1
        assert raw["dest_snapshot_ids"] == [100, 200]
        assert raw["completed"] is False
        assert "started_at" in raw
        assert raw["job"] is None

    def test_writes_are_pretty_printed_and_sorted(self, tmp_path: Path) -> None:
        # Pretty-printed JSON is easier to diff post-mortem and sorted
        # keys keep the on-disk layout stable across runs.
        cp_path = tmp_path / "cp.json"
        store = CheckpointStore(cp_path)
        store.start()
        text = cp_path.read_text()
        # Pretty-printed: at least one newline.
        assert "\n" in text
        # Sorted: completed comes before version alphabetically.
        completed_pos = text.index('"completed"')
        version_pos = text.index('"version"')
        assert completed_pos < version_pos


class TestCheckpointStoreLazyLoading:
    def test_accessor_lazy_loads(self, tmp_path: Path) -> None:
        # Two stores against the same path; the second one only reads
        # when an accessor is called.
        cp_path = tmp_path / "cp.json"
        s1 = CheckpointStore(cp_path)
        s1.start(source_snapshot_id=42)
        s1.mark_committed(0, 100)

        s2 = CheckpointStore(cp_path)
        # No load yet - the constructor doesn't touch disk.
        # Accessor triggers load.
        assert s2.source_snapshot_id == 42

    def test_state_cached_after_first_load(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cp_path = tmp_path / "cp.json"
        s = CheckpointStore(cp_path)
        s.start()
        s.mark_committed(0, 100)

        # Build a fresh store, force a load via one accessor, then
        # delete the file out from under it. Subsequent accessors must
        # still work - proves state was cached in memory.
        s2 = CheckpointStore(cp_path)
        assert s2.last_committed() == 0
        cp_path.unlink()
        # Cached state still serves these reads.
        assert s2.last_committed() == 0
        assert s2.dest_snapshot_ids == [100]


class TestCheckpointStoreJob:
    def test_checkpoint_error_is_runtime_error(self) -> None:
        # Callers (the CLI among them) that catch RuntimeError also
        # catch checkpoint refusals.
        assert issubclass(CheckpointError, RuntimeError)

    def test_start_with_job_round_trips(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        job = {"batch_rows": 5, "reconciler": {"column_mapping": {"a": "b"}}, "keys": ["id"]}
        CheckpointStore(cp_path).start(source_snapshot_id=7, job=job)
        reloaded = CheckpointStore(cp_path)
        assert reloaded.job == job
        assert reloaded.source_snapshot_id == 7

    def test_job_property_returns_a_copy(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start(job={"batch_rows": 5})
        got = store.job
        assert got is not None
        got["batch_rows"] = 99
        assert store.job == {"batch_rows": 5}

    def test_file_without_job_loads_with_none(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "cp.json"
        cp_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source_snapshot_id": None,
                    "last_committed_batch": 0,
                    "dest_snapshot_ids": [1],
                    "completed": False,
                    "started_at": "2026-01-01T00:00:00Z",
                }
            )
        )
        store = CheckpointStore(cp_path)
        assert store.job is None
        assert store.last_committed() == 0

    def test_mark_committed_accepts_list(self, tmp_path: Path) -> None:
        store = CheckpointStore(tmp_path / "cp.json")
        store.start()
        store.mark_committed(0, [10, 11, 12])
        store.mark_committed(1, [])
        store.mark_committed(2, 20)
        assert store.dest_snapshot_ids == [10, 11, 12, 20]
        assert store.last_committed() == 2

    def test_writes_are_fsynced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from iceberg_ops.data_migration import checkpoint as checkpoint_module

        calls: list[int] = []
        real_fsync = checkpoint_module.os.fsync

        def _record(fd: int) -> None:
            calls.append(fd)
            real_fsync(fd)

        monkeypatch.setattr(checkpoint_module.os, "fsync", _record)
        CheckpointStore(tmp_path / "cp.json").start()
        assert len(calls) >= 1


# ===========================================================================
# DataMigrator
# ===========================================================================


def _make_table(catalog, ns: str, name: str, schema: IcebergSchema):
    catalog.create_namespace_if_not_exists(ns)
    return catalog.create_table(f"{ns}.{name}", schema)


_BASIC_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "name", ice_types.StringType()),
    NestedField(3, "amount", ice_types.DoubleType()),
)


def _ids(table) -> list[int]:
    """Return the table's ``id`` column, refreshed and sorted."""
    table.refresh()
    return sorted(table.scan(selected_fields=("id",)).to_arrow().column("id").to_pylist())


def _seed(table, n_rows: int, *, start: int = 0) -> None:
    """Append *n_rows* rows starting at id=*start*."""
    arrow_schema = table.schema().as_arrow()
    table.append(
        pa.Table.from_pydict(
            {
                "id": list(range(start, start + n_rows)),
                "name": [f"row_{i}" for i in range(start, start + n_rows)],
                "amount": [float(i) for i in range(start, start + n_rows)],
            },
            schema=arrow_schema,
        )
    )


class TestDataMigratorConstruction:
    def test_invalid_batch_rows(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_ic", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_ic", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="batch_rows"):
            DataMigrator(src, dst, batch_rows=0)
        with pytest.raises(ValueError, match="batch_rows"):
            DataMigrator(src, dst, batch_rows=-1)

    def test_invalid_write_mode(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_iw", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_iw", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="write_mode"):
            DataMigrator(src, dst, write_mode="append_only")  # type: ignore[arg-type]

    def test_upsert_requires_keys(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_iu", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_iu", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="upsert_keys"):
            DataMigrator(src, dst, write_mode="upsert")
        with pytest.raises(ValueError, match="upsert_keys"):
            DataMigrator(src, dst, write_mode="upsert", upsert_keys=[])

    def test_unsupported_partition_strategy(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_ip", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_ip", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="partition_strategy"):
            DataMigrator(src, dst, partition_strategy="flatten")  # type: ignore[arg-type]

    def test_same_table_rejected(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_same", _BASIC_SCHEMA)
        _seed(src, 3)
        snapshot_before = src.current_snapshot()
        with pytest.raises(ValueError, match="same table"):
            DataMigrator(src, src)
        # A second handle on the same table is the same table too.
        with pytest.raises(ValueError, match="same table"):
            DataMigrator(src, memory_catalog.load_table("dm.src_same"))
        src.refresh()
        assert src.current_snapshot() == snapshot_before
        assert _ids(src) == [0, 1, 2]


class TestDataMigratorBasic:
    def test_same_schema_same_catalog(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_a", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_a", _BASIC_SCHEMA)
        _seed(src, 10)

        result = DataMigrator(src, dst, batch_rows=4).run()

        assert result.rows_moved == 10
        assert result.bytes_moved > 0
        assert result.dry_run is False
        # 10 rows / 4 per batch -> 3 commits (4, 4, 2)
        assert len(result.new_snapshots) == 3
        assert result.checkpoint_path is None
        assert result.duration_seconds > 0

        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == list(range(10))

    def test_empty_source_is_noop(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_b", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_b", _BASIC_SCHEMA)
        # No data appended.
        result = DataMigrator(src, dst).run()
        assert result.rows_moved == 0
        assert result.new_snapshots == []
        assert dst.scan().to_arrow().num_rows == 0

    def test_single_batch_smaller_than_batch_rows(self, memory_catalog) -> None:
        # 3 rows but batch_rows=100 - one commit, all rows.
        src = _make_table(memory_catalog, "dm", "src_c", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_c", _BASIC_SCHEMA)
        _seed(src, 3)
        result = DataMigrator(src, dst, batch_rows=100).run()
        assert result.rows_moved == 3
        assert len(result.new_snapshots) == 1

    def test_batch_rows_slices_oversized_source_batch(self, memory_catalog) -> None:
        # One source batch of 7 rows split into batch_rows=2 -> 4 commits.
        src = _make_table(memory_catalog, "dm", "src_d", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_d", _BASIC_SCHEMA)
        _seed(src, 7)
        result = DataMigrator(src, dst, batch_rows=2).run()
        assert result.rows_moved == 7
        # 7 rows / 2 per batch -> 4 commits (2, 2, 2, 1)
        assert len(result.new_snapshots) == 4

    def test_progress_callback_fires_per_commit(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_e", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_e", _BASIC_SCHEMA)
        _seed(src, 6)
        seen: list[tuple[int, int | None]] = []

        DataMigrator(
            src,
            dst,
            batch_rows=2,
            progress=lambda done, total: seen.append((done, total)),
        ).run()

        # 6 rows / 2 per batch -> 3 commits -> 3 callbacks.
        assert len(seen) == 3
        assert [d for d, _ in seen] == [2, 4, 6]
        assert all(t is None for _, t in seen)


class TestDataMigratorCrossCatalog:
    def test_cross_catalog(self, memory_catalog, sqlite_catalog) -> None:
        # source = memory, dest = sqlite
        src = _make_table(memory_catalog, "dm", "src_x", _BASIC_SCHEMA)
        dst = _make_table(sqlite_catalog, "dm", "dst_x", _BASIC_SCHEMA)
        _seed(src, 5)

        result = DataMigrator(src, dst, batch_rows=10).run()
        assert result.rows_moved == 5
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == list(range(5))


class TestDataMigratorSchema:
    def test_with_column_mapping(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "order_total", ice_types.DoubleType()),
            NestedField(2, "qty", ice_types.LongType()),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "total_amount", ice_types.DoubleType()),
            NestedField(2, "qty", ice_types.LongType()),
        )
        src = _make_table(memory_catalog, "dm", "src_m", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_m", dst_schema)
        src.append(
            pa.Table.from_pydict(
                {"order_total": [1.5, 2.5, 3.5], "qty": [10, 20, 30]},
                schema=src.schema().as_arrow(),
            )
        )

        result = DataMigrator(
            src,
            dst,
            schema_reconciler=SchemaReconciler(column_mapping={"order_total": "total_amount"}),
        ).run()

        assert result.rows_moved == 3
        rows = dst.scan().to_arrow().sort_by("qty").to_pylist()
        assert rows == [
            {"total_amount": 1.5, "qty": 10},
            {"total_amount": 2.5, "qty": 20},
            {"total_amount": 3.5, "qty": 30},
        ]

    def test_with_type_coercion(self, memory_catalog) -> None:
        src_schema = IcebergSchema(NestedField(1, "qty", ice_types.IntegerType()))
        dst_schema = IcebergSchema(NestedField(1, "qty", ice_types.LongType()))
        src = _make_table(memory_catalog, "dm", "src_t", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_t", dst_schema)
        src.append(pa.Table.from_pydict({"qty": [100, 200, 300]}, schema=src.schema().as_arrow()))

        result = DataMigrator(src, dst).run()

        assert result.rows_moved == 3
        rows = dst.scan().to_arrow().sort_by("qty").to_pylist()
        assert rows == [{"qty": 100}, {"qty": 200}, {"qty": 300}]

    def test_auto_add_extends_dest_schema(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "extra", ice_types.StringType()),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
        )
        src = _make_table(memory_catalog, "dm", "src_aa", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_aa", dst_schema)
        src.append(
            pa.Table.from_pydict(
                {"id": [1, 2], "extra": ["a", "b"]},
                schema=src.schema().as_arrow(),
            )
        )

        result = DataMigrator(
            src,
            dst,
            schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
        ).run()

        assert result.schema_changes == ["extra"]
        assert result.rows_moved == 2
        # Dest now has the new column.
        dst.refresh()
        assert "extra" in [f.name for f in dst.schema().fields]
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert rows == [{"id": 1, "extra": "a"}, {"id": 2, "extra": "b"}]

    def test_strict_reconciler_raises_on_mismatch(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "extra_only_in_src", ice_types.LongType()),
        )
        dst_schema = IcebergSchema(NestedField(1, "id", ice_types.LongType()))
        src = _make_table(memory_catalog, "dm", "src_strict", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_strict", dst_schema)
        with pytest.raises(ValueError, match="unresolved difference"):
            DataMigrator(src, dst).run()


class TestDataMigratorScanOptions:
    def test_with_row_filter(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_rf", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rf", _BASIC_SCHEMA)
        _seed(src, 10)
        # Filter: only ids >= 5.
        result = DataMigrator(src, dst, row_filter="id >= 5").run()
        assert result.rows_moved == 5
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == [5, 6, 7, 8, 9]

    def test_with_source_snapshot_pinned(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_pin", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_pin", _BASIC_SCHEMA)
        _seed(src, 3)  # snapshot A: rows 0, 1, 2
        src.refresh()
        snap_a = src.current_snapshot().snapshot_id  # type: ignore[union-attr]
        _seed(src, 3, start=10)  # snapshot B: adds rows 10, 11, 12

        # Pin to snapshot A - should only see the first 3 rows.
        result = DataMigrator(src, dst, source_snapshot=snap_a).run()
        assert result.rows_moved == 3
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == [0, 1, 2]


class TestDataMigratorWriteModes:
    def test_append_default(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_app", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_app", _BASIC_SCHEMA)
        _seed(src, 3)
        # Pre-existing dest data - append must not replace it.
        _seed(dst, 2, start=100)
        DataMigrator(src, dst).run()
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == [0, 1, 2, 100, 101]

    def test_upsert_replaces_matching_keys(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_up", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_up", _BASIC_SCHEMA)
        # Seed dest with rows that will be replaced.
        dst.append(
            pa.Table.from_pydict(
                {"id": [1, 2, 3], "name": ["old1", "old2", "old3"], "amount": [0.0, 0.0, 0.0]},
                schema=dst.schema().as_arrow(),
            )
        )
        # Source has new values for ids 2, 3 plus a new id 4.
        src.append(
            pa.Table.from_pydict(
                {"id": [2, 3, 4], "name": ["new2", "new3", "new4"], "amount": [2.0, 3.0, 4.0]},
                schema=src.schema().as_arrow(),
            )
        )
        DataMigrator(src, dst, write_mode="upsert", upsert_keys=["id"]).run()
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        names = {r["id"]: r["name"] for r in rows}
        assert names == {1: "old1", 2: "new2", 3: "new3", 4: "new4"}


class TestDataMigratorDryRun:
    def test_dry_run_writes_nothing(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_dr", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_dr", _BASIC_SCHEMA)
        _seed(src, 5)
        result = DataMigrator(src, dst, dry_run=True).run()
        assert result.dry_run is True
        assert result.rows_moved == 0
        assert result.bytes_moved == 0
        assert result.new_snapshots == []
        assert result.estimated_rows == 5
        assert result.estimated_bytes is not None and result.estimated_bytes > 0
        assert dst.scan().to_arrow().num_rows == 0

    def test_dry_run_does_not_apply_auto_add(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "extra", ice_types.StringType()),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
        )
        src = _make_table(memory_catalog, "dm", "src_drs", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_drs", dst_schema)
        src.append(
            pa.Table.from_pydict(
                {"id": [1], "extra": ["x"]},
                schema=src.schema().as_arrow(),
            )
        )
        result = DataMigrator(
            src,
            dst,
            dry_run=True,
            schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
        ).run()
        assert result.dry_run is True
        assert result.schema_changes == ["extra"]
        # Dest schema must NOT have "extra" - dry-run mustn't mutate.
        dst.refresh()
        assert "extra" not in [f.name for f in dst.schema().fields]


class _AppendErrorAfterN:
    """Wrap a Table-like object so ``append`` errors after the Nth call.

    Used by the resume test to simulate a crash mid-run without
    actually killing the test process. Forwards every other
    attribute to the wrapped table.
    """

    def __init__(self, inner, error_after: int) -> None:
        self._inner = inner
        self._error_after = error_after
        self._appends = 0

    def append(self, *args, **kwargs):
        self._appends += 1
        if self._appends > self._error_after:
            raise RuntimeError("simulated crash mid-run")
        return self._inner.append(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TestDataMigratorCheckpoint:
    def test_checkpoint_records_per_batch(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_cp", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_cp", _BASIC_SCHEMA)
        _seed(src, 6)
        cp = CheckpointStore(tmp_path / "cp.json")

        result = DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()

        assert result.rows_moved == 6
        # 3 commits of 2 rows each -> 3 dest_snapshot_ids in checkpoint.
        assert cp.last_committed() == 2
        assert len(cp.dest_snapshot_ids) == 3
        assert cp.completed is True
        assert result.checkpoint_path == cp.path

    def test_resume_skips_already_committed(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_re", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_re", _BASIC_SCHEMA)
        _seed(src, 6)
        cp = CheckpointStore(tmp_path / "cp.json")

        # First run: simulate a crash after 2 commits.
        wrapped = _AppendErrorAfterN(dst, error_after=2)
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(src, wrapped, batch_rows=2, checkpoint=cp).run()

        # Crash captured: dest has 4 rows, checkpoint marks batch_idx=1.
        dst.refresh()
        assert dst.scan().to_arrow().num_rows == 4
        assert cp.last_committed() == 1
        # Migration didn't complete.
        assert cp.completed is False

        # Resume: new DataMigrator pointed at same checkpoint.
        result = DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        # Only the third (final) batch should be written.
        assert result.rows_moved == 2
        # Total dest rows = 4 (from crash) + 2 (resumed) = 6.
        dst.refresh()
        rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        assert [r["id"] for r in rows] == list(range(6))
        # Checkpoint advanced + completed.
        assert cp.last_committed() == 2
        assert cp.completed is True

    def test_rerun_completed_checkpoint_raises(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_rn", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rn", _BASIC_SCHEMA)
        _seed(src, 4)
        cp = CheckpointStore(tmp_path / "cp.json")
        # First full run.
        DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert cp.completed is True
        # The source grows; re-running with the same checkpoint must
        # refuse rather than skip batches positionally.
        _seed(src, 4, start=100)
        with pytest.raises(CheckpointError, match="completed"):
            DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        dst.refresh()
        assert _ids(dst) == list(range(4))

        # After reset() the checkpoint path is reusable for a new job.
        cp.reset()
        fresh = _make_table(memory_catalog, "dm", "dst_rn2", _BASIC_SCHEMA)
        result = DataMigrator(src, fresh, batch_rows=2, checkpoint=cp).run()
        assert result.rows_moved == 8
        assert _ids(fresh) == [0, 1, 2, 3, 100, 101, 102, 103]


class TestDataMigratorMemoryBound:
    def test_batch_rows_slicing_keeps_python_heap_bounded(self, memory_catalog) -> None:
        """Python-heap allocations stay bounded when batch_rows slices
        a large source batch.

        ``tracemalloc`` sees only Python-heap allocations, not Arrow's
        memory pool, so this does not measure the migrator's peak
        memory (which grows with the source data files - see the
        module docstring). It only guards the slicing path against a
        regression that builds the whole table in Python objects.
        """
        import tracemalloc

        src = _make_table(memory_catalog, "dm", "src_mem", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_mem", _BASIC_SCHEMA)
        # Seed with 25_000 rows in one go - drives the slicing path.
        n = 25_000
        src.append(
            pa.Table.from_pydict(
                {
                    "id": list(range(n)),
                    "name": [f"r_{i}" for i in range(n)],
                    "amount": [float(i) for i in range(n)],
                },
                schema=src.schema().as_arrow(),
            )
        )

        tracemalloc.start()
        DataMigrator(src, dst, batch_rows=10_000).run()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Generous ceiling. PyIceberg's writer + pyarrow buffers eat
        # tens of MB; 128 MB still catches a regression that
        # materialises the entire 25k-row table at once.
        ceiling = 128 * 1024 * 1024
        assert peak < ceiling, (
            f"DataMigrator Python-heap peak {peak / 1024 / 1024:.1f} MB exceeds "
            f"{ceiling / 1024 / 1024:.0f} MB ceiling (tracemalloc; Arrow "
            "allocations are not measured) - likely a regression where source "
            "batches are no longer being sliced to batch_rows."
        )
        # Sanity - actual dest rows match expectation.
        dst.refresh()
        assert dst.scan().to_arrow().num_rows == n


class TestMigrationResult:
    def test_result_defaults(self) -> None:
        r = MigrationResult()
        assert r.rows_moved == 0
        assert r.bytes_moved == 0
        assert r.new_snapshots == []
        assert r.schema_changes == []
        assert r.duration_seconds == 0.0
        assert r.checkpoint_path is None
        assert r.dry_run is False
        assert r.estimated_rows is None
        assert r.estimated_bytes is None
        assert r.plan is None
        assert r.dest_snapshot_before is None
        assert r.rows_inserted is None
        assert r.rows_updated is None

    def test_partition_strategy_only_preserve(self) -> None:
        # The enum is exported; only PRESERVE is currently a member.
        assert PartitionStrategy.PRESERVE.value == "preserve"
        assert list(PartitionStrategy) == [PartitionStrategy.PRESERVE]

    def test_partition_strategy_is_str_enum(self) -> None:
        # PartitionStrategy is a StrEnum, so PRESERVE compares equal to
        # the bare string "preserve".
        assert PartitionStrategy.PRESERVE == "preserve"
        assert isinstance(PartitionStrategy.PRESERVE, str)


# ===========================================================================
# DataMigrator.verify
# ===========================================================================


class TestVerifyRowCount:
    def test_match_after_clean_migration(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vrm", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vrm", _BASIC_SCHEMA)
        _seed(src, 7)
        m = DataMigrator(src, dst)
        m.run()
        result = m.verify(method="row_count")
        assert isinstance(result, VerificationResult)
        assert result.method == "row_count"
        assert result.matches is True
        assert result.source_count == 7
        assert result.dest_count == 7
        assert result.details == []

    def test_mismatch_when_dest_has_extra_rows(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vmm", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vmm", _BASIC_SCHEMA)
        _seed(src, 5)
        m = DataMigrator(src, dst)
        m.run()
        # Append extra rows to dest after migration.
        dst.append(
            pa.Table.from_pydict(
                {"id": [99, 100], "name": ["x", "y"], "amount": [0.0, 0.0]},
                schema=dst.schema().as_arrow(),
            )
        )
        result = m.verify(method="row_count")
        assert result.matches is False
        assert result.source_count == 5
        assert result.dest_count == 7
        assert any("delta=2" in d for d in result.details)

    def test_with_row_filter_applied_to_source(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vrf", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vrf", _BASIC_SCHEMA)
        _seed(src, 10)
        # Migrate only ids >= 5, then verify with same filter.
        m = DataMigrator(src, dst, row_filter="id >= 5")
        m.run()
        result = m.verify(method="row_count")
        assert result.matches is True
        assert result.source_count == 5
        assert result.dest_count == 5

    def test_with_pinned_source_snapshot(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vps", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vps", _BASIC_SCHEMA)
        _seed(src, 3)
        src.refresh()
        snap_a = src.current_snapshot().snapshot_id  # type: ignore[union-attr]
        # Pin migrator to snap_a, migrate.
        m = DataMigrator(src, dst, source_snapshot=snap_a)
        m.run()
        # Append more to source AFTER migration.
        _seed(src, 5, start=100)
        # Verify still uses the pinned snapshot - should match.
        result = m.verify(method="row_count")
        assert result.matches is True
        assert result.source_count == 3
        assert result.dest_count == 3

    def test_empty_source_and_dest_match(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_ve", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_ve", _BASIC_SCHEMA)
        m = DataMigrator(src, dst)
        m.run()
        result = m.verify(method="row_count")
        assert result.matches is True
        assert result.source_count == 0
        assert result.dest_count == 0


class TestVerifyPartitionHashUnpartitioned:
    def test_match_full_content(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vph", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vph", _BASIC_SCHEMA)
        _seed(src, 4)
        m = DataMigrator(src, dst)
        m.run()
        result = m.verify(method="partition_hash")
        assert result.matches is True
        assert result.mismatched_partitions == []
        assert result.details == []

    def test_mismatch_when_dest_content_differs(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vphd", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vphd", _BASIC_SCHEMA)
        _seed(src, 4)
        m = DataMigrator(src, dst)
        m.run()
        # Mutate dest by appending a row that's not in source.
        dst.append(
            pa.Table.from_pydict(
                {"id": [999], "name": ["intruder"], "amount": [9.9]},
                schema=dst.schema().as_arrow(),
            )
        )
        result = m.verify(method="partition_hash")
        assert result.matches is False
        assert result.mismatched_partitions == ["(unpartitioned)"]
        assert any("hash mismatch" in d for d in result.details)

    def test_empty_tables_match(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vphe", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vphe", _BASIC_SCHEMA)
        m = DataMigrator(src, dst)
        m.run()
        result = m.verify(method="partition_hash")
        assert result.matches is True


class TestVerifyPartitionHashPartitioned:
    def _make_partitioned_dest(self, catalog, ns: str, name: str):
        """Create a dest table with an identity partition on ``region``."""
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "region", ice_types.StringType(), required=True),
            NestedField(3, "value", ice_types.DoubleType()),
        )
        catalog.create_namespace_if_not_exists(ns)
        spec = PartitionSpec(
            PartitionField(
                source_id=2,
                field_id=1000,
                transform=IdentityTransform(),
                name="region",
            )
        )
        return catalog.create_table(f"{ns}.{name}", schema, partition_spec=spec)

    def test_match_partitioned(self, memory_catalog) -> None:
        # Source unpartitioned, dest partitioned by region - both
        # contain the same data after migration.
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "region", ice_types.StringType(), required=True),
            NestedField(3, "value", ice_types.DoubleType()),
        )
        src = _make_table(memory_catalog, "dm", "src_vpp", src_schema)
        dst = self._make_partitioned_dest(memory_catalog, "dm", "dst_vpp")
        src.append(
            pa.Table.from_pydict(
                {
                    "id": [1, 2, 3, 4],
                    "region": ["us", "us", "eu", "eu"],
                    "value": [1.0, 2.0, 3.0, 4.0],
                },
                schema=src.schema().as_arrow(),
            )
        )

        m = DataMigrator(src, dst)
        m.run()
        result = m.verify(method="partition_hash")
        assert result.matches is True
        assert result.mismatched_partitions == []

    def test_mismatch_in_one_partition(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "region", ice_types.StringType(), required=True),
            NestedField(3, "value", ice_types.DoubleType()),
        )
        src = _make_table(memory_catalog, "dm", "src_vpm", src_schema)
        dst = self._make_partitioned_dest(memory_catalog, "dm", "dst_vpm")
        src.append(
            pa.Table.from_pydict(
                {
                    "id": [1, 2, 3, 4],
                    "region": ["us", "us", "eu", "eu"],
                    "value": [1.0, 2.0, 3.0, 4.0],
                },
                schema=src.schema().as_arrow(),
            )
        )
        m = DataMigrator(src, dst)
        m.run()
        # Mutate the "us" partition only.
        dst.append(
            pa.Table.from_pydict(
                {"id": [99], "region": ["us"], "value": [99.9]},
                schema=dst.schema().as_arrow(),
            )
        )
        result = m.verify(method="partition_hash")
        assert result.matches is False
        # One partition mismatched, the other intact.
        assert len(result.mismatched_partitions) == 1
        assert "region=us" in result.mismatched_partitions[0]


class TestVerifySample:
    def test_sample_with_n_gte_total_matches(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vs", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vs", _BASIC_SCHEMA)
        _seed(src, 5)
        m = DataMigrator(src, dst)
        m.run()
        # sample_size larger than data -> degenerates to full check.
        result = m.verify(method="sample", sample_size=100)
        assert result.matches is True
        assert result.sample_size == 5
        assert result.source_count == 5
        assert result.dest_count == 5

    def test_sample_with_mismatched_data(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vsm", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vsm", _BASIC_SCHEMA)
        _seed(src, 3)
        m = DataMigrator(src, dst)
        m.run()
        # Mutate dest.
        dst.append(
            pa.Table.from_pydict(
                {"id": [42], "name": ["m"], "amount": [4.2]},
                schema=dst.schema().as_arrow(),
            )
        )
        # Use sample_size larger than total so we compare full data.
        result = m.verify(method="sample", sample_size=100)
        assert result.matches is False
        assert any("hash mismatch" in d for d in result.details)

    def test_sample_invalid_size_raises(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vsz", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vsz", _BASIC_SCHEMA)
        m = DataMigrator(src, dst)
        with pytest.raises(ValueError, match="sample_size"):
            m.verify(method="sample", sample_size=0)
        with pytest.raises(ValueError, match="sample_size"):
            m.verify(method="sample", sample_size=-5)


class TestVerifyMisc:
    def test_invalid_method_raises(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vim", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vim", _BASIC_SCHEMA)
        m = DataMigrator(src, dst)
        with pytest.raises(ValueError, match="method"):
            m.verify(method="bogus")  # type: ignore[arg-type]

    def test_default_method_is_row_count(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vd", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vd", _BASIC_SCHEMA)
        _seed(src, 2)
        m = DataMigrator(src, dst)
        m.run()
        result = m.verify()
        assert result.method == "row_count"

    def test_verification_result_dataclass_defaults(self) -> None:
        r = VerificationResult(method="row_count", matches=True)
        assert r.source_count is None
        assert r.dest_count is None
        assert r.sample_size is None
        assert r.mismatched_partitions == []
        assert r.details == []

    def test_verify_with_column_mapping(self, memory_catalog) -> None:
        # Reconciler renames source 'order_total' -> dest 'total_amount'.
        # row_count verify should still succeed because counts are
        # rename-insensitive.
        src_schema = IcebergSchema(
            NestedField(1, "order_total", ice_types.DoubleType()),
            NestedField(2, "qty", ice_types.LongType()),
        )
        dst_schema = IcebergSchema(
            NestedField(1, "total_amount", ice_types.DoubleType()),
            NestedField(2, "qty", ice_types.LongType()),
        )
        src = _make_table(memory_catalog, "dm", "src_vcm", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_vcm", dst_schema)
        src.append(
            pa.Table.from_pydict(
                {"order_total": [1.0, 2.0, 3.0], "qty": [10, 20, 30]},
                schema=src.schema().as_arrow(),
            )
        )
        m = DataMigrator(
            src,
            dst,
            schema_reconciler=SchemaReconciler(column_mapping={"order_total": "total_amount"}),
        )
        m.run()
        # row_count: should match.
        rc = m.verify(method="row_count")
        assert rc.matches is True
        # partition_hash unpartitioned: should also match because the
        # reconciler-projected source aligns with dest's column names.
        ph = m.verify(method="partition_hash")
        assert ph.matches is True


# ===========================================================================
# Pre-flight, resume and write-mode safety
# ===========================================================================


_REGION_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "region", ice_types.StringType(), required=True),
    NestedField(3, "value", ice_types.DoubleType()),
)


def _make_region_partitioned(catalog, name: str, *, field_name: str = "region"):
    """Create ``dm.<name>`` with an identity partition on ``region``."""
    catalog.create_namespace_if_not_exists("dm")
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name=field_name)
    )
    return catalog.create_table(f"dm.{name}", _REGION_SCHEMA, partition_spec=spec)


def _seed_region(table, ids: list[int], region: str) -> None:
    table.append(
        pa.Table.from_pydict(
            {"id": ids, "region": [region] * len(ids), "value": [float(i) for i in ids]},
            schema=table.schema().as_arrow(),
        )
    )


def _rows_by_region(table) -> dict[str, list[int]]:
    table.refresh()
    out: dict[str, list[int]] = {}
    for row in table.scan().to_arrow().to_pylist():
        out.setdefault(row["region"], []).append(row["id"])
    return {k: sorted(v) for k, v in out.items()}


def _snapshot_count(table) -> int:
    table.refresh()
    return len(table.metadata.snapshots)


def _column_names(table) -> list[str]:
    table.refresh()
    return [f.name for f in table.schema().fields]


_DAY_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "d", ice_types.DateType()),
)


def _make_day_partitioned(catalog, name: str, schema: IcebergSchema = _DAY_SCHEMA):
    catalog.create_namespace_if_not_exists("dm")
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="d_day")
    )
    return catalog.create_table(f"dm.{name}", schema, partition_spec=spec)


def _seed_days(table, ids: list[int]) -> None:
    table.append(
        pa.Table.from_pydict(
            {"id": ids, "d": [datetime.date(2026, 1, 1 + i % 3) for i in ids]},
            schema=table.schema().as_arrow(),
        )
    )


class TestOverwritePartition:
    def test_more_rows_than_one_commit_raises_before_writing(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_op_a", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_op_a")
        _seed_region(src, list(range(10)), "us")
        _seed_region(dst, [500, 501], "eu")
        before = _snapshot_count(dst)

        with pytest.raises(ValueError, match="one commit"):
            DataMigrator(src, dst, write_mode="overwrite_partition", batch_rows=4).run()

        assert _snapshot_count(dst) == before
        assert _rows_by_region(dst) == {"eu": [500, 501]}

    def test_single_commit_replaces_only_touched_partitions(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_op_b", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_op_b")
        _seed_region(src, list(range(10)), "us")
        _seed_region(dst, [900, 901], "us")
        _seed_region(dst, [500, 501], "eu")

        result = DataMigrator(src, dst, write_mode="overwrite_partition", batch_rows=100).run()

        assert result.rows_moved == 10
        assert _rows_by_region(dst) == {"us": list(range(10)), "eu": [500, 501]}

    def test_unpartitioned_dest_rejected_before_auto_add(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "extra", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_op_c", src_schema)
        dst = _make_table(
            memory_catalog,
            "dm",
            "dst_op_c",
            IcebergSchema(NestedField(1, "id", ice_types.LongType(), required=True)),
        )
        src.append(
            pa.Table.from_pydict({"id": [1], "extra": ["x"]}, schema=src.schema().as_arrow())
        )

        with pytest.raises(ValueError, match="identity"):
            DataMigrator(
                src,
                dst,
                write_mode="overwrite_partition",
                schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
            ).run()

        assert _column_names(dst) == ["id"]

    def test_row_filter_counts_rows_not_files(self, memory_catalog) -> None:
        # One 10-row data file: the file-stats bound (10) exceeds
        # batch_rows, but only 2 rows pass the filter.
        src = _make_table(memory_catalog, "dm", "src_op_d", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_op_d")
        _seed_region(src, list(range(10)), "us")
        _seed_region(dst, [900], "us")

        result = DataMigrator(
            src,
            dst,
            write_mode="overwrite_partition",
            batch_rows=4,
            row_filter="id >= 8",
        ).run()

        assert result.rows_moved == 2
        assert _rows_by_region(dst) == {"us": [8, 9]}

    def test_dry_run_also_rejects_oversize_source(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_op_e", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_op_e")
        _seed_region(src, list(range(10)), "us")

        with pytest.raises(ValueError, match="one commit"):
            DataMigrator(
                src, dst, write_mode="overwrite_partition", batch_rows=4, dry_run=True
            ).run()

    def test_second_commit_is_refused(self, memory_catalog) -> None:
        # Defence in depth behind the pre-flight bound.
        src = _make_table(memory_catalog, "dm", "src_op_f", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_op_f")
        m = DataMigrator(src, dst, write_mode="overwrite_partition")
        plan = SchemaReconciler().compute_plan(src.schema(), dst.schema())
        batch = pa.RecordBatch.from_pydict(
            {"id": [1], "region": ["us"], "value": [1.0]}, schema=plan.dest_arrow_schema
        )
        with pytest.raises(RuntimeError, match="second commit"):
            m._commit_chunk([batch], 1, plan, MigrationResult())
        assert _snapshot_count(dst) == 0


class TestDataMigratorResume:
    def _crash_after_one_batch(self, src, dst, cp: CheckpointStore) -> None:
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=1), batch_rows=4, checkpoint=cp
            ).run()

    def test_resume_reads_the_checkpointed_snapshot(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_rs_a", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rs_a", _BASIC_SCHEMA)
        _seed(src, 4)
        _seed(src, 4, start=4)
        cp = CheckpointStore(tmp_path / "cp.json")
        self._crash_after_one_batch(src, dst, cp)
        assert cp.last_committed() == 0

        # The source moves on after the crash.
        _seed(src, 4, start=100)

        m = DataMigrator(src, dst, batch_rows=4, checkpoint=cp)
        m.run()
        assert _ids(dst) == list(range(8))
        verification = m.verify()
        assert verification.matches is True
        assert verification.source_count == 8

    def test_resume_with_different_explicit_snapshot_raises(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rs_b", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rs_b", _BASIC_SCHEMA)
        _seed(src, 8)
        cp = CheckpointStore(tmp_path / "cp.json")
        self._crash_after_one_batch(src, dst, cp)
        _seed(src, 4, start=100)
        src.refresh()
        newer = src.current_snapshot().snapshot_id  # type: ignore[union-attr]

        with pytest.raises(CheckpointError, match="differs"):
            DataMigrator(src, dst, batch_rows=4, checkpoint=cp, source_snapshot=newer).run()
        assert _ids(dst) == list(range(4))

    def test_resume_with_vanished_snapshot_raises(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_rs_c", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rs_c", _BASIC_SCHEMA)
        _seed(src, 8)
        cp_path = tmp_path / "cp.json"
        self._crash_after_one_batch(src, dst, CheckpointStore(cp_path))
        raw = json.loads(cp_path.read_text())
        raw["source_snapshot_id"] = 123
        cp_path.write_text(json.dumps(raw))

        with pytest.raises(CheckpointError, match="no longer exists"):
            DataMigrator(src, dst, batch_rows=4, checkpoint=CheckpointStore(cp_path)).run()

    def test_resume_when_source_gained_its_first_snapshot_raises(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rs_d", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rs_d", _BASIC_SCHEMA)
        _seed(src, 8)
        cp_path = tmp_path / "cp.json"
        self._crash_after_one_batch(src, dst, CheckpointStore(cp_path))
        raw = json.loads(cp_path.read_text())
        raw["source_snapshot_id"] = None
        cp_path.write_text(json.dumps(raw))

        with pytest.raises(CheckpointError, match="source had no snapshot"):
            DataMigrator(src, dst, batch_rows=4, checkpoint=CheckpointStore(cp_path)).run()

    def test_resume_after_source_column_added(self, memory_catalog, tmp_path: Path) -> None:
        # The plan is built from the checkpointed snapshot's schema, so
        # a column added to the source after the crash does not break
        # the strict reconciler on resume.
        src = _make_table(memory_catalog, "dm", "src_rs_e", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rs_e", _BASIC_SCHEMA)
        _seed(src, 8)
        cp = CheckpointStore(tmp_path / "cp.json")
        self._crash_after_one_batch(src, dst, cp)
        with src.update_schema() as upd:
            upd.add_column("late", ice_types.StringType())

        DataMigrator(src, dst, batch_rows=4, checkpoint=cp).run()
        assert _ids(dst) == list(range(8))
        assert "late" not in _column_names(dst)


class TestDataMigratorJobFingerprint:
    def _crash_after_two_batches(self, src, dst, cp: CheckpointStore) -> None:
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=2), batch_rows=2, checkpoint=cp
            ).run()
        assert _ids(dst) == [0, 1, 2, 3]

    @pytest.fixture
    def crashed(self, memory_catalog, tmp_path: Path):
        src = _make_table(memory_catalog, "dm", "src_fp", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_fp", _BASIC_SCHEMA)
        _seed(src, 12)
        cp = CheckpointStore(tmp_path / "cp.json")
        self._crash_after_two_batches(src, dst, cp)
        return src, dst, cp

    def test_checkpoint_records_the_job(self, crashed) -> None:
        src, dst, cp = crashed
        job = cp.job
        assert job is not None
        assert job["batch_rows"] == 2
        assert job["write_mode"] == "append"
        assert job["source_table_uuid"] == str(src.metadata.table_uuid)
        assert job["dest_table_uuid"] == str(dst.metadata.table_uuid)
        assert job["dest_snapshot_before"] is None
        assert job["job_id"]

    def test_changed_batch_rows_refused(self, crashed) -> None:
        src, dst, cp = crashed
        with pytest.raises(CheckpointError, match="batch_rows"):
            DataMigrator(src, dst, batch_rows=5, checkpoint=cp).run()
        assert _ids(dst) == [0, 1, 2, 3]

    def test_changed_row_filter_refused(self, crashed) -> None:
        src, dst, cp = crashed
        with pytest.raises(CheckpointError, match="row_filter"):
            DataMigrator(src, dst, batch_rows=2, checkpoint=cp, row_filter="id >= 3").run()
        assert _ids(dst) == [0, 1, 2, 3]

    def test_recreated_dest_refused(self, crashed, memory_catalog) -> None:
        src, _dst, cp = crashed
        memory_catalog.drop_table("dm.dst_fp")
        recreated = _make_table(memory_catalog, "dm", "dst_fp", _BASIC_SCHEMA)
        with pytest.raises(CheckpointError, match="dest_table_uuid"):
            DataMigrator(src, recreated, batch_rows=2, checkpoint=cp).run()
        assert _ids(recreated) == []

    def test_changed_reconciler_refused(self, crashed) -> None:
        src, dst, cp = crashed
        with pytest.raises(CheckpointError, match="reconciler"):
            DataMigrator(
                src,
                dst,
                batch_rows=2,
                checkpoint=cp,
                schema_reconciler=SchemaReconciler(column_mapping={"name": "name"}),
            ).run()

    def test_identical_settings_resume(self, crashed) -> None:
        src, dst, cp = crashed
        result = DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert result.rows_moved == 8
        assert _ids(dst) == list(range(12))
        assert cp.completed is True

    def test_checkpoint_without_job_refused(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_nj", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_nj", _BASIC_SCHEMA)
        _seed(src, 4)
        src.refresh()
        cp_path = tmp_path / "cp.json"
        cp_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source_snapshot_id": src.current_snapshot().snapshot_id,  # type: ignore[union-attr]
                    "last_committed_batch": None,
                    "dest_snapshot_ids": [],
                    "completed": False,
                    "started_at": "2026-01-01T00:00:00Z",
                }
            )
        )
        with pytest.raises(CheckpointError, match="fingerprint"):
            DataMigrator(src, dst, checkpoint=CheckpointStore(cp_path)).run()
        assert _snapshot_count(dst) == 0


class _FailMarkCommittedOnce(CheckpointStore):
    """Checkpoint whose ``mark_committed`` fails once for one batch.

    Simulates a crash after the destination commit landed but before
    the checkpoint recorded it.
    """

    def __init__(self, path: Path, fail_batch: int) -> None:
        super().__init__(path)
        self._fail_batch = fail_batch
        self._failed = False

    def mark_committed(self, batch_idx: int, dest_snapshot_id: int | list[int]) -> None:
        if batch_idx == self._fail_batch and not self._failed:
            self._failed = True
            raise RuntimeError("simulated crash before checkpointing")
        super().mark_committed(batch_idx, dest_snapshot_id)


class TestDataMigratorUncheckpointedBatch:
    def test_landed_batch_is_not_written_twice(self, memory_catalog, tmp_path: Path) -> None:
        src = _make_table(memory_catalog, "dm", "src_uc", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uc", _BASIC_SCHEMA)
        _seed(src, 6)
        cp_path = tmp_path / "cp.json"

        with pytest.raises(RuntimeError, match="before checkpointing"):
            DataMigrator(
                src, dst, batch_rows=2, checkpoint=_FailMarkCommittedOnce(cp_path, 1)
            ).run()
        # Batch 1 landed, but the checkpoint stops at batch 0.
        assert _ids(dst) == [0, 1, 2, 3]
        assert CheckpointStore(cp_path).last_committed() == 0

        resumed = CheckpointStore(cp_path)
        result = DataMigrator(src, dst, batch_rows=2, checkpoint=resumed).run()

        assert result.rows_moved == 2
        assert _ids(dst) == list(range(6))
        assert resumed.last_committed() == 2
        assert len(resumed.dest_snapshot_ids) == 3

        job_id = resumed.job["job_id"]  # type: ignore[index]
        for snap in dst.metadata.snapshots:
            assert snap.summary is not None
            assert snap.summary.get("iceberg-ops.migration-job") == job_id
            assert snap.summary.get("iceberg-ops.migration-batch") in {"0", "1", "2"}

    def test_upsert_recovery_walks_past_the_schema_stamp(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "name", ice_types.StringType()),
            NestedField(3, "extra", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_uc_up", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_uc_up", _NAME_SCHEMA)
        dst.append(
            pa.Table.from_pydict(
                {"id": [0, 1], "name": ["old", "old"]}, schema=dst.schema().as_arrow()
            )
        )
        src.append(
            pa.Table.from_pydict(
                {
                    "id": list(range(6)),
                    "name": [f"n{i}" for i in range(6)],
                    "extra": [f"x{i}" for i in range(6)],
                },
                schema=src.schema().as_arrow(),
            )
        )
        cp_path = tmp_path / "cp.json"

        def migrator(cp: CheckpointStore) -> DataMigrator:
            return DataMigrator(
                src,
                dst,
                write_mode="upsert",
                upsert_keys=["id"],
                batch_rows=2,
                schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
                checkpoint=cp,
            )

        # Batch 0 lands behind the job-stamped schema snapshot, then
        # the checkpoint write fails.
        with pytest.raises(RuntimeError, match="before checkpointing"):
            migrator(_FailMarkCommittedOnce(cp_path, 0)).run()
        resumed = CheckpointStore(cp_path)
        assert resumed.last_committed() is None
        job_id = resumed.job["job_id"]  # type: ignore[index]
        dst.refresh()
        stamps = [
            s.snapshot_id
            for s in dst.metadata.snapshots
            if s.summary is not None
            and s.summary.get("iceberg-ops.migration-job") == job_id
            and s.summary.get("iceberg-ops.migration-batch") is None
        ]
        assert len(stamps) == 1

        result = migrator(resumed).run()

        assert result.rows_moved == 4
        assert resumed.last_committed() == 2
        assert stamps[0] not in resumed.dest_snapshot_ids
        dst.refresh()
        rows = {r["id"]: (r["name"], r["extra"]) for r in dst.scan().to_arrow().to_pylist()}
        assert rows == {i: (f"n{i}", f"x{i}") for i in range(6)}

    def test_foreign_snapshot_between_crash_and_resume(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_uc_fw", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uc_fw", _BASIC_SCHEMA)
        _seed(src, 6)
        cp_path = tmp_path / "cp.json"
        with pytest.raises(RuntimeError, match="before checkpointing"):
            DataMigrator(
                src, dst, batch_rows=2, checkpoint=_FailMarkCommittedOnce(cp_path, 1)
            ).run()
        # Another writer commits on top of the uncheckpointed batch.
        _seed(dst, 1, start=5000)
        dst.refresh()
        foreign = dst.current_snapshot().snapshot_id  # type: ignore[union-attr]

        resumed = CheckpointStore(cp_path)
        result = DataMigrator(src, dst, batch_rows=2, checkpoint=resumed).run()

        assert result.rows_moved == 2
        assert _ids(dst) == [*range(6), 5000]
        assert resumed.last_committed() == 2
        assert len(resumed.dest_snapshot_ids) == 3
        assert foreign not in resumed.dest_snapshot_ids
        assert foreign not in result.new_snapshots

    def test_stamped_batches_that_do_not_follow_on_refused(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_uc_nf", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uc_nf", _BASIC_SCHEMA)
        _seed(src, 8)
        cp_path = tmp_path / "cp.json"
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src,
                _AppendErrorAfterN(dst, error_after=3),
                batch_rows=2,
                checkpoint=CheckpointStore(cp_path),
            ).run()
        raw = json.loads(cp_path.read_text())
        assert raw["last_committed_batch"] == 2
        # The checkpoint knows batches 0-1 but says it is at batch 0, so
        # the walk stops at batch 1 and finds only batch 2.
        raw["last_committed_batch"] = 0
        raw["dest_snapshot_ids"] = raw["dest_snapshot_ids"][:2]
        cp_path.write_text(json.dumps(raw))
        snapshots = _snapshot_count(dst)

        with pytest.raises(CheckpointError, match="do not follow on"):
            DataMigrator(src, dst, batch_rows=2, checkpoint=CheckpointStore(cp_path)).run()

        assert _ids(dst) == list(range(6))
        assert _snapshot_count(dst) == snapshots
        assert json.loads(cp_path.read_text()) == raw


class TestDataMigratorRolledBackDestination:
    def _crash_after_two_batches(self, catalog, tmp_path: Path, suffix: str):
        """Seed the destination with ids 1000 and 1001 (two snapshots), then crash."""
        src = _make_table(catalog, "dm", f"src_rb_{suffix}", _BASIC_SCHEMA)
        dst = _make_table(catalog, "dm", f"dst_rb_{suffix}", _BASIC_SCHEMA)
        _seed(src, 6)
        _seed(dst, 1, start=1000)
        _seed(dst, 1, start=1001)
        cp = CheckpointStore(tmp_path / "cp.json")
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=2), batch_rows=2, checkpoint=cp
            ).run()
        assert _ids(dst) == [0, 1, 2, 3, 1000, 1001]
        assert cp.last_committed() == 1
        return src, dst, cp

    def test_rollback_to_job_start_then_resume_refused(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src, dst, cp = self._crash_after_two_batches(memory_catalog, tmp_path, "a")
        before = cp.job["dest_snapshot_before"]  # type: ignore[index]
        rollback_to_snapshot(dst, before)
        assert _ids(dst) == [1000, 1001]
        snapshots = _snapshot_count(dst)

        with pytest.raises(CheckpointError, match="no longer contains") as exc_info:
            DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()

        assert f"rollback_to_snapshot(dest, {before})" in str(exc_info.value)
        assert _ids(dst) == [1000, 1001]
        assert _snapshot_count(dst) == snapshots
        assert CheckpointStore(cp.path).completed is False

        # The remedy the error gives: reset, then run the job again.
        cp.reset()
        DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert _ids(dst) == [*range(6), 1000, 1001]

    def test_rollback_to_mid_run_snapshot_then_resume_refused(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src, dst, cp = self._crash_after_two_batches(memory_catalog, tmp_path, "b")
        rollback_to_snapshot(dst, cp.dest_snapshot_ids[0])
        assert _ids(dst) == [0, 1, 1000, 1001]

        with pytest.raises(CheckpointError, match="no longer contains"):
            DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert _ids(dst) == [0, 1, 1000, 1001]

        # Batch 0 is still there: undo it as the error says, then reset.
        rollback_to_snapshot(dst, cp.job["dest_snapshot_before"])  # type: ignore[index]
        cp.reset()
        DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert _ids(dst) == [*range(6), 1000, 1001]

    def test_rollback_to_before_job_start_then_resume_refused(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src, dst, cp = self._crash_after_two_batches(memory_catalog, tmp_path, "d")
        dst.refresh()
        first = min(dst.metadata.snapshots, key=lambda s: s.sequence_number).snapshot_id
        # Further back than the job's starting snapshot: the walk never
        # meets it and ends at the table's first snapshot.
        rollback_to_snapshot(dst, first)
        assert _ids(dst) == [1000]

        with pytest.raises(CheckpointError, match="no longer contains"):
            DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()
        assert _ids(dst) == [1000]

    def test_expiry_gap_in_history_does_not_block_resume(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src, dst, cp = self._crash_after_two_batches(memory_catalog, tmp_path, "c")
        _seed(dst, 1, start=5000)  # another writer commits after the crash
        # Expiring batch 1's snapshot leaves batch 0's in the metadata
        # but cuts the history walk short; that is not a rollback.
        dst.maintenance.expire_snapshots().by_id(cp.dest_snapshot_ids[-1]).commit()

        result = DataMigrator(src, dst, batch_rows=2, checkpoint=cp).run()

        assert result.rows_moved == 2
        assert _ids(dst) == [*range(6), 1000, 1001, 5000]
        assert cp.completed is True


class TestDataMigratorResetHint:
    def test_expired_source_snapshot_refusal_names_the_rollback_target(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rh_a", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rh_a", _BASIC_SCHEMA)
        _seed(src, 6)
        _seed(dst, 1, start=1000)
        cp = CheckpointStore(tmp_path / "cp.json")
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=1), batch_rows=3, checkpoint=cp
            ).run()
        assert _ids(dst) == [0, 1, 2, 1000]
        before = cp.job["dest_snapshot_before"]  # type: ignore[index]
        pinned = cp.source_snapshot_id
        assert pinned is not None
        _seed(src, 1, start=6)  # the source moves on
        src.maintenance.expire_snapshots().by_id(pinned).commit()

        with pytest.raises(CheckpointError, match="no longer exists") as exc_info:
            DataMigrator(src, dst, batch_rows=3, checkpoint=cp).run()

        message = str(exc_info.value)
        assert "1 batch(es) of this job were committed" in message
        assert f"rollback_to_snapshot(dest, {before})" in message
        assert _ids(dst) == [0, 1, 2, 1000]

        # Following the advice re-runs the job without duplicating batch 0.
        rollback_to_snapshot(dst, before)
        cp.reset()
        DataMigrator(src, dst, batch_rows=3, checkpoint=cp).run()
        assert _ids(dst) == [*range(7), 1000]

    def test_refusal_without_a_starting_snapshot_says_delete_rows(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rh_b", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rh_b", _BASIC_SCHEMA)
        _seed(src, 6)
        cp = CheckpointStore(tmp_path / "cp.json")
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=2), batch_rows=2, checkpoint=cp
            ).run()

        with pytest.raises(CheckpointError, match="batch_rows") as exc_info:
            DataMigrator(src, dst, batch_rows=3, checkpoint=cp).run()

        message = str(exc_info.value)
        assert "2 batch(es) of this job were committed" in message
        assert "delete this job's rows" in message
        assert "no snapshot rollback can undo the job" in message

    def test_refusal_counts_a_batch_that_landed_before_checkpointing(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rh_c", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rh_c", _BASIC_SCHEMA)
        _seed(src, 6)
        cp_path = tmp_path / "cp.json"
        with pytest.raises(RuntimeError, match="before checkpointing"):
            DataMigrator(
                src, dst, batch_rows=2, checkpoint=_FailMarkCommittedOnce(cp_path, 0)
            ).run()
        assert CheckpointStore(cp_path).last_committed() is None

        with pytest.raises(CheckpointError, match="batch_rows") as exc_info:
            DataMigrator(src, dst, batch_rows=3, checkpoint=CheckpointStore(cp_path)).run()

        assert "A batch of this job was committed" in str(exc_info.value)

    def test_refusal_before_any_write_gives_the_plain_remedy(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_rh_d", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_rh_d", _BASIC_SCHEMA)
        _seed(src, 6)
        cp = CheckpointStore(tmp_path / "cp.json")
        with pytest.raises(RuntimeError, match="simulated crash"):
            DataMigrator(
                src, _AppendErrorAfterN(dst, error_after=0), batch_rows=2, checkpoint=cp
            ).run()
        assert _snapshot_count(dst) == 0

        with pytest.raises(CheckpointError, match="batch_rows") as exc_info:
            DataMigrator(src, dst, batch_rows=3, checkpoint=cp).run()

        message = str(exc_info.value)
        assert "Call CheckpointStore.reset() (or delete the file) to start over." in message
        assert "rollback_to_snapshot" not in message


class TestDataMigratorPreflightValidation:
    def test_blank_upsert_key_rejected(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_uk_a", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uk_a", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="blank"):
            DataMigrator(src, dst, write_mode="upsert", upsert_keys=["id", ""])

    def test_repeated_upsert_key_rejected(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_uk_b", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uk_b", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="repeat"):
            DataMigrator(src, dst, write_mode="upsert", upsert_keys=["id", "id"])

    def test_upsert_keys_with_append_rejected(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_uk_c", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_uk_c", _BASIC_SCHEMA)
        with pytest.raises(ValueError, match="upsert_keys"):
            DataMigrator(src, dst, write_mode="append", upsert_keys=["id"])

    def test_unknown_upsert_key_rejected_before_auto_add(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "extra", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_uk_d", src_schema)
        dst = _make_table(
            memory_catalog,
            "dm",
            "dst_uk_d",
            IcebergSchema(NestedField(1, "id", ice_types.LongType(), required=True)),
        )
        src.append(
            pa.Table.from_pydict({"id": [1], "extra": ["x"]}, schema=src.schema().as_arrow())
        )
        with pytest.raises(ValueError, match="upsert_keys"):
            DataMigrator(
                src,
                dst,
                write_mode="upsert",
                upsert_keys=["idd"],
                schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
            ).run()
        assert _column_names(dst) == ["id"]


_EXTRA_DEST_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "name", ice_types.StringType()),
    NestedField(3, "extra", ice_types.StringType()),
)

_NAME_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "name", ice_types.StringType()),
)


class TestDataMigratorUpsertSafety:
    def _seeded(self, catalog, suffix: str):
        src = _make_table(catalog, "dm", f"src_us_{suffix}", _NAME_SCHEMA)
        dst = _make_table(catalog, "dm", f"dst_us_{suffix}", _EXTRA_DEST_SCHEMA)
        dst.append(
            pa.Table.from_pydict(
                {"id": [1, 2], "name": ["a", "b"], "extra": ["K", "K"]},
                schema=dst.schema().as_arrow(),
            )
        )
        src.append(
            pa.Table.from_pydict(
                {"id": [2, 3], "name": ["B", "c"]}, schema=src.schema().as_arrow()
            )
        )
        return src, dst

    def test_skip_policy_rejected_for_upsert(self, memory_catalog) -> None:
        src, dst = self._seeded(memory_catalog, "skip")
        before = _snapshot_count(dst)
        with pytest.raises(ValueError, match="skipped"):
            DataMigrator(
                src,
                dst,
                write_mode="upsert",
                upsert_keys=["id"],
                schema_reconciler=SchemaReconciler(on_missing_in_source="skip"),
            ).run()
        assert _snapshot_count(dst) == before

    def test_null_policy_warns_that_values_are_overwritten(self, memory_catalog) -> None:
        src, dst = self._seeded(memory_catalog, "null")
        result = DataMigrator(
            src,
            dst,
            write_mode="upsert",
            upsert_keys=["id"],
            schema_reconciler=SchemaReconciler(on_missing_in_source="null"),
        ).run()
        assert result.plan is not None
        assert any("overwrites existing values" in w for w in result.plan.warnings)
        dst.refresh()
        rows = {r["id"]: r["extra"] for r in dst.scan().to_arrow().to_pylist()}
        # The matched key loses its value - which is what the warning says.
        assert rows == {1: "K", 2: None, 3: None}

    def test_null_filled_upsert_key_rejected_before_any_write(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "name", ice_types.StringType()),
            NestedField(3, "more", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_us_nk", src_schema)
        dst = _make_table(memory_catalog, "dm", "dst_us_nk", _EXTRA_DEST_SCHEMA)
        dst.append(
            pa.Table.from_pydict(
                {"id": [1], "name": ["a"], "extra": ["K"]}, schema=dst.schema().as_arrow()
            )
        )
        src.append(
            pa.Table.from_pydict(
                {"id": [1, 2], "name": ["b", "c"], "more": ["m", "m"]},
                schema=src.schema().as_arrow(),
            )
        )
        before = _snapshot_count(dst)
        cp_path = tmp_path / "cp.json"

        # "extra" has no source column, so the plan fills it with null.
        for dry_run in (True, False):
            with pytest.raises(ValueError, match="no source column"):
                DataMigrator(
                    src,
                    dst,
                    write_mode="upsert",
                    upsert_keys=["id", "extra"],
                    schema_reconciler=SchemaReconciler(
                        on_missing_in_source="null", on_missing_in_dest="auto_add"
                    ),
                    checkpoint=CheckpointStore(cp_path),
                    dry_run=dry_run,
                ).run()

        assert _column_names(dst) == ["id", "name", "extra"]
        assert _snapshot_count(dst) == before
        assert not cp_path.exists()


class TestDataMigratorUpsertWorkarounds:
    def test_upsert_with_auto_add_into_non_empty_dest(self, memory_catalog) -> None:
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "extra", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_ua", src_schema)
        dst = _make_table(
            memory_catalog,
            "dm",
            "dst_ua",
            IcebergSchema(NestedField(1, "id", ice_types.LongType(), required=True)),
        )
        dst.append(pa.Table.from_pydict({"id": [1, 2]}, schema=dst.schema().as_arrow()))
        src.append(
            pa.Table.from_pydict(
                {"id": [2, 3], "extra": ["b", "c"]}, schema=src.schema().as_arrow()
            )
        )

        result = DataMigrator(
            src,
            dst,
            write_mode="upsert",
            upsert_keys=["id"],
            schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add"),
        ).run()

        dst.refresh()
        rows = {r["id"]: r["extra"] for r in dst.scan().to_arrow().to_pylist()}
        assert rows == {1: None, 2: "b", 3: "c"}
        # The empty schema-stamp snapshot is reported first.
        stamp = dst.metadata.snapshot_by_id(result.new_snapshots[0])
        assert stamp is not None and stamp.summary is not None
        assert stamp.summary.get("added-records") in (None, "0")
        assert stamp.summary.get("iceberg-ops.migration-job")
        assert stamp.summary.get("iceberg-ops.migration-batch") is None

    def test_composite_key_upsert_sub_batches(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import iceberg_ops.core.utils as utils_module

        monkeypatch.setattr(utils_module, "_COMPOSITE_UPSERT_MAX_KEYS", 3)
        schema = IcebergSchema(
            NestedField(1, "a", ice_types.LongType(), required=True),
            NestedField(2, "b", ice_types.LongType(), required=True),
            NestedField(3, "v", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_ck", schema)
        dst = _make_table(memory_catalog, "dm", "dst_ck", schema)
        dst.append(
            pa.Table.from_pydict(
                {"a": list(range(5)), "b": list(range(5)), "v": ["old"] * 5},
                schema=dst.schema().as_arrow(),
            )
        )
        src.append(
            pa.Table.from_pydict(
                {"a": list(range(10)), "b": list(range(10)), "v": ["new"] * 10},
                schema=src.schema().as_arrow(),
            )
        )

        result = DataMigrator(src, dst, write_mode="upsert", upsert_keys=["a", "b"]).run()

        dst.refresh()
        rows = sorted(dst.scan().to_arrow().to_pylist(), key=lambda r: r["a"])
        assert rows == [{"a": i, "b": i, "v": "new"} for i in range(10)]
        assert result.rows_updated == 5
        assert result.rows_inserted == 5


class TestDataMigratorSnapshotBookkeeping:
    def test_upsert_records_every_snapshot_and_counts(
        self, memory_catalog, tmp_path: Path
    ) -> None:
        src = _make_table(memory_catalog, "dm", "src_sb", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_sb", _BASIC_SCHEMA)
        _seed(dst, 3, start=1)  # ids 1, 2, 3
        dst.refresh()
        pre_rows = dst.scan().to_arrow().sort_by("id").to_pylist()
        pre_ids = {s.snapshot_id for s in dst.metadata.snapshots}
        src.append(
            pa.Table.from_pydict(
                {
                    "id": [1, 2, 10, 11, 12],
                    "name": ["changed", "row_2", "n10", "n11", "n12"],
                    "amount": [1.0, 2.0, 10.0, 11.0, 12.0],
                },
                schema=src.schema().as_arrow(),
            )
        )
        cp = CheckpointStore(tmp_path / "cp.json")

        result = DataMigrator(
            src, dst, write_mode="upsert", upsert_keys=["id"], checkpoint=cp
        ).run()

        dst.refresh()
        all_ids = {s.snapshot_id for s in dst.metadata.snapshots}
        assert set(result.new_snapshots) == all_ids - pre_ids
        assert len(result.new_snapshots) > 1
        assert result.rows_updated == 1
        assert result.rows_inserted == 3
        assert cp.dest_snapshot_ids == result.new_snapshots
        for sid in result.new_snapshots:
            snap = dst.metadata.snapshot_by_id(sid)
            assert snap is not None and snap.summary is not None
            assert snap.summary.get("iceberg-ops.migration-job") == cp.job["job_id"]  # type: ignore[index]

        assert result.dest_snapshot_before is not None
        rollback_to_snapshot(dst, result.dest_snapshot_before)
        dst.refresh()
        assert dst.scan().to_arrow().sort_by("id").to_pylist() == pre_rows

    def test_append_counts_are_none(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_sb2", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_sb2", _BASIC_SCHEMA)
        _seed(src, 3)
        result = DataMigrator(src, dst).run()
        assert result.rows_inserted is None
        assert result.rows_updated is None
        assert result.dest_snapshot_before is None


class TestDataMigratorPinnedSchema:
    def test_plan_uses_the_pinned_snapshot_schema(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_ps", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_ps", _BASIC_SCHEMA)
        _seed(src, 2)
        src.refresh()
        pinned = src.current_snapshot().snapshot_id  # type: ignore[union-attr]
        with src.update_schema() as upd:
            upd.add_column("late", ice_types.StringType())

        m = DataMigrator(src, dst, source_snapshot=pinned)
        result = m.run()

        assert result.rows_moved == 2
        assert _column_names(dst) == ["id", "name", "amount"]
        assert m.verify(method="sample").matches is True
        assert m.verify(method="partition_hash").matches is True


class TestVerifyUnpinnedRun:
    def test_verify_reads_the_snapshot_the_run_read(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vu", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vu", _BASIC_SCHEMA)
        _seed(src, 5)
        m = DataMigrator(src, dst)
        m.run()
        # Rows arriving after the run are not part of what was migrated.
        _seed(src, 3, start=100)
        result = m.verify(method="row_count")
        assert result.matches is True
        assert result.source_count == 5


class TestColumnMappingAndPartitionNames:
    def test_unknown_column_mapping_key_rejected(self) -> None:
        src = pa.schema([pa.field("order_total", pa.float64())])
        dst = pa.schema([pa.field("total_amount", pa.float64())])
        r = SchemaReconciler(
            column_mapping={"ordr_total": "total_amount"}, on_missing_in_source="null"
        )
        with pytest.raises(ValueError, match="not a source column"):
            r.compute_plan(src, dst)

    def test_partition_hash_resolves_partition_source_column(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_pn", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_pn", field_name="region_part")
        src.append(
            pa.Table.from_pydict(
                {"id": [1, 2, 3], "region": ["us", "eu", "us"], "value": [1.0, 2.0, 3.0]},
                schema=src.schema().as_arrow(),
            )
        )
        m = DataMigrator(src, dst)
        m.run()
        assert m.verify(method="partition_hash").matches is True


class TestTransformsPreflight:
    def _no_core(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import iceberg_ops.core.utils as utils_module

        monkeypatch.setattr(utils_module, "_has_pyiceberg_core", lambda: False)

    def test_missing_core_fails_before_any_write(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._no_core(monkeypatch)
        src = _make_table(memory_catalog, "dm", "src_tf_a", _DAY_SCHEMA)
        dst = _make_day_partitioned(memory_catalog, "dst_tf_a")
        _seed_days(src, [0, 1, 2])

        with pytest.raises(ImportError, match=r"iceberg-ops\[transforms\]"):
            DataMigrator(src, dst).run()
        assert _snapshot_count(dst) == 0

    def test_missing_core_fails_before_auto_add(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._no_core(monkeypatch)
        src_schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "d", ice_types.DateType()),
            NestedField(3, "extra", ice_types.StringType()),
        )
        src = _make_table(memory_catalog, "dm", "src_tf_b", src_schema)
        dst = _make_day_partitioned(memory_catalog, "dst_tf_b")
        with pytest.raises(ImportError, match="pyiceberg-core"):
            DataMigrator(
                src, dst, schema_reconciler=SchemaReconciler(on_missing_in_dest="auto_add")
            ).run()
        assert _column_names(dst) == ["id", "d"]

    def test_dry_run_reports_missing_core_as_warning(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._no_core(monkeypatch)
        src = _make_table(memory_catalog, "dm", "src_tf_c", _DAY_SCHEMA)
        dst = _make_day_partitioned(memory_catalog, "dst_tf_c")
        _seed_days(src, [0, 1])
        result = DataMigrator(src, dst, dry_run=True).run()
        assert result.plan is not None
        assert any("iceberg-ops[transforms]" in w for w in result.plan.warnings)

    def test_identity_spec_needs_no_core(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._no_core(monkeypatch)
        src = _make_table(memory_catalog, "dm", "src_tf_d", _REGION_SCHEMA)
        dst = _make_region_partitioned(memory_catalog, "dst_tf_d")
        _seed_region(src, [1, 2], "us")
        assert DataMigrator(src, dst).run().rows_moved == 2

    def test_day_partitioned_write_with_core_installed(self, memory_catalog) -> None:
        pytest.importorskip("pyiceberg_core")
        src = _make_table(memory_catalog, "dm", "src_tf_e", _DAY_SCHEMA)
        dst = _make_day_partitioned(memory_catalog, "dst_tf_e")
        _seed_days(src, list(range(6)))
        result = DataMigrator(src, dst, batch_rows=4).run()
        assert result.rows_moved == 6
        assert _ids(dst) == list(range(6))


class TestVerifyUnsupportedAndCounts:
    def test_partition_hash_on_non_identity_spec_raises(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vx_a", _DAY_SCHEMA)
        dst = _make_day_partitioned(memory_catalog, "dst_vx_a")
        _seed_days(src, [0, 1])
        m = DataMigrator(src, dst)
        # Not a mismatch: the method cannot check this spec at all. A
        # RuntimeError, so the CLI reports it as a verify error.
        with pytest.raises(NotImplementedError, match="identity-transform") as exc_info:
            m.verify(method="partition_hash")
        assert isinstance(exc_info.value, RuntimeError)
        assert "DayTransform" in str(exc_info.value)

    def test_partition_hash_after_day_partitioned_migration_raises(self, memory_catalog) -> None:
        pytest.importorskip("pyiceberg_core")
        src = _make_table(memory_catalog, "dm", "src_vx_b", _DAY_SCHEMA)
        dst = _make_day_partitioned(memory_catalog, "dst_vx_b")
        _seed_days(src, [0, 1, 2])
        m = DataMigrator(src, dst)
        m.run()
        assert m.verify(method="row_count").matches is True
        with pytest.raises(NotImplementedError):
            m.verify(method="partition_hash")

    def test_row_count_after_rows_deleted_from_dest(self, memory_catalog) -> None:
        src = _make_table(memory_catalog, "dm", "src_vx_c", _BASIC_SCHEMA)
        dst = _make_table(memory_catalog, "dm", "dst_vx_c", _BASIC_SCHEMA)
        _seed(src, 6)
        m = DataMigrator(src, dst, batch_rows=4)
        m.run()
        dst.delete("id >= 4")

        result = m.verify(method="row_count")

        assert result.matches is False
        assert (result.source_count, result.dest_count) == (6, 4)

    def test_row_count_with_filter_on_partition_column(self, memory_catalog) -> None:
        src = _make_region_partitioned(memory_catalog, "src_vx_d")
        dst = _make_table(memory_catalog, "dm", "dst_vx_d", _REGION_SCHEMA)
        _seed_region(src, [1, 2, 3], "us")
        _seed_region(src, [4, 5], "eu")
        m = DataMigrator(src, dst, row_filter="region = 'eu'")
        m.run()

        result = m.verify(method="row_count")

        assert result.matches is True
        assert (result.source_count, result.dest_count) == (2, 2)
