"""Tests for iceberg_ops.core.utils."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg import types as ice_types
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import NestedField

import iceberg_ops.core.utils as utils_module
from iceberg_ops.core.utils import (
    _ensure_schema_snapshot,
    _has_pyiceberg_core,
    _require_transforms_extra,
    _upsert_chunk,
    create_arrow_table,
    get_snapshots,
    rollback_to_snapshot,
    trace_field_history,
    upsert_streaming,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

_COMPOSITE_SCHEMA = IcebergSchema(
    NestedField(1, "a", ice_types.LongType(), required=True),
    NestedField(2, "b", ice_types.LongType(), required=True),
    NestedField(3, "v", ice_types.StringType()),
)


def _composite_table(catalog, name: str, n_old: int):
    """Create ``ns.<name>`` keyed by (a, b) with *n_old* rows of v='old'."""
    catalog.create_namespace_if_not_exists("ns")
    tbl = catalog.create_table(f"ns.{name}", _COMPOSITE_SCHEMA)
    if n_old:
        tbl.append(
            pa.Table.from_pydict(
                {"a": list(range(n_old)), "b": list(range(n_old)), "v": ["old"] * n_old},
                schema=tbl.schema().as_arrow(),
            )
        )
    return tbl


def _composite_rows(tbl, n: int, value: str = "new") -> pa.Table:
    return pa.Table.from_pydict(
        {"a": list(range(n)), "b": list(range(n)), "v": [value] * n},
        schema=tbl.schema().as_arrow(),
    )


# ===========================================================================
# create_arrow_table
# ===========================================================================


class TestCreateArrowTable:
    def test_full_rows(self) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.float64())])
        tbl = create_arrow_table(schema, [{"id": 1, "v": 1.5}, {"id": 2, "v": 2.5}])
        assert tbl.num_rows == 2
        assert tbl.column("id").to_pylist() == [1, 2]
        assert tbl.column("v").to_pylist() == [1.5, 2.5]

    def test_partial_rows_get_null(self) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.float64())])
        tbl = create_arrow_table(schema, [{"id": 1}, {"id": 2, "v": 3.14}])
        assert tbl.column("v").to_pylist() == [None, 3.14]

    def test_unknown_keys_ignored(self) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = create_arrow_table(schema, [{"id": 1, "extra": "ignored"}])
        assert tbl.column_names == ["id"]
        assert tbl.column("id").to_pylist() == [1]

    def test_empty_rows(self) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = create_arrow_table(schema, [])
        assert tbl.num_rows == 0
        assert tbl.column_names == ["id"]


# ===========================================================================
# upsert_streaming
# ===========================================================================


class TestUpsertStreaming:
    def test_invalid_batch_rows(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        with pytest.raises(ValueError, match="batch_rows must be positive"):
            upsert_streaming(tbl, iter([]), join_cols=["id"], batch_rows=0)

    def test_negative_batch_rows(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        with pytest.raises(ValueError, match="batch_rows must be positive"):
            upsert_streaming(tbl, iter([]), join_cols=["id"], batch_rows=-5)

    def test_empty_source_returns_zero(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        n = upsert_streaming(tbl, iter([]), join_cols=["id"], batch_rows=10)
        assert n == 0
        tbl.refresh()
        assert get_snapshots(tbl) == []

    def test_inserts_then_updates(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)

        initial = pa.RecordBatch.from_pylist(
            [{"id": 1, "v": "one"}, {"id": 2, "v": "two"}],
            schema=schema,
        )
        n = upsert_streaming(tbl, iter([initial]), join_cols=["id"], batch_rows=10)
        assert n == 2

        update = pa.RecordBatch.from_pylist(
            [{"id": 2, "v": "TWO"}, {"id": 3, "v": "three"}],
            schema=schema,
        )
        n = upsert_streaming(tbl, iter([update]), join_cols=["id"], batch_rows=10)
        assert n == 2

        tbl.refresh()
        rows = sorted(tbl.scan().to_arrow().to_pylist(), key=lambda r: r["id"])
        assert rows == [
            {"id": 1, "v": "one"},
            {"id": 2, "v": "TWO"},
            {"id": 3, "v": "three"},
        ]

    def test_flush_at_threshold(self, memory_catalog, make_table) -> None:
        """Three batches of 10 rows with batch_rows=15 -> 2 flushes."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)

        batches = [
            pa.RecordBatch.from_pylist(
                [{"id": j * 100 + i, "v": f"v{j * 100 + i}"} for i in range(10)],
                schema=schema,
            )
            for j in range(3)
        ]
        n = upsert_streaming(tbl, iter(batches), join_cols=["id"], batch_rows=15)
        assert n == 30

        tbl.refresh()
        snaps = get_snapshots(tbl)
        # batch 1 (10) -> below threshold; batch 2 (20 cumulative) -> flush;
        # batch 3 (10) -> below; loop ends -> flush leftover. Total: 2 flushes.
        assert len(snaps) == 2

    def test_first_upsert_after_add_column_succeeds(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1, 2]}, schema=tbl.schema().as_arrow()))
        with tbl.update_schema() as upd:
            upd.add_column("extra", ice_types.StringType())
        tbl.refresh()

        incoming = pa.RecordBatch.from_pylist(
            [{"id": 2, "extra": "b"}, {"id": 3, "extra": "c"}],
            schema=tbl.schema().as_arrow(),
        )
        assert upsert_streaming(tbl, iter([incoming]), join_cols=["id"]) == 2

        tbl.refresh()
        rows = {r["id"]: r["extra"] for r in tbl.scan().to_arrow().to_pylist()}
        assert rows == {1: None, 2: "b", 3: "c"}

    def test_missing_transforms_extra_fails_before_any_snapshot(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils_module, "_has_pyiceberg_core", lambda: False)
        memory_catalog.create_namespace("ns")
        schema = IcebergSchema(
            NestedField(1, "id", ice_types.LongType(), required=True),
            NestedField(2, "v", ice_types.StringType()),
        )
        spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="id_b")
        )
        tbl = memory_catalog.create_table("ns.bucketed", schema, partition_spec=spec)

        consumed: list[int] = []

        def _source():
            consumed.append(1)
            yield pa.RecordBatch.from_pylist([{"id": 1, "v": "x"}], schema=tbl.schema().as_arrow())

        with pytest.raises(ImportError, match=r"iceberg-ops\[transforms\]"):
            upsert_streaming(tbl, _source(), join_cols=["id"])
        assert consumed == []
        tbl.refresh()
        assert tbl.metadata.snapshots == []

    def test_composite_keys_are_sub_batched_in_one_commit(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils_module, "_COMPOSITE_UPSERT_MAX_KEYS", 3)
        tbl = _composite_table(memory_catalog, "ck", n_old=5)
        log_before = len(tbl.metadata.metadata_log)

        incoming = _composite_rows(tbl, 10)
        n = upsert_streaming(tbl, incoming.to_batches(), join_cols=["a", "b"], batch_rows=100)

        assert n == 10
        tbl.refresh()
        rows = sorted(tbl.scan().to_arrow().to_pylist(), key=lambda r: r["a"])
        assert rows == [{"a": i, "b": i, "v": "new"} for i in range(10)]
        # One catalog commit for the whole chunk.
        assert len(tbl.metadata.metadata_log) == log_before + 1


# ===========================================================================
# Write-path helpers
# ===========================================================================


class TestTransformsProbe:
    def test_probe_uses_find_spec(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib.util

        real_find_spec = importlib.util.find_spec

        def _fake(name: str, *args, **kwargs):
            if name == "pyiceberg_core":
                return None
            return real_find_spec(name, *args, **kwargs)

        monkeypatch.setattr(importlib.util, "find_spec", _fake)
        assert _has_pyiceberg_core() is False

    def test_unpartitioned_table_needs_nothing(
        self, memory_catalog, make_table, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils_module, "_has_pyiceberg_core", lambda: False)
        tbl = make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        _require_transforms_extra(tbl)  # does not raise

    def test_message_names_table_and_transform(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils_module, "_has_pyiceberg_core", lambda: False)
        memory_catalog.create_namespace("ns")
        schema = IcebergSchema(NestedField(1, "id", ice_types.LongType(), required=True))
        spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=BucketTransform(8), name="id_b")
        )
        tbl = memory_catalog.create_table("ns.bucketed", schema, partition_spec=spec)
        with pytest.raises(ImportError) as excinfo:
            _require_transforms_extra(tbl)
        msg = str(excinfo.value)
        assert "ns.bucketed" in msg
        assert "bucket[8]" in msg
        assert "pip install 'iceberg-ops[transforms]'" in msg


class TestEnsureSchemaSnapshot:
    def test_no_snapshot_when_schema_ids_match(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1]}, schema=tbl.schema().as_arrow()))
        before = len(tbl.metadata.snapshots)
        assert _ensure_schema_snapshot(tbl) is False
        tbl.refresh()
        assert len(tbl.metadata.snapshots) == before

    def test_no_snapshot_for_empty_table(self, memory_catalog, make_table) -> None:
        tbl = make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        with tbl.update_schema() as upd:
            upd.add_column("extra", ice_types.StringType())
        assert _ensure_schema_snapshot(tbl) is False
        tbl.refresh()
        assert tbl.metadata.snapshots == []

    def test_empty_append_after_schema_change(self, memory_catalog, make_table) -> None:
        tbl = make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        tbl.append(pa.table({"id": [1]}, schema=tbl.schema().as_arrow()))
        with tbl.update_schema() as upd:
            upd.add_column("extra", ice_types.StringType())
        tbl.refresh()

        assert _ensure_schema_snapshot(tbl, {"marker": "yes"}) is True
        tbl.refresh()
        snap = tbl.current_snapshot()
        assert snap is not None and snap.summary is not None
        assert snap.schema_id == tbl.metadata.current_schema_id
        assert snap.summary.get("marker") == "yes"
        assert tbl.scan().to_arrow().num_rows == 1


class TestUpsertChunk:
    def test_single_key_returns_counts(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.large_string())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1, 2], "v": ["a", "b"]}, schema=schema))
        chunk = pa.table({"id": [2, 3], "v": ["B", "c"]}, schema=schema)
        assert _upsert_chunk(tbl, chunk, ["id"]) == (1, 1)

    def test_duplicate_composite_key_across_sub_batches_rejected(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(utils_module, "_COMPOSITE_UPSERT_MAX_KEYS", 3)
        tbl = _composite_table(memory_catalog, "dup", n_old=2)
        before = tbl.scan().to_arrow().sort_by("a").to_pylist()
        snapshots_before = len(tbl.metadata.snapshots)

        rows = _composite_rows(tbl, 6).to_pylist()
        rows[5] = {"a": 0, "b": 0, "v": "dup"}  # same key as row 0
        chunk = pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow())

        with pytest.raises(ValueError, match="Duplicate rows"):
            _upsert_chunk(tbl, chunk, ["a", "b"])
        tbl.refresh()
        assert len(tbl.metadata.snapshots) == snapshots_before
        assert tbl.scan().to_arrow().sort_by("a").to_pylist() == before

    def test_thousands_of_composite_keys_do_not_crash(self, tmp_path: Path) -> None:
        # Unguarded, a few thousand composite keys overflow the native
        # stack and kill the interpreter (apache/iceberg-python#3508).
        # Run it in a child so a regression fails this test instead of
        # killing pytest.
        script = tmp_path / "composite_upsert.py"
        script.write_text(
            textwrap.dedent(
                """
                import sys
                import pyarrow as pa
                from pyiceberg import types as ice_types
                from pyiceberg.schema import Schema
                from pyiceberg.types import NestedField
                from iceberg_ops.core.catalog import CatalogFactory
                from iceberg_ops.core.utils import upsert_streaming

                n = 3_000
                cat = CatalogFactory.build("memory", warehouse_dir=sys.argv[1])
                cat.create_namespace("ns")
                tbl = cat.create_table(
                    "ns.t",
                    Schema(
                        NestedField(1, "a", ice_types.LongType(), required=True),
                        NestedField(2, "b", ice_types.LongType(), required=True),
                        NestedField(3, "v", ice_types.StringType()),
                    ),
                )
                schema = tbl.schema().as_arrow()
                half = n // 2
                tbl.append(pa.Table.from_pydict(
                    {"a": list(range(half)), "b": list(range(half)), "v": ["old"] * half},
                    schema=schema,
                ))
                incoming = pa.Table.from_pydict(
                    {"a": list(range(n)), "b": list(range(n)), "v": ["new"] * n},
                    schema=schema,
                )
                upsert_streaming(tbl, incoming.to_batches(), join_cols=["a", "b"])
                tbl.refresh()
                new = tbl.scan(row_filter="v == 'new'").to_arrow().num_rows
                assert new == n, new
                assert tbl.scan().to_arrow().num_rows == n
                print("ok")
                """
            )
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            p for p in (str(_REPO_ROOT), env.get("PYTHONPATH", "")) if p
        )
        proc = subprocess.run(
            [sys.executable, str(script), str(tmp_path / "warehouse")],
            capture_output=True,
            text=True,
            env=env,
            timeout=600,
        )
        assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
        assert "ok" in proc.stdout


# ===========================================================================
# Snapshot helpers
# ===========================================================================


class TestGetSnapshots:
    def test_empty_table_has_no_snapshots(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        assert get_snapshots(tbl) == []

    def test_lists_snapshots_after_appends(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1, 2]}, schema=schema))
        tbl.append(pa.table({"id": [3, 4]}, schema=schema))
        tbl.refresh()
        snaps = get_snapshots(tbl)
        assert len(snaps) == 2
        for s in snaps:
            assert isinstance(s["snapshot_id"], int)
            assert isinstance(s["timestamp"], str)
            assert s["timestamp"].endswith("Z")
            assert isinstance(s["schema_id"], int)
            assert s["operation"] in {"append", "overwrite", "delete", "unknown"}


class TestRollbackToSnapshot:
    def test_rolls_back_to_earlier(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1, 2]}, schema=schema))
        tbl.refresh()
        first_snap = tbl.current_snapshot().snapshot_id

        tbl.append(pa.table({"id": [3, 4]}, schema=schema))
        tbl.refresh()

        rollback_to_snapshot(tbl, first_snap)
        tbl.refresh()
        ids = sorted(r["id"] for r in tbl.scan().to_arrow().to_pylist())
        assert ids == [1, 2]

    def test_invalid_id_raises(self, memory_catalog, make_table) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        tbl.append(pa.table({"id": [1]}, schema=schema))
        with pytest.raises(ValueError, match="not found"):
            rollback_to_snapshot(tbl, 999_999_999)


# ===========================================================================
# trace_field_history
# ===========================================================================


class TestTraceFieldHistory:
    def test_prints_history(
        self,
        memory_catalog,
        make_table,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl = make_table(memory_catalog, "ns.t", schema)
        fid = tbl.schema().fields[0].field_id
        trace_field_history(tbl, field_id=fid)
        out = capsys.readouterr().out
        assert "FIELD HISTORY" in out
        assert f"field ID {fid}" in out
        assert "id" in out

    def test_unknown_field_id(
        self,
        memory_catalog,
        make_table,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        trace_field_history(tbl, field_id=999)
        out = capsys.readouterr().out
        assert "did not exist" in out
