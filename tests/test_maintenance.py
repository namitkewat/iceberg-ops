"""
tests/test_maintenance.py
=========================
Tests for the storage maintenance operations:

* :func:`table_health_report` - read-only diagnostic
* :func:`expire_snapshots` - snapshot lifecycle
* :func:`compact_small_files_streaming` - streaming compaction
* :func:`remove_orphan_files` - recursive orphan detection

All tests use a real in-memory catalog with a local filesystem warehouse
(via the :func:`memory_catalog` fixture); no mocking of PyIceberg.
"""

from __future__ import annotations

import inspect
import os
import time
import tracemalloc
import uuid
import warnings

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from iceberg_ops.core.utils import create_arrow_table
from iceberg_ops.maintenance.advisor import TableHealthReport, table_health_report
from iceberg_ops.maintenance.compaction import (
    CompactionResult,
    compact_small_files_streaming,
)
from iceberg_ops.maintenance.orphans import (
    OrphanCleanupResult,
    _file_uri_to_local_path,
    remove_orphan_files,
)
from iceberg_ops.maintenance.snapshots import SnapshotExpiryResult, expire_snapshots

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TABLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("value", pa.string()),
    ]
)


def _make_table(catalog, name: str = "ns.t"):
    """Create an empty table with the standard test schema."""
    ns = name.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(ns)
    return catalog.create_table(name, schema=TABLE_SCHEMA)


def _append_rows(table, start: int, count: int) -> None:
    """Append *count* rows starting from *start*."""
    rows = [{"id": i, "value": f"row_{i}"} for i in range(start, start + count)]
    data = create_arrow_table(TABLE_SCHEMA, rows)
    table.append(data)


def _make_table_with_data(catalog, name: str, row_count: int = 100):
    """Create table and append *row_count* rows in a single batch."""
    tbl = _make_table(catalog, name)
    _append_rows(tbl, 0, row_count)
    return tbl


def _make_table_multi_append(catalog, name: str, appends: int = 5, rows_per: int = 20):
    """Create table with multiple small appends to produce multiple data files."""
    tbl = _make_table(catalog, name)
    for i in range(appends):
        _append_rows(tbl, i * rows_per, rows_per)
    return tbl


def _staged_compaction_files(table) -> list[str]:
    """Return every compaction-staged Parquet file under the table location."""
    root = _file_uri_to_local_path(table.location())
    found: list[str] = []
    for dirpath, _dirs, files in os.walk(root):
        found.extend(
            os.path.join(dirpath, f) for f in files if f.startswith("_iceberg_ops_compacted_")
        )
    return found


def _commit_position_delete(table, data_uri: str, positions: list[int]) -> str:
    """Commit a position-delete file for *data_uri*; return its local path.

    PyIceberg cannot write delete files itself, so this builds the
    delete Parquet file, a delete manifest and a manifest list by hand
    and commits them as a new ``main`` snapshot through a low-level
    transaction. It relies on PyIceberg internals (``ManifestWriterV2``,
    ``write_manifest_list``, ``Transaction._apply``) and needs a v2 table.
    """
    import pyarrow.parquet as pq
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
        ManifestContent,
        ManifestEntry,
        ManifestEntryStatus,
        ManifestWriterV2,
        write_manifest_list,
    )
    from pyiceberg.table.refs import SnapshotRefType
    from pyiceberg.table.snapshots import Operation, Snapshot, Summary
    from pyiceberg.table.update import (
        AddSnapshotUpdate,
        AssertRefSnapshotId,
        SetSnapshotRefUpdate,
    )
    from pyiceberg.typedef import Record

    class _DeleteManifestWriterV2(ManifestWriterV2):
        def content(self) -> ManifestContent:
            return ManifestContent.DELETES

        @property
        def _meta(self) -> dict[str, str]:
            return {**super()._meta, "content": "deletes"}

    table.refresh()
    metadata = table.metadata
    assert metadata.format_version == 2
    parent = table.current_snapshot()
    assert parent is not None
    io = table.io
    spec = table.spec()
    schema = table.schema()
    base = table.location().rstrip("/")
    tag = uuid.uuid4().hex

    delete_uri = f"{base}/data/{tag}-deletes.parquet"
    delete_local = _file_uri_to_local_path(delete_uri)
    delete_schema = pa.schema(
        [
            pa.field(
                "file_path",
                pa.string(),
                nullable=False,
                metadata={"PARQUET:field_id": "2147483546"},
            ),
            pa.field(
                "pos", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "2147483545"}
            ),
        ]
    )
    pq.write_table(
        pa.table(
            {"file_path": [data_uri] * len(positions), "pos": positions},
            schema=delete_schema,
        ),
        delete_local,
    )

    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=len(positions),
        file_size_in_bytes=os.path.getsize(delete_local),
    )
    delete_file.spec_id = spec.spec_id

    snapshot_id = metadata.new_snapshot_id()
    sequence_number = metadata.next_sequence_number()

    with _DeleteManifestWriterV2(
        spec, schema, io.new_output(f"{base}/metadata/{tag}-m0.avro"), snapshot_id, "deflate"
    ) as writer:
        writer.add(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=None,
                file_sequence_number=None,
                data_file=delete_file,
            )
        )
    delete_manifest = writer.to_manifest_file()

    manifest_list_uri = f"{base}/metadata/snap-{snapshot_id}-0-{tag}.avro"
    with write_manifest_list(
        format_version=2,
        output_file=io.new_output(manifest_list_uri),
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence_number,
        avro_compression="deflate",
    ) as list_writer:
        list_writer.add_manifests([*parent.manifests(io), delete_manifest])

    snapshot = Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence_number,
        manifest_list=manifest_list_uri,
        summary=Summary(Operation.DELETE),
        schema_id=schema.schema_id,
    )
    tx = table.transaction()
    tx._apply(
        (
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate(
                ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=snapshot_id
            ),
        ),
        (AssertRefSnapshotId(snapshot_id=parent.snapshot_id, ref="main"),),
    )
    tx.commit_transaction()
    table.refresh()
    return delete_local


# ---------------------------------------------------------------------------
# table_health_report
# ---------------------------------------------------------------------------


class TestTableHealthReport:
    def test_report_on_table_with_data(self, memory_catalog):
        """Report on a table with data shows nonzero file stats."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=50)
        report = table_health_report(tbl)
        assert isinstance(report, TableHealthReport)
        assert report.total_files > 0
        assert report.total_size_bytes > 0
        assert report.avg_file_size_bytes > 0
        assert report.min_file_size_bytes > 0
        assert report.max_file_size_bytes > 0

    def test_report_on_empty_table(self, memory_catalog):
        """Report on a table with no data returns zero file counts."""
        tbl = _make_table(memory_catalog, "ns.empty")
        report = table_health_report(tbl)
        assert report.total_files == 0
        assert report.total_size_bytes == 0
        assert report.small_file_count == 0

    def test_report_snapshot_count_matches(self, memory_catalog):
        """Report reflects the correct number of snapshots."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=4)
        report = table_health_report(tbl)
        # 4 appends = 4 snapshots
        assert report.snapshot_count == 4

    def test_report_small_file_detection(self, memory_catalog):
        """Small files are counted at the given threshold."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        # Test files are ~1 KB each, threshold=100 MB -> all are "small".
        report = table_health_report(tbl, small_file_threshold_mb=100)
        assert report.small_file_count == report.total_files
        assert report.small_file_percentage == 100.0

    def test_report_recommendations_for_small_files(self, memory_catalog):
        """Recommendations include compaction when small file % is high."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        report = table_health_report(tbl, small_file_threshold_mb=100)
        assert any("compaction" in r.lower() for r in report.recommendations)

    def test_report_on_empty_table_recommends_append(self, memory_catalog):
        """Empty table gets a recommendation to append data."""
        tbl = _make_table(memory_catalog, "ns.empty")
        report = table_health_report(tbl)
        assert any("no data files" in r.lower() for r in report.recommendations)

    def test_report_returns_all_fields(self, memory_catalog):
        """All dataclass fields are populated (not None)."""
        tbl = _make_table_with_data(memory_catalog, "ns.t")
        report = table_health_report(tbl)
        assert report.total_files is not None
        assert report.snapshot_count is not None
        assert report.oldest_snapshot_age_hours is not None
        assert report.partition_field_count is not None
        assert isinstance(report.recommendations, list)

    def test_report_partition_field_count(self, memory_catalog):
        """Partition field count reflects the spec."""
        from pyiceberg.transforms import DayTransform

        ns = "ns"
        memory_catalog.create_namespace_if_not_exists(ns)
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("ts", pa.timestamp("us", tz="UTC")),
            ]
        )
        tbl = memory_catalog.create_table("ns.parts", schema=schema)
        with tbl.update_spec() as update:
            update.add_field("ts", DayTransform(), "ts_day")

        report = table_health_report(tbl)
        assert report.partition_field_count == 1

    def test_report_byte_identical_to_to_pylist_implementation(self, memory_catalog):
        """Vectorised file-stats must agree exactly with a ``to_pylist`` baseline.

        Computes the reference values by materialising
        ``files_arrow.column("file_size_in_bytes").to_pylist()`` and
        comparing each numeric field of the report. Any drift here
        would mean the :mod:`pyarrow.compute` implementation changed
        user-visible output.
        """
        tbl = _make_table_multi_append(memory_catalog, "ns.equiv", appends=4, rows_per=10)

        files_arrow = tbl.inspect.files()
        ref_sizes = files_arrow.column("file_size_in_bytes").to_pylist()
        ref_total_files = len(ref_sizes)
        ref_total_size_bytes = sum(ref_sizes)
        ref_min = min(ref_sizes)
        ref_max = max(ref_sizes)
        ref_avg = ref_total_size_bytes / ref_total_files
        threshold_mb = 100
        threshold_bytes = threshold_mb * 1024 * 1024
        ref_small_count = sum(1 for s in ref_sizes if s < threshold_bytes)
        ref_small_pct = (ref_small_count / ref_total_files) * 100.0

        report = table_health_report(tbl, small_file_threshold_mb=threshold_mb)

        assert report.total_files == ref_total_files
        assert report.total_size_bytes == ref_total_size_bytes
        assert report.min_file_size_bytes == ref_min
        assert report.max_file_size_bytes == ref_max
        assert report.avg_file_size_bytes == ref_avg
        assert report.small_file_count == ref_small_count
        assert report.small_file_percentage == ref_small_pct

    @pytest.mark.bench
    def test_health_report_handles_million_files(self, memory_catalog, monkeypatch):
        """Vectorised path runs over 1 M synthesised file rows in well under 5 s.

        Synthesising 1 M *real* Parquet files is impractical, so this
        replaces ``Table.inspect`` at the class level with a stub that
        returns a 1 M-row Arrow table. The wall-clock ceiling is loose
        and only fails if a future change reintroduces an O(n)
        Python-loop fallback. Skipped by default; run with
        ``pytest -m bench``.
        """
        from iceberg_ops.maintenance.advisor import table_health_report as thr

        tbl = _make_table_with_data(memory_catalog, "ns.bench", row_count=10)

        n = 1_000_000
        synthetic_sizes = pa.array([1024 * 1024] * n, type=pa.int64())
        fake_files = pa.table({"file_size_in_bytes": synthetic_sizes})

        class _StubInspect:
            def files(self):
                return fake_files

        # ``Table.inspect`` is a class-level property; replace at the
        # class so any access returns our stub. monkeypatch restores it
        # at test teardown.
        monkeypatch.setattr(type(tbl), "inspect", property(lambda self: _StubInspect()))

        start = time.perf_counter()
        report = thr(tbl, small_file_threshold_mb=2)
        elapsed = time.perf_counter() - start

        assert report.total_files == n
        assert report.total_size_bytes == n * 1024 * 1024
        # Every synthetic file is exactly 1 MiB and threshold is 2 MiB.
        assert report.small_file_count == n
        assert elapsed < 5.0, f"vectorised health report took {elapsed:.2f}s on 1 M files"


# ---------------------------------------------------------------------------
# expire_snapshots
# ---------------------------------------------------------------------------


class TestExpireSnapshots:
    def test_expire_no_candidates(self, memory_catalog):
        """Fresh snapshots are not expired when max_age is very large."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        result = expire_snapshots(tbl, max_age_hours=99999, min_snapshots=1)
        assert isinstance(result, SnapshotExpiryResult)
        assert result.expired_count == 0
        assert result.total_snapshots == 3
        assert result.retained_count == 3

    def test_expire_respects_min_snapshots(self, memory_catalog):
        """Even with max_age=0, at least min_snapshots are kept."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=4)
        # max_age_hours=0 means everything is a candidate, but keep at least 2.
        result = expire_snapshots(tbl, max_age_hours=0, min_snapshots=2)
        tbl.refresh()
        remaining = len(tbl.metadata.snapshots)
        assert remaining >= 2
        assert result.expired_count == 2
        assert result.retained_count == 2

    def test_expire_dry_run_does_not_delete(self, memory_catalog):
        """Dry-run reports candidates but does not expire them."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        result = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1, dry_run=True)
        tbl.refresh()
        # Snapshots should still be there.
        assert len(tbl.metadata.snapshots) == 3
        assert result.expired_count == 2  # 3 total, keep 1 -> expire 2

    def test_expire_all_but_one(self, memory_catalog):
        """Expire all but the latest snapshot."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5)
        result = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1)
        tbl.refresh()
        assert len(tbl.metadata.snapshots) == 1
        assert result.expired_count == 4

    def test_expire_returns_ids(self, memory_catalog):
        """Expired snapshot IDs are returned in the result."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        before_ids = {s.snapshot_id for s in tbl.metadata.snapshots}
        result = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1)
        assert len(result.expired_ids) == 2
        assert set(result.expired_ids).issubset(before_ids)

    def test_tagged_snapshot_is_kept(self, memory_catalog):
        """A tagged snapshot is excluded from dry-run and real runs alike."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=4)
        oldest = min(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms).snapshot_id
        tbl.manage_snapshots().create_tag(oldest, "keep-me").commit()

        dry = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1, dry_run=True)
        assert oldest not in dry.expired_ids
        assert dry.protected_ids == [oldest]
        assert dry.expired_count == 2

        real = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1)
        assert real.expired_ids == dry.expired_ids
        tbl.refresh()
        remaining = {s.snapshot_id for s in tbl.metadata.snapshots}
        assert oldest in remaining
        assert len(remaining) == 2

    def test_expiry_after_rollback(self, memory_catalog):
        """After a rollback the (older) current snapshot is never expired."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=4)
        ordered = sorted(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms)
        target = ordered[1].snapshot_id
        tbl.manage_snapshots().set_current_snapshot(target).commit()

        result = expire_snapshots(tbl, max_age_hours=0, min_snapshots=1)

        tbl.refresh()
        assert target not in result.expired_ids
        assert tbl.current_snapshot().snapshot_id == target  # type: ignore[union-attr]
        assert tbl.scan().to_arrow().num_rows == 40

    def test_min_snapshots_zero_raises(self, memory_catalog):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=2)
        with pytest.raises(ValueError, match="min_snapshots"):
            expire_snapshots(tbl, max_age_hours=0, min_snapshots=0)

    def test_negative_age_raises(self, memory_catalog):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=2)
        with pytest.raises(ValueError, match="max_age_hours"):
            expire_snapshots(tbl, max_age_hours=-1, min_snapshots=1)

    def test_arguments_are_keyword_only(self, memory_catalog):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=2)
        with pytest.raises(TypeError):
            expire_snapshots(tbl, 1, 1)  # type: ignore[misc]

    def test_fractional_age_hours_accepted(self, memory_catalog):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        result = expire_snapshots(tbl, max_age_hours=0.5, min_snapshots=1, dry_run=True)
        assert result.expired_count == 0  # all snapshots are seconds old

    def test_defaults_come_from_table_properties(self, memory_catalog):
        """``history.expire.*`` properties drive expiry when arguments are omitted."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5)
        with tbl.transaction() as tx:
            tx.set_properties(
                {
                    "history.expire.min-snapshots-to-keep": "3",
                    "history.expire.max-snapshot-age-ms": "0",
                }
            )

        result = expire_snapshots(tbl)

        tbl.refresh()
        assert result.expired_count == 2
        assert len(tbl.metadata.snapshots) == 3

    def test_explicit_arguments_override_table_properties(self, memory_catalog):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5)
        with tbl.transaction() as tx:
            tx.set_properties(
                {
                    "history.expire.min-snapshots-to-keep": "3",
                    "history.expire.max-snapshot-age-ms": "0",
                }
            )

        kept_by_count = expire_snapshots(tbl, min_snapshots=4, dry_run=True)
        assert kept_by_count.expired_count == 1
        kept_by_age = expire_snapshots(tbl, max_age_hours=99999, dry_run=True)
        assert kept_by_age.expired_count == 0

    def test_unset_properties_keep_iceberg_defaults(self, memory_catalog):
        """Without properties the defaults are 5 days / 1 snapshot: fresh
        snapshots survive."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        result = expire_snapshots(tbl, dry_run=True)
        assert result.expired_count == 0

    def test_set_snapshot_retention_round_trip(self, memory_catalog):
        """Retention declared by a migration is honoured by expiry."""
        from iceberg_ops.migration import MigrationContext

        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5)
        ctx = MigrationContext(
            catalog=memory_catalog, namespace="ns", version="0001", description="retention"
        )
        ctx.set_snapshot_retention("t", min_snapshots=4, max_age_hours=0)

        result = expire_snapshots(tbl)

        tbl.refresh()
        assert result.expired_count == 1
        assert len(tbl.metadata.snapshots) == 4

    @pytest.mark.parametrize("dry_run", [True, False])
    def test_long_max_age_property_expires_nothing(self, memory_catalog, dry_run):
        """Java's Long.MAX_VALUE ("never expire by age") must not overflow."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        with tbl.transaction() as tx:
            tx.set_properties({"history.expire.max-snapshot-age-ms": str(2**63 - 1)})

        result = expire_snapshots(tbl, min_snapshots=1, dry_run=dry_run)

        tbl.refresh()
        assert result.expired_count == 0
        assert len(tbl.metadata.snapshots) == 3

    @pytest.mark.parametrize("max_age_hours", [10**15, 1e300, float("inf")])
    def test_huge_explicit_max_age_expires_nothing(self, memory_catalog, max_age_hours):
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        result = expire_snapshots(tbl, max_age_hours=max_age_hours, min_snapshots=1, dry_run=True)
        assert result.expired_count == 0
        assert result.retained_count == 3


# ---------------------------------------------------------------------------
# compact_small_files_streaming
# ---------------------------------------------------------------------------


class TestCompactSmallFilesStreaming:
    def test_compact_dry_run(self, memory_catalog):
        """Dry-run reports file counts but does not rewrite."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6)
        tbl.refresh()
        files_before = len(list(tbl.scan().plan_files()))
        result = compact_small_files_streaming(
            tbl, target_size_mb=512, min_file_count=5, dry_run=True
        )
        assert isinstance(result, CompactionResult)
        assert result.files_rewritten == 6  # all are small
        tbl.refresh()
        files_after = len(list(tbl.scan().plan_files()))
        assert files_after == files_before  # no change

    def test_compact_below_min_count_skips(self, memory_catalog):
        """Compaction is skipped when there are fewer small files than min_file_count."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=2)
        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=5)
        assert result.skipped is True
        assert result.files_rewritten == 0

    def test_compact_actually_reduces_files(self, memory_catalog):
        """Actual compaction reduces the number of data files."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        tbl.refresh()
        files_before = len(list(tbl.scan().plan_files()))
        assert files_before == 6

        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)
        assert result.skipped is False
        assert result.files_after < result.files_before

        # Data should still be intact.
        tbl.refresh()
        all_data = tbl.scan().to_arrow()
        assert all_data.num_rows == 60  # 6 * 10

    def test_compact_preserves_data(self, memory_catalog):
        """Compaction does not lose any rows."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5, rows_per=20)
        compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)
        tbl.refresh()
        all_data = tbl.scan().to_arrow()
        assert all_data.num_rows == 100

    def test_compact_result_fields(self, memory_catalog):
        """CompactionResult has correct before/after statistics."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=5)
        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)
        assert result.files_before == 5
        assert result.bytes_before > 0
        assert result.bytes_after > 0
        assert result.files_after >= 1

    def test_refuses_when_above_limit(self, memory_catalog):
        """Optional ``max_table_size_bytes`` guard raises when exceeded."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        with pytest.raises(RuntimeError, match="refuses to run"):
            compact_small_files_streaming(
                tbl,
                target_size_mb=512,
                min_file_count=3,
                max_table_size_bytes=1,
            )

    def test_guard_disabled_with_none(self, memory_catalog):
        """``max_table_size_bytes=None`` (the default) disables the guard entirely."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        result = compact_small_files_streaming(
            tbl,
            target_size_mb=512,
            min_file_count=3,
            max_table_size_bytes=None,
        )
        assert result.skipped is False
        assert result.files_after < result.files_before

    def test_guard_does_not_block_dry_run(self, memory_catalog):
        """Dry-run path is not blocked by the guard (no rewrite happens)."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        result = compact_small_files_streaming(
            tbl,
            target_size_mb=512,
            min_file_count=3,
            max_table_size_bytes=1,
            dry_run=True,
        )
        assert result.files_rewritten == 6

    def test_no_batch_rows_parameter(self):
        """The unused ``batch_rows`` knob is not part of the signature."""
        params = inspect.signature(compact_small_files_streaming).parameters
        assert "batch_rows" not in params

    def test_commit_state_unknown_keeps_staged_files(self, memory_catalog, monkeypatch):
        """If the commit lands but reports an unknown state, the staged
        files are kept so the table stays readable."""
        from pyiceberg.exceptions import CommitStateUnknownException

        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        original = memory_catalog.commit_table

        def lands_then_unknown(*args, **kwargs):
            original(*args, **kwargs)
            raise CommitStateUnknownException("lost response")

        monkeypatch.setattr(memory_catalog, "commit_table", lands_then_unknown)
        with pytest.raises(CommitStateUnknownException):
            compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)
        monkeypatch.undo()

        tbl.refresh()
        assert tbl.scan().to_arrow().num_rows == 60
        assert _staged_compaction_files(tbl)

    def test_commit_failed_removes_staged_files(self, memory_catalog, monkeypatch):
        """A definitive commit rejection removes the staged files and
        leaves the table untouched."""
        from pyiceberg.exceptions import CommitFailedException

        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=6, rows_per=10)
        # No retries: the rejection below is permanent.
        with tbl.transaction() as tx:
            tx.set_properties({"commit.retry.num-retries": "0"})
        tbl.refresh()
        snapshot_before = tbl.current_snapshot().snapshot_id  # type: ignore[union-attr]

        def rejects(*_args, **_kwargs):
            raise CommitFailedException("conflict")

        monkeypatch.setattr(memory_catalog, "commit_table", rejects)
        with pytest.raises(CommitFailedException):
            compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)
        monkeypatch.undo()

        tbl.refresh()
        assert tbl.current_snapshot().snapshot_id == snapshot_before  # type: ignore[union-attr]
        assert tbl.scan().to_arrow().num_rows == 60
        assert _staged_compaction_files(tbl) == []

    def test_staging_failure_removes_staged_files(self, memory_catalog, monkeypatch):
        """A failure while staging removes every file staged so far."""
        from iceberg_ops.maintenance import compaction as compaction_mod

        tbl = _make_partitioned_table(memory_catalog, "ns.parted_fail")
        for i in range(3):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
            _append_partition_rows(tbl, "us", start=i * 10 + 100, count=10)
        tbl.refresh()
        snapshot_before = tbl.current_snapshot().snapshot_id  # type: ignore[union-attr]

        real_flush = compaction_mod._flush_buffer
        calls = {"n": 0}

        def flaky_flush(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("disk full")
            return real_flush(*args, **kwargs)

        monkeypatch.setattr(compaction_mod, "_flush_buffer", flaky_flush)
        with pytest.raises(OSError, match="disk full"):
            compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)

        assert calls["n"] == 2
        assert _staged_compaction_files(tbl) == []
        tbl.refresh()
        assert tbl.current_snapshot().snapshot_id == snapshot_before  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# compact_small_files_streaming - partitioned + sort-order + memory tests
# ---------------------------------------------------------------------------


_PARTITIONED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32()),
        pa.field("value", pa.string()),
        pa.field("region", pa.string()),
    ]
)


def _make_partitioned_table(catalog, name: str = "ns.parted"):
    """Create a region-partitioned Iceberg table (Identity transform)."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField, StringType

    ns = name.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(ns)
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "value", StringType(), required=False),
        NestedField(3, "region", StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="region"),
    )
    return catalog.create_table(name, schema=schema, partition_spec=spec)


def _append_partition_rows(table, region: str, start: int, count: int) -> None:
    rows = pa.table(
        {
            "id": list(range(start, start + count)),
            "value": [f"r{i}" for i in range(start, start + count)],
            "region": [region] * count,
        },
        schema=_PARTITIONED_ARROW_SCHEMA,
    )
    table.append(rows)


class TestCompactSmallFilesStreamingPartitioned:
    """Partition-aware streaming compaction behaviours."""

    def test_partitioned_table_compacts_each_partition_to_one_file(self, memory_catalog):
        """8 small files (4 per partition) + min_file_count=3 -> after
        compaction each partition holds exactly 1 file (8 -> 2)."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_a")
        for i in range(4):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
            _append_partition_rows(tbl, "us", start=i * 10 + 100, count=10)
        tbl.refresh()
        assert tbl.inspect.files().num_rows == 8

        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)

        tbl.refresh()
        assert result.skipped is False
        assert result.files_before == 8
        assert tbl.inspect.files().num_rows == 2  # one per partition
        assert result.files_rewritten == 8

    def test_partitioned_compaction_only_touches_qualifying_partitions(self, memory_catalog):
        """If only ``eu`` has enough small files, ``us`` is left alone."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_b")
        for i in range(5):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
        # Only 2 us files -> below min_file_count=3 -> not touched.
        for i in range(2):
            _append_partition_rows(tbl, "us", start=i * 10 + 200, count=10)
        tbl.refresh()
        assert tbl.inspect.files().num_rows == 7

        compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)

        tbl.refresh()
        files = tbl.inspect.files().to_pylist()
        eu_files = [f for f in files if f["partition"]["region"] == "eu"]
        us_files = [f for f in files if f["partition"]["region"] == "us"]
        assert len(eu_files) == 1  # compacted
        assert len(us_files) == 2  # untouched

    def test_partitioned_compaction_preserves_all_rows(self, memory_catalog):
        """Row count is identical before and after, per partition."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_c")
        for i in range(4):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
            _append_partition_rows(tbl, "us", start=i * 10 + 100, count=10)
        tbl.refresh()
        before = tbl.scan().to_arrow()
        eu_rows_before = before.filter(pc.equal(before["region"], "eu")).num_rows
        us_rows_before = before.filter(pc.equal(before["region"], "us")).num_rows

        compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)

        tbl.refresh()
        after = tbl.scan().to_arrow()
        assert after.num_rows == before.num_rows
        assert after.filter(pc.equal(after["region"], "eu")).num_rows == eu_rows_before
        assert after.filter(pc.equal(after["region"], "us")).num_rows == us_rows_before
        # Identity check on ids - sums of the integer column must match.
        assert pc.sum(after["id"]).as_py() == pc.sum(before["id"]).as_py()

    def test_streaming_compact_respects_sort_order(self, memory_catalog):
        """When the table has a sort order, compacted output is pre-sorted."""
        from pyiceberg.transforms import IdentityTransform

        tbl = _make_partitioned_table(memory_catalog, "ns.parted_sorted")
        # Apply a descending sort order on `id` via the public PyIceberg API.
        with tbl.update_sort_order() as upd:
            upd.desc("id", IdentityTransform())
        tbl.refresh()
        assert len(tbl.sort_order().fields) == 1

        # Append unsorted ids across appends.
        _append_partition_rows(tbl, "eu", start=0, count=10)
        _append_partition_rows(tbl, "eu", start=20, count=10)
        _append_partition_rows(tbl, "eu", start=10, count=10)
        _append_partition_rows(tbl, "eu", start=40, count=10)
        _append_partition_rows(tbl, "eu", start=30, count=10)
        tbl.refresh()

        compact_small_files_streaming(
            tbl, target_size_mb=512, min_file_count=3, sort_by_iceberg_order=True
        )

        tbl.refresh()
        # Read the single compacted file's rows in disk order (no extra sort).
        ids = tbl.scan().to_arrow().column("id").to_pylist()
        # Pre-sort would put ids in [49, 48, ..., 0] (descending).
        assert ids == sorted(ids, reverse=True)

    def test_streaming_compact_dry_run_no_writes(self, memory_catalog):
        """Dry-run reports stats but does not write any new file."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_dry")
        for i in range(4):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
        tbl.refresh()
        files_before = tbl.inspect.files().to_pylist()

        result = compact_small_files_streaming(
            tbl, target_size_mb=512, min_file_count=3, dry_run=True
        )

        tbl.refresh()
        files_after = tbl.inspect.files().to_pylist()
        assert result.files_rewritten == 4
        assert result.files_before == 4
        assert result.files_after == 4  # dry-run reports same count
        # File paths must be unchanged - no rewrite happened.
        assert {f["file_path"] for f in files_after} == {f["file_path"] for f in files_before}

    def test_streaming_compact_memory_regression_ceiling(self, memory_catalog):
        """Loose tracemalloc regression ceiling on a small table.

        This is not a memory bound: PyIceberg's batch reader decodes
        whole source files, so peak memory grows with the files per
        partition. The ceiling only catches gross regressions."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_mem")
        # 10 small files x ~10 KB each ~= 100 KB of source data per partition.
        for i in range(10):
            _append_partition_rows(tbl, "eu", start=i * 100, count=100)
        tbl.refresh()

        target_mb = 4  # tiny target so the ceiling is meaningful

        tracemalloc.start()
        try:
            compact_small_files_streaming(tbl, target_size_mb=target_mb, min_file_count=3)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        # Loose regression ceiling for this tiny table only.
        ceiling_bytes = max(target_mb * 1024 * 1024 * 2, 32 * 1024 * 1024)
        assert peak < ceiling_bytes, (
            f"streaming compaction peak {peak} bytes exceeds {ceiling_bytes}-byte ceiling"
        )

    def test_streaming_compact_unpartitioned_table_with_threshold(self, memory_catalog):
        """Unpartitioned table follows the same algorithm with a single
        partition group keyed by the empty struct."""
        tbl = _make_table_multi_append(memory_catalog, "ns.unparted_stream", appends=8)
        tbl.refresh()
        before_count = tbl.inspect.files().num_rows
        assert before_count == 8

        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=3)

        tbl.refresh()
        assert result.skipped is False
        assert result.files_before == 8
        assert tbl.inspect.files().num_rows < before_count

    def test_streaming_compact_rejects_invalid_target_size(self, memory_catalog):
        """``target_size_mb`` must be positive."""
        tbl = _make_table_multi_append(memory_catalog, "ns.invalid", appends=2)
        with pytest.raises(ValueError, match="positive"):
            compact_small_files_streaming(tbl, target_size_mb=0, min_file_count=1)
        with pytest.raises(ValueError, match="positive"):
            compact_small_files_streaming(tbl, target_size_mb=-1, min_file_count=1)

    def test_streaming_compact_non_local_scheme_raises(self, memory_catalog, monkeypatch):
        """``s3://`` (or other non-local) locations raise NotImplementedError."""
        tbl = _make_table_with_data(memory_catalog, "ns.s3_stub", row_count=5)
        monkeypatch.setattr(tbl, "location", lambda: "s3://bucket/path", raising=False)
        with pytest.raises(NotImplementedError, match="s3"):
            compact_small_files_streaming(tbl)

    @pytest.mark.parametrize(
        "region",
        ["../../x", "../../../../escaped", "a/b", "a#b", "C#", "50%", "a b", "x/C:\\evil", "q?"],
    )
    def test_partition_value_stays_inside_table_data(self, memory_catalog, tmp_path, region):
        """Identity values are escaped like PyIceberg's own layout: a
        value with ``..``, ``/``, ``#`` or a drive cannot move the
        staged file out of ``<table>/data``, and compaction succeeds."""
        tbl = _make_partitioned_table(memory_catalog, "ns.parted_escape")
        for i in range(5):
            _append_partition_rows(tbl, region, start=i * 10, count=10)
        tbl.refresh()

        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=5)

        tbl.refresh()
        assert result.skipped is False
        assert result.files_after == 1
        data_root = os.path.realpath(os.path.join(_file_uri_to_local_path(tbl.location()), "data"))
        for uri in tbl.inspect.files().column("file_path").to_pylist():
            local = os.path.realpath(_file_uri_to_local_path(uri))
            assert os.path.commonpath([data_root, local]) == data_root, local
            assert os.path.exists(local)
        compacted = [
            os.path.join(dirpath, f)
            for dirpath, _dirs, files in os.walk(tmp_path)
            for f in files
            if f.startswith("_iceberg_ops_compacted_")
        ]
        assert len(compacted) == 1
        assert os.path.commonpath([data_root, os.path.realpath(compacted[0])]) == data_root
        rows = tbl.scan().to_arrow()
        assert rows.num_rows == 50
        assert set(rows.column("region").to_pylist()) == {region}

    def test_staging_dir_outside_data_is_refused(self, memory_catalog, tmp_path, monkeypatch):
        """The containment guard refuses a staging directory outside
        ``<table>/data`` before writing anything, and commits nothing."""
        from iceberg_ops.maintenance import compaction

        tbl = _make_partitioned_table(memory_catalog, "ns.parted_guard")
        for i in range(5):
            _append_partition_rows(tbl, "eu", start=i * 10, count=10)
        tbl.refresh()
        files_before = set(tbl.inspect.files().column("file_path").to_pylist())
        monkeypatch.setattr(
            compaction, "_partition_dir_segment", lambda *_a: "../../escaped_by_guard"
        )

        with pytest.raises(ValueError, match="outside the table's data directory"):
            compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=5)

        tbl.refresh()
        assert set(tbl.inspect.files().column("file_path").to_pylist()) == files_before
        assert not any("escaped_by_guard" in dirpath for dirpath, _d, _f in os.walk(tmp_path))
        assert tbl.scan().to_arrow().num_rows == 50


# ---------------------------------------------------------------------------
# _iter_file_records (streaming inspect.files() iterator)
# ---------------------------------------------------------------------------


class TestIterFileRecords:
    """Tests for the private streaming file-metadata iterator."""

    def test_yields_all_files(self, memory_catalog):
        """Iterating yields one record per data file, with the expected keys."""
        from iceberg_ops.maintenance.compaction import _iter_file_records

        tbl = _make_table_multi_append(memory_catalog, "ns.iter", appends=4, rows_per=5)
        tbl.refresh()
        records = list(_iter_file_records(tbl))
        assert len(records) == 4
        for rec in records:
            assert set(rec.keys()) == {
                "path",
                "size_bytes",
                "record_count",
                "partition",
                "spec_id",
            }
            assert isinstance(rec["path"], str)
            assert int(rec["size_bytes"]) > 0
            assert int(rec["record_count"]) == 5
            assert rec["partition"] == {}  # unpartitioned table
            assert rec["spec_id"] == 0

    def test_is_a_generator(self, memory_catalog):
        """The helper returns a generator object, not a list, so it does
        not hold a second Python copy of the file list."""
        import inspect as _inspect

        from iceberg_ops.maintenance.compaction import _iter_file_records

        tbl = _make_table_multi_append(memory_catalog, "ns.iter", appends=2, rows_per=5)
        tbl.refresh()
        it = _iter_file_records(tbl)
        assert _inspect.isgenerator(it)
        assert not isinstance(it, list)

    def test_respects_batch_size(self, memory_catalog):
        """A small ``batch_size`` still yields every record correctly."""
        from iceberg_ops.maintenance.compaction import _iter_file_records

        tbl = _make_table_multi_append(memory_catalog, "ns.iter", appends=5, rows_per=4)
        tbl.refresh()
        # batch_size=2 forces multiple Arrow batches over the 5 rows.
        records = list(_iter_file_records(tbl, batch_size=2))
        assert len(records) == 5

    def test_compact_small_files_handles_huge_inspect_via_streaming(
        self, memory_catalog, monkeypatch
    ):
        """``compact_small_files_streaming(dry_run=True)`` does NOT
        materialise the full file list. Forced via a stub ``inspect``
        that returns a 10 001-row synthetic Arrow table - the
        assertion proves the streaming path flows through
        :func:`_iter_file_records` end-to-end (no ``to_pylist()`` on
        the file column).
        """
        tbl = _make_table_with_data(memory_catalog, "ns.huge", row_count=10)

        n = 10_001
        partition_struct_type = pa.struct([])
        synthetic = pa.table(
            {
                "file_path": pa.array([f"s3://fake/{i:08d}.parquet" for i in range(n)]),
                "file_size_in_bytes": pa.array([1024 * 1024] * n, type=pa.int64()),
                "record_count": pa.array([100] * n, type=pa.int64()),
                "partition": pa.array([{}] * n, type=partition_struct_type),
                "spec_id": pa.array([0] * n, type=pa.int32()),
            }
        )

        class _StubInspect:
            def files(self):
                return synthetic

        monkeypatch.setattr(type(tbl), "inspect", property(lambda self: _StubInspect()))

        result = compact_small_files_streaming(
            tbl,
            target_size_mb=512,
            min_file_count=3,
            dry_run=True,
        )
        assert result.files_before == n
        assert result.bytes_before == n * 1024 * 1024
        assert result.files_rewritten == n  # every file is below the 512 MB threshold


# ---------------------------------------------------------------------------
# remove_orphan_files
# ---------------------------------------------------------------------------


def _table_local_dir(table) -> str:
    """Resolve the table's location to a local filesystem path."""
    return _file_uri_to_local_path(table.location())


def _backdate(path: str, hours: float = 96) -> None:
    """Set *path*'s mtime ``hours`` hours in the past so age-cutoff
    tests don't depend on filesystem timestamp resolution."""
    past = time.time() - hours * 3600
    os.utime(path, (past, past))


def _drop_synthetic_orphan(directory: str, name: str, hours_old: float = 96) -> str:
    """Create a stub Iceberg-style file at ``directory/name``, backdated."""
    os.makedirs(directory, exist_ok=True)
    full = os.path.join(directory, name)
    with open(full, "wb") as fh:
        fh.write(b"orphan-stub")
    _backdate(full, hours=hours_old)
    return full


class TestRemoveOrphanFiles:
    def test_no_orphans_on_clean_table(self, memory_catalog):
        """A freshly created table with data has zero orphan files."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=50)
        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=True)
        assert isinstance(result, OrphanCleanupResult)
        assert result.orphan_count == 0

    def test_dry_run_does_not_delete(self, memory_catalog):
        """Dry-run never deletes files, even when orphans exist."""
        tbl = _make_table_with_data(memory_catalog, "ns.t")
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        orphan = _drop_synthetic_orphan(data_dir, "00000-9-deadbeef.parquet")

        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=True)

        assert result.deleted is False
        assert result.orphan_count >= 1
        assert os.path.exists(orphan)

    def test_empty_table_no_orphans(self, memory_catalog):
        """Empty table has only its boot metadata.json - no orphans."""
        tbl = _make_table(memory_catalog, "ns.empty")
        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=True)
        assert result.orphan_count == 0

    def test_referenced_count_includes_metadata(self, memory_catalog):
        """Referenced count covers data files + manifests + manifest
        lists + metadata JSONs - strictly more than the data-file
        count."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        tbl.refresh()
        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=True)
        data_file_count = len(list(tbl.scan().plan_files()))
        # data files + at least one manifest list + one metadata.json
        assert result.referenced_count > data_file_count

    def test_keeps_live_position_delete_file(self, memory_catalog):
        """A position-delete file referenced by a delete manifest survives."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        data_uri = next(iter(tbl.scan().plan_files())).file.file_path
        delete_local = _commit_position_delete(tbl, data_uri, positions=[0, 1, 2, 3, 4])
        assert tbl.scan().to_arrow().num_rows == 5
        _backdate(delete_local, hours=96)

        result = remove_orphan_files(tbl, older_than_hours=1, dry_run=False)

        assert delete_local not in result.orphan_paths
        assert os.path.exists(delete_local)
        tbl.refresh()
        assert tbl.scan().to_arrow().num_rows == 5

    def test_keeps_statistics_files(self, memory_catalog):
        """Registered table and partition statistics files survive."""
        from pyiceberg.table.statistics import PartitionStatisticsFile, StatisticsFile
        from pyiceberg.table.update import SetPartitionStatisticsUpdate

        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        snapshot_id = tbl.current_snapshot().snapshot_id  # type: ignore[union-attr]
        base = tbl.location().rstrip("/")
        metadata_dir = os.path.join(_table_local_dir(tbl), "metadata")
        puffin = _drop_synthetic_orphan(metadata_dir, f"{uuid.uuid4().hex}.stats.puffin")
        part_stats = _drop_synthetic_orphan(
            metadata_dir, f"partition-stats-{uuid.uuid4().hex}.parquet"
        )

        with tbl.update_statistics() as update:
            update.set_statistics(
                StatisticsFile(
                    snapshot_id=snapshot_id,
                    statistics_path=f"{base}/metadata/{os.path.basename(puffin)}",
                    file_size_in_bytes=os.path.getsize(puffin),
                    file_footer_size_in_bytes=0,
                    blob_metadata=[],
                )
            )
        tx = tbl.transaction()
        tx._apply(
            (
                SetPartitionStatisticsUpdate(
                    partition_statistics=PartitionStatisticsFile(
                        snapshot_id=snapshot_id,
                        statistics_path=f"{base}/metadata/{os.path.basename(part_stats)}",
                        file_size_in_bytes=os.path.getsize(part_stats),
                    )
                ),
            )
        )
        tx.commit_transaction()
        tbl.refresh()
        assert tbl.metadata.statistics and tbl.metadata.partition_statistics

        result = remove_orphan_files(tbl, older_than_hours=1, dry_run=False)

        assert os.path.exists(puffin)
        assert os.path.exists(part_stats)
        assert puffin not in result.orphan_paths
        assert part_stats not in result.orphan_paths

    def test_default_is_dry_run(self, memory_catalog):
        """Without ``dry_run=False`` nothing is deleted."""
        tbl = _make_table_with_data(memory_catalog, "ns.t")
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        orphan = _drop_synthetic_orphan(data_dir, "00000-9-default.parquet")

        result = remove_orphan_files(tbl, older_than_hours=1)

        assert result.deleted is False
        assert orphan in result.orphan_paths
        assert os.path.exists(orphan)

    @pytest.mark.parametrize("dry_run", [True, False])
    def test_refuses_when_gc_disabled(self, memory_catalog, dry_run):
        tbl = _make_table_with_data(memory_catalog, "ns.t")
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        orphan = _drop_synthetic_orphan(data_dir, "00000-9-shared.parquet")
        with tbl.transaction() as tx:
            tx.set_properties({"gc.enabled": "false"})

        with pytest.raises(ValueError, match=r"gc\.enabled=false"):
            remove_orphan_files(tbl, older_than_hours=1, dry_run=dry_run)
        assert os.path.exists(orphan)

    @pytest.mark.parametrize("sweep", [("../other/data",), ("data/../../x",)])
    def test_sweep_escaping_table_root_raises(self, memory_catalog, sweep):
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        with pytest.raises(ValueError, match="outside the table location"):
            remove_orphan_files(tbl, older_than_hours=1, sweep=sweep)

    def test_absolute_sweep_entry_raises(self, memory_catalog, tmp_path):
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        with pytest.raises(ValueError, match="relative"):
            remove_orphan_files(tbl, older_than_hours=1, sweep=(str(tmp_path),))

    def test_negative_age_raises(self, memory_catalog):
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        with pytest.raises(ValueError, match="older_than_hours"):
            remove_orphan_files(tbl, older_than_hours=-1)

    def test_failed_delete_is_reported(self, memory_catalog, monkeypatch):
        """A path whose removal fails lands in ``failed_paths``; the rest
        are still deleted."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        stuck = _drop_synthetic_orphan(data_dir, "00000-9-stuck.parquet")
        gone = _drop_synthetic_orphan(data_dir, "00000-9-gone.parquet")

        real_remove = os.remove

        def flaky_remove(path, *args, **kwargs):
            if os.path.basename(path) == os.path.basename(stuck):
                raise PermissionError("locked")
            return real_remove(path, *args, **kwargs)

        monkeypatch.setattr(os, "remove", flaky_remove)
        result = remove_orphan_files(tbl, older_than_hours=1, dry_run=False)
        monkeypatch.undo()

        assert result.failed_paths == [stuck]
        assert result.orphan_count == 2
        assert result.deleted is True
        assert os.path.exists(stuck)
        assert not os.path.exists(gone)


class TestRemoveOrphanFilesRecursive:
    """Recursive walk over data/ + metadata/ sweep."""

    def test_orphan_cleanup_removes_data_orphans_recursively(self, memory_catalog):
        """An orphan inside a partition subdirectory of ``data/`` is
        found and deleted - a non-recursive scan would miss these."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        partition_dir = os.path.join(_table_local_dir(tbl), "data", "region=eu")
        orphan = _drop_synthetic_orphan(partition_dir, "00000-9-deadbeef.parquet")

        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=False)

        assert result.deleted is True
        assert orphan in result.orphan_paths
        assert not os.path.exists(orphan)

    def test_orphan_cleanup_removes_metadata_orphans(self, memory_catalog):
        """An orphan ``.avro`` file in ``metadata/`` is found and deleted."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        metadata_dir = os.path.join(_table_local_dir(tbl), "metadata")
        orphan = _drop_synthetic_orphan(metadata_dir, "deadbeef-orphan-m0.avro")

        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=False)

        assert result.deleted is True
        assert orphan in result.orphan_paths
        assert not os.path.exists(orphan)

    def test_orphan_cleanup_respects_age_threshold(self, memory_catalog):
        """A recently-created orphan is preserved when older_than_hours
        is greater than the file's actual age."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        # Create orphan but DO NOT backdate it.
        os.makedirs(data_dir, exist_ok=True)
        orphan = os.path.join(data_dir, "00000-9-fresh-orphan.parquet")
        with open(orphan, "wb") as fh:
            fh.write(b"fresh")

        # Cutoff is 72 hours ago; orphan is brand new -> preserved.
        result = remove_orphan_files(tbl, older_than_hours=72, dry_run=False)

        assert orphan not in result.orphan_paths
        assert os.path.exists(orphan)

    def test_orphan_cleanup_does_not_delete_referenced_files(self, memory_catalog):
        """Every file Iceberg currently references survives a non-dry-run
        sweep, regardless of mtime."""
        tbl = _make_table_multi_append(memory_catalog, "ns.t", appends=3)
        tbl.refresh()
        data_files_before = [pf.file.file_path for pf in tbl.scan().plan_files()]
        local_data_files = [_file_uri_to_local_path(p) for p in data_files_before]
        # Backdate every referenced data file so the age cutoff alone
        # cannot save them - only the referenced-set check does.
        for p in local_data_files:
            if os.path.exists(p):
                _backdate(p, hours=96)

        remove_orphan_files(tbl, older_than_hours=0, dry_run=False)

        for p in local_data_files:
            assert os.path.exists(p), f"referenced file was deleted: {p}"

    def test_orphan_cleanup_dry_run_no_deletes(self, memory_catalog):
        """Dry-run reports orphans but leaves them on disk."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        orphan = _drop_synthetic_orphan(data_dir, "00000-9-dryrun.parquet")

        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=True)

        assert result.deleted is False
        assert orphan in result.orphan_paths
        assert os.path.exists(orphan)

    def test_no_future_warning_emitted(self, memory_catalog):
        """No FutureWarning is emitted from the orphan path."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            remove_orphan_files(tbl, older_than_hours=0, dry_run=True)

        assert not any(issubclass(w.category, FutureWarning) for w in captured)

    def test_non_iceberg_files_are_left_alone(self, memory_catalog):
        """A user-deposited file with a non-Iceberg extension is preserved
        even if it's old and unreferenced."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        os.makedirs(data_dir, exist_ok=True)
        unknown = os.path.join(data_dir, "operator-notes.txt")
        with open(unknown, "wb") as fh:
            fh.write(b"hello")
        _backdate(unknown, hours=96)

        result = remove_orphan_files(tbl, older_than_hours=0, dry_run=False)

        assert unknown not in result.orphan_paths
        assert os.path.exists(unknown)

    def test_sweep_subset_skips_metadata(self, memory_catalog):
        """Passing ``sweep=("data",)`` leaves orphan metadata files alone."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        metadata_dir = os.path.join(_table_local_dir(tbl), "metadata")
        orphan = _drop_synthetic_orphan(metadata_dir, "deadbeef-skip-m0.avro")

        result = remove_orphan_files(
            tbl,
            older_than_hours=0,
            sweep=("data",),
            dry_run=False,
        )

        assert orphan not in result.orphan_paths
        assert os.path.exists(orphan)

    def test_non_local_scheme_raises_not_implemented(self, monkeypatch, memory_catalog):
        """``s3://`` (or other non-local) locations raise cleanly."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=10)
        monkeypatch.setattr(tbl, "location", lambda: "s3://bucket/path", raising=False)
        with pytest.raises(NotImplementedError, match="s3"):
            remove_orphan_files(tbl)


def _backdate_tree(root: str, hours: float = 200) -> None:
    """Backdate every file under *root*."""
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            _backdate(os.path.join(dirpath, f), hours=hours)


class TestRemoveOrphanFilesSafety:
    """Reference-set completeness, name matching and walk containment."""

    def test_files_reachable_only_from_refs_and_old_snapshots_survive(self, memory_catalog):
        """Files needed only by a tag, a branch head or an older main
        snapshot are kept, and every ref stays readable."""
        schema = pa.schema([("id", pa.int64())])
        memory_catalog.create_namespace("ns")
        tbl = memory_catalog.create_table("ns.refs", schema)
        tbl.append(pa.table({"id": [1]}, schema=schema))
        first = tbl.current_snapshot().snapshot_id  # type: ignore[union-attr]
        tbl.manage_snapshots().create_tag(first, "v1").commit()
        tbl.manage_snapshots().create_branch(first, "dev").commit()
        tbl.append(pa.table({"id": [2]}, schema=schema), branch="dev")
        tbl.overwrite(pa.table({"id": [3]}, schema=schema))
        tbl.refresh()
        _backdate_tree(_table_local_dir(tbl))

        result = remove_orphan_files(tbl, older_than_hours=72, dry_run=False)

        assert result.orphan_paths == []
        tbl.refresh()

        def ids(snapshot_id=None):
            return sorted(tbl.scan(snapshot_id=snapshot_id).to_arrow()["id"].to_pylist())

        assert ids() == [3]
        assert ids(tbl.snapshot_by_name("v1").snapshot_id) == [1]  # type: ignore[union-attr]
        assert ids(tbl.snapshot_by_name("dev").snapshot_id) == [1, 2]  # type: ignore[union-attr]

    def test_name_key_folds_case_and_unicode_form(self):
        from iceberg_ops.maintenance.orphans import _name_key

        assert _name_key("IMPORT-PART-0001.parquet") == _name_key("import-part-0001.parquet")
        nfd = "cafe\u0301.parquet"
        nfc = "caf\u00e9.parquet"
        assert _name_key(nfd) == _name_key(nfc)

    def test_case_mismatched_registration_survives(self, memory_catalog):
        """A live file registered through a differently-cased path is
        not an orphan on a case-insensitive filesystem."""
        import pyarrow.parquet as pq

        tbl = _make_table(memory_catalog, "ns.t")
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        os.makedirs(data_dir, exist_ok=True)
        on_disk = os.path.join(data_dir, "import-part-0001.parquet")
        pq.write_table(
            create_arrow_table(TABLE_SCHEMA, [{"id": i, "value": "v"} for i in range(3)]),
            on_disk,
        )
        if not os.path.exists(os.path.join(data_dir, "IMPORT-PART-0001.parquet")):
            pytest.skip("filesystem is case-sensitive")
        tbl.add_files([tbl.location().rstrip("/") + "/data/IMPORT-PART-0001.parquet"])
        tbl.refresh()
        _backdate(on_disk, hours=96)

        result = remove_orphan_files(tbl, older_than_hours=72, dry_run=False)

        assert result.orphan_paths == []
        assert os.path.exists(on_disk)
        tbl.refresh()
        assert tbl.scan().to_arrow().num_rows == 3

    def test_case_variant_of_referenced_name_is_kept(self, memory_catalog):
        """Folding is conservative: an unreferenced file whose name
        differs from a referenced one only in case is kept."""
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        data_uri = next(iter(tbl.scan().plan_files())).file.file_path
        live = _file_uri_to_local_path(data_uri)
        stem = os.path.basename(live).removesuffix(".parquet")
        assert stem.upper() != stem
        variant = os.path.join(os.path.dirname(live), stem.upper() + ".parquet")
        if not os.path.exists(variant):  # case-sensitive filesystem
            _drop_synthetic_orphan(os.path.dirname(live), os.path.basename(variant))
        _backdate(live, hours=96)

        result = remove_orphan_files(tbl, older_than_hours=72, dry_run=False)

        assert result.orphan_paths == []
        assert os.path.exists(variant)
        assert os.path.exists(live)

    @pytest.mark.skipif(os.name != "nt", reason="NTFS junctions are Windows-only")
    def test_junction_inside_sweep_is_not_followed(self, memory_catalog, tmp_path):
        """A junction inside ``data/`` pointing outside the table is
        neither listed nor followed; real subdirectories still are."""
        import _winapi  # type: ignore[import-not-found]

        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        outside = tmp_path / "other_table_data"
        outside.mkdir()
        victim = _drop_synthetic_orphan(str(outside), "00000-0-live-of-other-table.parquet")
        link = os.path.join(data_dir, "region=x")
        _winapi.CreateJunction(str(outside), link)
        assert os.path.isjunction(link)
        real_orphan = _drop_synthetic_orphan(
            os.path.join(data_dir, "region=y"), "00000-9-real.parquet"
        )

        dry = remove_orphan_files(tbl, older_than_hours=24, dry_run=True)
        assert dry.orphan_paths == [real_orphan]

        result = remove_orphan_files(tbl, older_than_hours=24, dry_run=False)

        assert result.orphan_paths == [real_orphan]
        assert os.path.exists(victim)
        assert not os.path.exists(real_orphan)

    def test_directory_symlink_inside_sweep_is_not_followed(self, memory_catalog, tmp_path):
        tbl = _make_table_with_data(memory_catalog, "ns.t", row_count=5)
        data_dir = os.path.join(_table_local_dir(tbl), "data")
        outside = tmp_path / "elsewhere"
        outside.mkdir()
        victim = _drop_synthetic_orphan(str(outside), "00000-0-keep.parquet")
        link = os.path.join(data_dir, "region=s")
        try:
            os.symlink(str(outside), link, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("cannot create directory symlinks here")

        result = remove_orphan_files(tbl, older_than_hours=24, dry_run=False)

        assert result.orphan_paths == []
        assert os.path.exists(victim)


class TestMaintenancePlainPathLocation:
    """Tables whose location is a plain path (on Windows a drive-letter
    path such as ``C:\\wh/ns/t``) are local, as PyIceberg treats them."""

    @pytest.fixture
    def plain_path_catalog(self, tmp_path):
        from pyiceberg.catalog.sql import SqlCatalog

        warehouse = tmp_path / "wh"
        warehouse.mkdir()
        catalog = SqlCatalog(
            "plain", uri=f"sqlite:///{(tmp_path / 'c.db').as_posix()}", warehouse=str(warehouse)
        )
        yield catalog
        catalog.close()

    def _table(self, catalog):
        catalog.create_namespace("ns")
        tbl = catalog.create_table("ns.t", schema=TABLE_SCHEMA)
        for i in range(5):
            _append_rows(tbl, i * 10, 10)
        tbl.refresh()
        assert not tbl.location().startswith("file:")
        return tbl

    def test_remove_orphan_files(self, plain_path_catalog):
        tbl = self._table(plain_path_catalog)
        root = _table_local_dir(tbl)
        orphan = _drop_synthetic_orphan(os.path.join(root, "data"), "00000-9-plain.parquet")
        _backdate_tree(root)

        result = remove_orphan_files(tbl, older_than_hours=72, dry_run=False)

        assert result.orphan_paths == [orphan]
        assert not os.path.exists(orphan)
        tbl.refresh()
        assert tbl.scan().to_arrow().num_rows == 50

    def test_compact_small_files_streaming(self, plain_path_catalog):
        tbl = self._table(plain_path_catalog)

        result = compact_small_files_streaming(tbl, target_size_mb=512, min_file_count=5)

        tbl.refresh()
        assert result.files_before == 5
        assert result.files_after == 1
        assert tbl.scan().to_arrow().num_rows == 50


# ---------------------------------------------------------------------------
# Import convenience
# ---------------------------------------------------------------------------


class TestMaintenanceImports:
    def test_top_level_imports(self):
        """All four functions are importable from the maintenance package."""
        from iceberg_ops.maintenance import (
            compact_small_files_streaming,
            expire_snapshots,
            remove_orphan_files,
            table_health_report,
        )

        assert callable(compact_small_files_streaming)
        assert callable(expire_snapshots)
        assert callable(remove_orphan_files)
        assert callable(table_health_report)

    def test_result_attributes_used_in_docs_exist(self):
        """Attributes the maintenance guide prints are real dataclass fields."""
        import dataclasses

        def names(cls):
            return {f.name for f in dataclasses.fields(cls)}

        assert {"files_rewritten", "bytes_before", "bytes_after"} <= names(CompactionResult)
        assert {"expired_count", "total_snapshots", "protected_ids"} <= names(SnapshotExpiryResult)
        assert {"orphan_count", "orphan_paths", "failed_paths", "deleted"} <= names(
            OrphanCleanupResult
        )
        assert {"snapshot_count", "small_file_count", "total_files", "recommendations"} <= names(
            TableHealthReport
        )

    def test_documented_defaults(self):
        """The safety-relevant defaults match the docs."""
        orphan_params = inspect.signature(remove_orphan_files).parameters
        assert orphan_params["dry_run"].default is True
        expiry_params = inspect.signature(expire_snapshots).parameters
        assert expiry_params["max_age_hours"].default is None
        assert expiry_params["min_snapshots"].default is None
        assert all(
            p.kind is inspect.Parameter.KEYWORD_ONLY
            for name, p in expiry_params.items()
            if name != "table"
        )
