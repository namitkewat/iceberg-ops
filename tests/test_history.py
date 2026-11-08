"""Tests for iceberg_ops.history.MigrationHistory."""

from __future__ import annotations

import datetime
import threading
import time
import types
import warnings

import pytest
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError

from iceberg_ops.core.utils import create_arrow_table
from iceberg_ops.history import (
    DEFAULT_META_NAMESPACE,
    ConcurrentMigrationError,
    MigrationHistory,
)

HISTORY_ID = f"{DEFAULT_META_NAMESPACE}.migration_history"


def _raw_rows(h: MigrationHistory) -> list[tuple[str, str, str]]:
    """Every physical row in the history table as (version, namespace, status)."""
    tbl = h._load()
    assert tbl is not None
    tbl.refresh()
    return sorted(
        (r["version"], r["namespace"], r["status"]) for r in tbl.scan().to_arrow().to_pylist()
    )


def _two_loaded_handles(catalog) -> tuple[MigrationHistory, MigrationHistory]:
    """Two handles on one catalog, both having read the (existing) history table."""
    h1 = MigrationHistory(catalog)
    h1._load_or_create()
    h2 = MigrationHistory(catalog)
    h1.get_all("s")
    h2.get_all("s")
    return h1, h2


class TestBootstrap:
    def test_first_write_creates_table(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_applied("0001", "sales", "create orders")
        tbl = memory_catalog.load_table(HISTORY_ID)
        names = {f.name for f in tbl.schema().fields}
        assert {
            "version",
            "namespace",
            "description",
            "applied_at",
            "status",
            "error",
        } <= names
        assert h.get_applied_versions("sales") == {"0001"}

    def test_reads_do_not_create_table(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        assert h.get_all("x") == []
        assert h.get_applied_versions("x") == set()
        assert memory_catalog.list_namespaces() == []

    def test_idempotent_bootstrap(self, memory_catalog) -> None:
        MigrationHistory(memory_catalog).record_applied("0001", "a", "x")
        # A second instance reuses the existing table.
        h2 = MigrationHistory(memory_catalog)
        assert h2.get_applied_versions("a") == {"0001"}
        h2.record_applied("0002", "a", "y")
        assert h2.get_applied_versions("a") == {"0001", "0002"}

    def test_custom_meta_namespace(self, memory_catalog) -> None:
        MigrationHistory(memory_catalog, meta_namespace="_custom_ops").record_applied(
            "0001", "a", "x"
        )
        tbl = memory_catalog.load_table("_custom_ops.migration_history")
        assert tbl is not None

    def test_key_columns_keep_full_metrics(self, memory_catalog) -> None:
        MigrationHistory(memory_catalog).record_applied("0001", "a", "x")
        props = memory_catalog.load_table(HISTORY_ID).properties
        assert props["write.metadata.metrics.column.version"] == "full"
        assert props["write.metadata.metrics.column.namespace"] == "full"

    def test_commit_retry_budget_raised(self, memory_catalog) -> None:
        MigrationHistory(memory_catalog).record_applied("0001", "a", "x")
        props = memory_catalog.load_table(HISTORY_ID).properties
        assert props["commit.retry.num-retries"] == "20"
        assert props["commit.retry.max-wait-ms"] == "5000"

    def test_create_race_loads_existing_table(
        self,
        sqlite_catalog,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Another process creates the table between our load and create."""
        MigrationHistory(sqlite_catalog).record_applied("0001", "other", "x")
        real_load = sqlite_catalog.load_table
        calls = {"n": 0}

        def flaky_load(identifier):
            calls["n"] += 1
            if calls["n"] == 1:
                raise NoSuchTableError(identifier)
            return real_load(identifier)

        monkeypatch.setattr(sqlite_catalog, "load_table", flaky_load)
        h = MigrationHistory(sqlite_catalog)
        h.record_applied("0001", "sales", "x")
        assert h.get_applied_versions("sales") == {"0001"}
        assert h.get_applied_versions("other") == {"0001"}


class TestRecordAndQuery:
    def test_record_applied_then_query(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_applied("0001", "sales", "create orders")
        h.record_applied("0002", "sales", "add sku")
        assert h.get_applied_versions("sales") == {"0001", "0002"}

    def test_record_rolled_back_excludes_from_applied(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_applied("0001", "sales", "x")
        h.record_rolled_back("0001", "sales", "x")
        assert h.get_applied_versions("sales") == set()

    def test_record_failed_excludes_from_applied(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_failed("0001", "sales", "x", "ValueError: bad input")
        assert h.get_applied_versions("sales") == set()
        rows = h.get_all("sales")
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"
        assert "bad input" in rows[0]["error"]

    def test_failed_error_truncated(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        long_msg = "x" * 5000
        h.record_failed("0001", "sales", "x", long_msg)
        rows = h.get_all("sales")
        assert len(rows[0]["error"]) == 2000

    def test_namespace_isolation(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_applied("0001", "sales", "s1")
        h.record_applied("0001", "finance", "f1")
        assert h.get_applied_versions("sales") == {"0001"}
        assert h.get_applied_versions("finance") == {"0001"}
        assert len(h.get_all("sales")) == 1
        assert len(h.get_all("finance")) == 1

    def test_get_all_orders_by_version(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        h.record_applied("0003", "sales", "third")
        h.record_applied("0001", "sales", "first")
        h.record_applied("0002", "sales", "second")
        rows = h.get_all("sales")
        assert [r["version"] for r in rows] == ["0001", "0002", "0003"]

    def test_record_applied_replaces_failed(self, memory_catalog) -> None:
        """Re-recording a (version, ns) row replaces the prior status."""
        h = MigrationHistory(memory_catalog)
        h.record_failed("0001", "sales", "x", "boom")
        h.record_applied("0001", "sales", "x")
        assert h.get_applied_versions("sales") == {"0001"}
        rows = h.get_all("sales")
        assert len(rows) == 1
        assert rows[0]["status"] == "applied"
        assert _raw_rows(h) == [("0001", "sales", "applied")]

    def test_get_all_empty(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        assert h.get_all("nope") == []

    def test_get_applied_versions_empty(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        assert h.get_applied_versions("nope") == set()

    def test_first_write_emits_no_warning(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            h.record_applied("0001", "sales", "x")
        assert h.get_applied_versions("sales") == {"0001"}


class TestDuplicateRowsHeal:
    def _append_raw(self, h: MigrationHistory, status: str, applied_at: str) -> None:
        tbl = h._load_or_create()
        tbl.append(
            create_arrow_table(
                MigrationHistory.SCHEMA,
                [
                    {
                        "version": "0001",
                        "namespace": "sales",
                        "description": "x",
                        "applied_at": applied_at,
                        "status": status,
                        "error": None,
                    }
                ],
            )
        )

    def test_readers_and_next_write_collapse_duplicates(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        now = datetime.datetime.now(datetime.UTC)
        self._append_raw(h, "applied", now.isoformat())
        self._append_raw(h, "applied", (now + datetime.timedelta(seconds=1)).isoformat())
        assert len(_raw_rows(h)) == 2

        rows = h.get_all("sales")
        assert len(rows) == 1
        assert rows[0]["status"] == "applied"

        h.record_rolled_back("0001", "sales", "x")
        assert _raw_rows(h) == [("0001", "sales", "rolled_back")]

        h.record_applied("0001", "sales", "x")
        assert _raw_rows(h) == [("0001", "sales", "applied")]

    def test_latest_row_wins(self, memory_catalog) -> None:
        h = MigrationHistory(memory_catalog)
        now = datetime.datetime.now(datetime.UTC)
        self._append_raw(h, "applied", now.isoformat())
        self._append_raw(h, "rolled_back", (now + datetime.timedelta(seconds=1)).isoformat())
        assert h.get_applied_versions("sales") == set()
        assert [r["status"] for r in h.get_all("sales")] == ["rolled_back"]


class TestConcurrentWrites:
    """Two handles on one catalog simulate two runner processes."""

    def test_same_key_race_raises_and_keeps_one_row(self, sqlite_catalog) -> None:
        h1, h2 = _two_loaded_handles(sqlite_catalog)
        h1.record_applied("0001", "s", "x")
        with pytest.raises(ConcurrentMigrationError, match="0001"):
            h2.record_applied("0001", "s", "x")
        assert _raw_rows(h1) == [("0001", "s", "applied")]

    def test_stale_failed_does_not_clobber_applied(self, sqlite_catalog) -> None:
        h1, h2 = _two_loaded_handles(sqlite_catalog)
        h1.record_applied("0001", "s", "x")
        with pytest.raises(ConcurrentMigrationError, match="failed"):
            h2.record_failed("0001", "s", "x", "boom")
        assert _raw_rows(h1) == [("0001", "s", "applied")]
        assert h2.get_applied_versions("s") == {"0001"}

    def test_other_namespace_write_retries_and_lands(self, sqlite_catalog) -> None:
        h1, h2 = _two_loaded_handles(sqlite_catalog)
        h1.record_applied("0001", "sales", "x")
        h2.record_applied("0001", "finance", "y")
        assert _raw_rows(h1) == [
            ("0001", "finance", "applied"),
            ("0001", "sales", "applied"),
        ]

    def test_race_after_reading_no_table_raises(self, sqlite_catalog) -> None:
        """A handle whose read found no table still detects a same-key write."""
        h1 = MigrationHistory(sqlite_catalog)
        h2 = MigrationHistory(sqlite_catalog)
        assert h1.get_all("s") == []
        assert h2.get_all("s") == []
        h1.record_applied("0001", "s", "x")
        with pytest.raises(ConcurrentMigrationError):
            h2.record_applied("0001", "s", "x")
        assert _raw_rows(h1) == [("0001", "s", "applied")]

    def test_other_namespace_after_reading_no_table_lands(self, sqlite_catalog) -> None:
        h1 = MigrationHistory(sqlite_catalog)
        h2 = MigrationHistory(sqlite_catalog)
        h1.get_all("sales")
        h2.get_all("finance")
        h1.record_applied("0001", "sales", "x")
        h2.record_applied("0001", "finance", "y")
        assert _raw_rows(h1) == [
            ("0001", "finance", "applied"),
            ("0001", "sales", "applied"),
        ]

    def test_exhausted_retries_report_contention(self, sqlite_catalog) -> None:
        """A lost race against another namespace is not reported as same-key."""
        h1, h2 = _two_loaded_handles(sqlite_catalog)
        tbl = h1._load()
        assert tbl is not None
        with tbl.transaction() as txn:
            txn.set_properties({"commit.retry.num-retries": "0"})
        h2.get_all("finance")  # h2 now reads the zero retry budget
        h1.record_applied("0001", "sales", "x")
        with pytest.raises(ConcurrentMigrationError, match="contended") as info:
            h2.record_applied("0001", "finance", "y")
        assert "migrating this namespace" not in str(info.value)
        assert isinstance(info.value.__cause__, CommitFailedException)
        assert _raw_rows(h1) == [("0001", "sales", "applied")]


class TestManyNamespaceWriters:
    def test_distinct_namespaces_written_at_once_all_land(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ten runners, one namespace each, write history at the same moment.

        Each thread has its own catalog on one SQLite file. A writer can
        lose at most one race per other writer, so every write lands
        within the history table's retry budget. PyIceberg's backoff
        sleeps are skipped to keep the test fast; every commit is real.
        """
        import pyiceberg.table as pyiceberg_table

        from iceberg_ops.core.catalog import CatalogFactory

        monkeypatch.setattr(
            pyiceberg_table,
            "time",
            types.SimpleNamespace(monotonic=time.monotonic, sleep=lambda _seconds: None),
        )
        kwargs = {
            "db_path": str(tmp_path / "catalog.db"),
            "warehouse_dir": str(tmp_path / "warehouse"),
        }
        writers = 10
        namespaces = [f"tenant_{i:02d}" for i in range(writers)]
        setup = CatalogFactory.build("sqlite", **kwargs)
        MigrationHistory(setup).record_applied("0000", "warm", "x")
        barrier = threading.Barrier(writers, timeout=60)
        errors: dict[str, BaseException] = {}

        def write(namespace: str) -> None:
            catalog = CatalogFactory.build("sqlite", **kwargs)
            try:
                history = MigrationHistory(catalog)
                history.get_all(namespace)  # baseline read, as migrate() does
                barrier.wait()
                history.record_applied("0001", namespace, "x")
            except BaseException as exc:  # reported by the assertion below
                errors[namespace] = exc
            finally:
                catalog.close()

        threads = [threading.Thread(target=write, args=(ns,)) for ns in namespaces]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        try:
            assert errors == {}
            assert _raw_rows(MigrationHistory(setup)) == sorted(
                [("0000", "warm", "applied")] + [("0001", ns, "applied") for ns in namespaces]
            )
        finally:
            setup.close()
