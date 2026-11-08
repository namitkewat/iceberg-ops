"""Concurrent-runner tests for iceberg_ops.runner and iceberg_ops.history.

Two catalogs opened on the same SQLite database and warehouse stand in
for two runner processes. The interleaving is made deterministic by
running the first runner to completion inside the second runner's
baseline read.
"""

from __future__ import annotations

import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.exceptions import CommitFailedException

from iceberg_ops.core.catalog import CatalogFactory
from iceberg_ops.history import ConcurrentMigrationError, MigrationHistory
from iceberg_ops.runner import MigrationRunner

_CREATE_ORDERS = """
import pyarrow as pa
from iceberg_ops.migration import BaseMigration

class Migration(BaseMigration):
    version = "0001"
    description = "create orders"

    def upgrade(self, ctx):
        ctx.create_table_if_not_exists("orders", pa.schema([pa.field("id", pa.int64())]))

    def downgrade(self, ctx):
        ctx.drop_table_if_exists("orders")
"""


@pytest.fixture
def two_catalogs(tmp_path: Path) -> Iterator[tuple[Any, Any]]:
    """Two independent catalog handles on one SQLite database and warehouse."""
    kwargs = {
        "db_path": str(tmp_path / "catalog.db"),
        "warehouse_dir": str(tmp_path / "warehouse"),
    }
    ca = CatalogFactory.build("sqlite", **kwargs)
    cb = CatalogFactory.build("sqlite", **kwargs)
    yield ca, cb
    ca.close()
    cb.close()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> str:
    d = tmp_path / "migrations"
    d.mkdir()
    (d / "0001_create_orders.py").write_text(textwrap.dedent(_CREATE_ORDERS))
    return str(d)


def _raw_history(catalog: Any) -> list[tuple[str, str, str]]:
    """Every physical history row as (version, namespace, status)."""
    tbl = catalog.load_table("_iceberg_ops.migration_history")
    return sorted(
        (r["version"], r["namespace"], r["status"]) for r in tbl.scan().to_arrow().to_pylist()
    )


def _run_inside_baseline(
    monkeypatch: pytest.MonkeyPatch,
    loser: MigrationRunner,
    winner: MigrationRunner,
) -> None:
    """Make *winner* run its whole migrate() right after *loser* reads its baseline."""
    original = loser._check_no_concurrent_migration

    def interleaved() -> set[str]:
        baseline = original()
        winner.migrate()
        return baseline

    monkeypatch.setattr(loser, "_check_no_concurrent_migration", interleaved)


@pytest.mark.parametrize("history_exists", [False, True], ids=["fresh", "existing-history"])
class TestConcurrencyRace:
    def test_same_namespace_second_runner_raises(
        self,
        two_catalogs,
        migrations_dir,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        history_exists: bool,
    ) -> None:
        ca, cb = two_catalogs
        if history_exists:
            MigrationHistory(ca).record_applied("0001", "warmup", "x")
        ra = MigrationRunner(ca, namespace="sales", migrations_dir=migrations_dir)
        rb = MigrationRunner(cb, namespace="sales", migrations_dir=migrations_dir)
        _run_inside_baseline(monkeypatch, loser=rb, winner=ra)

        with pytest.raises(ConcurrentMigrationError, match="0001"):
            rb.migrate()

        rows = [r for r in _raw_history(ca) if r[1] == "sales"]
        assert rows == [("0001", "sales", "applied")]
        # Both upgrade() calls ran; the idempotent one left a single table.
        assert ca.load_table("sales.orders") is not None

    def test_other_namespace_both_recorded(
        self,
        two_catalogs,
        migrations_dir,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        history_exists: bool,
    ) -> None:
        ca, cb = two_catalogs
        if history_exists:
            MigrationHistory(ca).record_applied("0001", "warmup", "x")
        ra = MigrationRunner(ca, namespace="sales", migrations_dir=migrations_dir)
        rb = MigrationRunner(cb, namespace="finance", migrations_dir=migrations_dir)
        _run_inside_baseline(monkeypatch, loser=rb, winner=ra)

        rb.migrate()

        rows = [r for r in _raw_history(ca) if r[1] != "warmup"]
        assert rows == [("0001", "finance", "applied"), ("0001", "sales", "applied")]


class TestCommitConflictInMigration:
    _UPGRADE_CONFLICT = """
    from pyiceberg.exceptions import CommitFailedException
    from iceberg_ops.migration import BaseMigration

    class Migration(BaseMigration):
        version = "0001"
        description = "conflicting upgrade"

        def upgrade(self, ctx):
            raise CommitFailedException("branch main was created concurrently")

        def downgrade(self, ctx):
            pass
    """

    _DOWNGRADE_CONFLICT = """
    from pyiceberg.exceptions import CommitFailedException
    from iceberg_ops.migration import BaseMigration

    class Migration(BaseMigration):
        version = "0001"
        description = "conflicting downgrade"

        def upgrade(self, ctx):
            pass

        def downgrade(self, ctx):
            raise CommitFailedException("requirement failed")
    """

    def test_upgrade_commit_conflict_records_nothing(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_conflict.py").write_text(textwrap.dedent(self._UPGRADE_CONFLICT))
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(ConcurrentMigrationError, match="concurrent commit") as excinfo:
            runner.migrate()
        assert isinstance(excinfo.value.__cause__, CommitFailedException)
        assert runner._history.get_all("ns") == []

    def test_downgrade_commit_conflict_maps_to_concurrent_error(
        self,
        sqlite_catalog,
        tmp_path,
    ) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_conflict.py").write_text(textwrap.dedent(self._DOWNGRADE_CONFLICT))
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        runner.migrate()
        with pytest.raises(ConcurrentMigrationError, match="concurrent commit"):
            runner.rollback(steps=1)
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_error_is_still_a_runtime_error(self) -> None:
        # Existing ``except RuntimeError`` handlers keep catching it.
        assert issubclass(ConcurrentMigrationError, RuntimeError)
