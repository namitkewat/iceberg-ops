"""Tests for iceberg_ops.runner.MigrationRunner."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from iceberg_ops.runner import (
    DEFAULT_META_NAMESPACE,
    ConcurrentMigrationError,
    MigrationRunner,
    normalize_version,
)

# Test-fixture migrations live alongside this file at tests/migrations/.
TESTS_DIR = Path(__file__).parent
SAMPLE_MIGRATIONS_DIR = str(TESTS_DIR / "migrations")


def _write_migration(target: Path, version: str, body: str) -> None:
    """Write *body* to ``target/<version>_test.py``."""
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{version}_test.py").write_text(textwrap.dedent(body))


class TestDiscover:
    def test_finds_files_in_flat_dir(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        migs = runner._discover()
        versions = {m.version for m in migs}
        assert {"0001", "0002"} <= versions

    def test_raises_when_dir_missing(self, sqlite_catalog, tmp_path) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=str(tmp_path / "absent"),
        )
        with pytest.raises(FileNotFoundError, match="absent"):
            runner._discover()

    def test_empty_dir_returns_empty(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        assert runner._discover() == []

    def test_skips_non_migration_files(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "README.md").write_text("docs")
        (d / "helper.py").write_text("# not a migration")
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "x"
                def upgrade(self, ctx): pass
                def downgrade(self, ctx): pass
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        assert {m.version for m in runner._discover()} == {"0001"}


class TestMigrate:
    def test_applies_all_pending(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    def test_target_limits_application(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate(target="0001")
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_already_up_to_date_is_noop(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        # Second migrate should not error and applied set unchanged.
        runner.migrate()
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    def test_failure_is_recorded(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "intentional fail"
                def upgrade(self, ctx):
                    raise RuntimeError("boom")
                def downgrade(self, ctx):
                    pass
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match="boom"):
            runner.migrate()
        rows = runner._history.get_all("ns")
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"
        assert "boom" in rows[0]["error"]

    def test_failure_row_holds_message_not_traceback(
        self,
        sqlite_catalog,
        tmp_path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The shared history table must not carry host paths or source lines."""
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "intentional fail"
                def upgrade(self, ctx):
                    raise RuntimeError("connect failed")
                def downgrade(self, ctx):
                    pass
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with (
            caplog.at_level("DEBUG", logger="iceberg_ops.runner"),
            pytest.raises(RuntimeError, match="connect failed"),
        ):
            runner.migrate()
        error = runner._history.get_all("ns")[0]["error"]
        assert error == "RuntimeError: connect failed"
        assert "Traceback" not in error
        assert str(d) not in error
        # The traceback stays in local logs.
        assert any(r.levelname == "DEBUG" and r.exc_info is not None for r in caplog.records)

    def test_failed_history_write_keeps_original_error(
        self,
        sqlite_catalog,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An error while recording the failure must not replace the real one."""
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "intentional fail"
                def upgrade(self, ctx):
                    raise ValueError("original problem")
                def downgrade(self, ctx):
                    pass
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))

        def broken_record_failed(*args: object, **kwargs: object) -> None:
            raise OSError("history write failed")

        monkeypatch.setattr(runner._history, "record_failed", broken_record_failed)
        with (
            caplog.at_level("ERROR", logger="iceberg_ops.runner"),
            pytest.raises(RuntimeError, match="original problem") as excinfo,
        ):
            runner.migrate()
        assert isinstance(excinfo.value.__cause__, ValueError)
        assert any("history write failed" in r.getMessage() for r in caplog.records)

    def test_concurrent_error_while_recording_failure_propagates(
        self,
        sqlite_catalog,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "intentional fail"
                def upgrade(self, ctx):
                    raise ValueError("original problem")
                def downgrade(self, ctx):
                    pass
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))

        def racing_record_failed(*args: object, **kwargs: object) -> None:
            raise ConcurrentMigrationError("another runner recorded 0001")

        monkeypatch.setattr(runner._history, "record_failed", racing_record_failed)
        with pytest.raises(ConcurrentMigrationError, match="another runner"):
            runner.migrate()


class TestRollback:
    def test_undoes_last(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        runner.rollback(steps=1)
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_target_rolls_back_above(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        runner.rollback(target="0001")
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_nothing_applied_is_noop(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.rollback(steps=1)  # should not raise
        assert runner._history.get_applied_versions("ns") == set()

    def test_propagates_not_implemented(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "no rollback"
                def upgrade(self, ctx): pass
                def downgrade(self, ctx):
                    raise NotImplementedError("not reversible")
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        runner.migrate()
        with pytest.raises(NotImplementedError):
            runner.rollback(steps=1)


class TestStamp:
    def test_marks_applied_without_running(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.stamp()
        # Both versions should appear as applied even though upgrade() never ran.
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}
        # No user table should exist (upgrade wasn't called).
        from pyiceberg.exceptions import NoSuchTableError

        with pytest.raises(NoSuchTableError):
            sqlite_catalog.load_table("ns.products")

    def test_idempotent(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.stamp()
        runner.stamp()  # no error, no duplicates
        rows = runner._history.get_all("ns")
        assert len(rows) == 2

    def test_target_limits_stamping(self, sqlite_catalog) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.stamp(target="0001")
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_failed_and_rolled_back_versions_are_stamped_applied(self, sqlite_catalog) -> None:
        """Documented behaviour: every non-applied version is stamped, unrun."""
        from pyiceberg.exceptions import NoSuchTableError

        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner._history.record_rolled_back("0001", "ns", "x")
        runner._history.record_failed("0002", "ns", "x", "RuntimeError: halfway")
        runner.stamp()
        rows = runner._history.get_all("ns")
        assert [(r["version"], r["status"]) for r in rows] == [
            ("0001", "applied"),
            ("0002", "applied"),
        ]
        assert all(r["error"] is None for r in rows)
        with pytest.raises(NoSuchTableError):
            sqlite_catalog.load_table("ns.products")


class TestStatusAndPlan:
    def test_status_shows_pending_then_applied(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.status()
        out_before = capsys.readouterr().out
        assert "[pending" in out_before

        runner.migrate(target="0001")
        runner.status()
        out_after = capsys.readouterr().out
        assert "[applied" in out_after
        assert "0001" in out_after

    def test_plan_lists_pending(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.plan()
        out = capsys.readouterr().out
        assert "[pending]" in out
        assert "0001" in out
        assert "0002" in out

    def test_plan_empty_when_up_to_date(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        runner.plan()
        out = capsys.readouterr().out
        assert "up to date" in out


class TestHistory:
    def test_empty_when_nothing_applied(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.history()
        out = capsys.readouterr().out
        assert "no migration history" in out

    def test_after_migrate(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        runner.history()
        out = capsys.readouterr().out
        assert "0001" in out
        assert "0002" in out
        assert "applied" in out

    def test_columns_line_up(
        self,
        memory_catalog,
        tmp_path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Full ISO-8601 timestamps never push DESCRIPTION out of its column."""
        d = tmp_path / "migrations"
        _write_noop(d, "0001", description="Create orders table")
        _write_migration(
            d,
            "0002",
            """
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0002"
                description = "intentional fail"
                def upgrade(self, ctx):
                    raise RuntimeError("boom\\n  second line")
                def downgrade(self, ctx):
                    pass
            """,
        )
        runner = MigrationRunner(memory_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match="boom"):
            runner.migrate()
        capsys.readouterr()

        runner.history()
        lines = capsys.readouterr().out.splitlines()
        header = next(line for line in lines if line.lstrip().startswith("VERSION"))
        cols = [header.index(h) for h in ("VERSION", "STATUS", "APPLIED AT", "DESCRIPTION")]
        records = runner._history.get_all("ns")
        assert {r["status"] for r in records} == {"applied", "failed"}
        for rec in records:
            line = next(row for row in lines if row.lstrip().startswith(rec["version"] + " "))
            values = (rec["version"], rec["status"], rec["applied_at"], rec["description"])
            for col, value in zip(cols, values, strict=True):
                assert line[col:].startswith(value), (line, value)
                assert line[col - 1] == " ", (line, value)
        error_line = next(row for row in lines if "ERROR:" in row)
        assert error_line.index("ERROR:") == cols[1]
        # A multi-line message is folded onto the ERROR line.
        assert error_line.endswith("RuntimeError: boom second line")
        assert not any(row.lstrip().startswith("second line") for row in lines)


class TestConcurrencyDetection:
    def test_foreign_version_during_run_detected(
        self,
        sqlite_catalog,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        original = runner._history.get_applied_versions
        call_count = {"n": 0}

        def fake_get_applied(namespace: str) -> set[str]:
            call_count["n"] += 1
            base = original(namespace)
            # The first call (baseline) returns the genuine state; from
            # the second call onwards, simulate a foreign version
            # appearing in history (as if another process had run).
            if call_count["n"] == 1:
                return base
            return base | {"9999"}

        monkeypatch.setattr(runner._history, "get_applied_versions", fake_get_applied)

        with pytest.raises(ConcurrentMigrationError, match="9999"):
            runner.migrate()
        # The version this run applied and recorded stays applied.
        assert original("ns") == {"0001"}

    @staticmethod
    def _fail_reads_after(
        monkeypatch: pytest.MonkeyPatch, runner: MigrationRunner, ok_calls: int
    ) -> None:
        """Make ``get_applied_versions`` raise OSError after *ok_calls* calls."""
        original = runner._history.get_applied_versions
        calls = {"n": 0}

        def flaky(namespace: str) -> set[str]:
            calls["n"] += 1
            if calls["n"] > ok_calls:
                raise OSError("transient read error on manifest list")
            return original(namespace)

        monkeypatch.setattr(runner._history, "get_applied_versions", flaky)

    def test_read_error_after_recording_keeps_applied_row(
        self,
        sqlite_catalog,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failing post-record re-read must not rewrite 'applied' as 'failed'."""
        d = tmp_path / "migrations"
        _write_migration(
            d,
            "0001",
            """
            import pyarrow as pa
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "create orders"
                def upgrade(self, ctx):
                    ctx.create_table_if_not_exists(
                        "orders", pa.schema([pa.field("id", pa.int64())])
                    )
                def downgrade(self, ctx):
                    ctx.drop_table_if_exists("orders")
            """,
        )
        runner = MigrationRunner(sqlite_catalog, namespace="sales", migrations_dir=str(d))
        self._fail_reads_after(monkeypatch, runner, ok_calls=1)  # baseline read only
        with pytest.raises(OSError, match="transient read error"):
            runner.migrate()
        assert sqlite_catalog.load_table("sales.orders") is not None
        fresh = MigrationRunner(sqlite_catalog, namespace="sales", migrations_dir=str(d))
        assert [(r["version"], r["status"]) for r in fresh._history.get_all("sales")] == [
            ("0001", "applied")
        ]

    def test_rollback_read_error_after_recording_propagates(
        self,
        sqlite_catalog,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.migrate()
        self._fail_reads_after(monkeypatch, runner, ok_calls=1)
        with pytest.raises(OSError, match="transient read error"):
            runner.rollback(steps=1)
        statuses = {r["version"]: r["status"] for r in runner._history.get_all("ns")}
        assert statuses == {"0001": "applied", "0002": "rolled_back"}


class TestMetaNamespaceConstant:
    def test_runner_uses_iceberg_ops_meta(self) -> None:
        assert DEFAULT_META_NAMESPACE == "_iceberg_ops"


# ===========================================================================
# Version normalisation and --target handling
# ===========================================================================


def _write_noop(
    target: Path,
    version: str,
    *,
    name: str = "noop",
    declared: str | None = None,
    description: str | None = None,
) -> Path:
    """Write a no-op migration ``target/<version>_<name>.py`` and return its path."""
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{version}_{name}.py"
    path.write_text(
        textwrap.dedent(
            f"""
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "{declared if declared is not None else version}"
                description = "{description or name}"
                def upgrade(self, ctx): pass
                def downgrade(self, ctx): pass
            """
        )
    )
    return path


class TestNormalizeVersion:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [("2", "0002"), ("0002", "0002"), (" 10 ", "0010"), (7, "0007"), ("0", "0000")],
    )
    def test_valid(self, value, expected) -> None:
        assert normalize_version(value) == expected

    @pytest.mark.parametrize("value", ["abc", "", "00002", "-1", "1.0", -1, True])
    def test_invalid(self, value) -> None:
        with pytest.raises(ValueError, match="expected 1-4 digits"):
            normalize_version(value)


class TestTargetHandling:
    @pytest.fixture
    def runner(self, sqlite_catalog, tmp_path) -> MigrationRunner:
        d = tmp_path / "migrations"
        for v in ("0001", "0002", "0010"):
            _write_noop(d, v)
        return MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))

    def test_short_target_is_numeric(self, runner) -> None:
        # "2" must not be compared as a string ("0010" <= "2").
        runner.migrate(target="2")
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    def test_int_target(self, runner) -> None:
        runner.migrate(target=2)
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    @pytest.mark.parametrize("bad", ["0009", "abc"])
    def test_bad_target_raises_and_writes_nothing(self, runner, sqlite_catalog, bad) -> None:
        with pytest.raises(ValueError):
            runner.migrate(target=bad)
        assert runner._history.get_all("ns") == []
        assert sqlite_catalog.list_namespaces() == []

    def test_stamp_short_target(self, runner) -> None:
        runner.stamp(target="2")
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    def test_stamp_unknown_target_raises(self, runner) -> None:
        with pytest.raises(ValueError, match="Unknown target"):
            runner.stamp(target="0003")
        assert runner._history.get_all("ns") == []

    def test_plan_short_target(self, runner, capsys: pytest.CaptureFixture[str]) -> None:
        runner.plan(target="1")
        out = capsys.readouterr().out
        assert "0001" in out
        assert "0002" not in out
        assert "0010" not in out

    def test_rollback_short_target(self, runner) -> None:
        runner.migrate()
        runner.rollback(target="1")
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_rollback_to_base_undoes_all(self, runner) -> None:
        runner.migrate()
        runner.rollback(target="0000")
        assert runner._history.get_applied_versions("ns") == set()

    @pytest.mark.parametrize("steps", [0, -1])
    def test_rollback_rejects_non_positive_steps(self, runner, steps) -> None:
        runner.migrate()
        with pytest.raises(ValueError, match="steps"):
            runner.rollback(steps=steps)
        assert runner._history.get_applied_versions("ns") == {"0001", "0002", "0010"}


# ===========================================================================
# Discovery hardening
# ===========================================================================


class TestDiscoverValidation:
    def test_dataclass_helper_with_future_annotations(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_dc.py").write_text(
            textwrap.dedent(
                """
                from __future__ import annotations

                from dataclasses import dataclass

                from iceberg_ops.migration import BaseMigration


                @dataclass
                class _Spec:
                    name: str
                    width: int = 1


                class Migration(BaseMigration):
                    version = "0001"
                    description = "dataclass helper"

                    def upgrade(self, ctx):
                        assert _Spec("a").width == 1

                    def downgrade(self, ctx):
                        pass
                """
            )
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        assert [m.version for m in runner._discover()] == ["0001"]
        runner.migrate()
        assert runner._history.get_applied_versions("ns") == {"0001"}

    def test_syntax_error_names_the_file(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_broken.py").write_text('description = "a "b""\n')
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match=r"0001_broken\.py.*SyntaxError"):
            runner._discover()

    def test_constructor_error_names_the_file(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_init_fails.py").write_text(
            textwrap.dedent(
                """
                from iceberg_ops.migration import BaseMigration
                class Migration(BaseMigration):
                    version = "0001"
                    def __init__(self):
                        raise KeyError("missing setting")
                """
            )
        )
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match=r"0001_init_fails\.py.*KeyError"):
            runner._discover()

    def test_same_filename_in_two_namespace_dirs(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_noop(d / "sales", "0001", name="x", description="sales one")
        _write_noop(d / "finance", "0001", name="x", description="finance one")
        sales = MigrationRunner(sqlite_catalog, namespace="sales", migrations_dir=str(d))
        finance = MigrationRunner(sqlite_catalog, namespace="finance", migrations_dir=str(d))
        [m_sales] = sales._discover()
        [m_fin] = finance._discover()
        assert m_sales.description == "sales one"
        assert m_fin.description == "finance one"
        assert type(m_sales).__module__ != type(m_fin).__module__
        assert sys.modules[type(m_sales).__module__].Migration is type(m_sales)

    def test_duplicate_versions_raise(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_noop(d, "0003", name="a")
        _write_noop(d, "0003", name="b")
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match=r"0003_a\.py.*0003_b\.py"):
            runner._discover()

    def test_version_must_match_filename_prefix(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_noop(d, "0004", name="c", declared="0002")
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match="does not match filename prefix '0004'"):
            runner._discover()

    def test_empty_version_rejected(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        _write_noop(d, "0001", name="blank", declared="")
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        with pytest.raises(RuntimeError, match="does not match filename prefix"):
            runner._discover()

    def test_valid_set_sorted_by_version(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        for v in ("0010", "0002", "0001"):
            _write_noop(d, v)
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        assert [m.version for m in runner._discover()] == ["0001", "0002", "0010"]


# ===========================================================================
# Missing migration files
# ===========================================================================


class TestMissingMigrationFile:
    def test_rollback_checks_files_before_any_downgrade(self, sqlite_catalog, tmp_path) -> None:
        d = tmp_path / "migrations"
        first = _write_noop(d, "0001")
        _write_noop(d, "0002")
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        runner.migrate()
        first.unlink()
        with pytest.raises(RuntimeError, match=r"\['0001'\].*nothing rolled back"):
            runner.rollback(steps=2)
        # 0002's downgrade did not run.
        assert runner._history.get_applied_versions("ns") == {"0001", "0002"}

    def test_status_lists_history_without_file(
        self,
        sqlite_catalog,
        tmp_path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        d = tmp_path / "migrations"
        first = _write_noop(d, "0001", description="first")
        _write_noop(d, "0002")
        runner = MigrationRunner(sqlite_catalog, namespace="ns", migrations_dir=str(d))
        runner.migrate()
        first.unlink()
        capsys.readouterr()
        runner.status()
        out = capsys.readouterr().out
        line = next(ln for ln in out.splitlines() if "0001" in ln)
        assert "[applied" in line
        assert "first" in line
        assert "migration file missing" in line


# ===========================================================================
# Read-only commands
# ===========================================================================


class TestReadOnlyCommands:
    def test_plan_status_history_create_nothing(
        self,
        sqlite_catalog,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        runner = MigrationRunner(
            sqlite_catalog,
            namespace="ns",
            migrations_dir=SAMPLE_MIGRATIONS_DIR,
        )
        runner.plan()
        runner.status()
        runner.history()
        assert sqlite_catalog.list_namespaces() == []
        assert "[pending]" in capsys.readouterr().out
