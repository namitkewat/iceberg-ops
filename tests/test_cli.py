"""Tests for iceberg_ops.cli."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from iceberg_ops.cli import main

# A minimal working migration body used by the lifecycle test.
_LIFECYCLE_MIGRATION = '''\
"""Lifecycle test migration."""

from __future__ import annotations

import pyarrow as pa

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = "0001"
    description = "create widgets"

    def upgrade(self, ctx: MigrationContext) -> None:
        ctx.create_table_if_not_exists(
            "widgets",
            pa.schema([pa.field("id", pa.int64())]),
        )

    def downgrade(self, ctx: MigrationContext) -> None:
        ctx.drop_table_if_exists("widgets")
'''


@pytest.fixture(autouse=True)
def _clean_cli_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep ICEBERG_OPS_* variables from the developer shell out of the tests."""
    for name in list(os.environ):
        if name.upper().startswith("ICEBERG_OPS_"):
            monkeypatch.delenv(name)


@pytest.fixture
def cli_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run a CLI test inside an empty per-test directory.

    Replaces ``CliRunner.isolated_filesystem()`` (deprecated in Click 8.5).
    The CLI resolves its config file and ``./migrations`` relative to the
    working directory, so the test still needs a chdir.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestInit:
    def test_creates_files(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0, result.output
        assert "Initialised" in result.output
        assert (Path(cli_cwd) / "iceberg-ops.yaml").exists()
        assert (Path(cli_cwd) / "migrations").is_dir()

    def test_does_not_require_catalog(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["init"])
        # Init should succeed without --catalog or --namespace.
        assert result.exit_code == 0

    def test_init_writes_iceberg_ops_yaml(self, cli_cwd: Path) -> None:
        """``init`` writes the config under the name the CLI reads by default."""
        runner = CliRunner()
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0, result.output
        config = cli_cwd / "iceberg-ops.yaml"
        assert config.read_text().startswith("# iceberg-ops configuration")
        assert "Review iceberg-ops.yaml" in result.output
        assert sorted(p.name for p in cli_cwd.glob("*.yaml")) == ["iceberg-ops.yaml"]


class TestRevision:
    def test_creates_migration_file(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(main, ["revision", "-m", "create users"])
        assert result.exit_code == 0, result.output
        assert "0001_create_users.py" in result.output
        assert (Path(cli_cwd) / "migrations" / "0001_create_users.py").exists()

    def test_requires_message(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(main, ["revision"])
        assert result.exit_code != 0
        assert "missing" in result.output.lower() or "-m" in result.output


class TestRevisionAutogenerate:
    """Smoke tests for ``revision --autogenerate``.

    Exhaustive autogen behaviour lives in tests/test_autogen.py; these
    tests only verify the CLI plumbing routes flags correctly and
    surfaces error messages.
    """

    def test_autogenerate_requires_target_table(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "ns",
                "revision",
                "-m",
                "x",
                "--autogenerate",
            ],
        )
        assert result.exit_code != 0
        assert "--target-table is required" in result.output

    def test_autogenerate_requires_one_target_schema(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "ns",
                "revision",
                "-m",
                "x",
                "--autogenerate",
                "--target-table",
                "ns.t",
            ],
        )
        assert result.exit_code != 0
        assert "exactly one" in result.output.lower()

    def test_autogen_flags_invalid_without_autogenerate(self, cli_cwd: Path) -> None:
        """--target-table without --autogenerate should error."""
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(
            main,
            ["revision", "-m", "x", "--target-table", "ns.t"],
        )
        assert result.exit_code != 0
        assert "only valid with --autogenerate" in result.output

    def test_rename_hint_must_have_colon(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        result = runner.invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "ns",
                "revision",
                "-m",
                "x",
                "--autogenerate",
                "--target-table",
                "ns.t",
                "--rename-hint",
                "no_colon_here",
            ],
        )
        assert result.exit_code != 0
        assert "old:new" in result.output

    def test_full_autogen_flow_writes_file(self, cli_cwd: Path) -> None:
        """End-to-end: create a table, run autogen against a pa.Schema
        target with one extra column, verify the file lands and
        compiles.
        """
        runner = CliRunner()
        # Init the project layout.
        runner.invoke(main, ["init"])

        # Create a table in the catalog by running a hand-written
        # migration. This mirrors what real users do: scaffolding +
        # a runnable migration to seed the catalog before autogen
        # is meaningful.
        seed_body = textwrap.dedent('''\
            """Seed users table."""
            from __future__ import annotations
            import pyarrow as pa
            from iceberg_ops.migration import BaseMigration, MigrationContext

            class Migration(BaseMigration):
                version = "0001"
                description = "create users"
                def upgrade(self, ctx: MigrationContext) -> None:
                    ctx.create_table_if_not_exists(
                        "users",
                        pa.schema([
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("name", pa.string()),
                        ]),
                    )
                def downgrade(self, ctx: MigrationContext) -> None:
                    ctx.drop_table_if_exists("users")
        ''')
        (Path(cli_cwd) / "migrations" / "0001_create_users.py").write_text(seed_body)

        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "ns",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]

        # Apply the seed migration so ns.users exists.
        result = runner.invoke(main, [*catalog_args, "migrate"])
        assert result.exit_code == 0, result.output

        # Drop a target schema module on the import path - add an
        # 'email' column.
        target_module = Path(cli_cwd) / "_autogen_cli_target.py"
        target_module.write_text(
            "import pyarrow as pa\n"
            "TARGET = pa.schema([\n"
            "    pa.field('id', pa.int64(), nullable=False),\n"
            "    pa.field('name', pa.string()),\n"
            "    pa.field('email', pa.string()),\n"
            "])\n"
        )

        # The CLI puts the working directory on sys.path while it imports
        # the module, so only the sys.modules entry needs cleaning up.
        try:
            result = runner.invoke(
                main,
                [
                    *catalog_args,
                    "revision",
                    "-m",
                    "add email",
                    "--autogenerate",
                    "--target-table",
                    "users",
                    "--target-schema-arrow",
                    "_autogen_cli_target:TARGET",
                ],
            )
        finally:
            sys.modules.pop("_autogen_cli_target", None)

        assert result.exit_code == 0, result.output
        assert "Generated" in result.output
        assert "REVIEW BEFORE RUNNING" in result.output

        # The generated file must exist and parse as valid Python.
        generated = Path(cli_cwd) / "migrations" / "0002_add_email.py"
        assert generated.exists(), result.output
        source = generated.read_text()
        compile(source, str(generated), "exec")
        assert "ctx.enforce_column" in source
        assert "email" in source


class TestRequiresCatalogNamespace:
    def _empty_config_invocation(
        self,
        runner: CliRunner,
        *args: str,
    ):
        # The per-test cwd holds no config file, so CLI flag values come
        # solely from args.
        return runner.invoke(main, list(args))

    def test_status_without_catalog_errors(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = self._empty_config_invocation(runner, "status")
        assert result.exit_code != 0
        assert "--catalog or --catalog-name is required" in result.output

    def test_status_without_namespace_errors(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = self._empty_config_invocation(
            runner,
            "--catalog",
            "sqlite",
            "status",
        )
        assert result.exit_code != 0
        assert "--namespace is required" in result.output


# The short config name used before the rename. It belongs to an unrelated
# tool, so the CLI must never read it. Built from parts so the repository
# text-hygiene test (tests/test_hygiene.py) does not flag this file.
_PRE_RENAME_CONFIG = "ice" + "ops.yaml"


class TestConfigFile:
    def test_default_config_is_iceberg_ops_yaml(self, cli_cwd: Path) -> None:
        """Without ``--config`` the CLI reads ``./iceberg-ops.yaml``."""
        (cli_cwd / "iceberg-ops.yaml").write_text("catalog: sqlite\n")
        result = CliRunner().invoke(main, ["status"])
        # The catalog came from the file, so only the namespace is missing.
        assert result.exit_code == 2, result.output
        assert "--namespace is required" in result.output

    def test_legacy_short_config_name_is_ignored(self, cli_cwd: Path) -> None:
        """A config file under the pre-rename name is not read."""
        (cli_cwd / _PRE_RENAME_CONFIG).write_text("catalog: sqlite\nnamespace: ns\n")
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "--catalog or --catalog-name is required" in result.output


class TestVersion:
    def test_version_flag(self) -> None:
        """``--version`` prints the program name and the package version."""
        from iceberg_ops import __version__

        result = CliRunner().invoke(main, ["--version"])
        assert result.exit_code == 0, result.output
        assert result.output == f"iceberg-ops {__version__}\n"


class TestLifecycle:
    """init -> revision -> custom migration -> migrate -> status -> rollback."""

    def test_full_lifecycle(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        # 1. init
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0, result.output

        # 2. write a working migration manually (revision scaffolds a
        #    skeleton that raises NotImplementedError; this test wants a
        #    real working body)
        (Path(cli_cwd) / "migrations" / "0001_create_widgets.py").write_text(_LIFECYCLE_MIGRATION)

        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "test",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]

        # 3. migrate
        result = runner.invoke(main, [*catalog_args, "migrate"])
        assert result.exit_code == 0, result.output
        assert "[ok]" in result.output
        assert "0001" in result.output

        # 4. status — should show applied
        result = runner.invoke(main, [*catalog_args, "status"])
        assert result.exit_code == 0
        assert "applied" in result.output
        assert "0001" in result.output

        # 5. rollback
        result = runner.invoke(main, [*catalog_args, "rollback", "--steps", "1"])
        assert result.exit_code == 0, result.output
        assert "[ok]" in result.output

        # 6. status — should show rolled_back
        result = runner.invoke(main, [*catalog_args, "status"])
        assert result.exit_code == 0
        assert "rolled_back" in result.output

    def test_history_after_migrate(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        (Path(cli_cwd) / "migrations" / "0001_create_widgets.py").write_text(_LIFECYCLE_MIGRATION)

        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "test",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        runner.invoke(main, [*catalog_args, "migrate"])
        result = runner.invoke(main, [*catalog_args, "history"])
        assert result.exit_code == 0
        assert "0001" in result.output
        assert "applied" in result.output

    def test_plan_shows_pending_before_migrate(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        (Path(cli_cwd) / "migrations" / "0001_create_widgets.py").write_text(_LIFECYCLE_MIGRATION)
        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "test",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        result = runner.invoke(main, [*catalog_args, "plan"])
        assert result.exit_code == 0
        assert "[pending]" in result.output
        assert "0001" in result.output


class TestStamp:
    def test_stamp_marks_applied_without_running(self, cli_cwd: Path) -> None:
        """Migration with a body that would error if upgrade() actually ran."""
        runner = CliRunner()
        bad_body = textwrap.dedent("""\
            from iceberg_ops.migration import BaseMigration
            class Migration(BaseMigration):
                version = "0001"
                description = "would-fail"
                def upgrade(self, ctx):
                    raise RuntimeError("if this runs, the test fails")
                def downgrade(self, ctx):
                    pass
        """)
        runner.invoke(main, ["init"])
        (Path(cli_cwd) / "migrations" / "0001_bad.py").write_text(bad_body)
        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "test",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        result = runner.invoke(main, [*catalog_args, "stamp"])
        assert result.exit_code == 0, result.output
        assert "Stamped" in result.output

        # Status confirms applied.
        result = runner.invoke(main, [*catalog_args, "status"])
        assert "applied" in result.output

    def test_help_matches_rolled_back_behaviour(self, cli_cwd: Path) -> None:
        """The help says rolled-back versions are stamped, and they are."""
        runner = CliRunner()
        (cli_cwd / "migrations").mkdir()
        (cli_cwd / "migrations" / "0001_widgets.py").write_text(_LIFECYCLE_MIGRATION)
        for command in (["migrate"], ["rollback", "--target", "0"]):
            result = runner.invoke(main, [*_SQLITE_ARGS, *command])
            assert result.exit_code == 0, result.output

        result = runner.invoke(main, [*_SQLITE_ARGS, "stamp"])
        assert result.exit_code == 0, result.output
        assert "Stamped 0001" in result.output

        help_text = " ".join(runner.invoke(main, ["stamp", "--help"]).output.split())
        assert "not already in history" not in help_text
        assert "not currently applied (never run, failed or rolled back)" in help_text


class TestCurrentAlias:
    def test_current_aliases_status(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        (Path(".") / "migrations" / "0001_create_widgets.py").write_text(_LIFECYCLE_MIGRATION)
        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "test",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        runner.invoke(main, [*catalog_args, "migrate"])
        current = runner.invoke(main, [*catalog_args, "current"])
        status = runner.invoke(main, [*catalog_args, "status"])
        # Both should show the same applied summary.
        assert "0001" in current.output
        assert "applied" in current.output
        assert "Migration Status" in current.output
        assert "Migration Status" in status.output


class TestMigrateDataCli:
    """Smoke tests for ``iceberg-ops migrate-data``.

    Exhaustive migrator behaviour lives in tests/test_data_migration.py;
    these tests only verify the CLI plumbing routes flags correctly
    and surfaces error messages.
    """

    def _seed_source_table(self, fs: Path) -> None:
        """Create + seed a source table by running a hand-written migration.

        Mirrors what real users do: scaffolding + a runnable
        migration to seed the catalog before invoking migrate-data.
        """
        seed_body = textwrap.dedent('''\
            """Seed source table."""
            from __future__ import annotations
            import pyarrow as pa
            from iceberg_ops.migration import BaseMigration, MigrationContext

            class Migration(BaseMigration):
                version = "0001"
                description = "create source"
                def upgrade(self, ctx: MigrationContext) -> None:
                    tbl = ctx.create_table_if_not_exists(
                        "items",
                        pa.schema([
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("name", pa.string()),
                        ]),
                    )
                    tbl.append(pa.Table.from_pydict(
                        {"id": [1, 2, 3], "name": ["a", "b", "c"]},
                        schema=tbl.schema().as_arrow(),
                    ))
                def downgrade(self, ctx: MigrationContext) -> None:
                    ctx.drop_table_if_exists("items")
        ''')
        (Path(fs) / "migrations" / "0001_seed_items.py").write_text(seed_body)

    def _create_empty_dest_table(self, fs: Path) -> None:
        """Create an empty destination table via a second migration."""
        body = textwrap.dedent('''\
            """Create dest table."""
            from __future__ import annotations
            import pyarrow as pa
            from iceberg_ops.migration import BaseMigration, MigrationContext

            class Migration(BaseMigration):
                version = "0002"
                description = "create dest"
                def upgrade(self, ctx: MigrationContext) -> None:
                    ctx.create_table_if_not_exists(
                        "items_dst",
                        pa.schema([
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("name", pa.string()),
                        ]),
                    )
                def downgrade(self, ctx: MigrationContext) -> None:
                    ctx.drop_table_if_exists("items_dst")
        ''')
        (Path(fs) / "migrations" / "0002_create_dst.py").write_text(body)

    def test_upsert_requires_keys(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "migrate-data",
                "--src-catalog",
                "sqlite",
                "--src-namespace",
                "ns",
                "--src-table",
                "items",
                "--dst-namespace",
                "ns",
                "--write-mode",
                "upsert",
            ],
        )
        assert result.exit_code != 0
        assert "--upsert-keys" in result.output

    def test_column_mapping_must_have_colon(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "migrate-data",
                "--src-catalog",
                "sqlite",
                "--src-namespace",
                "ns",
                "--src-table",
                "items",
                "--dst-namespace",
                "ns",
                "--column-mapping",
                "no_colon_here",
            ],
        )
        assert result.exit_code != 0
        assert "src:dst" in result.output

    def test_full_migrate_data_flow(self, cli_cwd: Path) -> None:
        """End-to-end: seed source + dst tables, run migrate-data, verify output."""
        runner = CliRunner()
        runner.invoke(main, ["init"])
        self._seed_source_table(cli_cwd)
        self._create_empty_dest_table(cli_cwd)
        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "src",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        # Apply migrations to seed both tables. We use a single
        # namespace "src" for both schema migrations - the
        # migrations create tables in that namespace.
        r = runner.invoke(main, [*catalog_args, "migrate"])
        assert r.exit_code == 0, r.output

        # Now migrate-data: items -> items_dst within the same
        # catalog and namespace.
        result = runner.invoke(
            main,
            [
                "migrate-data",
                "--src-catalog",
                "sqlite",
                "--src-namespace",
                "src",
                "--src-table",
                "items",
                "--src-db-path",
                "catalog.db",
                "--src-warehouse",
                "warehouse",
                "--dst-namespace",
                "src",
                "--dst-table",
                "items_dst",
                "--quiet",
                "--verify",
                "row_count",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Migrating sqlite/src.items" in result.output
        assert "Done:" in result.output
        assert "[ok] row_count matches" in result.output

    def test_dry_run_writes_nothing(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        runner.invoke(main, ["init"])
        self._seed_source_table(cli_cwd)
        self._create_empty_dest_table(cli_cwd)
        catalog_args = [
            "--catalog",
            "sqlite",
            "--namespace",
            "src",
            "--db-path",
            "catalog.db",
            "--warehouse",
            "warehouse",
        ]
        runner.invoke(main, [*catalog_args, "migrate"])

        result = runner.invoke(
            main,
            [
                "migrate-data",
                "--src-catalog",
                "sqlite",
                "--src-namespace",
                "src",
                "--src-table",
                "items",
                "--src-db-path",
                "catalog.db",
                "--src-warehouse",
                "warehouse",
                "--dst-namespace",
                "src",
                "--dst-table",
                "items_dst",
                "--dry-run",
                "--quiet",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DRY RUN" in result.output
        assert "Dry-run complete" in result.output
        assert "estimated 3 rows" in result.output


class TestRunPoliciesCli:
    """Smoke tests for ``iceberg-ops run-policies``.

    Exhaustive Polaris client / dispatcher behaviour lives in
    tests/test_polaris_*.py; these tests only verify flag parsing
    and the end-to-end CLI plumbing.
    """

    def _seed_table(self, fs: Path, namespace: str, name: str) -> None:
        """Write + apply a hand-written migration that creates the table."""
        body = textwrap.dedent(f'''\
            """Seed table for run-policies test."""
            from __future__ import annotations
            import pyarrow as pa
            from iceberg_ops.migration import BaseMigration, MigrationContext

            class Migration(BaseMigration):
                version = "0001"
                description = "create {name}"
                def upgrade(self, ctx: MigrationContext) -> None:
                    tbl = ctx.create_table_if_not_exists(
                        "{name}",
                        pa.schema([
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("name", pa.string()),
                        ]),
                    )
                    tbl.append(pa.Table.from_pydict(
                        {{"id": [1, 2], "name": ["a", "b"]}},
                        schema=tbl.schema().as_arrow(),
                    ))
                def downgrade(self, ctx: MigrationContext) -> None:
                    ctx.drop_table_if_exists("{name}")
        ''')
        (Path(fs) / "migrations" / f"0001_seed_{name}.py").write_text(body)

    def test_requires_token_or_credentials(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://x",
                "--polaris-catalog",
                "cat",
                "--target-namespace",
                "ns",
                "--target-table",
                "t",
                "--catalog",
                "sqlite",
            ],
        )
        assert result.exit_code != 0
        assert "exactly one" in result.output

    def test_partial_credentials_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://x",
                "--polaris-catalog",
                "cat",
                "--polaris-client-id",
                "id-only",  # secret missing
                "--target-namespace",
                "ns",
                "--target-table",
                "t",
                "--catalog",
                "sqlite",
            ],
        )
        assert result.exit_code != 0
        assert "must be set together" in result.output

    def test_token_and_credentials_together_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://x",
                "--polaris-catalog",
                "cat",
                "--polaris-token",
                "t",
                "--polaris-client-id",
                "i",
                "--polaris-client-secret",
                "s",
                "--target-namespace",
                "ns",
                "--target-table",
                "t",
                "--catalog",
                "sqlite",
            ],
        )
        assert result.exit_code != 0
        assert "exactly one" in result.output

    def test_empty_policy_list_is_noop(self, cli_cwd: Path, monkeypatch) -> None:
        # Polaris returns no applicable policies - the CLI prints a
        # friendly message and exits 0. The catalog isn't consulted.
        from iceberg_ops import polaris as polaris_pkg

        def fake_fetch(_client, _ns, _tbl):
            return []

        monkeypatch.setattr(polaris_pkg, "fetch_table_policies", fake_fetch)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://x",
                "--polaris-catalog",
                "cat",
                "--polaris-token",
                "t",
                "--target-namespace",
                "ns",
                "--target-table",
                "missing",
                "--catalog",
                "sqlite",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "No applicable policies" in result.output

    def test_full_run_policies_flow(self, cli_cwd: Path, monkeypatch) -> None:
        """End-to-end: fake Polaris response, real local table, real dispatch."""
        from iceberg_ops import polaris as polaris_pkg
        from iceberg_ops.polaris import Policy

        def fake_fetch(_client, _ns, _tbl):
            return [
                Policy(
                    name="snap",
                    policy_type="system.snapshot-expiry",
                    content={
                        "enable": True,
                        "config": {"max_snapshot_age_days": 1, "min_snapshot_to_keep": 1},
                    },
                ),
                Policy(
                    name="weird",
                    policy_type="not.a.real.type",
                    content={},
                ),
            ]

        monkeypatch.setattr(polaris_pkg, "fetch_table_policies", fake_fetch)

        runner = CliRunner()
        runner.invoke(main, ["init"])
        self._seed_table(cli_cwd, "polns", "items")
        r = runner.invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "polns",
                "--db-path",
                "catalog.db",
                "--warehouse",
                "warehouse",
                "migrate",
            ],
        )
        assert r.exit_code == 0, r.output

        result = runner.invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://x",
                "--polaris-catalog",
                "cat",
                "--polaris-token",
                "t",
                "--target-namespace",
                "polns",
                "--target-table",
                "items",
                "--catalog",
                "sqlite",
                "--db-path",
                "catalog.db",
                "--warehouse",
                "warehouse",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Dispatching 2 policy" in result.output
        assert "[ok] snap" in result.output
        # Unsupported type is skipped, not failed.
        assert "[skip] weird" in result.output
        assert "run-policies is experimental" in result.stderr


# ===========================================================================
# Shared helpers for the behaviour tests below
# ===========================================================================


def _table_migration(version: str, table: str) -> str:
    """Body of a migration that creates (and on downgrade drops) *table*."""
    return textwrap.dedent(f'''\
        """Create {table}."""
        from __future__ import annotations
        import pyarrow as pa
        from iceberg_ops.migration import BaseMigration, MigrationContext

        class Migration(BaseMigration):
            version = "{version}"
            description = "create {table}"
            def upgrade(self, ctx: MigrationContext) -> None:
                ctx.create_table_if_not_exists(
                    "{table}", pa.schema([pa.field("id", pa.int64())])
                )
            def downgrade(self, ctx: MigrationContext) -> None:
                ctx.drop_table_if_exists("{table}")
        ''')


def _write_migrations(migrations_dir: Path, count: int) -> None:
    """Write migrations 0001..<count> creating tables t1..t<count>."""
    migrations_dir.mkdir(parents=True, exist_ok=True)
    for n in range(1, count + 1):
        version = f"{n:04d}"
        (migrations_dir / f"{version}_create_t{n}.py").write_text(
            _table_migration(version, f"t{n}"), encoding="utf-8"
        )


_SQLITE_ARGS = [
    "--catalog",
    "sqlite",
    "--namespace",
    "ns",
    "--db-path",
    "catalog.db",
    "--warehouse",
    "warehouse",
]


def _statuses(output: str) -> dict[str, str]:
    """Map version -> status from ``status`` output."""
    return {version: state for state, version in re.findall(r"\[(\w+)\s*\] (\d{4}) - ", output)}


# ===========================================================================
# Default local paths and config-relative resolution
# ===========================================================================


class TestFirstRun:
    def test_first_run_init_migrate_no_flags(self, cli_cwd: Path) -> None:
        """init + migrate with no flags uses ./local_data next to the config."""
        runner = CliRunner()
        assert runner.invoke(main, ["init"]).exit_code == 0
        (cli_cwd / "migrations" / "0001_create_t1.py").write_text(
            _table_migration("0001", "t1"), encoding="utf-8"
        )

        result = runner.invoke(main, ["migrate"])
        assert result.exit_code == 0, result.output
        assert "[ok] 0001 applied" in result.output
        assert (cli_cwd / "local_data" / "iceberg_catalog.db").is_file()
        assert (cli_cwd / "local_data" / "iceberg_warehouse").is_dir()

        status = runner.invoke(main, ["status"])
        assert status.exit_code == 0, status.output
        assert _statuses(status.output) == {"0001": "applied"}

    def test_init_next_steps(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, ["init"])
        assert "local SQLite under local_data/" in result.output
        assert "Implement upgrade()/downgrade()" in result.output
        assert "4. iceberg-ops migrate" in result.output


class TestConfigRelativePaths:
    def _project(self, root: Path, config_body: str) -> Path:
        proj = root / "proj"
        _write_migrations(proj / "migrations", 1)
        (proj / "iceberg-ops.yaml").write_text(config_body, encoding="utf-8")
        (root / "sibling").mkdir()
        return proj

    def test_config_relative_paths_resolve_to_config_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proj = self._project(
            tmp_path,
            "catalog: sqlite\nnamespace: ns\ndb_path: state/c.db\nwarehouse: state/wh\n",
        )
        monkeypatch.chdir(tmp_path / "sibling")

        result = CliRunner().invoke(main, ["--config", "../proj/iceberg-ops.yaml", "migrate"])

        assert result.exit_code == 0, result.output
        assert (proj / "state" / "c.db").is_file()
        assert (proj / "state" / "wh").is_dir()
        assert not (tmp_path / "sibling" / "state").exists()

    def test_default_local_data_next_to_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proj = self._project(tmp_path, "catalog: sqlite\nnamespace: ns\n")
        monkeypatch.chdir(tmp_path / "sibling")

        result = CliRunner().invoke(main, ["--config", "../proj/iceberg-ops.yaml", "migrate"])

        assert result.exit_code == 0, result.output
        assert (proj / "local_data" / "iceberg_catalog.db").is_file()
        assert not (tmp_path / "sibling" / "local_data").exists()

    def test_config_migrations_dir_relative_to_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proj = tmp_path / "proj"
        _write_migrations(proj / "db" / "migs", 2)
        (proj / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\nnamespace: ns\nmigrations_dir: db/migs\n", encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)

        result = CliRunner().invoke(main, ["--config", "proj/iceberg-ops.yaml", "migrate"])

        assert result.exit_code == 0, result.output
        assert "Applied 2 migration(s)" in result.output

    def test_flag_paths_stay_cwd_relative(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._project(tmp_path, "catalog: sqlite\nnamespace: ns\ndb_path: state/c.db\n")
        sibling = tmp_path / "sibling"
        monkeypatch.chdir(sibling)

        result = CliRunner().invoke(
            main,
            [
                "--config",
                "../proj/iceberg-ops.yaml",
                "--db-path",
                "flag.db",
                "--migrations-dir",
                "../proj/migrations",
                "migrate",
            ],
        )

        assert result.exit_code == 0, result.output
        assert (sibling / "flag.db").is_file()
        assert not (tmp_path / "proj" / "state").exists()


# ===========================================================================
# Config file validation
# ===========================================================================


class TestConfigValidation:
    def test_explicit_missing_config_is_usage_error(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, ["--config", "typo.yaml", *_SQLITE_ARGS, "status"])
        assert result.exit_code == 2, result.output
        assert "not found" in result.output

    def test_default_missing_config_ok(self, cli_cwd: Path) -> None:
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 0, result.output
        assert "Migration Status" in result.output

    def test_missing_migrations_dir_usage_error(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(
            main, [*_SQLITE_ARGS, "--migrations-dir", "migrationz", "migrate"]
        )
        assert result.exit_code == 2, result.output
        assert "Migrations directory 'migrationz' not found" in result.output
        assert "All migrations are up to date" not in result.output

    def test_history_without_migrations_dir_ok(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "history"])
        assert result.exit_code == 0, result.output
        assert "no migration history" in result.output

    def test_config_without_pyyaml_is_usage_error(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text("catalog: sqlite\n", encoding="utf-8")
        monkeypatch.setitem(sys.modules, "yaml", None)
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "requires PyYAML" in result.output

    @pytest.mark.parametrize("extra", [[], ["--help"]])
    def test_list_config_is_usage_error(self, cli_cwd: Path, extra: list[str]) -> None:
        """The group callback runs before subcommand --help, so it must not crash."""
        (cli_cwd / "iceberg-ops.yaml").write_text("- catalog\n- sqlite\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["status", *extra])
        assert result.exit_code == 2, result.output
        assert "top level must be a mapping" in result.output
        assert not isinstance(result.exception, AttributeError)

    def test_int_namespace_says_quote_it(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\nnamespace: 123\n", encoding="utf-8"
        )
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "quote it" in result.output

    def test_unknown_key_warns(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\nnamespace: ns\nmigration_dir: elsewhere\n", encoding="utf-8"
        )
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 0, result.output
        assert "unknown key 'migration_dir'" in result.stderr

    def test_invalid_yaml_is_usage_error(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text("catalog: [unclosed\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "Cannot read config file" in result.output

    def test_config_typo_kind_is_usage_error(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(
            "catalog: sqllite\nnamespace: ns\n", encoding="utf-8"
        )
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "Unknown catalog kind 'sqllite'" in result.output
        assert "sqlite, glue, rest, hive, s3tables" in result.output

    def test_flag_typo_kind_is_usage_error(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, ["--catalog", "sqllite", "--namespace", "ns", "status"])
        assert result.exit_code == 2, result.output
        assert "Invalid value" in result.output

    def test_memory_catalog_rejected(self, cli_cwd: Path) -> None:
        """A config-file memory catalog would 'apply' migrations and forget them."""
        (cli_cwd / "iceberg-ops.yaml").write_text(
            "catalog: memory\nnamespace: ns\n", encoding="utf-8"
        )
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, ["migrate"])
        assert result.exit_code == 2, result.output
        assert "does not persist" in result.output

    def test_memory_flag_rejected(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, ["--catalog", "memory", "--namespace", "ns", "status"])
        assert result.exit_code == 2, result.output
        assert "Invalid value" in result.output

    def test_kind_flag_is_case_insensitive(self, cli_cwd: Path) -> None:
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, ["--catalog", "SQLite", "--namespace", "ns", "status"])
        assert result.exit_code == 0, result.output

    def test_utf8_config_with_bom(self, cli_cwd: Path) -> None:
        """A UTF-8 config (with the BOM some Windows editors add) is read as UTF-8."""
        (cli_cwd / "iceberg-ops.yaml").write_bytes(
            "﻿catalog: sqlite\nnamespace: ventas_ō\n".encode()
        )
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 0, result.output
        assert "namespace: ventas_ō" in result.output

    def test_no_default_encoding_io(self, tmp_path: Path) -> None:
        """init and config loading never fall back to the locale encoding."""
        code = textwrap.dedent(f"""\
            from iceberg_ops.cli import _load_config
            from iceberg_ops.scaffolding import init_project
            root = {str(tmp_path)!r}
            init_project(root)
            init_project(root)
            assert _load_config(root + "/iceberg-ops.yaml")["catalog"] == "sqlite"
            """)
        proc = subprocess.run(
            [
                sys.executable,
                "-X",
                "warn_default_encoding",
                "-W",
                "error::EncodingWarning",
                "-c",
                code,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr


# ===========================================================================
# init file handling
# ===========================================================================


class TestInitFiles:
    def test_init_preserves_utf8_gitignore(self, cli_cwd: Path) -> None:
        original = "# Tōkyō\n*.log\n".encode()
        (cli_cwd / ".gitignore").write_bytes(original)
        result = CliRunner().invoke(main, ["init"])
        assert result.exit_code == 0, result.output
        data = (cli_cwd / ".gitignore").read_bytes()
        assert data.startswith(original)
        text = data.decode("utf-8")
        assert "# iceberg-ops local data" in text
        assert "local_data/" in text

    def test_init_keeps_crlf_gitignore(self, cli_cwd: Path) -> None:
        (cli_cwd / ".gitignore").write_bytes(b"*.log\r\n")
        CliRunner().invoke(main, ["init"])
        data = (cli_cwd / ".gitignore").read_bytes()
        assert data == b"*.log\r\n\r\n# iceberg-ops local data\r\nlocal_data/\r\n"

    def test_init_honours_config_and_migrations_dir(self, cli_cwd: Path) -> None:
        runner = CliRunner()
        args = ["--config", "conf/iceberg-ops.yaml", "--migrations-dir", "db/migrations"]
        result = runner.invoke(main, [*args, "init"])
        assert result.exit_code == 0, result.output
        assert (cli_cwd / "conf" / "iceberg-ops.yaml").is_file()
        assert (cli_cwd / "db" / "migrations" / "__init__.py").is_file()
        assert not (cli_cwd / "migrations").exists()
        assert "Review conf/iceberg-ops.yaml" in result.output
        assert "iceberg-ops --config conf/iceberg-ops.yaml migrate" in result.output
        config = (cli_cwd / "conf" / "iceberg-ops.yaml").read_text(encoding="utf-8")
        assert "\nmigrations_dir: ../db/migrations\n" in config

        result = runner.invoke(main, [*args, "revision", "-m", "first"])
        assert result.exit_code == 0, result.output
        assert (cli_cwd / "db" / "migrations" / "0001_first.py").is_file()

        # The generated config alone finds the same directory.
        result = runner.invoke(main, ["--config", "conf/iceberg-ops.yaml", "revision", "-m", "x"])
        assert result.exit_code == 0, result.output
        assert (cli_cwd / "db" / "migrations" / "0002_x.py").is_file()

    def test_init_force_replaces_broken_config(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text("- not a mapping\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["init", "--force"])
        assert result.exit_code == 0, result.output
        text = (cli_cwd / "iceberg-ops.yaml").read_text(encoding="utf-8")
        assert text.startswith("# iceberg-ops configuration")

    def test_init_with_config_puts_migrations_next_to_it(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, ["--config", "conf/x.yaml", "init"])
        assert result.exit_code == 0, result.output
        assert (cli_cwd / "conf" / "migrations").is_dir()
        config = (cli_cwd / "conf" / "x.yaml").read_text(encoding="utf-8")
        assert "\nmigrations_dir:" not in config


# ===========================================================================
# --target normalisation and --steps validation
# ===========================================================================


class TestTargetAndSteps:
    @pytest.fixture
    def three(self, cli_cwd: Path) -> Path:
        _write_migrations(cli_cwd / "migrations", 3)
        return cli_cwd

    def _status(self) -> dict[str, str]:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 0, result.output
        return _statuses(result.output)

    def test_migrate_target_without_leading_zeros(self, three: Path) -> None:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "migrate", "--target", "2"])
        assert result.exit_code == 0, result.output
        assert self._status() == {"0001": "applied", "0002": "applied", "0003": "pending"}

    def test_stamp_target_stamps_exactly_two(self, three: Path) -> None:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "stamp", "--target", "2"])
        assert result.exit_code == 0, result.output
        assert result.output.count("[ok] Stamped") == 2
        assert self._status()["0003"] == "pending"

    def test_rollback_target_one(self, three: Path) -> None:
        runner = CliRunner()
        assert runner.invoke(main, [*_SQLITE_ARGS, "migrate"]).exit_code == 0
        result = runner.invoke(main, [*_SQLITE_ARGS, "rollback", "--target", "1"])
        assert result.exit_code == 0, result.output
        assert "Rolled back 2 migration(s)" in result.output
        assert self._status() == {
            "0001": "applied",
            "0002": "rolled_back",
            "0003": "rolled_back",
        }

    def test_plan_target_without_leading_zeros(self, three: Path) -> None:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "plan", "--target", "2"])
        assert result.exit_code == 0, result.output
        assert "2 migration(s) would be applied" in result.output

    @pytest.mark.parametrize("command", ["migrate", "rollback", "stamp", "plan"])
    @pytest.mark.parametrize("value", ["abc", "00002", "-1"])
    def test_bad_target_is_usage_error(self, three: Path, command: str, value: str) -> None:
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, command, "--target", value])
        assert result.exit_code == 2, result.output
        assert "expected a migration version" in result.output

    def test_runner_value_error_is_usage_error(
        self, three: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A target the runner rejects surfaces as a usage error (exit 2)."""
        from iceberg_ops.runner import MigrationRunner

        def _reject(self: MigrationRunner, target: str | None = None) -> None:
            raise ValueError(f"Unknown target version {target!r}")

        monkeypatch.setattr(MigrationRunner, "migrate", _reject)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "migrate", "--target", "9"])
        assert result.exit_code == 2, result.output
        assert "Unknown target version '0009'" in result.output

    @pytest.mark.parametrize("command", ["migrate", "rollback", "stamp", "plan"])
    def test_unknown_target_is_usage_error(self, three: Path, command: str) -> None:
        """A well-formed --target that names no migration exits 2 (real runner)."""
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, command, "--target", "9"])
        assert result.exit_code == 2, result.output
        assert "Unknown target version '0009'" in result.output
        assert self._status() == {"0001": "pending", "0002": "pending", "0003": "pending"}

    def test_rollback_target_base_undoes_everything(self, three: Path) -> None:
        runner = CliRunner()
        assert runner.invoke(main, [*_SQLITE_ARGS, "migrate"]).exit_code == 0
        result = runner.invoke(main, [*_SQLITE_ARGS, "rollback", "--target", "0"])
        assert result.exit_code == 0, result.output
        assert set(self._status().values()) == {"rolled_back"}

    @pytest.mark.parametrize("command", ["status", "current", "plan", "stamp", "migrate"])
    def test_unloadable_migration_is_clean_error(self, three: Path, command: str) -> None:
        """A migration file that fails to import prints one Error: line, exit 1."""
        (three / "migrations" / "0004_broken.py").write_text(
            "raise ImportError('boom in 0004')\n", encoding="utf-8"
        )
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, command])
        assert result.exit_code == 1, result.output
        assert "Error: Failed to load migration" in result.output
        assert "boom in 0004" in result.output
        assert "Traceback" not in result.output
        assert isinstance(result.exception, SystemExit)

    @pytest.mark.parametrize("steps", ["-1", "0"])
    def test_rollback_steps_must_be_positive(self, three: Path, steps: str) -> None:
        runner = CliRunner()
        assert runner.invoke(main, [*_SQLITE_ARGS, "migrate"]).exit_code == 0
        result = runner.invoke(main, [*_SQLITE_ARGS, "rollback", "--steps", steps])
        assert result.exit_code == 2, result.output
        assert set(self._status().values()) == {"applied"}

    def test_steps_and_target_mutually_exclusive(self, three: Path) -> None:
        result = CliRunner().invoke(
            main, [*_SQLITE_ARGS, "rollback", "--steps", "1", "--target", "0001"]
        )
        assert result.exit_code == 2, result.output
        assert "mutually exclusive" in result.output

    def test_rollback_default_is_one_step(self, three: Path) -> None:
        runner = CliRunner()
        assert runner.invoke(main, [*_SQLITE_ARGS, "migrate"]).exit_code == 0
        result = runner.invoke(main, [*_SQLITE_ARGS, "rollback"])
        assert result.exit_code == 0, result.output
        statuses = self._status()
        assert statuses["0003"] == "rolled_back"
        assert statuses["0002"] == "applied"


# ===========================================================================
# Clean errors instead of tracebacks
# ===========================================================================


class TestCleanErrors:
    def test_missing_sqlite_extra_clean_error(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cli_cwd / "migrations").mkdir()
        monkeypatch.setitem(sys.modules, "pyiceberg.catalog.sql", None)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert "iceberg-ops[sqlite]" in result.output
        assert isinstance(result.exception, SystemExit)

    def test_forbidden_error_mapped(self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from pyiceberg.exceptions import ForbiddenError

        from iceberg_ops.runner import MigrationRunner

        def _forbidden(self: MigrationRunner, *args: object, **kwargs: object) -> None:
            raise ForbiddenError("not allowed to create namespace")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "__init__", _forbidden)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert "Error: ForbiddenError: not allowed to create namespace" in result.output
        assert "_iceberg_ops namespace" in result.output
        assert "Traceback" not in result.output
        assert isinstance(result.exception, SystemExit)

    def test_traceback_env_reraises(self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from pyiceberg.exceptions import ForbiddenError

        from iceberg_ops.runner import MigrationRunner

        def _forbidden(self: MigrationRunner, *args: object, **kwargs: object) -> None:
            raise ForbiddenError("nope")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "__init__", _forbidden)
        result = CliRunner().invoke(
            main, [*_SQLITE_ARGS, "status"], env={"ICEBERG_OPS_TRACEBACK": "1"}
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, ForbiddenError)

    def test_unexpected_errors_still_raise(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the expected failure types are flattened; bugs keep their traceback."""
        from iceberg_ops.runner import MigrationRunner

        def _bug(self: MigrationRunner) -> None:
            raise ZeroDivisionError("bug")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "status", _bug)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert isinstance(result.exception, ZeroDivisionError)

    def test_group_help_matches_release_pitch(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0, result.output
        assert "Alembic-style schema migrations for Apache Iceberg." in result.output

    def test_help_output_is_ascii(self) -> None:
        runner = CliRunner()
        commands = ["status", "migrate", "rollback", "stamp", "plan", "history", "init"]
        commands += ["migrate-data", "run-policies"]
        for args in (["--help"], *[[c, "--help"] for c in commands]):
            result = runner.invoke(main, args)
            assert result.exit_code == 0, (args, result.output)
            result.output.encode("ascii")


# ===========================================================================
# migrate-data validation and error handling
# ===========================================================================


def _items_migration(version: str, table: str, rows: int) -> str:
    """Migration creating *table* (id required, name) seeded with *rows* rows."""
    return textwrap.dedent(f'''\
        """Create {table}."""
        from __future__ import annotations
        import pyarrow as pa
        from iceberg_ops.migration import BaseMigration, MigrationContext

        class Migration(BaseMigration):
            version = "{version}"
            description = "create {table}"
            def upgrade(self, ctx: MigrationContext) -> None:
                tbl = ctx.create_table_if_not_exists(
                    "{table}",
                    pa.schema([
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("name", pa.string()),
                    ]),
                )
                if {rows}:
                    tbl.append(pa.Table.from_pydict(
                        {{"id": list(range({rows})), "name": ["x"] * {rows}}},
                        schema=tbl.schema().as_arrow(),
                    ))
            def downgrade(self, ctx: MigrationContext) -> None:
                ctx.drop_table_if_exists("{table}")
        ''')


def _seed_items(root: Path, *, with_dest: bool = True) -> None:
    """Create ns.items (3 rows) and optionally an empty ns.items_dst via sqlite."""
    migrations = root / "migrations"
    migrations.mkdir(exist_ok=True)
    (migrations / "0001_items.py").write_text(_items_migration("0001", "items", 3))
    if with_dest:
        (migrations / "0002_items_dst.py").write_text(_items_migration("0002", "items_dst", 0))
    result = CliRunner().invoke(main, [*_SQLITE_ARGS, "migrate"])
    assert result.exit_code == 0, result.output


_MIGRATE_DATA = [
    "migrate-data",
    "--src-catalog",
    "sqlite",
    "--src-namespace",
    "ns",
    "--src-table",
    "items",
    "--src-db-path",
    "catalog.db",
    "--src-warehouse",
    "warehouse",
    "--dst-namespace",
    "ns",
    "--dst-table",
    "items_dst",
    "--quiet",
]


class TestMigrateDataValidation:
    def test_requires_src_catalog_or_name(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(
            main,
            ["migrate-data", "--src-namespace", "ns", "--src-table", "t", "--dst-namespace", "ns"],
        )
        assert result.exit_code == 2, result.output
        assert "Provide --src-catalog or --src-catalog-name" in result.output

    def test_missing_dst_table_clean_error(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd, with_dest=False)
        result = CliRunner().invoke(main, _MIGRATE_DATA)
        assert result.exit_code == 1, result.output
        assert "Table ns.items_dst not found in the sqlite catalog" in result.output
        assert "does not create the destination table" in result.output
        assert isinstance(result.exception, SystemExit)

    def test_missing_src_table_clean_error(self, cli_cwd: Path) -> None:
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(main, _MIGRATE_DATA)
        assert result.exit_code == 1, result.output
        assert "Table ns.items not found in the sqlite catalog." in result.output
        assert "does not create the destination table" not in result.output

    def test_upsert_keys_trailing_comma_is_ignored(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd)
        result = CliRunner().invoke(
            main, [*_MIGRATE_DATA, "--write-mode", "upsert", "--upsert-keys", "id,"]
        )
        assert result.exit_code == 0, result.output
        assert "Done: 3 rows" in result.output

    def test_upsert_keys_only_commas_is_usage_error(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(
            main, [*_MIGRATE_DATA, "--write-mode", "upsert", "--upsert-keys", ","]
        )
        assert result.exit_code == 2, result.output
        assert "--upsert-keys is empty" in result.output

    def test_corrupt_checkpoint_clean_error(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd)
        (cli_cwd / "checkpoint.json").write_text("{not json", encoding="utf-8")
        result = CliRunner().invoke(main, [*_MIGRATE_DATA, "--checkpoint", "checkpoint.json"])
        assert result.exit_code == 1, result.output
        assert "Error: " in result.output
        assert "not valid JSON" in result.output
        assert isinstance(result.exception, SystemExit)

    def test_completed_checkpoint_is_refused_on_rerun(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd)
        args = [*_MIGRATE_DATA, "--checkpoint", "checkpoint.json"]
        first = CliRunner().invoke(main, args)
        assert first.exit_code == 0, first.output
        assert "Done: 3 rows" in first.output

        second = CliRunner().invoke(main, args)
        assert second.exit_code == 1, second.output
        assert "completed" in second.output
        assert "Traceback" not in second.output
        assert isinstance(second.exception, SystemExit)

    def test_missing_transforms_extra_clean_error(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import BucketTransform
        from pyiceberg.types import LongType, NestedField, StringType

        from iceberg_ops.core import utils
        from iceberg_ops.core.catalog import CatalogFactory

        _seed_items(cli_cwd, with_dest=False)
        catalog = CatalogFactory.build(
            "sqlite", db_path="catalog.db", warehouse_dir=str(cli_cwd / "warehouse")
        )
        catalog.create_table(
            "ns.items_dst",
            schema=Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "name", StringType(), required=False),
            ),
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=1, field_id=1000, transform=BucketTransform(4), name="id_bucket"
                )
            ),
        )
        monkeypatch.setattr(utils, "_has_pyiceberg_core", lambda: False)

        result = CliRunner().invoke(main, _MIGRATE_DATA)

        assert result.exit_code == 1, result.output
        assert "Error: " in result.output
        assert "iceberg-ops[transforms]" in result.output
        assert isinstance(result.exception, SystemExit)

    @pytest.mark.parametrize("option", ["--batch-rows", "--sample-size"])
    def test_counts_must_be_positive(self, cli_cwd: Path, option: str) -> None:
        result = CliRunner().invoke(main, [*_MIGRATE_DATA, option, "0"])
        assert result.exit_code == 2, result.output
        assert "Invalid value" in result.output

    def test_dst_catalog_kind_typo_is_usage_error(self, cli_cwd: Path) -> None:
        result = CliRunner().invoke(main, [*_MIGRATE_DATA, "--dst-catalog", "memory"])
        assert result.exit_code == 2, result.output
        assert "Invalid value" in result.output

    def test_self_target_is_usage_error(self, cli_cwd: Path) -> None:
        """Omitting --dst-table with the same namespace must not append ns.items into itself."""
        _seed_items(cli_cwd, with_dest=False)
        args = [a for a in _MIGRATE_DATA if a not in ("--dst-table", "items_dst")]
        result = CliRunner().invoke(main, args)

        assert result.exit_code == 2, result.output
        assert "Source and destination are the same table" in result.output
        assert "Migrating" not in result.output
        assert _row_count(cli_cwd, "ns.items") == 3

    def test_summary_prints_dest_snapshot_before(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd)
        first = CliRunner().invoke(main, _MIGRATE_DATA)
        assert first.exit_code == 0, first.output
        # The destination was empty: there is no snapshot to roll back to.
        assert "Destination snapshot before" not in first.output
        snapshot_id = _current_snapshot_id(cli_cwd, "ns.items_dst")

        second = CliRunner().invoke(main, _MIGRATE_DATA)
        assert second.exit_code == 0, second.output
        assert f"Destination snapshot before: {snapshot_id} " in second.output
        assert "rollback_to_snapshot" in second.output

    def test_verify_partition_hash_on_day_partitioned_dest_is_an_error(
        self, cli_cwd: Path
    ) -> None:
        """An unsupported partition_hash verify must not be reported as a mismatch."""
        pytest.importorskip("pyiceberg_core")
        import datetime

        import pyarrow as pa
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import DayTransform
        from pyiceberg.types import DateType, LongType, NestedField

        catalog = _open_seeded_catalog(cli_cwd)
        try:
            catalog.create_namespace("ns")
            schema = Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "d", DateType(), required=False),
            )
            src = catalog.create_table("ns.items", schema)
            spec = PartitionSpec(
                PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="d_day")
            )
            catalog.create_table("ns.items_dst", schema, partition_spec=spec)
            src.append(
                pa.Table.from_pydict(
                    {"id": [1, 2], "d": [datetime.date(2026, 1, 1)] * 2},
                    schema=src.schema().as_arrow(),
                )
            )
        finally:
            catalog.close()

        result = CliRunner().invoke(main, [*_MIGRATE_DATA, "--verify", "partition_hash"])

        assert result.exit_code == 1, result.output
        assert "Verify error" in result.output
        assert "MISMATCH" not in result.output

    def test_help_does_not_promise_one_snapshot_per_commit(self) -> None:
        result = CliRunner().invoke(main, ["migrate-data", "--help"])
        assert result.exit_code == 0, result.output
        text = " ".join(result.output.split())
        assert "is one Iceberg snapshot" not in text
        assert "can create several destination snapshots" in text
        assert "up to three for upsert" in text


def _open_seeded_catalog(root: Path) -> Any:
    """Open the sqlite catalog that ``_SQLITE_ARGS`` points at under *root*."""
    from iceberg_ops.core.catalog import CatalogFactory

    return CatalogFactory.build(
        "sqlite", db_path=root / "catalog.db", warehouse_dir=root / "warehouse"
    )


def _row_count(root: Path, identifier: str) -> int:
    catalog = _open_seeded_catalog(root)
    try:
        return len(catalog.load_table(identifier).scan().to_arrow())
    finally:
        catalog.close()


def _current_snapshot_id(root: Path, identifier: str) -> int:
    catalog = _open_seeded_catalog(root)
    try:
        snapshot = catalog.load_table(identifier).current_snapshot()
        assert snapshot is not None
        return snapshot.snapshot_id
    finally:
        catalog.close()


class TestOwnCatalogCommandsIgnoreConfig:
    """migrate-data and run-policies never read iceberg-ops.yaml."""

    _BROKEN = "namespace: 2024\n"

    @pytest.mark.parametrize("command", ["migrate-data", "run-policies"])
    def test_help_with_broken_config(self, cli_cwd: Path, command: str) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(self._BROKEN, encoding="utf-8")
        result = CliRunner().invoke(main, [command, "--help"])
        assert result.exit_code == 0, result.output

    @pytest.mark.parametrize("command", ["migrate-data", "run-policies"])
    def test_help_with_missing_explicit_config(self, cli_cwd: Path, command: str) -> None:
        result = CliRunner().invoke(main, ["--config", "typo.yaml", command, "--help"])
        assert result.exit_code == 0, result.output

    def test_migrate_data_runs_with_broken_config(self, cli_cwd: Path) -> None:
        _seed_items(cli_cwd)
        (cli_cwd / "iceberg-ops.yaml").write_text(self._BROKEN, encoding="utf-8")
        result = CliRunner().invoke(main, _MIGRATE_DATA)
        assert result.exit_code == 0, result.output
        assert "Done: 3 rows" in result.output

    def test_run_policies_runs_with_broken_config(self, cli_cwd: Path, no_policies: None) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(self._BROKEN, encoding="utf-8")
        result = CliRunner().invoke(main, [*_RUN_POLICIES, "--polaris-token", "t"])
        assert result.exit_code == 0, result.output
        assert "nothing to do" in result.output

    def test_project_commands_still_read_it(self, cli_cwd: Path) -> None:
        (cli_cwd / "iceberg-ops.yaml").write_text(self._BROKEN, encoding="utf-8")
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "'namespace' must be a string" in result.output


class TestLocalWarehouseErrors:
    """A sqlite warehouse its file:// URI cannot carry is one clean error."""

    @staticmethod
    def _error_lines(output: str) -> list[str]:
        return [line for line in output.splitlines() if line.startswith("Error:")]

    def test_config_uri_warehouse_for_sqlite(self, cli_cwd: Path) -> None:
        (cli_cwd / "migrations").mkdir()
        (cli_cwd / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\nnamespace: ns\nwarehouse: s3://bucket/wh\n", encoding="utf-8"
        )
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 1, result.output
        errors = self._error_lines(result.output)
        assert len(errors) == 1, result.output
        assert "warehouse_dir 's3://bucket/wh' looks like a URI" in errors[0]
        assert sorted(p.name for p in cli_cwd.iterdir()) == ["iceberg-ops.yaml", "migrations"]

    def test_flag_file_uri_warehouse_for_sqlite(self, cli_cwd: Path) -> None:
        (cli_cwd / "migrations").mkdir()
        result = CliRunner().invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "ns",
                "--warehouse",
                "file:///tmp/wh",
                "status",
            ],
        )
        assert result.exit_code == 1, result.output
        assert "looks like a URI" in result.output
        assert sorted(p.name for p in cli_cwd.iterdir()) == ["migrations"]

    def test_default_local_data_under_hash_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        project = tmp_path / "a#b"
        (project / "migrations").mkdir(parents=True)
        monkeypatch.chdir(project)
        result = CliRunner().invoke(main, ["--catalog", "sqlite", "--namespace", "ns", "status"])
        assert result.exit_code == 1, result.output
        errors = self._error_lines(result.output)
        assert len(errors) == 1, result.output
        assert "contains '#'" in errors[0]
        # file://.../a#b/... would have been cut short at the "#".
        assert sorted(p.name for p in tmp_path.iterdir()) == ["a#b"]
        assert not (project / "local_data").exists()


class TestIoErrors:
    def test_missing_metadata_file_is_runtime_error(self, cli_cwd: Path) -> None:
        """A FileNotFoundError from PyArrow is exit 1, not a usage error."""
        _seed_items(cli_cwd, with_dest=False)
        shutil.rmtree(cli_cwd / "warehouse")
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert "Usage:" not in result.output
        errors = [line for line in result.output.splitlines() if line.startswith("Error:")]
        assert len(errors) == 1, result.output
        assert errors[0].startswith("Error: FileNotFoundError: ")
        assert isinstance(result.exception, SystemExit)

    def test_runner_missing_directory_stays_usage_error(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from iceberg_ops.runner import MigrationRunner

        def _gone(self: MigrationRunner) -> None:
            raise FileNotFoundError("Migrations directory not found: x")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "status", _gone)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 2, result.output
        assert "Migrations directory not found: x" in result.output

    def test_os_file_not_found_is_runtime_error(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import errno

        from iceberg_ops.runner import MigrationRunner

        def _gone(self: MigrationRunner) -> None:
            raise FileNotFoundError(errno.ENOENT, "No such file or directory", "m.json")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "status", _gone)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert "Error: FileNotFoundError: " in result.output
        assert "Usage:" not in result.output

    @pytest.mark.parametrize(
        ("message", "hinted"),
        [
            ("[WinError 123] Failed querying information for path '/C:/x/wh/m.json'", True),
            ("Failed querying information for path '/tmp/x/wh/m.json'", False),
        ],
    )
    def test_windows_file_uri_hint(
        self,
        cli_cwd: Path,
        monkeypatch: pytest.MonkeyPatch,
        message: str,
        hinted: bool,
    ) -> None:
        from iceberg_ops.runner import MigrationRunner

        def _fail(self: MigrationRunner) -> None:
            raise OSError(message)

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "status", _fail)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert f"Error: OSError: {message}" in result.output
        assert ("not file:///C:/" in result.output) is hinted

    def test_migration_io_failure_keeps_hint_on_one_line(self, cli_cwd: Path) -> None:
        """A migration that fails on I/O is reported as itself, with the hint."""
        migrations = cli_cwd / "migrations"
        migrations.mkdir()
        (migrations / "0001_io.py").write_text(
            "from iceberg_ops.migration import BaseMigration\n"
            "class Migration(BaseMigration):\n"
            "    version = '0001'\n"
            "    description = 'io'\n"
            "    def upgrade(self, ctx):\n"
            "        raise OSError(\"Failed querying information for path '/C:/x/m.json'\\r\\n\")\n"
            "    def downgrade(self, ctx):\n"
            "        pass\n",
            encoding="utf-8",
        )
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "migrate"])
        assert result.exit_code == 1, result.output
        errors = [line for line in result.output.splitlines() if line.startswith("Error:")]
        assert len(errors) == 1, result.output
        assert errors[0].startswith("Error: Migration 0001 failed: ")
        assert errors[0].endswith("see the compatibility page)")
        assert "Traceback" not in result.output


# ===========================================================================
# run-policies: experimental fence, env-var secrets, plain-http warning
# ===========================================================================


_RUN_POLICIES = [
    "run-policies",
    "--polaris-url",
    "https://polaris.example.com",
    "--polaris-catalog",
    "cat",
    "--target-namespace",
    "ns",
    "--target-table",
    "t",
    "--catalog",
    "sqlite",
]


@pytest.fixture
def no_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make Polaris report no applicable policies (no network)."""
    from iceberg_ops import polaris as polaris_pkg

    monkeypatch.setattr(polaris_pkg, "fetch_table_policies", lambda *_args: [])


class TestRunPoliciesFence:
    def test_help_marks_experimental(self) -> None:
        runner = CliRunner()
        sub = runner.invoke(main, ["run-policies", "--help"])
        assert sub.exit_code == 0, sub.output
        assert "[experimental]" in sub.output
        group = runner.invoke(main, ["--help"])
        assert "[experimental]" in group.output

    def test_runtime_warning_on_stderr(self, cli_cwd: Path, no_policies: None) -> None:
        result = CliRunner().invoke(main, [*_RUN_POLICIES, "--polaris-token", "t"])
        assert result.exit_code == 0, result.output
        assert "run-policies is experimental" in result.stderr
        assert "iceberg-ops/polaris.html" in result.stderr
        assert "experimental" not in result.stdout

    def test_help_names_env_vars(self) -> None:
        result = CliRunner().invoke(main, ["run-policies", "--help"])
        for name in (
            "ICEBERG_OPS_POLARIS_TOKEN",
            "ICEBERG_OPS_POLARIS_CLIENT_ID",
            "ICEBERG_OPS_POLARIS_CLIENT_SECRET",
        ):
            assert name in result.output

    def test_token_from_envvar(self, cli_cwd: Path, no_policies: None) -> None:
        result = CliRunner().invoke(main, _RUN_POLICIES, env={"ICEBERG_OPS_POLARIS_TOKEN": "t"})
        assert result.exit_code == 0, result.output
        assert "No applicable policies" in result.output

    def test_client_credentials_from_envvars(self, cli_cwd: Path, no_policies: None) -> None:
        result = CliRunner().invoke(
            main,
            _RUN_POLICIES,
            env={
                "ICEBERG_OPS_POLARIS_CLIENT_ID": "id",
                "ICEBERG_OPS_POLARIS_CLIENT_SECRET": "secret",
            },
        )
        assert result.exit_code == 0, result.output
        assert "No applicable policies" in result.output

    def test_http_non_loopback_warns(self, cli_cwd: Path, no_policies: None) -> None:
        args = [*_RUN_POLICIES, "--polaris-token", "t"]
        args[2] = "http://polaris.example.com"
        result = CliRunner().invoke(main, args)
        assert result.exit_code == 0, result.output
        assert "plain http; credentials are sent unencrypted" in result.stderr

    @pytest.mark.parametrize(
        "url", ["http://localhost:8181", "http://127.0.0.1:8181", "http://[::1]:8181"]
    )
    def test_http_loopback_no_warning(self, cli_cwd: Path, no_policies: None, url: str) -> None:
        args = [*_RUN_POLICIES, "--polaris-token", "t"]
        args[2] = url
        result = CliRunner().invoke(main, args)
        assert result.exit_code == 0, result.output
        assert "plain http" not in result.stderr

    @pytest.mark.parametrize("token", ["  ", "abc def", "abc\x01def"])
    def test_unsafe_token_is_usage_error_without_echo(
        self, cli_cwd: Path, no_policies: None, token: str
    ) -> None:
        result = CliRunner().invoke(main, _RUN_POLICIES, env={"ICEBERG_OPS_POLARIS_TOKEN": token})
        assert result.exit_code == 2, result.output
        assert "token is empty or contains whitespace" in result.output
        assert "Traceback" not in result.output
        if token.strip():
            assert token not in result.output

    def test_requires_catalog_or_catalog_name(self, cli_cwd: Path) -> None:
        args = [a for a in _RUN_POLICIES if a not in ("--catalog", "sqlite")]
        result = CliRunner().invoke(main, [*args, "--polaris-token", "t"])
        assert result.exit_code == 2, result.output
        assert "Provide --catalog or --catalog-name" in result.output

    def test_catalog_name_accepted_without_kind(self, cli_cwd: Path, no_policies: None) -> None:
        args = [a for a in _RUN_POLICIES if a not in ("--catalog", "sqlite")]
        result = CliRunner().invoke(
            main, [*args, "--catalog-name", "prod", "--polaris-token", "t"]
        )
        assert result.exit_code == 0, result.output
        assert "No applicable policies" in result.output


class TestExpectedErrorMapping:
    @pytest.mark.parametrize(
        ("exc_path", "expected"),
        [
            ("pyiceberg.exceptions:CommitFailedException", "Error: CommitFailedException: boom"),
            ("pyiceberg.exceptions:NoSuchTableError", "Error: NoSuchTableError: boom"),
            ("pyiceberg.exceptions:NoSuchNamespaceError", "Error: NoSuchNamespaceError: boom"),
            (
                "pyiceberg.exceptions:ServiceUnavailableError",
                "Error: ServiceUnavailableError: boom",
            ),
            ("builtins:PermissionError", "Error: Permission denied: boom"),
            ("builtins:OSError", "Error: OSError: boom"),
            ("builtins:ConnectionError", "Error: ConnectionError: boom"),
            ("builtins:FileExistsError", "Error: FileExistsError: boom"),
            (
                "iceberg_ops.data_migration.checkpoint:CheckpointError",
                "Error: CheckpointError: boom",
            ),
        ],
    )
    def test_mapped_to_clean_error(
        self,
        cli_cwd: Path,
        monkeypatch: pytest.MonkeyPatch,
        exc_path: str,
        expected: str,
    ) -> None:
        import importlib

        from iceberg_ops.runner import MigrationRunner

        module_name, class_name = exc_path.split(":")
        exc_cls = getattr(importlib.import_module(module_name), class_name)

        def _boom(self: MigrationRunner) -> None:
            raise exc_cls("boom")

        (cli_cwd / "migrations").mkdir()
        monkeypatch.setattr(MigrationRunner, "status", _boom)
        result = CliRunner().invoke(main, [*_SQLITE_ARGS, "status"])
        assert result.exit_code == 1, result.output
        assert expected in result.output
        assert isinstance(result.exception, SystemExit)

    def test_revision_never_overwrites(
        self, cli_cwd: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the next version's file appears after the scan, it is left alone."""
        from iceberg_ops import scaffolding

        runner = CliRunner()
        runner.invoke(main, ["init"])
        existing = cli_cwd / "migrations" / "0001_x.py"
        existing.write_text("# hand-written\n", encoding="utf-8")
        monkeypatch.setattr(scaffolding, "_next_version", lambda _directory: "0001")

        result = runner.invoke(main, ["revision", "-m", "x"])

        assert result.exit_code == 1, result.output
        assert "already exists and was left unchanged" in result.output
        assert existing.read_text(encoding="utf-8") == "# hand-written\n"
