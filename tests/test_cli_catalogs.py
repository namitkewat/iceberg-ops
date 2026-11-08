"""CLI catalog selection: --catalog-name, remote kinds, and their errors.

Named catalogs are real PyIceberg SQL catalogs declared in an isolated
``.pyiceberg.yaml`` (``PYICEBERG_HOME`` points at a per-test
directory). Remote kinds are exercised by swapping PyIceberg's loader
for the REST type with one that records the merged properties and
returns a real in-memory catalog.
"""

from __future__ import annotations

import os
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyiceberg.catalog as pyiceberg_catalog
import pytest
from click.testing import CliRunner
from pyiceberg.catalog import AVAILABLE_CATALOGS, CatalogType
from pyiceberg.utils.config import Config

from iceberg_ops.cli import _env_hint, _env_var_name, main
from iceberg_ops.core.catalog import CatalogFactory

_MIGRATION = textwrap.dedent('''\
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


def _write_migration(directory: Path, version: str, table: str, rows: int = 0) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{version}_create_{table}.py").write_text(
        _MIGRATION.format(version=version, table=table, rows=rows), encoding="utf-8"
    )


def _statuses(output: str) -> dict[str, str]:
    import re

    return {v: s for s, v in re.findall(r"\[(\w+)\s*\] (\d{4}) - ", output)}


# ---------------------------------------------------------------------------
# Fixtures (file-local)
# ---------------------------------------------------------------------------


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Empty per-test working directory."""
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    for name in list(os.environ):
        if name.upper().startswith("ICEBERG_OPS_"):
            monkeypatch.delenv(name)
    return work


@pytest.fixture
def pyiceberg_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    """Isolated ``.pyiceberg.yaml``; returns ``write(yaml_text)`` that reloads it.

    ``PYICEBERG_HOME`` is searched first and the first file found wins,
    so the developer's ``~/.pyiceberg.yaml`` never leaks in. PyIceberg
    reads its config once at import, so the module-level config object
    is rebuilt after each write.
    """
    home = tmp_path / "pyiceberg_home"
    home.mkdir()
    for name in list(os.environ):
        if name.upper().startswith("PYICEBERG_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("PYICEBERG_HOME", str(home))

    def write(text: str = "default-catalog: unused\n") -> None:
        (home / ".pyiceberg.yaml").write_text(text, encoding="utf-8")
        monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())

    write()
    return write


@pytest.fixture
def named_catalog(tmp_path: Path, pyiceberg_home: Callable[[str], None]) -> Path:
    """Declare catalog ``prod`` (a file-backed SQL catalog) in .pyiceberg.yaml."""
    store = tmp_path / "named"
    (store / "wh").mkdir(parents=True)
    pyiceberg_home(
        "catalog:\n"
        "  prod:\n"
        "    type: sql\n"
        f"    uri: 'sqlite:///{store / 'prod.db'}'\n"
        f"    warehouse: 'file://{store / 'wh'}'\n"
    )
    return store


@pytest.fixture
def rest_recorder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pyiceberg_home: Callable[[str], None],
) -> list[tuple[str, dict[str, Any]]]:
    """Record the REST loader's (name, properties); hand back a real catalog."""
    calls: list[tuple[str, dict[str, Any]]] = []
    backing = CatalogFactory.build("memory", warehouse_dir=str(tmp_path / "rest_wh"))

    def _load(name: str, conf: dict[str, Any]) -> Any:
        calls.append((name, dict(conf)))
        return backing

    monkeypatch.setitem(AVAILABLE_CATALOGS, CatalogType.REST, _load)
    return calls


def _named_status(runner: CliRunner) -> dict[str, str]:
    result = runner.invoke(main, ["--catalog-name", "prod", "status"])
    assert result.exit_code == 0, result.output
    return _statuses(result.output)


# ---------------------------------------------------------------------------
# --catalog-name
# ---------------------------------------------------------------------------


class TestCatalogName:
    def test_catalog_name_end_to_end(self, project: Path, named_catalog: Path) -> None:
        runner = CliRunner()
        assert runner.invoke(main, ["init"]).exit_code == 0
        _write_migration(project / "migrations", "0001", "items")

        result = runner.invoke(main, ["--catalog-name", "prod", "migrate"])

        assert result.exit_code == 0, result.output
        assert _named_status(runner) == {"0001": "applied"}
        assert (named_catalog / "prod.db").is_file()
        # init's 'catalog: sqlite' was not used: nothing local was created.
        assert not (project / "local_data").exists()

    def test_catalog_name_flag_beats_config_kind(self, project: Path, named_catalog: Path) -> None:
        """The config's 'catalog: sqlite' must not win over --catalog-name."""
        (project / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\nnamespace: ns\ndb_path: local.db\n", encoding="utf-8"
        )
        _write_migration(project / "migrations", "0001", "items")
        runner = CliRunner()

        result = runner.invoke(main, ["--catalog-name", "prod", "migrate"])

        assert result.exit_code == 0, result.output
        assert not (project / "local.db").exists()
        assert not (project / "local_data").exists()
        assert _named_status(runner) == {"0001": "applied"}

    def test_config_catalog_name(self, project: Path, named_catalog: Path) -> None:
        (project / "iceberg-ops.yaml").write_text(
            "catalog_name: prod\nnamespace: ns\n", encoding="utf-8"
        )
        _write_migration(project / "migrations", "0001", "items")
        runner = CliRunner()

        result = runner.invoke(main, ["migrate"])

        assert result.exit_code == 0, result.output
        assert _named_status(runner) == {"0001": "applied"}

    def test_catalog_flag_beats_config_catalog_name(
        self, project: Path, named_catalog: Path
    ) -> None:
        (project / "iceberg-ops.yaml").write_text(
            "catalog_name: prod\nnamespace: ns\n", encoding="utf-8"
        )
        _write_migration(project / "migrations", "0001", "items")
        runner = CliRunner()

        result = runner.invoke(main, ["--catalog", "sqlite", "migrate"])

        assert result.exit_code == 0, result.output
        assert (project / "local_data" / "iceberg_catalog.db").is_file()
        assert _named_status(runner) == {"0001": "pending"}

    def test_config_catalog_name_with_sqlite_kind_is_usage_error(
        self, project: Path, named_catalog: Path
    ) -> None:
        (project / "iceberg-ops.yaml").write_text(
            "catalog: sqlite\ncatalog_name: prod\nnamespace: ns\n", encoding="utf-8"
        )
        (project / "migrations").mkdir()
        result = CliRunner().invoke(main, ["status"])
        assert result.exit_code == 2, result.output
        assert "cannot use the local sqlite kind" in result.output

    def test_catalog_name_with_sqlite_usage_error(
        self, project: Path, named_catalog: Path
    ) -> None:
        (project / "migrations").mkdir()
        result = CliRunner().invoke(
            main, ["--catalog", "sqlite", "--catalog-name", "prod", "--namespace", "ns", "status"]
        )
        assert result.exit_code == 2, result.output
        assert "cannot use the local sqlite kind" in result.output

    def test_db_path_with_named_catalog_is_usage_error(
        self, project: Path, named_catalog: Path
    ) -> None:
        (project / "migrations").mkdir()
        result = CliRunner().invoke(
            main, ["--catalog-name", "prod", "--db-path", "x.db", "--namespace", "ns", "status"]
        )
        assert result.exit_code == 2, result.output
        assert "only applies to the sqlite catalog kind" in result.output

    def test_unknown_name_names_env_var(
        self, project: Path, pyiceberg_home: Callable[[str], None]
    ) -> None:
        (project / "migrations").mkdir()
        result = CliRunner().invoke(
            main, ["--catalog-name", "my-cat", "--namespace", "ns", "status"]
        )
        assert result.exit_code == 1, result.output
        assert "Catalog 'my-cat' is missing required property uri" in result.output
        assert "PYICEBERG_CATALOG__MY_CAT__URI" in result.output
        assert isinstance(result.exception, SystemExit)

    def test_underscore_name_hint_skips_env_var(
        self,
        project: Path,
        pyiceberg_home: Callable[[str], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PyIceberg reads _ as - in env var names, so no variable reaches 'my_cat'."""
        (project / "migrations").mkdir()
        # Exactly what the old hint suggested; PyIceberg files it under 'my-cat'.
        monkeypatch.setenv("PYICEBERG_CATALOG__MY_CAT__URI", "sqlite:///unused.db")
        monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())
        assert pyiceberg_catalog._ENV_CONFIG.get_catalog_config("my_cat") is None

        result = CliRunner().invoke(
            main, ["--catalog-name", "my_cat", "--namespace", "ns", "status"]
        )
        assert result.exit_code == 1, result.output
        assert "Catalog 'my_cat' is missing required property uri" in result.output
        assert "PYICEBERG_CATALOG__MY_CAT__URI" not in result.output
        assert "set it in .pyiceberg.yaml" in result.output
        assert "cannot address a catalog name containing '_'" in result.output

    def test_env_hint(self) -> None:
        assert _env_hint("my-cat", "uri") == (
            "set it in .pyiceberg.yaml or PYICEBERG_CATALOG__MY_CAT__URI"
        )
        assert "PYICEBERG_CATALOG__" not in _env_hint("my_cat", "uri")

    @pytest.mark.skipif(sys.platform != "win32", reason="file:///C:/ is a Windows path form")
    def test_windows_triple_slash_warehouse_is_one_error_line(
        self, project: Path, tmp_path: Path, pyiceberg_home: Callable[[str], None]
    ) -> None:
        """An I/O failure mid-migrate is one Error: line with the file://C:/ hint."""
        store = tmp_path / "named"
        (store / "wh").mkdir(parents=True)
        pyiceberg_home(
            "catalog:\n"
            "  prod:\n"
            "    type: sql\n"
            f"    uri: 'sqlite:///{store / 'prod.db'}'\n"
            f"    warehouse: '{(store / 'wh').as_uri()}'\n"
        )
        _write_migration(project / "migrations", "0001", "items", rows=3)

        result = CliRunner().invoke(
            main, ["--catalog-name", "prod", "--namespace", "ns", "migrate"]
        )

        assert result.exit_code == 1, result.output
        assert "Traceback" not in result.output
        errors = [line for line in result.output.splitlines() if line.startswith("Error:")]
        assert len(errors) == 1, result.output
        assert "not file:///C:/" in errors[0]
        assert isinstance(result.exception, SystemExit)


# ---------------------------------------------------------------------------
# Remote kinds through load_catalog
# ---------------------------------------------------------------------------


class TestRemoteKinds:
    def test_rest_kind_reads_env_and_maps_warehouse(
        self,
        project: Path,
        monkeypatch: pytest.MonkeyPatch,
        pyiceberg_home: Callable[[str], None],
        rest_recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__URI", "http://rest.example:8181")
        pyiceberg_home("default-catalog: unused\n")
        (project / "migrations").mkdir()

        result = CliRunner().invoke(
            main, ["--catalog", "rest", "--warehouse", "s3://b/wh", "--namespace", "ns", "status"]
        )

        assert result.exit_code == 0, result.output
        name, conf = rest_recorder[0]
        assert name == "rest"
        assert conf["type"] == "rest"
        assert conf["uri"] == "http://rest.example:8181"
        assert conf["warehouse"] == "s3://b/wh"

    def test_config_warehouse_passed_verbatim_for_remote(
        self,
        project: Path,
        monkeypatch: pytest.MonkeyPatch,
        pyiceberg_home: Callable[[str], None],
        rest_recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        """A REST warehouse name is not a path: it must not be made absolute."""
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__URI", "http://rest.example:8181")
        pyiceberg_home("default-catalog: unused\n")
        (project / "iceberg-ops.yaml").write_text(
            "catalog: rest\nnamespace: ns\nwarehouse: polaris_catalog\n", encoding="utf-8"
        )
        (project / "migrations").mkdir()

        result = CliRunner().invoke(main, ["status"])

        assert result.exit_code == 0, result.output
        assert rest_recorder[0][1]["warehouse"] == "polaris_catalog"

    def test_remote_kind_with_name(
        self,
        project: Path,
        pyiceberg_home: Callable[[str], None],
        rest_recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        pyiceberg_home("catalog:\n  lake:\n    uri: http://lake.example\n")
        (project / "migrations").mkdir()

        result = CliRunner().invoke(
            main, ["--catalog", "rest", "--catalog-name", "lake", "--namespace", "ns", "status"]
        )

        assert result.exit_code == 0, result.output
        assert rest_recorder[0][0] == "lake"
        assert rest_recorder[0][1]["uri"] == "http://lake.example"

    def test_rest_without_uri_clean_error(
        self, project: Path, pyiceberg_home: Callable[[str], None]
    ) -> None:
        (project / "migrations").mkdir()
        result = CliRunner().invoke(main, ["--catalog", "rest", "--namespace", "ns", "status"])
        assert result.exit_code == 1, result.output
        assert "PYICEBERG_CATALOG__REST__URI" in result.output
        assert not isinstance(result.exception, KeyError)
        assert isinstance(result.exception, SystemExit)

    def test_connection_failure_clean_error(
        self,
        project: Path,
        monkeypatch: pytest.MonkeyPatch,
        pyiceberg_home: Callable[[str], None],
    ) -> None:
        def _unreachable(name: str, conf: dict[str, Any]) -> Any:
            raise OSError("connection refused")

        monkeypatch.setitem(AVAILABLE_CATALOGS, CatalogType.REST, _unreachable)
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__URI", "http://down.example")
        pyiceberg_home("default-catalog: unused\n")
        (project / "migrations").mkdir()

        result = CliRunner().invoke(main, ["--catalog", "rest", "--namespace", "ns", "status"])

        assert result.exit_code == 1, result.output
        assert "Could not open rest catalog 'rest': OSError: connection refused" in result.output

    def test_db_path_with_remote_kind_is_usage_error(self, project: Path) -> None:
        (project / "migrations").mkdir()
        result = CliRunner().invoke(
            main, ["--catalog", "rest", "--db-path", "x.db", "--namespace", "ns", "status"]
        )
        assert result.exit_code == 2, result.output
        assert "only applies to the sqlite catalog kind" in result.output

    def test_s3tables_without_boto3_clean_error(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "boto3", None)
        (project / "migrations").mkdir()
        result = CliRunner().invoke(main, ["--catalog", "s3tables", "--namespace", "ns", "status"])
        assert result.exit_code == 1, result.output
        assert "iceberg-ops[s3tables]" in result.output
        assert isinstance(result.exception, SystemExit)

    def test_env_var_name_maps_dashes(self) -> None:
        assert _env_var_name("my-cat", "uri") == "PYICEBERG_CATALOG__MY_CAT__URI"
        assert (
            _env_var_name("prod", "rest.signing-region")
            == "PYICEBERG_CATALOG__PROD__REST__SIGNING_REGION"
        )


# ---------------------------------------------------------------------------
# migrate-data / run-policies with named catalogs
# ---------------------------------------------------------------------------


class TestCommandCatalogNames:
    def _seed_named(self, project: Path) -> None:
        _write_migration(project / "migrations", "0001", "items", rows=3)
        _write_migration(project / "migrations", "0002", "items_dst")
        result = CliRunner().invoke(
            main, ["--catalog-name", "prod", "--namespace", "ns", "migrate"]
        )
        assert result.exit_code == 0, result.output

    def test_autogenerate_with_catalog_name(self, project: Path, named_catalog: Path) -> None:
        self._seed_named(project)
        (project / "target.yaml").write_text(
            "columns:\n  - {name: id, type: long, required: true}\n  - {name: email, type: string}\n",
            encoding="utf-8",
        )
        result = CliRunner().invoke(
            main,
            [
                *("--catalog-name", "prod", "--namespace", "ns"),
                *("revision", "-m", "add email", "--autogenerate"),
                *("--target-table", "items", "--target-schema-yaml", "target.yaml"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Diffed against ns.items." in result.output
        assert (project / "migrations" / "0003_add_email.py").is_file()

    def test_autogenerate_uses_config_relative_sqlite_defaults(
        self, project: Path, tmp_path: Path
    ) -> None:
        conf_dir = tmp_path / "conf"
        conf_dir.mkdir()
        config = conf_dir / "iceberg-ops.yaml"
        config.write_text("catalog: sqlite\nnamespace: ns\n", encoding="utf-8")
        _write_migration(conf_dir / "migrations", "0001", "items")
        runner = CliRunner()
        migrate = runner.invoke(main, ["--config", str(config), "migrate"])
        assert migrate.exit_code == 0, migrate.output
        assert (conf_dir / "local_data").is_dir()
        assert not (project / "local_data").exists()

        (project / "target.yaml").write_text(
            "columns:\n  - {name: email, type: string}\n", encoding="utf-8"
        )
        result = runner.invoke(
            main,
            [
                *("--config", str(config)),
                *("revision", "-m", "add email", "--autogenerate"),
                *("--target-table", "items", "--target-schema-yaml", "target.yaml"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (conf_dir / "migrations" / "0002_add_email.py").is_file()
        assert not (project / "local_data").exists()

    def test_migrate_data_within_named_catalog(self, project: Path, named_catalog: Path) -> None:
        self._seed_named(project)
        result = CliRunner().invoke(
            main,
            [
                "migrate-data",
                "--src-catalog-name",
                "prod",
                "--src-namespace",
                "ns",
                "--src-table",
                "items",
                "--dst-namespace",
                "ns",
                "--dst-table",
                "items_dst",
                "--quiet",
                "--verify",
                "row_count",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Migrating prod/ns.items  ->  prod/ns.items_dst" in result.output
        assert "Done: 3 rows" in result.output
        assert "[ok] row_count matches" in result.output

    def test_migrate_data_sqlite_to_named_catalog(
        self, project: Path, named_catalog: Path
    ) -> None:
        """Local sqlite paths are not carried over to a named destination."""
        local = project / "local"
        _write_migration(local, "0001", "items", rows=2)
        result = CliRunner().invoke(
            main,
            [
                "--catalog",
                "sqlite",
                "--namespace",
                "ns",
                "--db-path",
                "src.db",
                "--warehouse",
                "src_wh",
                "--migrations-dir",
                str(local),
                "migrate",
            ],
        )
        assert result.exit_code == 0, result.output
        _write_migration(project / "named_migs", "0001", "items")
        result = CliRunner().invoke(
            main,
            [
                "--catalog-name",
                "prod",
                "--namespace",
                "ns",
                "--migrations-dir",
                str(project / "named_migs"),
                "migrate",
            ],
        )
        assert result.exit_code == 0, result.output

        result = CliRunner().invoke(
            main,
            [
                "migrate-data",
                "--src-catalog",
                "sqlite",
                "--src-db-path",
                "src.db",
                "--src-warehouse",
                "src_wh",
                "--src-namespace",
                "ns",
                "--src-table",
                "items",
                "--dst-catalog-name",
                "prod",
                "--dst-namespace",
                "ns",
                "--quiet",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Migrating sqlite/ns.items  ->  prod/ns.items" in result.output
        assert "Done: 2 rows" in result.output

    def test_migrate_data_missing_dst_in_named_catalog(
        self, project: Path, named_catalog: Path
    ) -> None:
        self._seed_named(project)
        result = CliRunner().invoke(
            main,
            [
                "migrate-data",
                "--src-catalog-name",
                "prod",
                "--src-namespace",
                "ns",
                "--src-table",
                "items",
                "--dst-namespace",
                "ns",
                "--dst-table",
                "nope",
            ],
        )
        assert result.exit_code == 1, result.output
        assert "Table ns.nope not found in the 'prod' catalog." in result.output

    def test_run_policies_with_catalog_name(
        self, project: Path, named_catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from iceberg_ops import polaris as polaris_pkg
        from iceberg_ops.polaris import Policy

        self._seed_named(project)
        policy = Policy(
            name="snap",
            policy_type="system.snapshot-expiry",
            content={
                "enable": True,
                "config": {"max_snapshot_age_days": 1, "min_snapshot_to_keep": 1},
            },
        )
        monkeypatch.setattr(polaris_pkg, "fetch_table_policies", lambda *_args: [policy])

        result = CliRunner().invoke(
            main,
            [
                "run-policies",
                "--polaris-url",
                "https://polaris.example.com",
                "--polaris-catalog",
                "cat",
                "--polaris-token",
                "t",
                "--target-namespace",
                "ns",
                "--target-table",
                "items",
                "--catalog-name",
                "prod",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "[ok] snap" in result.output
