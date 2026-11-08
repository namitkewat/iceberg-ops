"""CLI tests for ``iceberg-ops revision --autogenerate``.

Every test runs in its own ``tmp_path`` project (``init`` + a seed
migration) against a file-backed SQLite catalog, because each CLI call
re-opens the catalog.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from iceberg_ops.cli import main


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An initialised iceberg-ops project as the working directory."""
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(main, ["init"])
    assert result.exit_code == 0, result.output
    return tmp_path


def _catalog_args(namespace: str) -> list[str]:
    return [
        "--catalog",
        "sqlite",
        "--namespace",
        namespace,
        "--db-path",
        "catalog.db",
        "--warehouse",
        "warehouse",
    ]


def _cli(*args: str, namespace: str = "sales") -> Result:
    return CliRunner().invoke(main, [*_catalog_args(namespace), *args])


def _seed(project: Path, *columns: tuple[str, str, bool]) -> None:
    """Write migration 0001 creating ``orders`` with ``(name, pa type, nullable)`` columns."""
    fields = ", ".join(
        f"pa.field({name!r}, {type_src}, nullable={nullable})"
        for name, type_src, nullable in columns
    )
    body = (
        '"""Seed orders."""\n'
        "import pyarrow as pa\n"
        "from iceberg_ops.migration import BaseMigration, MigrationContext\n"
        "\n\n"
        "class Migration(BaseMigration):\n"
        "    version = '0001'\n"
        "    description = 'create orders'\n"
        "\n"
        "    def upgrade(self, ctx: MigrationContext) -> None:\n"
        f"        ctx.create_table_if_not_exists('orders', pa.schema([{fields}]))\n"
        "\n"
        "    def downgrade(self, ctx: MigrationContext) -> None:\n"
        "        ctx.drop_table_if_exists('orders')\n"
    )
    (project / "migrations" / "0001_create_orders.py").write_text(body, encoding="utf-8")


def _contract(project: Path, text: str, name: str = "contract.yaml") -> str:
    (project / name).write_text(text, encoding="utf-8")
    return name


def _columns(project: Path, identifier: str) -> list[str]:
    from iceberg_ops.core.catalog import CatalogFactory

    catalog = CatalogFactory.build(
        "sqlite",
        db_path=str(project / "catalog.db"),
        warehouse_dir=str(project / "warehouse"),
    )
    try:
        return [f.name for f in catalog.load_table(identifier).schema().fields]
    finally:
        catalog.close()


def _revision(*extra: str, message: str = "add email", namespace: str = "sales") -> Result:
    return _cli(
        "revision",
        "-m",
        message,
        "--autogenerate",
        "--target-table",
        "orders",
        *extra,
        namespace=namespace,
    )


_STRICT_ID_EMAIL = (
    "strict: true\n"
    "columns:\n"
    "  - {name: id, type: long, required: true}\n"
    "  - {name: email, type: string}\n"
)


class TestNamespaceResolution:
    def test_bare_table_name_applies_per_namespace(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False))
        assert _cli("migrate", namespace="sales").exit_code == 0
        assert _cli("migrate", namespace="finance").exit_code == 0

        yaml_path = _contract(project, _STRICT_ID_EMAIL)
        result = _revision("--target-schema-yaml", yaml_path)
        assert result.exit_code == 0, result.output
        assert "Diffed against sales.orders." in result.output
        assert "namespace-qualified" not in result.output

        generated = project / "migrations" / "0002_add_email.py"
        assert "target_table = 'orders'" in generated.read_text(encoding="utf-8")

        assert _cli("migrate", namespace="finance").exit_code == 0
        assert _columns(project, "finance.orders") == ["id", "email"]
        assert _columns(project, "sales.orders") == ["id"]

        assert _cli("migrate", namespace="sales").exit_code == 0
        assert _columns(project, "sales.orders") == ["id", "email"]
        for ns in ("sales", "finance"):
            status = _cli("status", namespace=ns)
            assert status.exit_code == 0, status.output
            line = next(ln for ln in status.output.splitlines() if " 0002 " in ln)
            assert "[applied" in line

    def test_qualified_target_table_kept_and_warns(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False))
        assert _cli("migrate", namespace="sales").exit_code == 0
        yaml_path = _contract(project, _STRICT_ID_EMAIL)

        result = _cli(
            "revision",
            "-m",
            "add email",
            "--autogenerate",
            "--target-table",
            "sales.orders",
            "--target-schema-yaml",
            yaml_path,
            namespace="finance",
        )
        assert result.exit_code == 0, result.output
        source = (project / "migrations" / "0002_add_email.py").read_text(encoding="utf-8")
        assert "target_table = 'sales.orders'" in source
        assert "is namespace-qualified" in result.stderr
        assert "whatever --namespace is used at migrate time" in result.stderr


class TestGeneratedSourceIsSafe:
    def test_hostile_message_and_names_compile_and_run(self, project: Path) -> None:
        payload = 'x"); print("INJECTED"); ("'
        _seed(project, ("id", "pa.int64()", False), (payload, "pa.string()", True))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "strict: true\n"
            "columns:\n"
            "  - {name: id, type: long, required: true}\n"
            "  - {name: 'say \"hi\" a\\b', type: string}\n",
        )
        message = 'add "email" col \\ with """ and\nnewline'
        result = _revision("--target-schema-yaml", yaml_path, message=message)
        assert result.exit_code == 0, result.output

        generated = next((project / "migrations").glob("0002_*.py"))
        compile(generated.read_text(encoding="utf-8"), str(generated), "exec")
        status = _cli("status")
        assert status.exit_code == 0, status.output

        migrate = _cli("migrate")
        assert migrate.exit_code == 0, migrate.output
        assert "INJECTED" not in migrate.output
        assert _columns(project, "sales.orders") == ["id", 'say "hi" a\\b']


class TestContracts:
    def test_non_strict_contract_keeps_unlisted_columns(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False), ("note", "pa.string()", True))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "columns:\n  - {name: id, type: long, required: true}\n  - {name: email, type: string}\n",
        )
        result = _revision("--target-schema-yaml", yaml_path)
        assert result.exit_code == 0, result.output
        assert "Kept 1 column(s) not listed in the non-strict contract: note" in result.output
        source = (project / "migrations" / "0002_add_email.py").read_text(encoding="utf-8")
        # 'note' is kept; the only delete is the downgrade of the new column.
        assert "'note'" not in source
        assert source.count("delete_column") == 1
        assert "ctx.delete_column(target_table, 'email')" in source
        assert "ctx.enforce_column(target_table, 'email'" in source

    def test_nullability_change_is_reported(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", True))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(project, "columns:\n  - {name: id, type: long, required: true}\n")
        result = _revision("--target-schema-yaml", yaml_path, message="tighten id")
        assert result.exit_code == 0, result.output
        assert "with 0 operation(s)" in result.output
        assert "does not change nullability" in result.output
        assert "id (optional -> required)" in result.output

    def test_blocked_steps_are_listed(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False), ("code", "pa.string()", True))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "strict: true\ncolumns:\n  - {name: id, type: long, required: true}\n"
            "  - {name: code, type: long}\n",
        )
        result = _revision("--target-schema-yaml", yaml_path, message="retype code")
        assert result.exit_code == 0, result.output
        assert "upgrade() raises until edited: column 'code': string -> long" in result.output
        assert "downgrade() raises until edited: type change on 'code'" in result.output


class TestInputErrors:
    @pytest.mark.parametrize(
        ("text", "fragment"),
        [
            ("", "expected a mapping"),
            ("columns: 5\n", "'columns' must be a non-empty list"),
            ("columns:\n  - {name: a, type: uint}\n", "Unknown column type"),
            ("columns: [\n", "not valid YAML"),
            (None, "contract.yaml"),
        ],
        ids=["empty", "columns-int", "uint", "malformed", "missing-file"],
    )
    def test_bad_yaml_is_a_usage_error(
        self, project: Path, text: str | None, fragment: str
    ) -> None:
        if text is not None:
            _contract(project, text)
        result = _revision("--target-schema-yaml", "contract.yaml")
        assert result.exit_code == 2, result.output
        assert "Error" in result.output
        assert "--target-schema-yaml 'contract.yaml'" in result.output
        assert fragment in result.output
        assert isinstance(result.exception, SystemExit)

    def test_duplicate_rename_hint_old_name(self, project: Path) -> None:
        yaml_path = _contract(project, _STRICT_ID_EMAIL)
        result = _revision(
            "--target-schema-yaml",
            yaml_path,
            "--rename-hint",
            "a:b",
            "--rename-hint",
            "a:c",
        )
        assert result.exit_code == 2, result.output
        assert "'a' is given twice" in result.output

    def test_rename_hint_typo_names_the_column(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False), ("name", "pa.string()", True))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "strict: true\ncolumns:\n  - {name: id, type: long}\n  - {name: full_name, type: string}\n",
        )
        result = _revision("--target-schema-yaml", yaml_path, "--rename-hint", "nmae:full_name")
        assert result.exit_code == 2, result.output
        assert "'nmae' is not a column of the current table" in result.output
        assert not list((project / "migrations").glob("0002_*.py"))


class TestArrowTarget:
    def test_arrow_target_importable_from_cwd(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os

        _seed(project, ("id", "pa.int64()", False))
        assert _cli("migrate").exit_code == 0

        module_name = f"_rev_target_{uuid.uuid4().hex}"
        (project / f"{module_name}.py").write_text(
            "import pyarrow as pa\n"
            "TARGET = pa.schema([pa.field('id', pa.int64(), nullable=False), "
            "pa.field('email', pa.string())])\n",
            encoding="utf-8",
        )
        # Mimic the console-script launcher: the working directory is
        # not on sys.path.
        cwd = os.getcwd()
        monkeypatch.setattr(sys, "path", [p for p in sys.path if p not in ("", cwd, str(project))])
        try:
            result = _cli(
                "revision",
                "-m",
                "add email",
                "--autogenerate",
                "--target-table",
                "orders",
                "--target-schema-arrow",
                f"{module_name}:TARGET",
            )
        finally:
            sys.modules.pop(module_name, None)
        assert result.exit_code == 0, result.output
        assert cwd not in sys.path
        source = (project / "migrations" / "0002_add_email.py").read_text(encoding="utf-8")
        assert "ctx.enforce_column(target_table, 'email'" in source

    def test_uncompared_nested_column_is_named(self, project: Path) -> None:
        """A changed struct in a pa.Schema target is not diffed; say so instead of 'no differences'."""
        _seed(
            project,
            ("id", "pa.int64()", False),
            ("addr", "pa.struct([pa.field('city', pa.string())])", True),
        )
        assert _cli("migrate").exit_code == 0

        module_name = f"_rev_nested_{uuid.uuid4().hex}"
        (project / f"{module_name}.py").write_text(
            "import pyarrow as pa\n"
            "TARGET = pa.schema([pa.field('id', pa.int64(), nullable=False), "
            "pa.field('addr', pa.struct([pa.field('city', pa.string()), "
            "pa.field('zip', pa.string())]))])\n",
            encoding="utf-8",
        )
        try:
            result = _revision(
                "--target-schema-arrow", f"{module_name}:TARGET", message="addr zip"
            )
        finally:
            sys.modules.pop(module_name, None)
        assert result.exit_code == 0, result.output
        assert "with 0 operation(s)" in result.stdout
        assert "No schema differences" not in result.output
        assert "nested column(s) addr were not compared" in result.stderr


class TestTableAwareChecks:
    """The CLI passes the table's layout and format version to the diff."""

    @pytest.mark.parametrize(
        "type_str",
        ["decimal(40,2)", "'decimal(5,6)'", "'fixed[0]'", "'fixed[-1]'", "timestamp_ns"],
    )
    def test_bad_or_too_new_type_is_usage_error(self, project: Path, type_str: str) -> None:
        _seed(project, ("id", "pa.int64()", False))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "strict: true\n"
            "columns:\n"
            "  - {name: id, type: long, required: true}\n"
            f"  - {{name: c, type: {type_str}}}\n",
        )
        result = _revision("--target-schema-yaml", yaml_path, message="add c")
        assert result.exit_code == 2, result.output
        assert "Error" in result.output
        assert not list((project / "migrations").glob("0002_*.py"))

    def test_dotted_column_name_is_usage_error(self, project: Path) -> None:
        _seed(project, ("id", "pa.int64()", False))
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project,
            "strict: true\n"
            "columns:\n"
            "  - {name: id, type: long, required: true}\n"
            "  - {name: addr.city, type: string}\n",
        )
        result = _revision("--target-schema-yaml", yaml_path, message="add city")
        assert result.exit_code == 2, result.output
        assert "nested struct path" in result.output
        assert not list((project / "migrations").glob("0002_*.py"))

    def test_dropping_partition_source_blocks_upgrade(self, project: Path) -> None:
        body = (
            '"""Seed a partitioned orders table."""\n'
            "from pyiceberg.partitioning import PartitionField, PartitionSpec\n"
            "from pyiceberg.schema import Schema\n"
            "from pyiceberg.transforms import IdentityTransform\n"
            "from pyiceberg.types import LongType, NestedField, StringType\n"
            "from iceberg_ops.migration import BaseMigration, MigrationContext\n"
            "\n\n"
            "class Migration(BaseMigration):\n"
            "    version = '0001'\n"
            "    description = 'create orders'\n"
            "\n"
            "    def upgrade(self, ctx: MigrationContext) -> None:\n"
            "        schema = Schema(\n"
            "            NestedField(1, 'id', LongType(), required=True),\n"
            "            NestedField(2, 'region', StringType(), required=False),\n"
            "        )\n"
            "        spec = PartitionSpec(\n"
            "            PartitionField(2, 1000, IdentityTransform(), 'region')\n"
            "        )\n"
            "        ctx.create_table_if_not_exists('orders', schema, partition_spec=spec)\n"
            "\n"
            "    def downgrade(self, ctx: MigrationContext) -> None:\n"
            "        ctx.drop_table_if_exists('orders')\n"
        )
        (project / "migrations" / "0001_create_orders.py").write_text(body, encoding="utf-8")
        assert _cli("migrate").exit_code == 0
        yaml_path = _contract(
            project, "strict: true\ncolumns:\n  - {name: id, type: long, required: true}\n"
        )
        result = _revision("--target-schema-yaml", yaml_path, message="drop region")
        assert result.exit_code == 0, result.output
        assert "upgrade() raises until edited" in result.output
        assert "partition field 'region'" in result.output

        migrate = _cli("migrate")
        assert migrate.exit_code == 1, migrate.output
        assert _columns(project, "sales.orders") == ["id", "region"]
