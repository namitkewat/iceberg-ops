"""Tests for iceberg_ops.core.catalog."""

from __future__ import annotations

import os
import shutil
import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyiceberg.catalog as pyiceberg_catalog
import pytest
from pyiceberg.catalog import AVAILABLE_CATALOGS, CatalogType
from pyiceberg.exceptions import NotInstalledError
from pyiceberg.utils.config import Config

import iceberg_ops
from iceberg_ops.core.catalog import (
    LOCAL_DB_PATH,
    LOCAL_WH_DIR,
    CatalogFactory,
    _lazy_import,
    default_local_paths,
)

_SQLITE_HEADER = b"SQLite format 3\x00"
_SCHEMA = pa.schema([pa.field("id", pa.int64())])


def _warehouse_path(catalog: Any) -> Path:
    """Local directory behind a catalog's ``file://`` warehouse property."""
    return Path(str(catalog.properties["warehouse"]).removeprefix("file://"))


# ---------------------------------------------------------------------------
# Fixtures (file-local)
# ---------------------------------------------------------------------------


@pytest.fixture
def pyiceberg_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    """Isolate PyIceberg's config lookup from the developer machine.

    Points ``PYICEBERG_HOME`` at a directory with a neutral
    ``.pyiceberg.yaml`` (the first file found wins, so ``~`` and the cwd
    are not consulted), drops inherited ``PYICEBERG_*`` variables, and
    installs a freshly built config. Returns a callable that rebuilds
    the config after a test sets environment variables.
    """
    home = tmp_path / "pyiceberg_home"
    home.mkdir()
    (home / ".pyiceberg.yaml").write_text("default-catalog: unused\n", encoding="utf-8")
    for name in list(os.environ):
        if name.upper().startswith("PYICEBERG_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("PYICEBERG_HOME", str(home))

    def reload() -> None:
        monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())

    reload()
    return reload


@pytest.fixture
def recorder(
    monkeypatch: pytest.MonkeyPatch,
    pyiceberg_env: Callable[[], None],
) -> list[tuple[str, dict[str, Any]]]:
    """Record what ``load_catalog`` would hand to the REST/Hive/Glue loaders."""
    calls: list[tuple[str, dict[str, Any]]] = []
    sentinel = object()

    def _record(name: str, conf: dict[str, Any]) -> Any:
        calls.append((name, dict(conf)))
        return sentinel

    for catalog_type in (CatalogType.REST, CatalogType.HIVE, CatalogType.GLUE):
        monkeypatch.setitem(AVAILABLE_CATALOGS, catalog_type, _record)
    return calls


@pytest.fixture
def fake_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``import boto3`` succeed without the real package."""
    module = types.ModuleType("boto3")
    module.Session = object  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", module)


@pytest.fixture
def memory_default() -> Iterator[list[Any]]:
    """Collect memory catalogs built without a warehouse; remove their temp dirs."""
    built: list[Any] = []
    yield built
    for catalog in built:
        catalog.close()
        shutil.rmtree(_warehouse_path(catalog), ignore_errors=True)


# ---------------------------------------------------------------------------
# Local backends
# ---------------------------------------------------------------------------


class TestBuildSqlite:
    def test_warehouse_directory_created(self, tmp_path: Path) -> None:
        db = tmp_path / "cat.db"
        wh = tmp_path / "wh"
        CatalogFactory.build("sqlite", db_path=str(db), warehouse_dir=str(wh))
        assert wh.is_dir()

    def test_can_create_and_load_table(self, tmp_path: Path) -> None:
        db = tmp_path / "cat.db"
        wh = tmp_path / "wh"
        catalog = CatalogFactory.build("sqlite", db_path=str(db), warehouse_dir=str(wh))
        catalog.create_namespace("ns")
        catalog.create_table("ns.t", _SCHEMA)
        loaded = catalog.load_table("ns.t")
        assert loaded.name() == ("ns", "t")

    def test_accepts_path_objects(self, tmp_path: Path) -> None:
        catalog = CatalogFactory.build(
            "sqlite", db_path=tmp_path / "cat.db", warehouse_dir=tmp_path / "wh"
        )
        catalog.close()
        assert (tmp_path / "cat.db").is_file()

    def test_case_insensitive_kind(self, tmp_path: Path) -> None:
        db = tmp_path / "cat.db"
        wh = tmp_path / "wh"
        catalog = CatalogFactory.build("SQLite", db_path=str(db), warehouse_dir=str(wh))
        assert catalog is not None

    def test_rejects_uri(self, tmp_path: Path) -> None:
        """A SQL URI must not silently fall back to a local SQLite file."""
        with pytest.raises(ValueError, match="use db_path / warehouse_dir"):
            CatalogFactory.build(
                "sqlite",
                db_path=str(tmp_path / "cat.db"),
                warehouse_dir=str(tmp_path / "wh"),
                uri="postgresql://x",
            )
        assert not (tmp_path / "cat.db").exists()

    def test_rejects_warehouse_property(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="warehouse"):
            CatalogFactory.build("sqlite", db_path=str(tmp_path / "c.db"), warehouse="s3://b")

    def test_extra_property_forwarded(self, tmp_path: Path) -> None:
        catalog = CatalogFactory.build(
            "sqlite",
            db_path=str(tmp_path / "cat.db"),
            warehouse_dir=str(tmp_path / "wh"),
            foo="bar",
        )
        assert catalog.properties["foo"] == "bar"
        catalog.close()


class TestDefaultPaths:
    def test_relative_constants(self) -> None:
        assert not os.path.isabs(LOCAL_DB_PATH)
        assert not os.path.isabs(LOCAL_WH_DIR)
        assert Path(LOCAL_DB_PATH).parts[0] == "local_data"

    def test_default_sqlite_paths_follow_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        catalog = CatalogFactory.build("sqlite")
        catalog.close()
        assert (tmp_path / "local_data" / "iceberg_catalog.db").is_file()
        assert (tmp_path / "local_data" / "iceberg_warehouse").is_dir()

    def test_default_paths_change_with_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first, second = tmp_path / "a", tmp_path / "b"
        first.mkdir()
        second.mkdir()
        package_root = Path(iceberg_ops.__file__).resolve().parent.parent

        monkeypatch.chdir(first)
        paths_a = default_local_paths()
        monkeypatch.chdir(second)
        paths_b = default_local_paths()

        assert paths_a != paths_b
        assert paths_a == (str(first / LOCAL_DB_PATH), str(first / LOCAL_WH_DIR))
        assert paths_b == (str(second / LOCAL_DB_PATH), str(second / LOCAL_WH_DIR))
        for path in (*paths_a, *paths_b):
            assert Path(path).parent.parent in (first, second)
            assert Path(path).parent != package_root / "local_data"

    def test_default_local_paths_base_dir(self, tmp_path: Path) -> None:
        db, wh = default_local_paths(tmp_path / "proj")
        assert db == str(tmp_path / "proj" / "local_data" / "iceberg_catalog.db")
        assert wh == str(tmp_path / "proj" / "local_data" / "iceberg_warehouse")


class TestBuildMemory:
    def test_no_db_file_created(self, tmp_path: Path) -> None:
        wh = tmp_path / "wh"
        sentinel = tmp_path / "should_not_be_created.db"
        CatalogFactory.build("memory", db_path=str(sentinel), warehouse_dir=str(wh))
        assert not sentinel.exists()
        assert wh.is_dir()

    def test_create_and_load_table(self, tmp_path: Path) -> None:
        wh = tmp_path / "wh"
        catalog = CatalogFactory.build("memory", warehouse_dir=str(wh))
        catalog.create_namespace("ns")
        catalog.create_table("ns.t", _SCHEMA)
        loaded = catalog.load_table("ns.t")
        assert loaded.name() == ("ns", "t")

    def test_memory_default_warehouse_not_in_cwd(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        memory_default: list[Any],
    ) -> None:
        monkeypatch.chdir(tmp_path)
        catalog = CatalogFactory.build("memory")
        memory_default.append(catalog)
        catalog.create_namespace("ns")
        table = catalog.create_table("ns.t", _SCHEMA)

        assert not (tmp_path / "local_data").exists()
        assert _warehouse_path(catalog).name.startswith("iceberg-ops-memory-")
        assert "iceberg-ops-memory-" in table.location()

    def test_memory_and_sqlite_defaults_disjoint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        memory_default: list[Any],
    ) -> None:
        """Same-named tables in default memory and sqlite catalogs never share files."""
        monkeypatch.chdir(tmp_path)
        sqlite = CatalogFactory.build("sqlite")
        memory = CatalogFactory.build("memory")
        memory_default.append(memory)
        for catalog in (sqlite, memory):
            catalog.create_namespace("ns")
        sqlite_location = sqlite.create_table("ns.t", _SCHEMA).location()
        memory_location = memory.create_table("ns.t", _SCHEMA).location()
        sqlite.close()

        assert sqlite_location != memory_location
        assert "local_data" in sqlite_location
        assert "local_data" not in memory_location

    def test_rejects_uri(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="use db_path / warehouse_dir"):
            CatalogFactory.build("memory", warehouse_dir=str(tmp_path), uri="sqlite://")


class TestLocalWarehouseChecks:
    """The local backends only take a local directory their file:// URI can carry."""

    @pytest.mark.parametrize("kind", ["sqlite", "memory"])
    @pytest.mark.parametrize("warehouse", ["s3://b/wh", "file:///tmp/wh"])
    def test_uri_warehouse_rejected(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        kind: str,
        warehouse: str,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValueError, match=rf"looks like a URI; the {kind} kind"):
            CatalogFactory.build(kind, db_path=tmp_path / "c.db", warehouse_dir=warehouse)
        # abspath would have turned the URI into ./s3:/b/wh or ./file:/tmp/wh.
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize("kind", ["sqlite", "memory"])
    @pytest.mark.parametrize("char", ["#", "?"])
    def test_uri_breaking_char_in_warehouse_rejected(
        self, tmp_path: Path, kind: str, char: str
    ) -> None:
        with pytest.raises(ValueError, match=rf"warehouse_dir .* contains '\{char}'"):
            CatalogFactory.build(
                kind, db_path=tmp_path / "c.db", warehouse_dir=tmp_path / f"a{char}b"
            )
        # file://.../a#b would have been cut at the "#" and written to .../a.
        assert list(tmp_path.iterdir()) == []

    def test_default_warehouse_under_hash_cwd_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        project = tmp_path / "a#b"
        project.mkdir()
        monkeypatch.chdir(project)
        with pytest.raises(ValueError, match=r"contains '#'"):
            CatalogFactory.build("sqlite")
        assert list(project.iterdir()) == []
        assert sorted(p.name for p in tmp_path.iterdir()) == ["a#b"]

    def test_question_mark_in_db_path_rejected(self, tmp_path: Path) -> None:
        # SQLAlchemy would read "?b/c.db" as a query string and open ./a.
        with pytest.raises(ValueError, match=r"db_path .* contains '\?'"):
            CatalogFactory.build(
                "sqlite", db_path=tmp_path / "a?b" / "c.db", warehouse_dir=tmp_path / "wh"
            )
        assert list(tmp_path.iterdir()) == []

    def test_hash_in_db_path_is_fine(self, tmp_path: Path) -> None:
        db = tmp_path / "a#b" / "c.db"
        catalog = CatalogFactory.build("sqlite", db_path=db, warehouse_dir=tmp_path / "wh")
        try:
            catalog.create_namespace("ns")
            catalog.create_table("ns.t", _SCHEMA).append(pa.table({"id": [1, 2]}))
            assert len(catalog.load_table("ns.t").scan().to_arrow()) == 2
        finally:
            catalog.close()
        assert db.is_file()

    def test_memory_default_under_hash_tempdir_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tempfile

        temp = tmp_path / "t#d"
        temp.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(temp))
        with pytest.raises(ValueError, match=r"temporary directory .* contains '#'"):
            CatalogFactory.build("memory")
        assert list(temp.iterdir()) == []


# ---------------------------------------------------------------------------
# Reset guard
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_helper_deletes_files(self, tmp_path: Path) -> None:
        """``_reset`` removes the catalog DB file and warehouse directory.

        Tested directly against the helper: SQLite on Windows holds an
        exclusive lock on the .db file while a connection is open.
        """
        db = tmp_path / "cat.db"
        wh = tmp_path / "wh"
        db.write_bytes(_SQLITE_HEADER + b"rest of the database")
        wh.mkdir()
        (wh / "data.parquet").write_bytes(b"placeholder")

        CatalogFactory._reset(str(db), str(wh))

        assert not db.exists()
        assert not wh.exists()

    def test_reset_allows_empty_db_file(self, tmp_path: Path) -> None:
        db = tmp_path / "cat.db"
        db.write_bytes(b"")
        CatalogFactory._reset(str(db), str(tmp_path / "wh"))
        assert not db.exists()

    def test_reset_through_build_starts_fresh(self, tmp_path: Path) -> None:
        db, wh = tmp_path / "cat.db", tmp_path / "wh"
        catalog = CatalogFactory.build("sqlite", db_path=db, warehouse_dir=wh)
        catalog.create_namespace("ns")
        catalog.close()

        fresh = CatalogFactory.build("sqlite", reset=True, db_path=db, warehouse_dir=wh)
        assert fresh.list_namespaces() == []
        fresh.close()

    def test_reset_refuses_cwd(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        keep = tmp_path / "precious.txt"
        keep.write_text("keep me", encoding="utf-8")

        with pytest.raises(ValueError, match="Refusing to reset warehouse_dir"):
            CatalogFactory.build(
                "sqlite", reset=True, db_path=str(tmp_path / "c.db"), warehouse_dir="."
            )
        assert keep.read_text(encoding="utf-8") == "keep me"

    def test_reset_refuses_parent_of_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        work = tmp_path / "work"
        work.mkdir()
        monkeypatch.chdir(work)
        with pytest.raises(ValueError, match="Refusing"):
            CatalogFactory._reset(str(tmp_path / "c.db"), str(tmp_path))
        assert work.is_dir()

    def test_reset_refuses_home(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Refusing"):
            CatalogFactory._reset(str(tmp_path / "c.db"), str(Path.home()))

    def test_reset_refuses_filesystem_root(self, tmp_path: Path) -> None:
        root = Path(tmp_path.anchor)
        with pytest.raises(ValueError, match="Refusing"):
            CatalogFactory._reset(str(tmp_path / "c.db"), str(root))

    def test_reset_refuses_non_sqlite_db(self, tmp_path: Path) -> None:
        db = tmp_path / "notes.txt"
        db.write_bytes(b"not a database at all")
        wh = tmp_path / "wh"
        wh.mkdir()

        with pytest.raises(ValueError, match="not an SQLite file"):
            CatalogFactory._reset(str(db), str(wh))
        assert db.exists()
        assert wh.exists()


# ---------------------------------------------------------------------------
# Unknown kinds / lazy imports
# ---------------------------------------------------------------------------


class TestUnknownKind:
    def test_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError, match="Catalog kind"):
            CatalogFactory.build("postgres")

    def test_message_lists_supported(self) -> None:
        with pytest.raises(NotImplementedError) as exc_info:
            CatalogFactory.build("foobar")
        msg = str(exc_info.value)
        assert "sqlite" in msg
        assert "memory" in msg
        assert "glue" in msg
        assert "BYOC" in msg


class TestLazyImport:
    def test_unknown_module_error_format(self) -> None:
        with pytest.raises(ImportError, match=r"iceberg-ops\[fake\]"):
            _lazy_import("definitely_not_a_real_module_xyz", "Anything", extra="fake")

    def test_glue_path_with_real_pyiceberg(self) -> None:
        # pyiceberg.catalog.glue.GlueCatalog exists in pyiceberg core but its
        # import-time `import boto3` may fail without the optional extras.
        # We verify the helper either returns the class or raises ImportError
        # with the right install hint — both are correct outcomes.
        try:
            cls = _lazy_import("pyiceberg.catalog.glue", "GlueCatalog", extra="glue")
            assert cls.__name__ == "GlueCatalog"
        except ImportError as exc:
            assert "iceberg-ops[glue]" in str(exc)


# ---------------------------------------------------------------------------
# Remote backends through pyiceberg.catalog.load_catalog
# ---------------------------------------------------------------------------


class TestRemoteKinds:
    @pytest.mark.parametrize("kind", ["rest", "hive", "glue"])
    def test_kind_maps_name_type_and_warehouse(
        self, kind: str, recorder: list[tuple[str, dict[str, Any]]]
    ) -> None:
        CatalogFactory.build(kind, warehouse_dir="s3://b/wh", uri="u")

        assert len(recorder) == 1
        name, conf = recorder[0]
        assert name == kind
        assert conf["type"] == kind
        assert conf["warehouse"] == "s3://b/wh"
        assert conf["uri"] == "u"
        assert "db_path" not in conf
        assert "warehouse_dir" not in conf

    def test_catalog_name_is_forwarded(self, recorder: list[tuple[str, dict[str, Any]]]) -> None:
        CatalogFactory.build("rest", catalog_name="prod", uri="http://x")
        assert recorder[0][0] == "prod"

    def test_explicit_warehouse_property_wins(
        self, recorder: list[tuple[str, dict[str, Any]]]
    ) -> None:
        CatalogFactory.build("rest", warehouse_dir="ignored", warehouse="explicit", uri="u")
        assert recorder[0][1]["warehouse"] == "explicit"

    def test_env_vars_are_merged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        pyiceberg_env: Callable[[], None],
        recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__URI", "http://from-env:8181")
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__CREDENTIAL", "id:secret")
        pyiceberg_env()

        CatalogFactory.build("rest")

        _, conf = recorder[0]
        assert conf["uri"] == "http://from-env:8181"
        assert conf["credential"] == "id:secret"

    def test_explicit_props_beat_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        pyiceberg_env: Callable[[], None],
        recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        monkeypatch.setenv("PYICEBERG_CATALOG__REST__URI", "http://from-env:8181")
        pyiceberg_env()

        CatalogFactory.build("rest", uri="http://explicit:8181")

        assert recorder[0][1]["uri"] == "http://explicit:8181"

    def test_pyiceberg_yaml_is_merged(
        self,
        tmp_path: Path,
        pyiceberg_env: Callable[[], None],
        recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        home = tmp_path / "pyiceberg_home"
        (home / ".pyiceberg.yaml").write_text(
            "catalog:\n  prod:\n    uri: http://yaml:8181\n    warehouse: wh_from_yaml\n",
            encoding="utf-8",
        )
        pyiceberg_env()

        CatalogFactory.build("rest", catalog_name="prod")

        name, conf = recorder[0]
        assert name == "prod"
        assert conf["uri"] == "http://yaml:8181"
        assert conf["warehouse"] == "wh_from_yaml"

    def test_db_path_rejected_for_remote(self) -> None:
        with pytest.raises(ValueError, match="db_path only applies to sqlite"):
            CatalogFactory.build("rest", db_path="catalog.db", uri="http://x")

    def test_rest_without_uri_names_env_var(self, pyiceberg_env: Callable[[], None]) -> None:
        with pytest.raises(ValueError, match="PYICEBERG_CATALOG__REST__URI"):
            CatalogFactory.build("rest")

    def test_not_installed_maps_to_extra_hint(
        self, monkeypatch: pytest.MonkeyPatch, pyiceberg_env: Callable[[], None]
    ) -> None:
        def _missing(name: str, conf: dict[str, Any]) -> Any:
            raise NotInstalledError("Apache Hive support not installed")

        monkeypatch.setitem(AVAILABLE_CATALOGS, CatalogType.HIVE, _missing)
        with pytest.raises(ImportError, match=r"iceberg-ops\[hive\]"):
            CatalogFactory.build("hive", uri="thrift://x:9083")


class TestS3Tables:
    def test_sets_sigv4_defaults(
        self,
        fake_boto3: None,
        recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        CatalogFactory.build(
            "s3tables",
            uri="https://s3tables.example/v1",
            warehouse="arn:aws:s3tables:us-east-1:123:bucket/foo",
            region="us-east-1",
        )

        name, conf = recorder[0]
        assert name == "s3tables"
        assert conf["type"] == "rest"
        assert conf["rest.sigv4-enabled"] == "true"
        assert conf["rest.signing-name"] == "s3tables"
        assert conf["rest.signing-region"] == "us-east-1"
        assert conf["warehouse"] == "arn:aws:s3tables:us-east-1:123:bucket/foo"
        assert "region" not in conf
        assert "db_path" not in conf
        assert "warehouse_dir" not in conf

    def test_explicit_signing_props_win(
        self,
        fake_boto3: None,
        recorder: list[tuple[str, dict[str, Any]]],
    ) -> None:
        CatalogFactory.build(
            "s3tables",
            uri="https://s3tables.example/v1",
            region="us-east-1",
            **{"rest.signing-region": "eu-west-1"},
        )
        assert recorder[0][1]["rest.signing-region"] == "eu-west-1"

    def test_s3tables_without_boto3(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "boto3", None)
        with pytest.raises(ImportError, match=r"iceberg-ops\[s3tables\]"):
            CatalogFactory.build(
                "s3tables",
                uri="https://s3tables.example/v1",
                warehouse="arn:aws:s3tables:us-east-1:123:bucket/foo",
                region="us-east-1",
            )
