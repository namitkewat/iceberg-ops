"""Shared pytest fixtures for the iceberg_ops test suite.

All fixtures use real PyIceberg catalogs — no mocking. Each test gets a
fully isolated catalog rooted in pytest's ``tmp_path``.

Fixtures
--------
memory_catalog
    In-memory SQLite catalog with a per-test warehouse directory. Fastest
    option; use for the majority of tests.

sqlite_catalog
    File-backed SQLite catalog with a per-test warehouse directory. Use
    when verifying persistence, reset, or CLI flows that re-open the
    catalog (the in-memory backend cannot survive a re-open).

make_table
    Helper for the common "create namespace + create table" pattern
    inside tests. Returns a callable so tests can build several tables
    against the same catalog.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table


@pytest.fixture
def memory_catalog(tmp_path: Path) -> Iterator[Catalog]:
    """Yield an in-memory SQLite-backed PyIceberg catalog.

    Built via :func:`iceberg_ops.core.catalog.CatalogFactory.build` so the
    warehouse-URI formatting goes through the same code path as production
    callers (``f"file://{abspath}"``). That form round-trips through
    PyIceberg's own file IO on POSIX and Windows, but it is not a
    canonical file URI on Windows: ``pyarrow.fs.FileSystem.from_uri``
    maps it to ``//C/...`` and other engines may reject it, so tests
    that hand paths to other tools should use ``Path.as_uri()``.
    Warehouse Parquet files land in
    ``tmp_path/warehouse`` and are cleaned up automatically when the test
    ends. The SQL catalog itself uses ``sqlite://`` (no file) so it does
    not survive a process restart — reach for :func:`sqlite_catalog` for
    tests that re-open the catalog (typically CLI flows). The catalog is
    closed when the test ends.
    """
    from iceberg_ops.core.catalog import CatalogFactory

    catalog = CatalogFactory.build(
        "memory",
        warehouse_dir=str(tmp_path / "warehouse"),
    )
    yield catalog
    catalog.close()


@pytest.fixture
def sqlite_catalog(tmp_path: Path) -> Iterator[Catalog]:
    """Yield a file-backed SQLite PyIceberg catalog.

    The catalog database lives at ``tmp_path/catalog.db`` and the
    warehouse at ``tmp_path/warehouse``. Both are isolated per test by
    pytest's ``tmp_path`` fixture. The catalog is closed when the test
    ends, which releases the SQLite connection pool.
    """
    from iceberg_ops.core.catalog import CatalogFactory

    catalog = CatalogFactory.build(
        "sqlite",
        db_path=str(tmp_path / "catalog.db"),
        warehouse_dir=str(tmp_path / "warehouse"),
    )
    yield catalog
    catalog.close()


@pytest.fixture
def make_table() -> Any:
    """Return a helper that creates a namespace + table inside *catalog*.

    The returned callable signature is
    ``(catalog, identifier, arrow_schema) -> Table``. The namespace
    portion of *identifier* (everything before the final dot) is created
    if missing; the call is idempotent on the namespace.
    """
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    def _make(catalog: Catalog, identifier: str, arrow_schema: pa.Schema) -> Table:
        ns = identifier.rsplit(".", 1)[0]
        with contextlib.suppress(NamespaceAlreadyExistsError):
            catalog.create_namespace(ns)
        return catalog.create_table(identifier, arrow_schema)

    return _make
