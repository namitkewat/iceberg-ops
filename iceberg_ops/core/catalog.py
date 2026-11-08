"""
iceberg_ops.core.catalog
========================
Catalog factory for PyIceberg.

``CatalogFactory.build(kind, ...)`` is the single entry point for
obtaining a catalog instance regardless of the backend. SQLite and
in-memory backends are implemented natively; the remote backends (Glue,
REST, Hive, S3 Tables) go through :func:`pyiceberg.catalog.load_catalog`,
so they also pick up ``~/.pyiceberg.yaml`` and
``PYICEBERG_CATALOG__<NAME>__*`` environment variables.

Bring Your Own Catalog (BYOC)
-----------------------------
Every iceberg-ops API that takes a catalog accepts any PyIceberg
``Catalog`` subclass. ``CatalogFactory`` is a convenience helper — you
are not required to use it::

    from pyiceberg.catalog.glue import GlueCatalog
    my_catalog = GlueCatalog("prod", region_name="us-east-1")

    from iceberg_ops.runner import MigrationRunner
    runner = MigrationRunner(my_catalog, namespace="sales")

Lazy imports
------------
Backend-specific modules are imported through the ``_lazy_import``
helper inside ``_build_*`` methods rather than at module load time, so
``iceberg_ops.core.catalog`` always imports cleanly even when only a
subset of the optional extras is installed.

Default local paths
-------------------
When ``build("sqlite")`` is called without ``db_path`` /
``warehouse_dir``, the catalog lives under ``./local_data/`` in the
current working directory *at call time* (see
:func:`default_local_paths`). ``LOCAL_DB_PATH`` and ``LOCAL_WH_DIR`` are
those defaults as relative paths. Add ``local_data/`` to
``.gitignore`` so generated files are not committed. The ``"memory"``
backend never uses ``./local_data/``: without ``warehouse_dir`` its data
files go to a fresh temporary directory.
"""

from __future__ import annotations

import importlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from pyiceberg.catalog import Catalog

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default local paths — relative names, resolved at call time
# ---------------------------------------------------------------------------
LOCAL_DB_PATH = os.path.join("local_data", "iceberg_catalog.db")
LOCAL_WH_DIR = os.path.join("local_data", "iceberg_warehouse")

_SQLITE_HEADER = b"SQLite format 3\x00"


def default_local_paths(base_dir: str | os.PathLike[str] | None = None) -> tuple[str, str]:
    """
    Return the default SQLite catalog paths under *base_dir*.

    Parameters
    ----------
    base_dir : str or os.PathLike, optional
        Directory that holds ``local_data/``. Defaults to the current
        working directory, read when this function is called.

    Returns
    -------
    tuple of (str, str)
        Absolute ``(db_path, warehouse_dir)``:
        ``<base_dir>/local_data/iceberg_catalog.db`` and
        ``<base_dir>/local_data/iceberg_warehouse``.
    """
    base = os.path.abspath(os.fspath(base_dir) if base_dir is not None else os.getcwd())
    return os.path.join(base, LOCAL_DB_PATH), os.path.join(base, LOCAL_WH_DIR)


def _lazy_import(module_path: str, attr: str, *, extra: str) -> Any:
    """
    Lazily import ``module_path.attr``.

    Raises a clear ``ImportError`` pointing at the relevant iceberg-ops
    optional extra if the module cannot be imported.

    Parameters
    ----------
    module_path : str
        Dotted module path to import (e.g. ``"pyiceberg.catalog.glue"``).
    attr : str
        Attribute name to fetch from the imported module
        (e.g. ``"GlueCatalog"``).
    extra : str
        The iceberg-ops optional-extra name shown in the error message
        (e.g. ``"glue"``).

    Returns
    -------
    Any
        The fetched attribute (typically a class).

    Raises
    ------
    ImportError
        If the module cannot be imported. The message points at the
        ``pip install 'iceberg-ops[<extra>]'`` invocation.
    """
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImportError(_extra_hint(extra)) from exc
    return getattr(module, attr)


def _extra_hint(extra: str) -> str:
    """Return the install hint for the iceberg-ops optional *extra*."""
    return (
        f"This backend requires the '{extra}' extra. "
        f"Install it with: pip install 'iceberg-ops[{extra}]'"
    )


class CatalogFactory:
    """
    Factory that creates PyIceberg ``Catalog`` instances.

    Supported backends
    ------------------
    ``"sqlite"``
        Local file-backed SQLite ``SqlCatalog``. Requires the ``sqlite``
        extra (``pyiceberg[sql-sqlite]``, which brings in SQLAlchemy).

    ``"memory"``
        In-memory SQLite ``SqlCatalog`` (``uri="sqlite://"``, no ``.db``
        file). Catalog state is lost when the process exits; Parquet
        data files land on *warehouse_dir*, or in a fresh temporary
        directory when it is omitted. Pair with pytest's ``tmp_path``
        for fully isolated tests.

    ``"glue"``
        AWS Glue Iceberg catalog. Requires the ``glue`` extra.

    ``"rest"``
        REST-protocol Iceberg catalogs (Apache Polaris, Nessie,
        Lakekeeper, the AWS Lakehouse REST endpoint, ...). PyIceberg's
        REST client is built in, so no extra is needed.

    ``"hive"``
        Hive Metastore Iceberg catalog. Requires the ``hive`` extra.

    ``"s3tables"``
        AWS S3 Tables (REST + SigV4 under the hood). Requires the
        ``s3tables`` extra (which pulls in ``pyiceberg[rest-sigv4]``). Pre-sets
        the SigV4 REST properties so callers only have to provide
        ``uri`` (REST endpoint), ``warehouse`` (bucket ARN) and
        ``region``. S3 Tables enforces single-level namespaces —
        namespaces with dots are rejected by the server.

    The remote backends are loaded with
    :func:`pyiceberg.catalog.load_catalog`, so the catalog's entry in
    ``.pyiceberg.yaml`` and its ``PYICEBERG_CATALOG__<NAME>__*``
    environment variables are merged in; explicit keyword arguments win.

    Notes
    -----
    To add a backend, implement a ``_build_<kind>`` static method and
    register it in the ``builders`` dict inside :meth:`build`.
    """

    @staticmethod
    def build(
        kind: str = "sqlite",
        *,
        reset: bool = False,
        db_path: str | os.PathLike[str] | None = None,
        warehouse_dir: str | os.PathLike[str] | None = None,
        **kwargs: Any,
    ) -> Catalog:
        """
        Build and return a ready-to-use PyIceberg catalog.

        Parameters
        ----------
        kind : str
            Backend identifier. Case-insensitive. Default ``"sqlite"``.
        reset : bool
            Delete the existing catalog DB and warehouse directory
            before creating a fresh one. Only applies to the
            ``"sqlite"`` backend (file-backed); ignored for ``"memory"``
            and the remote backends. Refuses to delete a filesystem
            root, the home directory, the working directory or one of
            its parents, and a ``db_path`` that is not an SQLite file.
            **Destructive** — use only in dev / test.
        db_path : str or os.PathLike, optional
            SQLite catalog database path. Only used by ``"sqlite"``,
            where it defaults to ``./local_data/iceberg_catalog.db``
            under the current working directory at call time. Ignored
            for ``"memory"`` (no file is created). Must not contain
            ``?``.
        warehouse_dir : str or os.PathLike, optional
            Root for Iceberg data files (Parquet / Avro). For
            ``"sqlite"`` it defaults to ``./local_data/iceberg_warehouse``
            under the current working directory; for ``"memory"`` to a
            new temporary directory. For the local backends it must be a
            local directory, not a URI, and its absolute path must not
            contain ``#`` or ``?`` (the backend passes it to PyIceberg
            as an unescaped ``file://`` URI). For the remote backends it
            sets the ``warehouse`` catalog property (a location or URI)
            unless ``warehouse`` is passed explicitly.
        **kwargs :
            ``catalog_name`` names the catalog (for the remote backends
            this is the name looked up in ``.pyiceberg.yaml`` and the
            environment; it defaults to *kind*). Every other keyword is
            a catalog property passed to the backend (e.g. ``uri``,
            ``region``, ``glue.region``). The local backends reject
            ``uri`` and ``warehouse``; use *db_path* / *warehouse_dir*.

        Returns
        -------
        Catalog
            A live PyIceberg catalog instance.

        Raises
        ------
        NotImplementedError
            If *kind* is not one of the supported backends.
        ImportError
            If the optional extra for *kind* is not installed.
        ValueError
            If *db_path* is given for a remote backend, if a local
            backend gets ``uri`` / ``warehouse``, a URI as
            *warehouse_dir*, or a *warehouse_dir* / *db_path* holding a
            character its URI form cannot carry (``#`` or ``?``), if
            *reset* targets an unsafe path, or if PyIceberg rejects the
            catalog properties (for example a missing ``uri``).
        """
        builders: dict[str, Any] = {
            "sqlite": CatalogFactory._build_sqlite,
            "memory": CatalogFactory._build_memory,
            "glue": CatalogFactory._build_glue,
            "rest": CatalogFactory._build_rest,
            "hive": CatalogFactory._build_hive,
            "s3tables": CatalogFactory._build_s3tables,
        }

        key = kind.lower()
        builder = builders.get(key)
        if builder is None:
            raise NotImplementedError(
                f"Catalog kind {kind!r} is not implemented. "
                f"Supported backends: {sorted(builders)}. "
                "If you need a backend not listed here, build it directly "
                "and pass the instance to iceberg-ops APIs (BYOC)."
            )

        if key in ("sqlite", "memory"):
            CatalogFactory._reject_location_props(kwargs)
            CatalogFactory._reject_uri_warehouse(key, warehouse_dir)

        if key == "sqlite":
            default_db, default_wh = default_local_paths()
            db = os.path.abspath(os.fspath(db_path)) if db_path is not None else default_db
            wh = (
                os.path.abspath(os.fspath(warehouse_dir))
                if warehouse_dir is not None
                else default_wh
            )
            CatalogFactory._check_uri_safe("warehouse_dir", wh, "#?", "--warehouse")
            # SQLAlchemy reads "?" in the sqlite:/// URL as the query start.
            CatalogFactory._check_uri_safe("db_path", db, "?", "--db-path")
            if reset:
                CatalogFactory._reset(db, wh)
            return CatalogFactory._build_sqlite(db, wh, **kwargs)

        if reset:
            logger.debug("reset=True ignored for %r backend (no persistent state).", kind)

        if key == "memory":
            if warehouse_dir is not None:
                wh = os.path.abspath(os.fspath(warehouse_dir))
                CatalogFactory._check_uri_safe("warehouse_dir", wh, "#?", "--warehouse")
            else:
                CatalogFactory._check_uri_safe(
                    "the temporary directory", tempfile.gettempdir(), "#?", None
                )
                wh = tempfile.mkdtemp(prefix="iceberg-ops-memory-")
                logger.info("In-memory catalog warehouse defaults to %s", wh)
            return CatalogFactory._build_memory(wh, **kwargs)

        if db_path is not None:
            raise ValueError("db_path only applies to sqlite")
        if warehouse_dir is not None:
            kwargs.setdefault("warehouse", os.fspath(warehouse_dir))
        return builder(**kwargs)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Local SQL backends
    # ------------------------------------------------------------------

    @staticmethod
    def _reject_location_props(props: dict[str, Any]) -> None:
        """Refuse ``uri`` / ``warehouse`` for the local backends."""
        clashes = sorted({"uri", "warehouse"} & props.keys())
        if clashes:
            raise ValueError(
                f"{', '.join(clashes)} not accepted for local catalogs: "
                "use db_path / warehouse_dir; for other SQL databases use "
                "pyiceberg load_catalog or BYOC"
            )

    @staticmethod
    def _reject_uri_warehouse(kind: str, warehouse_dir: str | os.PathLike[str] | None) -> None:
        """Refuse a URI (``s3://...``, ``file://...``) as a local *warehouse_dir*.

        Without this, ``os.path.abspath`` would turn the URI into a
        subdirectory of the working directory.
        """
        if warehouse_dir is None:
            return
        value = os.fspath(warehouse_dir)
        if "://" in value:
            raise ValueError(
                f"warehouse_dir {value!r} looks like a URI; the {kind} kind needs a "
                "local directory. Use a named catalog (--catalog-name / "
                "pyiceberg load_catalog) for a remote or URI warehouse."
            )

    @staticmethod
    def _check_uri_safe(label: str, path: str, chars: str, flag: str | None) -> None:
        """Refuse a local *path* holding characters that break its URI form.

        The local backends hand PyIceberg ``file://<path>`` (and SQLAlchemy
        ``sqlite:///<path>``), which are parsed as URIs without unquoting:
        ``#`` starts a fragment and ``?`` a query, so the path would be cut
        short there and files written elsewhere. *flag* is the CLI option
        that sets *label*; ``None`` when the path is not a parameter.
        """
        bad = [c for c in chars if c in path]
        if bad:
            where = f"{flag} / {label}" if flag else "warehouse_dir"
            raise ValueError(
                f"{label} {path!r} contains {' and '.join(map(repr, bad))}, which "
                "cannot be used in the local catalog's file URI; pass a path "
                f"without it ({where})."
            )

    @staticmethod
    def _build_sqlite(
        db_path: str,
        warehouse_dir: str,
        catalog_name: str = "local",
        **props: Any,
    ) -> Catalog:
        """Initialise (or reuse) a local SQLite-backed Iceberg catalog."""
        cls = _lazy_import("pyiceberg.catalog.sql", "SqlCatalog", extra="sqlite")

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        os.makedirs(warehouse_dir, exist_ok=True)

        catalog = cls(
            catalog_name,
            uri=f"sqlite:///{db_path}",
            warehouse=f"file://{warehouse_dir}",
            **props,
        )
        logger.debug(
            "SQLite catalog %r ready. db=%s warehouse=%s",
            catalog_name,
            db_path,
            warehouse_dir,
        )
        return catalog  # type: ignore[no-any-return]

    @staticmethod
    def _build_memory(
        warehouse_dir: str,
        catalog_name: str = "memory",
        **props: Any,
    ) -> Catalog:
        """In-memory SQLite catalog (no .db file).

        Parquet data files still land on *warehouse_dir*; pair with a
        pytest ``tmp_path`` for fully isolated tests.
        """
        cls = _lazy_import("pyiceberg.catalog.sql", "SqlCatalog", extra="sqlite")

        os.makedirs(warehouse_dir, exist_ok=True)
        catalog = cls(
            catalog_name,
            uri="sqlite://",  # in-memory, no file
            warehouse=f"file://{warehouse_dir}",
            **props,
        )
        logger.debug("In-memory catalog %r ready. warehouse=%s", catalog_name, warehouse_dir)
        return catalog  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Remote backends — loaded through pyiceberg.catalog.load_catalog
    # ------------------------------------------------------------------

    @staticmethod
    def _load_remote(
        catalog_type: str,
        catalog_name: str,
        extra: str,
        **props: Any,
    ) -> Catalog:
        """Load a remote catalog through :func:`pyiceberg.catalog.load_catalog`.

        ``load_catalog`` merges the catalog's ``.pyiceberg.yaml`` entry
        and ``PYICEBERG_CATALOG__<NAME>__*`` environment variables with
        *props*; explicit *props* win. A missing optional dependency is
        re-raised as ``ImportError`` naming the iceberg-ops *extra*.
        """
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import NotInstalledError

        try:
            catalog = load_catalog(catalog_name, type=catalog_type, **props)
        except (NotInstalledError, ImportError) as exc:
            raise ImportError(f"{_extra_hint(extra)} ({exc})") from exc
        logger.debug("%s catalog %r ready.", catalog_type, catalog_name)
        return catalog

    @staticmethod
    def _build_glue(catalog_name: str = "glue", **props: Any) -> Catalog:
        """Build an AWS Glue Iceberg catalog."""
        return CatalogFactory._load_remote("glue", catalog_name, "glue", **props)

    @staticmethod
    def _build_rest(catalog_name: str = "rest", **props: Any) -> Catalog:
        """Build a REST-protocol Iceberg catalog (Polaris, Nessie, ...)."""
        return CatalogFactory._load_remote("rest", catalog_name, "rest", **props)

    @staticmethod
    def _build_hive(catalog_name: str = "hive", **props: Any) -> Catalog:
        """Build a Hive-Metastore-backed Iceberg catalog."""
        return CatalogFactory._load_remote("hive", catalog_name, "hive", **props)

    @staticmethod
    def _build_s3tables(catalog_name: str = "s3tables", **props: Any) -> Catalog:
        """Build an AWS S3 Tables Iceberg catalog (REST + SigV4)."""
        # SigV4 signing imports boto3 deep inside the REST client; check
        # up front so the error names the extra instead of boto3.
        _lazy_import("boto3", "Session", extra="s3tables")

        props.setdefault("rest.sigv4-enabled", "true")
        props.setdefault("rest.signing-name", "s3tables")

        region = props.pop("region", None)
        if region:
            props.setdefault("rest.signing-region", region)

        return CatalogFactory._load_remote("rest", catalog_name, "s3tables", **props)

    # ------------------------------------------------------------------
    # Reset (SQLite only)
    # ------------------------------------------------------------------

    @staticmethod
    def _reset(db_path: str, warehouse_dir: str) -> None:
        """Delete the catalog DB file and the warehouse directory.

        Refuses (``ValueError``) to delete a filesystem root, the home
        directory, the working directory or any of its parents, a
        *db_path* that is not an SQLite file, and a *warehouse_dir* that
        is not a directory. Nothing is deleted when a check fails.
        """
        db = Path(db_path).resolve()
        wh = Path(warehouse_dir).resolve()

        cwd = Path.cwd().resolve()
        home = Path.home().resolve()
        protected = {cwd, home, *cwd.parents, *home.parents}
        if wh.parent == wh or wh in protected:
            raise ValueError(
                f"Refusing to reset warehouse_dir {str(wh)!r}: it is a filesystem "
                "root, the home directory, or the working directory or one of its "
                "parents."
            )
        if wh.exists() and not wh.is_dir():
            raise ValueError(f"Refusing to reset warehouse_dir {str(wh)!r}: not a directory.")
        if db.exists():
            if not db.is_file():
                raise ValueError(f"Refusing to reset db_path {str(db)!r}: not a file.")
            with open(db, "rb") as fh:
                header = fh.read(len(_SQLITE_HEADER))
            # A zero-length file is a valid, empty SQLite database.
            if header and header != _SQLITE_HEADER:
                raise ValueError(f"Refusing to reset db_path {str(db)!r}: not an SQLite file.")

        if db.exists():
            logger.warning("Catalog reset: removing database file %s", db)
            db.unlink()
        if wh.exists():
            logger.warning("Catalog reset: removing warehouse directory %s", wh)
            shutil.rmtree(wh)
        logger.info("Catalog reset complete.")
