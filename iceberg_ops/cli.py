"""
iceberg_ops.cli
===============
Command-line interface for iceberg-ops.

Exposes ``MigrationRunner`` commands as shell subcommands.

Usage
-----
::

    iceberg-ops init
    iceberg-ops revision -m "create orders table"
    iceberg-ops migrate                                     # settings from iceberg-ops.yaml
    iceberg-ops --catalog sqlite --namespace sales status
    iceberg-ops --catalog sqlite --namespace sales current   # alias of status
    iceberg-ops --catalog sqlite --namespace sales migrate
    iceberg-ops --catalog sqlite --namespace sales migrate --target 0002
    iceberg-ops --catalog sqlite --namespace sales rollback --steps 1
    iceberg-ops --catalog sqlite --namespace sales rollback --target 0001
    iceberg-ops --catalog sqlite --namespace sales stamp
    iceberg-ops --catalog sqlite --namespace sales stamp --target 0002
    iceberg-ops --catalog sqlite --namespace sales history
    iceberg-ops --catalog sqlite --namespace sales plan
    iceberg-ops --catalog-name prod --namespace sales migrate

Configuration can also be loaded from a YAML file (default:
``iceberg-ops.yaml`` in the current directory). CLI flags override config
file values. Relative paths in the config file (``db_path``,
``warehouse`` for sqlite, ``migrations_dir``) resolve against the
directory that holds the file; relative paths given as flags resolve
against the current directory.

Catalog selection
-----------------
``--catalog sqlite`` keeps a local SQLite catalog, by default under
``local_data/`` next to the config file (or in the current directory
when there is no config file). ``--catalog-name NAME`` opens a catalog
defined in ``.pyiceberg.yaml`` or ``PYICEBERG_CATALOG__<NAME>__*``
environment variables through :func:`pyiceberg.catalog.load_catalog`.
``--catalog glue|rest|hive|s3tables`` without a name loads the catalog
named after the kind the same way, so ``PYICEBERG_CATALOG__REST__URI``
configures ``--catalog rest``. A catalog flag replaces the config file's
catalog settings; the config's ``db_path`` / ``warehouse`` only carry
over when the flags select the same catalog as the config.

Errors
------
Usage errors exit with status 2; expected runtime failures (missing
extra, missing table, catalog permission or commit errors, file system
or connection errors) print one ``Error:`` line and exit with status 1. Set ``ICEBERG_OPS_TRACEBACK=1``
to get the original traceback instead.

ASCII-only convention
---------------------
Click renders the group-level ``--help`` before the group callback (and
its ``_ensure_utf8_stdio()`` call) runs, so all option ``help=...``
strings, command docstrings, and group docstrings stick to ASCII.
Subcommand ``--help`` renders after the group callback. Runtime
``click.echo`` / ``print`` inside command bodies may use UTF-8 since
they execute after the encoding fix.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, NoReturn
from urllib.parse import urlsplit

import click
from click.core import ParameterSource

from iceberg_ops import __version__

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

logger = logging.getLogger(__name__)


# Catalog kinds the CLI accepts. "memory" is deliberately absent: its
# state is gone as soon as the command exits.
_CATALOG_KINDS = ("sqlite", "glue", "rest", "hive", "s3tables")
_KIND_CHOICE = click.Choice(_CATALOG_KINDS, case_sensitive=False)

_CONFIG_FILENAME = "iceberg-ops.yaml"
_CONFIG_KEYS = (
    "catalog",
    "catalog_name",
    "namespace",
    "db_path",
    "warehouse",
    "migrations_dir",
)

_TRACEBACK_ENV = "ICEBERG_OPS_TRACEBACK"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# Subcommands that take their own catalog options and never read the
# config file.
_NO_CONFIG_COMMANDS = frozenset({"init", "migrate-data", "run-policies"})

# A path like '/C:/...' in an error: what a file:///C:/... warehouse
# turns into on Windows.
_WINDOWS_FILE_URI_PATH = re.compile(r"(?<![\w.])/[A-Za-z]:[/\\]")
_WINDOWS_FILE_URI_HINT = (
    " (on Windows write a local warehouse as file://C:/..., not "
    "file:///C:/...; see the compatibility page)"
)


def _windows_file_uri_hint(detail: str) -> str:
    """The ``file://C:/`` hint when *detail* names a ``/C:/`` style path."""
    return _WINDOWS_FILE_URI_HINT if _WINDOWS_FILE_URI_PATH.search(detail) else ""


_MISSING_CATALOG = (
    "--catalog or --catalog-name is required "
    "(or set 'catalog' / 'catalog_name' in the config file)."
)


def _ensure_utf8_stdio() -> None:
    """Reconfigure stdout / stderr to UTF-8 when the platform default isn't.

    Windows consoles default ``sys.stdout`` to cp1252, which can crash
    on non-ASCII glyphs in the runner output. ``errors="replace"`` keeps
    non-UTF-8 pipes / redirects from crashing the CLI - they get ``?``
    instead.
    """
    encoding = getattr(sys.stdout, "encoding", None)
    if encoding and encoding.lower() != "utf-8":
        for stream in (sys.stdout, sys.stderr):
            with contextlib.suppress(AttributeError, OSError):
                stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]


def _traceback_requested() -> bool:
    """Return True when ``ICEBERG_OPS_TRACEBACK=1`` asks for raw tracebacks."""
    return os.environ.get(_TRACEBACK_ENV) == "1"


def _env_var_name(catalog_name: str, key: str) -> str:
    """Return the ``PYICEBERG_CATALOG__*`` variable that sets *key* for a catalog.

    PyIceberg lower-cases variable names and maps ``_`` to ``-`` (and
    ``__`` inside the property part to ``.``), so ``-`` and ``.`` are
    written as ``_`` and ``__`` here.
    """
    name = catalog_name.upper().replace("-", "_")
    prop = key.upper().replace("-", "_").replace(".", "__")
    return f"PYICEBERG_CATALOG__{name}__{prop}"


def _env_hint(catalog_name: str, key: str) -> str:
    """Return where to set catalog property *key*: the YAML file or an env var.

    PyIceberg reads ``_`` in variable names as ``-``, so no variable can
    address a catalog whose name contains ``_``.
    """
    if "_" in catalog_name:
        return (
            "set it in .pyiceberg.yaml (environment variables cannot address a "
            "catalog name containing '_': PyIceberg reads _ in them as -; rename "
            "the catalog with - to use them)"
        )
    return f"set it in .pyiceberg.yaml or {_env_var_name(catalog_name, key)}"


def _load_config(path: str) -> dict[str, str]:
    """
    Load the YAML config file at *path*.

    Parameters
    ----------
    path : str
        Config file path.

    Returns
    -------
    dict of str to str
        The known keys that have a value. Empty when the file does not
        exist or is empty.

    Raises
    ------
    click.UsageError
        If PyYAML is not installed, the file cannot be read or parsed,
        the top level is not a mapping, or a known key holds a
        non-string value. Unknown keys only print a warning.
    """
    if not os.path.isfile(path):
        return {}
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise click.UsageError(f"{path} requires PyYAML: pip install pyyaml") from exc
    try:
        with open(path, encoding="utf-8-sig") as fh:
            data = yaml.safe_load(fh)
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise click.UsageError(f"Cannot read config file {path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise click.UsageError(f"{path}: top level must be a mapping of 'key: value' lines.")

    cfg: dict[str, str] = {}
    for key, value in data.items():
        if key not in _CONFIG_KEYS:
            click.echo(
                f"Warning: {path}: unknown key {key!r} ignored "
                f"(known keys: {', '.join(_CONFIG_KEYS)}).",
                err=True,
            )
            continue
        if value is None:
            continue
        if not isinstance(value, str):
            hint = ""
            if isinstance(value, int | float | bool):
                hint = f'; quote it, e.g. {key}: "{value}"'
            raise click.UsageError(
                f"{path}: {key!r} must be a string, got {type(value).__name__}{hint}."
            )
        cfg[key] = value
    return cfg


def _select_catalog(
    flag_kind: str | None,
    flag_name: str | None,
    cfg: dict[str, str],
) -> tuple[str | None, str | None, bool]:
    """Decide which catalog the group options select.

    A ``--catalog-name`` flag replaces the config's ``catalog``; a
    ``--catalog`` flag replaces the config's ``catalog_name``. The third
    value tells whether the config's ``db_path`` / ``warehouse`` belong
    to the selected catalog (no catalog flag, or flags naming the same
    catalog as the config).
    """
    cfg_kind = cfg.get("catalog") or None
    cfg_name = cfg.get("catalog_name") or None
    if flag_name:
        return flag_kind, flag_name, flag_name == cfg_name
    if flag_kind:
        same = cfg_name is None and flag_kind == (cfg_kind or "").strip().lower()
        return flag_kind, None, same
    return cfg_kind, cfg_name, True


def _check_kind(kind: str | None) -> str | None:
    """Normalise a catalog kind; raise ``UsageError`` for one the CLI rejects."""
    if kind is None:
        return None
    normalized = kind.strip().lower()
    if normalized in _CATALOG_KINDS:
        return normalized
    if normalized == "memory":
        raise click.UsageError(
            "--catalog memory does not persist between commands; use sqlite or --catalog-name."
        )
    raise click.UsageError(
        f"Unknown catalog kind {kind!r}; expected one of: {', '.join(_CATALOG_KINDS)}."
    )


def _describe_catalog_error(exc: Exception, kind: str | None, name: str) -> str:
    """Turn a catalog construction failure into a one-line message."""
    from pyiceberg.exceptions import NotInstalledError

    if isinstance(exc, ImportError | NotInstalledError):
        return str(exc)
    if isinstance(exc, KeyError):
        key = str(exc.args[0]) if exc.args else "?"
        return f"Catalog {name!r} is missing required property {key}; {_env_hint(name, key)}"
    if isinstance(exc, ValueError):
        message = str(exc)
        if message.startswith("URI missing"):
            return f"Catalog {name!r} is missing required property uri; {_env_hint(name, 'uri')}"
        return message
    return f"Could not open {kind or 'named'} catalog {name!r}: {type(exc).__name__}: {exc}"


def _catalog_label(kind: str | None, catalog_name: str | None) -> str:
    """Short catalog label for messages: the name if set, else the kind."""
    return catalog_name or str(kind)


def _build_catalog(
    kind: str | None,
    db_path: str | None,
    warehouse: str | None,
    *,
    catalog_name: str | None = None,
    base_dir: str | None = None,
) -> Catalog:
    """
    Build a catalog from CLI options.

    Parameters
    ----------
    kind : str or None
        Catalog kind (``sqlite``, ``glue``, ``rest``, ``hive``,
        ``s3tables``). ``None`` together with *catalog_name* loads the
        named catalog exactly as ``.pyiceberg.yaml`` / the environment
        define it.
    db_path : str or None
        SQLite DB path. Only valid for the ``sqlite`` kind.
    warehouse : str or None
        Warehouse directory (``sqlite``) or the ``warehouse`` catalog
        property (remote catalogs).
    catalog_name : str, optional
        Catalog name looked up in ``.pyiceberg.yaml`` and
        ``PYICEBERG_CATALOG__<NAME>__*`` environment variables.
    base_dir : str, optional
        Directory that holds the default ``local_data/`` of the
        ``sqlite`` kind (the config file's directory). Defaults to the
        current directory.

    Returns
    -------
    Catalog
        The opened catalog.

    Raises
    ------
    click.UsageError
        For a missing or unknown kind, a name combined with ``sqlite``,
        or a DB path for a non-sqlite kind.
    click.ClickException
        If the catalog cannot be opened (missing extra, missing
        property, connection failure). With ``ICEBERG_OPS_TRACEBACK=1``
        the original exception propagates instead.
    """
    from iceberg_ops.core.catalog import CatalogFactory, default_local_paths

    kind = _check_kind(kind)
    if kind is None and not catalog_name:
        raise click.UsageError(_MISSING_CATALOG)
    if catalog_name and kind == "sqlite":
        raise click.UsageError(
            "A named catalog (--catalog-name / catalog_name) cannot use the local "
            "sqlite kind; drop --catalog sqlite or remove 'catalog: sqlite' from "
            "the config file."
        )
    if db_path and kind != "sqlite":
        raise click.UsageError("--db-path / db_path only applies to the sqlite catalog kind.")

    label = _catalog_label(kind, catalog_name)
    try:
        if kind == "sqlite":
            default_db, default_wh = default_local_paths(base_dir)
            return CatalogFactory.build(
                "sqlite",
                db_path=db_path or default_db,
                warehouse_dir=warehouse or default_wh,
            )
        if kind is None:
            from pyiceberg.catalog import load_catalog

            return load_catalog(catalog_name, **({"warehouse": warehouse} if warehouse else {}))
        props: dict[str, Any] = {"catalog_name": catalog_name} if catalog_name else {}
        return CatalogFactory.build(kind, warehouse_dir=warehouse or None, **props)
    except Exception as exc:
        if _traceback_requested():
            raise
        raise click.ClickException(_describe_catalog_error(exc, kind, label)) from exc


def _catalog_from_ctx(ctx_obj: dict[str, Any]) -> Catalog:
    """
    Build the catalog selected by the group options / config file.

    Parameters
    ----------
    ctx_obj : dict
        The group's ``ctx.obj``.

    Returns
    -------
    Catalog
        The opened catalog.

    Raises
    ------
    click.UsageError
        If neither a catalog kind nor a catalog name is set, or the
        combination is invalid.
    click.ClickException
        If the catalog cannot be opened.
    """
    return _build_catalog(
        ctx_obj.get("catalog"),
        ctx_obj.get("db_path"),
        ctx_obj.get("warehouse"),
        catalog_name=ctx_obj.get("catalog_name"),
        base_dir=ctx_obj.get("config_dir"),
    )


def _build_runner(ctx_obj: dict[str, Any], *, need_migrations: bool = True) -> Any:
    """
    Build a ``MigrationRunner`` from the shared CLI context.

    Parameters
    ----------
    ctx_obj : dict
        The group's ``ctx.obj``.
    need_migrations : bool, optional
        Require the migrations directory to exist. ``history`` passes
        False because it only reads the catalog. Default True.

    Returns
    -------
    MigrationRunner
        A runner bound to the selected catalog and namespace.

    Raises
    ------
    click.UsageError
        If the catalog, the namespace or the migrations directory is
        missing. Subcommands that do not need a catalog (e.g. ``init``)
        skip this path and therefore do not require the flags.
    """
    if not ctx_obj.get("catalog") and not ctx_obj.get("catalog_name"):
        raise click.UsageError(_MISSING_CATALOG)
    if not ctx_obj.get("namespace"):
        raise click.UsageError("--namespace is required (or set 'namespace' in config file).")
    migrations_dir = ctx_obj.get("migrations_dir") or "./migrations"
    if need_migrations and not os.path.isdir(migrations_dir):
        raise click.UsageError(
            f"Migrations directory {migrations_dir!r} not found; "
            "run 'iceberg-ops init' or pass --migrations-dir."
        )

    from iceberg_ops.runner import MigrationRunner

    catalog = _catalog_from_ctx(ctx_obj)
    return MigrationRunner(
        catalog,
        namespace=ctx_obj["namespace"],
        migrations_dir=migrations_dir,
    )


def _describe_runtime_error(exc: Exception) -> str | None:
    """Return a one-line message for an expected failure, else None."""
    from pyiceberg.exceptions import (
        CommitFailedException,
        ForbiddenError,
        NoSuchNamespaceError,
        NoSuchTableError,
        RESTError,
        UnauthorizedError,
    )

    from iceberg_ops.data_migration.checkpoint import CheckpointError
    from iceberg_ops.history import DEFAULT_META_NAMESPACE

    if isinstance(exc, ForbiddenError | UnauthorizedError):
        return (
            f"{type(exc).__name__}: {exc} (check the catalog credentials; the first "
            "migration command against a catalog also needs permission to create the "
            f"{DEFAULT_META_NAMESPACE} namespace and history table)"
        )
    if isinstance(exc, PermissionError):
        return f"Permission denied: {exc}"
    expected = (
        RESTError,
        CommitFailedException,
        NoSuchNamespaceError,
        NoSuchTableError,
        CheckpointError,
    )
    if isinstance(exc, expected):
        return f"{type(exc).__name__}: {exc}"
    if isinstance(exc, OSError):
        # File system, object store and connection failures. PyArrow's
        # Windows messages end in "\r\n"; strip it to keep one line.
        detail = str(exc).strip()
        return f"{type(exc).__name__}: {detail}{_windows_file_uri_hint(detail)}"
    return None


def _exit_with_error(exc: Exception, prefix: str = "Error") -> NoReturn:
    """Print ``<prefix>: <exc>`` to stderr and exit 1 (re-raise on request)."""
    if _traceback_requested():
        raise exc
    # PyArrow's Windows messages end in "\r\n"; strip it to keep one line.
    detail = str(exc).strip()
    if isinstance(exc, KeyError | TypeError):
        detail = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, OSError) or isinstance(exc.__cause__, OSError):
        # A migration that failed on I/O gets the same hint as a bare OSError.
        detail += _windows_file_uri_hint(detail)
    click.echo(f"{prefix}: {detail}", err=True)
    sys.exit(1)


@contextlib.contextmanager
def _runner_errors(*, usage_value_errors: bool = True) -> Iterator[None]:
    """
    Map a runner command's expected failures to CLI errors.

    Parameters
    ----------
    usage_value_errors : bool, optional
        Treat ``ValueError`` (a bad ``--target`` or ``--steps``) as a
        usage error. Default True; commands without those options pass
        False.

    Raises
    ------
    click.UsageError
        For the runner's own ``FileNotFoundError`` (missing migrations
        directory) and, with *usage_value_errors*, ``ValueError``. A
        ``FileNotFoundError`` from the file system or PyArrow (it
        carries an ``errno`` or a file name) is a runtime failure and
        is left to the group, which reports it with status 1.
    SystemExit
        Status 1 after one ``Error:`` line for ``RuntimeError``: a
        migration that fails or cannot be loaded, a missing
        ``downgrade()``, or ``ConcurrentMigrationError``. Expected
        failure types the group already reports (e.g.
        ``CheckpointError``) are left to it.
    """
    try:
        yield
    except FileNotFoundError as exc:
        if exc.errno is not None or exc.filename is not None:
            raise
        raise click.UsageError(str(exc)) from exc
    except ValueError as exc:
        if not usage_value_errors:
            raise
        raise click.UsageError(str(exc)) from exc
    except RuntimeError as exc:
        if _describe_runtime_error(exc) is not None:
            raise
        _exit_with_error(exc)


class _CliGroup(click.Group):
    """Click group that reports expected failures without a traceback.

    Catalog permission / server errors, commit conflicts, missing tables
    or namespaces, checkpoint errors and I/O errors (``OSError``: file
    system, object store, connection) become a one-line ``Error:`` and
    exit status 1. ``ICEBERG_OPS_TRACEBACK=1`` re-raises the original
    exception instead.
    """

    def invoke(self, ctx: click.Context) -> Any:
        """
        Invoke the group, mapping expected failures to ``ClickException``.

        Parameters
        ----------
        ctx : click.Context
            The group's context.

        Returns
        -------
        Any
            Whatever the invoked subcommand returns.

        Raises
        ------
        click.ClickException
            For the expected failure types listed on the class.
        """
        try:
            return super().invoke(ctx)
        except click.ClickException:
            raise
        except Exception as exc:
            message = _describe_runtime_error(exc)
            if message is None or _traceback_requested():
                raise
            raise click.ClickException(message) from exc


def _normalize_target(
    ctx: click.Context,
    param: click.Parameter,
    value: str | None,
) -> str | None:
    """Click callback: zero-pad a ``--target`` version (``2`` -> ``0002``)."""
    if value is None:
        return None
    version = value.strip()
    if re.fullmatch(r"\d{1,4}", version):
        return version.zfill(4)
    raise click.BadParameter("expected a migration version such as 0002")


_TARGET_HELP = "Version, e.g. 0002 (leading zeros optional)."


@click.group(cls=_CliGroup)
@click.version_option(
    version=__version__,
    prog_name="iceberg-ops",
    message="%(prog)s %(version)s",
)
@click.option(
    "--catalog",
    type=_KIND_CHOICE,
    default=None,
    help="Catalog kind. sqlite keeps a local catalog under local_data/.",
)
@click.option(
    "--catalog-name",
    type=str,
    default=None,
    help="Catalog defined in .pyiceberg.yaml or PYICEBERG_CATALOG__<NAME>__* env vars.",
)
@click.option("--namespace", type=str, default=None, help="Target namespace.")
@click.option(
    "--db-path",
    type=str,
    default=None,
    help="SQLite catalog DB path (sqlite only).",
)
@click.option(
    "--warehouse",
    type=str,
    default=None,
    help="Warehouse directory (sqlite) or warehouse location/URI (remote catalogs).",
)
@click.option(
    "--migrations-dir",
    type=str,
    default=None,
    help="Migrations directory (default: migrations/ next to the config file).",
)
@click.option(
    "--config",
    type=str,
    default=_CONFIG_FILENAME,
    help="Config file path (default: iceberg-ops.yaml).",
)
@click.pass_context
def main(ctx, catalog, catalog_name, namespace, db_path, warehouse, migrations_dir, config):
    """Alembic-style schema migrations for Apache Iceberg.

    Project commands (status, current, migrate, rollback, stamp,
    history, plan) need a catalog and --namespace, from flags or the
    config file. Choose the catalog with --catalog (sqlite keeps a
    local catalog under local_data/) or with --catalog-name for a
    catalog defined in .pyiceberg.yaml or PYICEBERG_CATALOG__<NAME>__*
    environment variables. init and revision need no catalog
    (revision --autogenerate does); migrate-data and run-policies take
    their own catalog options.

    Set ICEBERG_OPS_TRACEBACK=1 to see full tracebacks.
    """
    _ensure_utf8_stdio()
    # init writes the config file, so it neither needs nor reads one; a
    # broken existing file must not stop 'init --force' from replacing it.
    # migrate-data and run-policies take their own catalog options, so an
    # unrelated config problem must not block them (or their --help).
    reads_config = ctx.invoked_subcommand not in _NO_CONFIG_COMMANDS
    explicit_config = ctx.get_parameter_source("config") is not ParameterSource.DEFAULT
    if reads_config and explicit_config and not os.path.isfile(config):
        raise click.BadParameter("file not found", param_hint="--config")

    cfg = _load_config(config) if reads_config else {}
    has_config = reads_config and os.path.isfile(config)
    config_dir = os.path.dirname(os.path.abspath(config)) if has_config else None

    def _from_config(value: str) -> str:
        """Resolve a relative path from the config file against its directory."""
        if config_dir is None or os.path.isabs(value):
            return value
        return os.path.join(config_dir, value)

    kind, name, same_catalog = _select_catalog(catalog, catalog_name, cfg)
    cfg_db = cfg.get("db_path") if same_catalog and not catalog_name else None
    cfg_wh = cfg.get("warehouse") if same_catalog else None

    if warehouse:
        eff_warehouse: str | None = warehouse
    elif cfg_wh and (kind or "").strip().lower() == "sqlite" and "://" not in cfg_wh:
        eff_warehouse = _from_config(cfg_wh)
    else:
        # A URI is passed through unchanged; for the sqlite kind the
        # catalog factory rejects it with a one-line error.
        eff_warehouse = cfg_wh

    if migrations_dir:
        eff_migrations = migrations_dir
    elif cfg.get("migrations_dir"):
        eff_migrations = _from_config(cfg["migrations_dir"])
    elif config_dir is not None:
        eff_migrations = os.path.join(config_dir, "migrations")
    else:
        eff_migrations = "./migrations"

    ctx.ensure_object(dict)
    ctx.obj["catalog"] = kind
    ctx.obj["catalog_name"] = name
    ctx.obj["namespace"] = namespace or cfg.get("namespace")
    ctx.obj["db_path"] = db_path or (_from_config(cfg_db) if cfg_db else None)
    ctx.obj["warehouse"] = eff_warehouse
    ctx.obj["migrations_dir"] = eff_migrations
    ctx.obj["config_dir"] = config_dir


@main.command()
@click.option("--force", is_flag=True, help="Overwrite an existing config file.")
@click.pass_context
def init(ctx, force):
    """Scaffold a new iceberg-ops project in the current directory.

    Creates migrations/ (with __init__.py), an iceberg-ops.yaml stub, and
    appends .gitignore rules for local state. The group --config and
    --migrations-dir options choose other locations; migrations/ goes
    next to the config file by default. Idempotent: running twice
    without --force leaves existing files untouched. Does not require
    --catalog or --namespace.
    """
    _ensure_utf8_stdio()
    from iceberg_ops.scaffolding import init_project

    params = ctx.parent.params if ctx.parent is not None else {}
    config = params.get("config") or _CONFIG_FILENAME
    init_project(
        target_dir=".",
        force=force,
        config_name=config,
        migrations_dir=params.get("migrations_dir"),
    )
    command = "iceberg-ops"
    if config != _CONFIG_FILENAME:
        command += f" --config {config}"
    click.echo("Initialised iceberg-ops project.")
    click.echo("Next steps:")
    click.echo(f"  1. Review {config} (default: local SQLite under local_data/ next to it)")
    click.echo(f"  2. {command} revision -m 'create initial schema'")
    click.echo("  3. Implement upgrade()/downgrade() in the new file")
    click.echo(f"  4. {command} migrate")


@main.command()
@click.option(
    "-m",
    "--message",
    required=True,
    help="Short description of the migration; used as the filename slug.",
)
@click.option(
    "--autogenerate/--no-autogenerate",
    default=False,
    help=(
        "Diff an existing Iceberg table against a target schema and emit "
        "matching add_column / rename_column / type_change / delete_column "
        "ops. Requires --target-table and exactly one of "
        "--target-schema-yaml / --target-schema-arrow."
    ),
)
@click.option(
    "--target-table",
    type=str,
    default=None,
    help=(
        "Existing table to compare against (required with --autogenerate). "
        "Use a bare name (e.g. 'orders'): it is loaded from --namespace now "
        "and resolved against the --namespace given at migrate time. A "
        "dotted name pins the generated migration to that one table."
    ),
)
@click.option(
    "--target-schema-yaml",
    type=str,
    default=None,
    help="Path to a YAML SchemaContract describing the desired schema.",
)
@click.option(
    "--target-schema-arrow",
    type=str,
    default=None,
    help=(
        "Importable target schema in 'module:attr' form. The attr must "
        "resolve to a pa.Schema (e.g. 'myproj.schemas:USERS_TARGET'); the "
        "module must be importable from the current directory or the "
        "environment."
    ),
)
@click.option(
    "--rename-hint",
    multiple=True,
    help=(
        "old:new pair telling autogen to emit a rename_column op instead "
        "of drop+add. Repeatable (e.g. --rename-hint name:full_name "
        "--rename-hint score:rating)."
    ),
)
@click.pass_context
def revision(
    ctx,
    message,
    autogenerate,
    target_table,
    target_schema_yaml,
    target_schema_arrow,
    rename_hint,
):
    """Create a new migration file with the next version number.

    Without --autogenerate: picks max(NNNN) + 1 from existing files in
    --migrations-dir (or the <namespace>/ subdir if it exists) and
    writes a BaseMigration skeleton. Does not require --catalog;
    --namespace is consulted only to pick a subdir.

    With --autogenerate: loads --target-table from the catalog, diffs
    it against the target schema (YAML SchemaContract or Python
    pa.Schema), and writes a migration that closes the gap. Requires a
    catalog (--catalog, --catalog-name or the config file) and
    --namespace to load the existing table.
    A bare --target-table stays bare in the file, so the migration
    applies to whichever --namespace it is run against. Steps that
    cannot run or cannot be reversed make upgrade() / downgrade()
    raise before any step runs; the command lists them. Always review
    the generated file before running it.
    """
    _ensure_utf8_stdio()

    if autogenerate:
        _do_revision_autogenerate(
            ctx,
            message=message,
            target_table=target_table,
            target_schema_yaml=target_schema_yaml,
            target_schema_arrow=target_schema_arrow,
            rename_hints=_parse_rename_hints(rename_hint),
        )
        return

    # Plain (non-autogen) path - none of the autogen flags should be set.
    if any(
        (
            target_table,
            target_schema_yaml,
            target_schema_arrow,
            rename_hint,
        )
    ):
        raise click.UsageError(
            "--target-table / --target-schema-yaml / --target-schema-arrow / "
            "--rename-hint are only valid with --autogenerate."
        )

    from iceberg_ops.scaffolding import create_revision

    try:
        path = create_revision(
            message=message,
            migrations_dir=ctx.obj.get("migrations_dir") or "./migrations",
            namespace=ctx.obj.get("namespace"),
        )
    except FileNotFoundError as exc:
        raise click.UsageError(str(exc)) from exc
    except FileExistsError as exc:
        raise click.ClickException(_revision_exists_message(exc.filename)) from exc

    click.echo(f"Created migration: {path}")


def _revision_exists_message(path: object) -> str:
    """Message for a revision file that appeared after the version scan."""
    return (
        f"Migration file {path} already exists and was left unchanged "
        "(was another revision created at the same time?); run the command again."
    )


def _parse_rename_hints(values: tuple[str, ...]) -> dict[str, str]:
    """Parse the repeatable ``--rename-hint old:new`` flag into a dict.

    Raises :class:`click.UsageError` for a malformed pair or when the
    same old name is given twice.
    """
    hints: dict[str, str] = {}
    for raw in values:
        if ":" not in raw:
            raise click.UsageError(f"--rename-hint expects 'old:new' (got {raw!r}).")
        old, new = raw.split(":", 1)
        old, new = old.strip(), new.strip()
        if not old or not new:
            raise click.UsageError(
                f"--rename-hint old and new names must be non-empty (got {raw!r})."
            )
        if old in hints:
            raise click.UsageError(
                f"--rename-hint {old!r} is given twice ({old}:{hints[old]} and {raw})."
            )
        hints[old] = new
    return hints


def _resolve_target_schema(
    *,
    yaml_path: str | None,
    arrow_path: str | None,
):
    """Resolve --target-schema-yaml / --target-schema-arrow to a real
    schema object (``SchemaContract`` or ``pa.Schema``).

    Exactly one of the two paths must be set; both routes raise
    :class:`click.UsageError` for any input problem so the CLI surfaces
    a single tidy error message.
    """
    import pyarrow as pa

    provided = [p for p in (yaml_path, arrow_path) if p]
    if len(provided) != 1:
        raise click.UsageError(
            "Provide exactly one of --target-schema-yaml or --target-schema-arrow."
        )

    if yaml_path:
        from iceberg_ops.core.schema import SchemaContract

        try:
            return SchemaContract.from_yaml(yaml_path)
        except (OSError, ValueError, ImportError) as exc:
            raise click.UsageError(f"--target-schema-yaml {yaml_path!r}: {exc}") from exc

    assert arrow_path is not None  # for mypy; len-check above guarantees this
    if ":" not in arrow_path:
        raise click.UsageError(
            f"--target-schema-arrow expects 'module:attr' (got {arrow_path!r})."
        )
    module_name, attr_name = arrow_path.split(":", 1)
    import importlib

    # Console-script launchers do not put the working directory on
    # sys.path, so a schema module next to the project would not import.
    cwd = os.getcwd()
    added = cwd not in sys.path
    if added:
        sys.path.insert(0, cwd)
    try:
        importlib.invalidate_caches()
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise click.UsageError(
            f"--target-schema-arrow: cannot import {module_name!r}: {exc}"
        ) from exc
    finally:
        if added and cwd in sys.path:
            sys.path.remove(cwd)
    if not hasattr(module, attr_name):
        raise click.UsageError(
            f"--target-schema-arrow: {module_name!r} has no attribute {attr_name!r}."
        )
    schema = getattr(module, attr_name)
    if not isinstance(schema, pa.Schema):
        raise click.UsageError(
            f"--target-schema-arrow: {arrow_path!r} resolved to "
            f"{type(schema).__name__}, expected pa.Schema."
        )
    return schema


def _do_revision_autogenerate(
    ctx,
    *,
    message: str,
    target_table: str | None,
    target_schema_yaml: str | None,
    target_schema_arrow: str | None,
    rename_hints: dict[str, str],
) -> None:
    """Body of ``revision --autogenerate``."""
    from pathlib import Path

    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    from iceberg_ops.autogen import (
        _blocking_reasons,
        _ignored_attribute_changes,
        _uncompared_nested_columns,
        _unlisted_columns_kept,
        schema_diff_to_ops,
        write_autogenerated_revision,
    )
    from iceberg_ops.scaffolding import _next_version, _slugify

    if not target_table:
        raise click.UsageError("--target-table is required with --autogenerate.")
    target = _resolve_target_schema(
        yaml_path=target_schema_yaml,
        arrow_path=target_schema_arrow,
    )

    if not ctx.obj.get("namespace"):
        raise click.UsageError(
            "--namespace is required with --autogenerate (or set 'namespace' in config)."
        )
    # Same catalog selection as the runner commands: --catalog,
    # --catalog-name or the config file, with config-relative defaults.
    catalog = _catalog_from_ctx(ctx.obj)

    # The qualified name is only used to load the table now; the
    # generated file keeps the name exactly as typed so a bare name is
    # resolved against the namespace given at migrate time.
    qualified = target_table if "." in target_table else f"{ctx.obj['namespace']}.{target_table}"
    # Permission and server errors propagate to the group handler.
    try:
        existing = catalog.load_table(qualified)
    except (NoSuchTableError, NoSuchNamespaceError, ValueError) as exc:
        raise click.UsageError(
            f"--autogenerate: cannot load --target-table {qualified!r}: {exc}"
        ) from exc
    finally:
        catalog.close()
    current_schema = existing.schema()

    try:
        # The table's layout and format version let autogenerate block a
        # drop of a partition / sort source column and reject types the
        # table cannot hold, instead of failing at migrate time.
        ops = schema_diff_to_ops(
            current=current_schema,
            target=target,
            rename_hints=rename_hints,
            format_version=existing.format_version,
            spec=existing.spec(),
            sort_order=existing.sort_order(),
        )
        kept = _unlisted_columns_kept(current_schema, target, rename_hints)
        ignored = _ignored_attribute_changes(current_schema, target, rename_hints)
        uncompared = _uncompared_nested_columns(current_schema, target, rename_hints)
    except (ValueError, TypeError) as exc:
        raise click.UsageError(f"--autogenerate: {exc}") from exc

    migrations_dir = Path(ctx.obj.get("migrations_dir") or "./migrations")
    if not migrations_dir.is_dir():
        raise click.UsageError(
            f"Migrations directory {str(migrations_dir)!r} does not exist - "
            "run 'iceberg-ops init' first."
        )
    ns_dir = migrations_dir / ctx.obj["namespace"]
    sub_dir = ns_dir if ns_dir.is_dir() else migrations_dir

    version = _next_version(sub_dir)
    slug = _slugify(message) or "migration"
    out_path = sub_dir / f"{version}_{slug}.py"

    try:
        write_autogenerated_revision(
            version=version,
            description=message,
            ops=ops,
            target_table=target_table,
            out_path=out_path,
        )
    except FileExistsError as exc:
        raise click.ClickException(_revision_exists_message(out_path)) from exc

    if "." in target_table:
        click.echo(
            f"Warning: --target-table {target_table!r} is namespace-qualified; the "
            "generated migration always modifies that table, whatever --namespace "
            "is used at migrate time.",
            err=True,
        )
    click.echo(f"Diffed against {qualified}.")
    click.echo(f"Generated {out_path} with {len(ops)} operation(s). REVIEW BEFORE RUNNING.")
    if not ops and not uncompared:
        click.echo("(No schema differences detected - the migration body is a no-op.)")
    if uncompared:
        click.echo(
            f"Warning: nested column(s) {', '.join(uncompared)} were not compared "
            "(pa.Schema target); write any change to them by hand.",
            err=True,
        )
    if kept:
        click.echo(
            f"Kept {len(kept)} column(s) not listed in the non-strict contract: "
            f"{', '.join(kept)} (set 'strict: true' to drop them)."
        )
    if ignored:
        click.echo(
            "Note: autogenerate does not change nullability or docs of existing "
            f"columns, and adds new columns as optional: {'; '.join(ignored)}."
        )
    upgrade_reasons, downgrade_reasons = _blocking_reasons(ops)
    if upgrade_reasons:
        click.echo(f"upgrade() raises until edited: {'; '.join(upgrade_reasons)}")
    if downgrade_reasons:
        click.echo(f"downgrade() raises until edited: {'; '.join(downgrade_reasons)}")


@main.command()
@click.pass_context
def status(ctx):
    """Show migration status for the namespace."""
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj)
    with _runner_errors(usage_value_errors=False):
        runner.status()


@main.command()
@click.pass_context
def current(ctx):
    """Alias of status - show the current migration state for the namespace."""
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj)
    with _runner_errors(usage_value_errors=False):
        runner.status()


@main.command()
@click.option(
    "--target",
    type=str,
    default=None,
    callback=_normalize_target,
    help=f"Apply up to (and including) this version. {_TARGET_HELP}",
)
@click.pass_context
def migrate(ctx, target):
    """Apply pending migrations."""
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj)
    with _runner_errors():
        runner.migrate(target=target)


@main.command()
@click.option(
    "--steps",
    type=click.IntRange(min=1),
    default=None,
    help="Number of migrations to undo (default 1). Not combinable with --target.",
)
@click.option(
    "--target",
    type=str,
    default=None,
    callback=_normalize_target,
    help=f"Roll back until this version is the latest applied. {_TARGET_HELP}",
)
@click.pass_context
def rollback(ctx, steps, target):
    """Roll back applied migrations."""
    _ensure_utf8_stdio()
    if steps is not None and target is not None:
        raise click.UsageError("--steps and --target are mutually exclusive.")
    runner = _build_runner(ctx.obj)
    # NotImplementedError (a downgrade() that cannot undo) is a RuntimeError.
    with _runner_errors():
        runner.rollback(steps=steps or 1, target=target)


@main.command()
@click.option(
    "--target",
    type=str,
    default=None,
    callback=_normalize_target,
    help=f"Mark migrations applied up to (and including) this version. {_TARGET_HELP}",
)
@click.pass_context
def stamp(ctx, target):
    """Mark migrations as applied without running them.

    Used to adopt iceberg-ops on an existing Iceberg deployment whose
    schema was created out-of-band. Every discovered migration that is
    not currently applied (never run, failed or rolled back) gets an
    applied row. A failed version is therefore marked applied without
    being re-run, even if its upgrade() stopped halfway: check such
    versions with status before stamping. No upgrade() is invoked and
    no user tables are created.
    """
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj)
    with _runner_errors():
        runner.stamp(target=target)


@main.command()
@click.pass_context
def history(ctx):
    """Print migration history for the namespace."""
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj, need_migrations=False)
    with _runner_errors(usage_value_errors=False):
        runner.history()


@main.command()
@click.option(
    "--target",
    type=str,
    default=None,
    callback=_normalize_target,
    help=f"Show only migrations up to this version. {_TARGET_HELP}",
)
@click.pass_context
def plan(ctx, target):
    """Preview pending migrations without applying."""
    _ensure_utf8_stdio()
    runner = _build_runner(ctx.obj)
    with _runner_errors():
        runner.plan(target=target)


# ===========================================================================
# migrate-data
# ===========================================================================


def _parse_column_mapping(pairs: tuple[str, ...]) -> dict[str, str]:
    """Parse repeated ``src:dst`` flags into a dict.

    Empty tuple -> empty dict. Each entry must split into exactly
    two non-empty parts on the first ``:``.
    """
    result: dict[str, str] = {}
    for pair in pairs:
        if ":" not in pair:
            raise click.UsageError(
                f"--column-mapping value {pair!r} must be 'src:dst' (missing ':')"
            )
        src, dst = pair.split(":", 1)
        src, dst = src.strip(), dst.strip()
        if not src or not dst:
            raise click.UsageError(
                f"--column-mapping value {pair!r} must be 'src:dst' (empty side)"
            )
        if src in result:
            raise click.UsageError(f"--column-mapping has duplicate source column {src!r}")
        result[src] = dst
    return result


def _load_table(
    *,
    kind: str | None,
    namespace: str,
    table_name: str,
    db_path: str | None,
    warehouse: str | None,
    catalog_name: str | None = None,
    role: str | None = None,
) -> Table:
    """Build a catalog and load ``namespace.table_name`` from it.

    A missing table or namespace becomes a ``click.ClickException``;
    for ``role="destination"`` the message adds that migrate-data does
    not create the destination table.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    catalog = _build_catalog(kind, db_path, warehouse, catalog_name=catalog_name)
    try:
        return catalog.load_table(f"{namespace}.{table_name}")
    except (NoSuchTableError, NoSuchNamespaceError) as exc:
        where = repr(catalog_name) if catalog_name else kind
        message = f"Table {namespace}.{table_name} not found in the {where} catalog."
        if role == "destination":
            message += (
                " migrate-data does not create the destination table; "
                "create it with a migration first."
            )
        raise click.ClickException(message) from exc


@main.command("migrate-data")
@click.option(
    "--src-catalog",
    type=_KIND_CHOICE,
    default=None,
    help="Source catalog kind. Give this or --src-catalog-name.",
)
@click.option(
    "--src-catalog-name",
    type=str,
    default=None,
    help="Source catalog defined in .pyiceberg.yaml or PYICEBERG_CATALOG__<NAME>__* env vars.",
)
@click.option("--src-namespace", required=True, help="Source namespace.")
@click.option("--src-table", required=True, help="Source table name.")
@click.option(
    "--src-db-path",
    type=str,
    default=None,
    help="SQLite DB path for source (when src-catalog=sqlite).",
)
@click.option(
    "--src-warehouse",
    type=str,
    default=None,
    help="Warehouse directory (sqlite) or warehouse location (remote) for source.",
)
@click.option(
    "--dst-catalog",
    type=_KIND_CHOICE,
    default=None,
    help="Destination catalog kind. Defaults to the source catalog.",
)
@click.option(
    "--dst-catalog-name",
    type=str,
    default=None,
    help=(
        "Destination catalog from .pyiceberg.yaml or PYICEBERG_CATALOG__<NAME>__* "
        "env vars. Defaults to --src-catalog-name when --dst-catalog is not given."
    ),
)
@click.option("--dst-namespace", required=True, help="Destination namespace.")
@click.option(
    "--dst-table",
    type=str,
    default=None,
    help="Destination table name. Defaults to --src-table.",
)
@click.option(
    "--dst-db-path",
    type=str,
    default=None,
    help="SQLite DB path for destination. Defaults to --src-db-path for the same catalog.",
)
@click.option(
    "--dst-warehouse",
    type=str,
    default=None,
    help="Warehouse for destination. Defaults to --src-warehouse for the same catalog.",
)
@click.option(
    "--row-filter",
    type=str,
    default=None,
    help="PyIceberg row filter expression applied to the source scan.",
)
@click.option(
    "--source-snapshot",
    type=int,
    default=None,
    help="Pin the source read to this snapshot ID (default: current).",
)
@click.option(
    "--batch-rows",
    type=click.IntRange(min=1),
    default=50_000,
    show_default=True,
    help="Target source rows per write commit. Bounds the rows in each commit, not peak memory.",
)
@click.option(
    "--write-mode",
    type=click.Choice(["append", "upsert", "overwrite_partition"]),
    default="append",
    show_default=True,
    help=(
        "How each chunk is written to the destination. overwrite_partition "
        "needs an identity-partitioned destination and a migration that fits "
        "in one commit (at most --batch-rows source rows)."
    ),
)
@click.option(
    "--upsert-keys",
    type=str,
    default=None,
    help="Comma-separated columns. Required when --write-mode=upsert.",
)
@click.option(
    "--checkpoint",
    type=str,
    default=None,
    help="Checkpoint JSON path for resumability.",
)
@click.option(
    "--column-mapping",
    multiple=True,
    metavar="SRC:DST",
    help="Source-to-dest column rename. Repeatable.",
)
@click.option(
    "--on-missing-in-source",
    type=click.Choice(["error", "null", "skip"]),
    default="error",
    show_default=True,
    help="Policy when a destination column has no source counterpart.",
)
@click.option(
    "--on-missing-in-dest",
    type=click.Choice(["error", "auto_add"]),
    default="error",
    show_default=True,
    help="Policy when a source column has no destination counterpart.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Pre-flight only: count rows + bytes, do not write.",
)
@click.option(
    "--verify",
    type=click.Choice(["row_count", "partition_hash", "sample"]),
    default=None,
    help=(
        "Run a post-migration check and print the result. partition_hash "
        "needs an identity-partitioned or unpartitioned destination and reads "
        "both tables in full; sample compares the first --sample-size rows of "
        "each side and can report a false mismatch on larger tables. Meant for "
        "an append into an empty destination."
    ),
)
@click.option(
    "--sample-size",
    type=click.IntRange(min=1),
    default=100,
    show_default=True,
    help="Sample size for --verify=sample.",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Suppress per-batch progress output.",
)
def migrate_data(
    src_catalog: str | None,
    src_catalog_name: str | None,
    src_namespace: str,
    src_table: str,
    src_db_path: str | None,
    src_warehouse: str | None,
    dst_catalog: str | None,
    dst_catalog_name: str | None,
    dst_namespace: str,
    dst_table: str | None,
    dst_db_path: str | None,
    dst_warehouse: str | None,
    row_filter: str | None,
    source_snapshot: int | None,
    batch_rows: int,
    write_mode: str,
    upsert_keys: str | None,
    checkpoint: str | None,
    column_mapping: tuple[str, ...],
    on_missing_in_source: str,
    on_missing_in_dest: str,
    dry_run: bool,
    verify: str | None,
    sample_size: int,
    quiet: bool,
) -> None:
    """Cross-table data migration: stream rows between Iceberg tables.

    Reads from --src-catalog/--src-namespace.--src-table and writes
    to --dst-catalog/--dst-namespace.--dst-table. Each commit holds at
    most --batch-rows source rows and is one catalog commit, but can
    create several destination snapshots: one for append, two for
    overwrite_partition, and up to three for upsert (three per
    1,000-key sub-batch when a composite-key batch is split; an upsert
    may also add one empty schema snapshot first). To undo a run, roll
    the destination back to the "Destination snapshot before" ID the
    summary prints. The destination table must already exist and must
    not be the source table.

    The destination catalog defaults to the source: useful for
    cross-namespace migration in one catalog. Pass --dst-catalog or
    --dst-catalog-name to cross catalogs (e.g. sqlite source, REST
    destination via --dst-catalog-name).

    Schema reconciliation is opt-in: by default the source and dest
    must have matching schemas. Use --column-mapping,
    --on-missing-in-source, and --on-missing-in-dest to enable
    renames, null-fill, or auto-add.

    Pass --checkpoint PATH to make the migration resumable: a
    crashed run picks up at the last committed batch on the next
    invocation.

    Pass --dry-run to estimate row + byte counts without writing.

    Pass --verify METHOD to run a post-migration consistency check
    (row_count, partition_hash, or sample).
    """
    _ensure_utf8_stdio()

    if src_catalog is None and src_catalog_name is None:
        raise click.UsageError("Provide --src-catalog or --src-catalog-name.")

    # Defaults: the destination catalog falls back to the source one.
    # Chosen so the common case (cross-namespace within one catalog)
    # needs minimal flag baggage. Local paths only carry over when both
    # ends are the same catalog.
    if dst_catalog is None and dst_catalog_name is None:
        eff_dst_catalog, eff_dst_name = src_catalog, src_catalog_name
    else:
        eff_dst_catalog, eff_dst_name = dst_catalog, dst_catalog_name
    same_catalog = (eff_dst_catalog, eff_dst_name) == (src_catalog, src_catalog_name)
    eff_dst_table = dst_table or src_table
    eff_dst_db_path = dst_db_path if dst_db_path is not None or not same_catalog else src_db_path
    eff_dst_warehouse = (
        dst_warehouse if dst_warehouse is not None or not same_catalog else src_warehouse
    )

    if write_mode == "upsert" and not upsert_keys:
        raise click.UsageError("--write-mode=upsert requires --upsert-keys (comma-separated).")
    upsert_keys_list: list[str] | None = None
    if upsert_keys is not None:
        upsert_keys_list = [k.strip() for k in upsert_keys.split(",") if k.strip()]
        if not upsert_keys_list:
            raise click.UsageError("--upsert-keys is empty.")

    # Validate flag-syntax errors before touching the catalog so
    # users see a clean "your CLI invocation is wrong" message
    # instead of a confusing NoSuchTableError that hides the typo.
    column_mapping_dict = _parse_column_mapping(column_mapping) or None

    # Lazy imports keep `iceberg-ops --help` snappy and avoid pulling
    # PyArrow into commands that don't need it.
    from iceberg_ops.data_migration import (
        CheckpointStore,
        DataMigrator,
        SchemaReconciler,
    )

    # Build / load both ends.
    src = _load_table(
        kind=src_catalog,
        namespace=src_namespace,
        table_name=src_table,
        db_path=src_db_path,
        warehouse=src_warehouse,
        catalog_name=src_catalog_name,
        role="source",
    )
    dst = _load_table(
        kind=eff_dst_catalog,
        namespace=dst_namespace,
        table_name=eff_dst_table,
        db_path=eff_dst_db_path,
        warehouse=eff_dst_warehouse,
        catalog_name=eff_dst_name,
        role="destination",
    )
    # The destination catalog and table default to the source ones, so a
    # forgotten --dst-table would otherwise copy the table onto itself.
    if str(src.metadata.table_uuid) == str(dst.metadata.table_uuid):
        raise click.UsageError(
            "Source and destination are the same table; pass --dst-table or --dst-namespace."
        )

    # Reconciler - strict by default; opt-in to lax behaviour via flags.
    reconciler = SchemaReconciler(
        column_mapping=column_mapping_dict,
        on_missing_in_source=on_missing_in_source,  # type: ignore[arg-type]
        on_missing_in_dest=on_missing_in_dest,  # type: ignore[arg-type]
    )

    # Checkpoint - only when explicitly requested.
    checkpoint_store: CheckpointStore | None = None
    if checkpoint:
        checkpoint_store = CheckpointStore(checkpoint)

    def _progress(rows_done: int, _total: int | None) -> None:
        if not quiet and not dry_run:
            click.echo(f"  ... {rows_done} rows committed")

    src_id = f"{_catalog_label(src_catalog, src_catalog_name)}/{src_namespace}.{src_table}"
    dst_id = f"{_catalog_label(eff_dst_catalog, eff_dst_name)}/{dst_namespace}.{eff_dst_table}"
    # ImportError: the [transforms] preflight for a destination with
    # non-identity partition transforms. CheckpointError is a RuntimeError.
    expected_errors = (RuntimeError, ValueError, KeyError, TypeError, ImportError)

    try:
        migrator = DataMigrator(
            source=src,
            dest=dst,
            schema_reconciler=reconciler,
            row_filter=row_filter,
            source_snapshot=source_snapshot,
            batch_rows=batch_rows,
            write_mode=write_mode,  # type: ignore[arg-type]
            upsert_keys=upsert_keys_list,
            checkpoint=checkpoint_store,
            progress=_progress,
            dry_run=dry_run,
        )
        if dry_run:
            click.echo(f"DRY RUN -- {src_id}  ->  {dst_id}")
        else:
            click.echo(f"Migrating {src_id}  ->  {dst_id}")
        result = migrator.run()
    except expected_errors as exc:
        _exit_with_error(exc)

    # Summary.
    if dry_run:
        click.echo(
            f"Dry-run complete: estimated {result.estimated_rows} rows / "
            f"{result.estimated_bytes} bytes ({result.duration_seconds:.2f}s)"
        )
    else:
        click.echo(
            f"Done: {result.rows_moved} rows / {result.bytes_moved} bytes / "
            f"{len(result.new_snapshots)} snapshot(s) ({result.duration_seconds:.2f}s)"
        )
        if result.dest_snapshot_before is not None:
            click.echo(
                f"Destination snapshot before: {result.dest_snapshot_before} "
                "(undo with iceberg_ops.core.utils.rollback_to_snapshot)"
            )

    if result.schema_changes:
        click.echo(f"Schema changes applied: {', '.join(result.schema_changes)}")
    if result.plan and result.plan.warnings:
        click.echo("Warnings:")
        for warning in result.plan.warnings:
            click.echo(f"  - {warning}")
    if result.checkpoint_path:
        click.echo(f"Checkpoint: {result.checkpoint_path}")

    # Verification.
    if verify and not dry_run:
        click.echo(f"Verifying ({verify}) ...")
        try:
            verification = migrator.verify(method=verify, sample_size=sample_size)  # type: ignore[arg-type]
        except expected_errors as exc:
            _exit_with_error(exc, prefix="Verify error")
        if verification.matches:
            click.echo(f"  [ok] {verification.method} matches")
        else:
            click.echo(f"  [x] {verification.method} MISMATCH")
            for detail in verification.details:
                click.echo(f"    - {detail}")
            sys.exit(2)


# ===========================================================================
# run-policies
# ===========================================================================

_POLARIS_DOCS_URL = "https://namitkewat.github.io/iceberg-ops/polaris.html"
_EXPERIMENTAL_POLICIES = (
    "run-policies is experimental: it has not been verified against a live Apache "
    "Polaris server and does not yet match the released Polaris policy API; see "
    f"{_POLARIS_DOCS_URL}"
)
_ARGV_SECRET_HELP = "Prefer the env var: argv is visible in process listings and CI logs."


@main.command(
    "run-policies",
    short_help="[experimental] Fetch Polaris policies for a table and dispatch each one.",
)
@click.option(
    "--polaris-url",
    required=True,
    help="Polaris base URL, e.g. https://polaris.example.com.",
)
@click.option(
    "--polaris-catalog",
    required=True,
    help="Polaris catalog name.",
)
@click.option(
    "--polaris-token",
    type=str,
    default=None,
    envvar="ICEBERG_OPS_POLARIS_TOKEN",
    show_envvar=True,
    help=(
        "Pre-fetched bearer token. Mutually exclusive with --polaris-client-id. "
        f"{_ARGV_SECRET_HELP}"
    ),
)
@click.option(
    "--polaris-client-id",
    type=str,
    default=None,
    envvar="ICEBERG_OPS_POLARIS_CLIENT_ID",
    show_envvar=True,
    help="OAuth2 client ID. Pair with --polaris-client-secret.",
)
@click.option(
    "--polaris-client-secret",
    type=str,
    default=None,
    envvar="ICEBERG_OPS_POLARIS_CLIENT_SECRET",
    show_envvar=True,
    help=f"OAuth2 client secret. Pair with --polaris-client-id. {_ARGV_SECRET_HELP}",
)
@click.option(
    "--polaris-scope",
    type=str,
    default=None,
    help="OAuth2 scope (default: PRINCIPAL_ROLE:ALL).",
)
@click.option(
    "--target-namespace",
    required=True,
    help="Target namespace (passed to Polaris and used for the local table load).",
)
@click.option(
    "--target-table",
    required=True,
    help="Target table name (without namespace prefix).",
)
@click.option(
    "--catalog",
    "local_catalog",
    type=_KIND_CHOICE,
    default=None,
    help="Catalog kind for the table the policies act on. Give this or --catalog-name.",
)
@click.option(
    "--catalog-name",
    "local_catalog_name",
    type=str,
    default=None,
    help="Catalog for the table, defined in .pyiceberg.yaml or PYICEBERG_CATALOG__<NAME>__*.",
)
@click.option(
    "--db-path",
    type=str,
    default=None,
    help="SQLite DB path for the catalog (sqlite only).",
)
@click.option(
    "--warehouse",
    type=str,
    default=None,
    help="Warehouse directory (sqlite) or warehouse location/URI (remote catalogs).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Forward dry_run=True to each maintenance op (no mutations).",
)
def run_policies(
    polaris_url: str,
    polaris_catalog: str,
    polaris_token: str | None,
    polaris_client_id: str | None,
    polaris_client_secret: str | None,
    polaris_scope: str | None,
    target_namespace: str,
    target_table: str,
    local_catalog: str | None,
    local_catalog_name: str | None,
    db_path: str | None,
    warehouse: str | None,
    dry_run: bool,
) -> None:
    """[experimental] Fetch Polaris policies for a table and dispatch each one.

    Experimental: the policy requests do not yet match the released
    Polaris policy API, so released Polaris servers are not supported.
    See https://namitkewat.github.io/iceberg-ops/polaris.html.

    Fetches the table's policies from Polaris for
    ``--target-namespace.--target-table``, then loads the same table
    from --catalog / --catalog-name and dispatches each policy. Only
    system.snapshot-expiry is executed (it must have "enable": true);
    every other policy type is reported as skipped.

    Auth is either ``--polaris-token`` (pre-fetched bearer) or the
    OAuth2 client-credentials pair (``--polaris-client-id`` plus
    ``--polaris-client-secret``). Exactly one of the two must be
    set. Prefer the ICEBERG_OPS_POLARIS_* environment variables over
    flags for secrets.

    Use ``--dry-run`` to forward ``dry_run=True`` to each
    maintenance op so the table is not mutated.
    """
    _ensure_utf8_stdio()
    click.echo(f"Warning: {_EXPERIMENTAL_POLICIES}", err=True)

    # Auth flag validation - mirror PolarisClient's exactly-one rule
    # at the CLI level so users see a clean UsageError rather than a
    # mid-construction ValueError.
    has_token = polaris_token is not None
    has_creds = polaris_client_id is not None and polaris_client_secret is not None
    partial_creds = (polaris_client_id is None) != (polaris_client_secret is None)
    if partial_creds:
        raise click.UsageError(
            "--polaris-client-id and --polaris-client-secret must be set together."
        )
    if has_token == has_creds:
        raise click.UsageError(
            "Provide exactly one of --polaris-token or "
            "(--polaris-client-id + --polaris-client-secret)."
        )
    if local_catalog is None and local_catalog_name is None:
        raise click.UsageError(
            "Provide --catalog or --catalog-name for the table the policies act on."
        )

    url = urlsplit(polaris_url)
    if url.scheme.lower() == "http" and (url.hostname or "") not in _LOOPBACK_HOSTS:
        click.echo(
            "Warning: --polaris-url uses plain http; credentials are sent unencrypted.",
            err=True,
        )

    from iceberg_ops.polaris import (
        PolarisClient,
        PolarisError,
        dispatch_policies,
        fetch_table_policies,
    )

    client_kwargs: dict[str, str | tuple[str, str]] = {}
    if has_token:
        assert polaris_token is not None
        client_kwargs["token"] = polaris_token
    else:
        assert polaris_client_id is not None and polaris_client_secret is not None
        client_kwargs["oauth2_credentials"] = (polaris_client_id, polaris_client_secret)
    if polaris_scope:
        client_kwargs["oauth2_scope"] = polaris_scope

    try:
        polaris_client = PolarisClient(
            polaris_url,
            polaris_catalog,
            **client_kwargs,  # type: ignore[arg-type]
        )
    except ValueError as exc:
        # The client's messages never contain the token or secret.
        raise click.UsageError(str(exc)) from exc

    try:
        policies = fetch_table_policies(polaris_client, target_namespace, target_table)
    except PolarisError as exc:
        click.echo(f"Polaris error: {exc}", err=True)
        sys.exit(1)

    if not policies:
        click.echo(f"No applicable policies for {target_namespace}.{target_table}; nothing to do.")
        return

    table = _load_table(
        kind=local_catalog,
        namespace=target_namespace,
        table_name=target_table,
        db_path=db_path,
        warehouse=warehouse,
        catalog_name=local_catalog_name,
    )

    click.echo(
        f"Dispatching {len(policies)} policy/policies for "
        f"{target_namespace}.{target_table} (dry_run={dry_run}):"
    )
    results = dispatch_policies(table, policies, dry_run=dry_run)

    failures = 0
    for r in results:
        if r.success:
            click.echo(f"  [ok] {r.policy_name} ({r.policy_type}) -> {r.op}")
        elif r.skipped:
            click.echo(f"  [skip] {r.policy_name} ({r.policy_type}): {r.skip_reason}")
        else:
            failures += 1
            click.echo(f"  [x] {r.policy_name} ({r.policy_type}): {r.error}", err=True)

    if failures:
        sys.exit(2)


if __name__ == "__main__":
    main()
