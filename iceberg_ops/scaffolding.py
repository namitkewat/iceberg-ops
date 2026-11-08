"""
iceberg_ops.scaffolding
=======================
Project scaffolding helpers used by the ``iceberg-ops init`` and
``iceberg-ops revision`` CLI commands.

Public API
----------
``init_project(target_dir, *, force=False, config_name=..., migrations_dir=None)``
    Create the ``migrations/`` directory, an ``iceberg-ops.yaml`` stub, and
    append ``.gitignore`` entries for local state.

``create_revision(*, message, migrations_dir, namespace=None)``
    Scaffold a new ``NNNN_<slug>.py`` migration file with the next
    sequential version number.
"""

from __future__ import annotations

import logging
import os
import re
import string
from pathlib import Path

logger = logging.getLogger(__name__)


CONFIG_TEMPLATE = """\
# iceberg-ops configuration
# CLI flags override these values. Relative paths in this file resolve
# against the directory that contains it.

catalog: sqlite          # one of: sqlite, glue, rest, hive, s3tables
namespace: default       # the namespace this project's migrations target

# Local SQLite catalog (catalog: sqlite). Defaults:
# db_path: ./local_data/iceberg_catalog.db   # default; relative to this file
# warehouse: ./local_data/iceberg_warehouse

# Named catalog instead of the local one: delete the 'catalog: sqlite'
# line above and set
# catalog_name: prod   # a catalog from .pyiceberg.yaml or PYICEBERG_CATALOG__PROD__* env vars

# Migration files location (default: ./migrations next to this file):
# migrations_dir: ./migrations
"""


GITIGNORE_MARKER = "# iceberg-ops local data"


GITIGNORE_BLOCK = f"""\
{GITIGNORE_MARKER}
local_data/
"""


# Same rule as the runner's discovery (iceberg_ops.runner): a file the
# runner would not load must not take a version number here either.
_VERSION_RE = re.compile(r"^(\d{4})_.+\.py$")


_REVISION_TEMPLATE = string.Template(
    '''\
"""Migration $version - $doc_description."""

from __future__ import annotations

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = $version_literal
    description = $description_literal

    def upgrade(self, ctx: MigrationContext) -> None:
        # Apply your schema changes here. Examples:
        #
        #   import pyarrow as pa
        #   from pyiceberg import types as ice_types
        #
        #   ctx.create_table_if_not_exists(
        #       ctx.qualified("orders"),
        #       pa.schema([pa.field("id", pa.int64())]),
        #   )
        #
        #   ctx.enforce_column("orders", "status", ice_types.StringType())
        raise NotImplementedError("Edit this method to implement the migration.")

    def downgrade(self, ctx: MigrationContext) -> None:
        # Reverse the upgrade. Raise NotImplementedError if a full undo
        # is impossible (e.g. a column drop requires a rewrite).
        raise NotImplementedError("Edit this method to implement the rollback.")
'''
)


def _docstring_text(text: str) -> str:
    """Make *text* safe to place inside a generated triple-quoted docstring.

    Non-printable characters (newlines, tabs, NUL, ...) become spaces,
    whitespace runs collapse to one space, backslashes are doubled and
    triple quotes are escaped, so the docstring always parses and reads
    back as the original one-line text.
    """
    one_line = " ".join("".join(ch if ch.isprintable() else " " for ch in text).split())
    return one_line.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def _slugify(message: str) -> str:
    """Lowercase, collapse non-alnum runs to ``_``, strip ends, cap at 50."""
    slug = re.sub(r"[^a-z0-9]+", "_", message.lower())
    return slug.strip("_")[:50]


def _next_version(directory: Path) -> str:
    """Return the next zero-padded 4-digit version (e.g. ``"0007"``)."""
    versions: list[int] = []
    for entry in directory.iterdir():
        if entry.is_file():
            match = _VERSION_RE.match(entry.name)
            if match:
                versions.append(int(match.group(1)))
    next_n = (max(versions) + 1) if versions else 1
    return f"{next_n:04d}"


def init_project(
    target_dir: str | Path,
    *,
    force: bool = False,
    config_name: str | Path = "iceberg-ops.yaml",
    migrations_dir: str | Path | None = None,
) -> None:
    """
    Scaffold a new iceberg-ops project.

    Creates the migrations directory (with an empty ``__init__.py``),
    a stub config file, and appends ``.gitignore`` entries for local
    runtime state to ``<target_dir>/.gitignore``. Idempotent: running
    twice without ``force`` is a safe no-op on existing files; the
    migrations directory is always ensured. Files are written as UTF-8.

    Parameters
    ----------
    target_dir : str or pathlib.Path
        Project root. Must already exist.
    force : bool, optional
        If True, overwrite an existing config file and
        ``migrations/__init__.py``. Never deletes user-authored
        migration files. Default: False.
    config_name : str or pathlib.Path, optional
        Config file path, relative to *target_dir* (or absolute).
        Missing parent directories are created. Default:
        ``"iceberg-ops.yaml"``.
    migrations_dir : str or pathlib.Path, optional
        Migrations directory, relative to *target_dir* (or absolute).
        Default: ``migrations/`` next to the config file. Any other
        location is written into the generated config as
        ``migrations_dir`` (relative to the config file).

    Raises
    ------
    FileNotFoundError
        If ``target_dir`` does not exist.

    Notes
    -----
    The ``.gitignore`` block is keyed off the marker comment
    ``# iceberg-ops local data`` so re-running ``init`` does not
    duplicate the entries even with ``force=True``. The block is
    appended as bytes, so existing ``.gitignore`` content is kept
    byte-for-byte whatever its encoding.
    """
    root = Path(target_dir).resolve()
    if not root.exists():
        raise FileNotFoundError(f"target_dir does not exist: {root}")

    config_path = root / config_name
    default_migrations = config_path.parent / "migrations"
    migrations = root / migrations_dir if migrations_dir is not None else default_migrations
    migrations.mkdir(parents=True, exist_ok=True)
    logger.info("Ensured migrations dir at %s", migrations)

    init_py = migrations / "__init__.py"
    if not init_py.exists() or force:
        init_py.write_text("", encoding="utf-8")
        logger.info("Wrote %s", init_py)

    if not config_path.exists() or force:
        config_text = CONFIG_TEMPLATE
        if migrations.resolve() != default_migrations.resolve():
            try:
                rel = Path(os.path.relpath(migrations, config_path.parent)).as_posix()
            except ValueError:  # different drive on Windows
                rel = migrations.as_posix()
            config_text += f"migrations_dir: {rel}\n"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(config_text, encoding="utf-8")
        logger.info("Wrote %s", config_path)
    else:
        logger.info(
            "%s already exists; left unchanged (pass force=True to overwrite)",
            config_path,
        )

    gitignore_path = root / ".gitignore"
    existing = gitignore_path.read_bytes() if gitignore_path.exists() else b""
    if GITIGNORE_MARKER.encode("utf-8") not in existing:
        newline = b"\r\n" if b"\r\n" in existing else b"\n"
        prefix = b""
        if existing:
            prefix = (b"" if existing.endswith(b"\n") else newline) + newline
        block = GITIGNORE_BLOCK.encode("utf-8").replace(b"\n", newline)
        with open(gitignore_path, "ab") as fh:
            fh.write(prefix + block)
        logger.info("Updated %s with iceberg-ops block", gitignore_path)
    else:
        logger.info(
            "%s already contains the iceberg-ops block; left unchanged",
            gitignore_path,
        )


def create_revision(
    *,
    message: str,
    migrations_dir: str | Path,
    namespace: str | None = None,
) -> str:
    """
    Scaffold a new migration file with the next sequential version.

    Scans *migrations_dir* (or the namespace subdirectory if it exists)
    for files the runner discovers (``NNNN_<slug>.py`` with a non-empty
    slug), picks ``max(NNNN) + 1`` (or ``"0001"`` if empty), slugifies
    *message*, and writes a
    :class:`~iceberg_ops.migration.BaseMigration` skeleton.

    Parameters
    ----------
    message : str
        Short human-readable description. Used as the ``description``
        class attribute and as the filename slug (lowercased,
        non-alphanumeric runs collapsed to ``_``, leading / trailing
        ``_`` stripped, capped at 50 characters).
    migrations_dir : str or pathlib.Path
        Directory where migration files live. Must already exist.
    namespace : str, optional
        If given and ``<migrations_dir>/<namespace>/`` exists, the new
        file is written to that subdirectory and the version counter
        is scoped to it. Otherwise the flat *migrations_dir* is used.

    Returns
    -------
    str
        Absolute path of the newly created file.

    Raises
    ------
    FileNotFoundError
        If *migrations_dir* does not exist.
    FileExistsError
        If the target file already exists (e.g. another process wrote
        the same version concurrently); it is never overwritten.

    Notes
    -----
    The version counter is per-directory: a flat ``migrations/`` and a
    ``migrations/<namespace>/`` subdirectory each have their own
    ``0001...`` series. This matches the runner's namespace-scoped
    discovery rule.

    A degenerate *message* whose slug collapses to the empty string
    (e.g. ``"!!!"``) falls back to the literal slug ``"migration"``.
    """
    base = Path(migrations_dir).resolve()
    if not base.is_dir():
        raise FileNotFoundError(f"migrations_dir does not exist: {base}")

    target = base
    if namespace:
        ns_dir = base / namespace
        if ns_dir.is_dir():
            target = ns_dir

    next_version = _next_version(target)
    slug = _slugify(message) or "migration"

    out_path = target / f"{next_version}_{slug}.py"
    # Mode "x": never overwrite a migration that appeared since the scan.
    with open(out_path, "x", encoding="utf-8") as fh:
        fh.write(
            _REVISION_TEMPLATE.substitute(
                version=next_version,
                version_literal=repr(next_version),
                description_literal=repr(message),
                doc_description=_docstring_text(message),
            )
        )
    logger.info("Created migration: %s", out_path)
    return str(out_path)
