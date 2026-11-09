"""
iceberg_ops.runner
==================
``MigrationRunner`` — discovers, applies, and rolls back Iceberg
migrations for a specific **namespace** inside a catalog.

The runner manages one namespace at a time. To manage multiple
namespaces (e.g. ``"sales"`` and ``"finance"``), create one runner per
namespace — they share the same catalog and the same history table but
track state independently via the ``namespace`` column in
``_iceberg_ops.migration_history``.

Migration directory layout
--------------------------
The runner supports two directory layouts:

1. **Flat** (default, backwards compatible)::

       migrations/
         0001_create_orders.py
         0002_add_sku.py

2. **Namespace-scoped** (preferred for multi-namespace projects)::

       migrations/
         sales/
           0001_create_orders.py
         finance/
           0001_create_invoices.py

   When the runner's namespace matches a subdirectory name under
   ``migrations_dir``, that subdirectory is used automatically. If no
   matching subdirectory exists, the flat layout is used as a fallback.

Concurrent runs
---------------
Concurrent runs are not guarded. Two ``migrate`` runs for one namespace
can both execute ``upgrade()``; the second run to record history raises
:class:`~iceberg_ops.history.ConcurrentMigrationError` and history keeps
one row per ``(version, namespace)``; its ``upgrade()`` may already have
run, so ``upgrade()`` must be idempotent. Run migrations for a namespace
from one job at a time (CI concurrency group) or use an external lock.
Runners for different namespaces may share the history table; a
history write that loses a race against other namespaces' writes is
retried, up to the history table's ``commit.retry.num-retries``.

Versions
--------
Migration files are named ``NNNN_<description>.py`` and the
``Migration.version`` attribute must equal the ``NNNN`` prefix. A
``target`` argument accepts 1-4 digits (``"2"``, ``"0002"``) or an
``int`` and is normalised with :func:`normalize_version`, so ``"2"``
and ``"10"`` order numerically.

Usage
-----
::

    from iceberg_ops.core.catalog import CatalogFactory
    from iceberg_ops.runner       import MigrationRunner

    catalog = CatalogFactory.build("sqlite")

    runner = MigrationRunner(
        catalog,
        namespace="sales",                     # required: target namespace
        meta_namespace="_iceberg_ops",         # optional: where history lives
        migrations_dir="./migrations",         # optional: migration files
    )

    runner.status()                            # show pending / applied state
    runner.migrate()                           # apply all pending migrations
    runner.migrate(target="0002")              # apply up to version 0002
                                               # ("2" and 2 work too)
    runner.rollback(steps=1)                   # undo the last migration
    runner.rollback(target="0001")             # roll back until 0001 is latest
    runner.stamp()                             # mark migrations applied
                                               # without running upgrade(),
                                               # used to adopt iceberg-ops on
                                               # an existing schema.
    runner.history()                           # print each version's latest
                                               # recorded status
"""

from __future__ import annotations

import hashlib
import importlib.util
import logging
import os
import re
import sys
from collections.abc import Iterable
from typing import Any

from pyiceberg.exceptions import CommitFailedException, ValidationException

from iceberg_ops.history import (
    DEFAULT_META_NAMESPACE,
    ConcurrentMigrationError,
    MigrationHistory,
)
from iceberg_ops.migration import BaseMigration, MigrationContext

logger = logging.getLogger(__name__)

_MIGRATION_FILENAME_RE = re.compile(r"^(\d{4})_.+\.py$")
_VERSION_RE = re.compile(r"[0-9]{1,4}")

DEFAULT_MIGRATIONS_DIR = "./migrations"

# Base version below every migration; ``rollback(target="0000")`` undoes all.
_BASE_VERSION = "0000"


def normalize_version(value: str | int) -> str:
    """
    Normalise a user-supplied migration version to its 4-digit form.

    Parameters
    ----------
    value : str or int
        A version such as ``"2"``, ``"0002"`` or ``2``. Surrounding
        whitespace is ignored.

    Returns
    -------
    str
        The zero-padded 4-digit version, e.g. ``"0002"``.

    Raises
    ------
    ValueError
        If *value* is not 1-4 decimal digits.

    Example
    -------
    >>> normalize_version("2")
    '0002'
    >>> normalize_version(10)
    '0010'
    """
    text = str(value).strip()
    if isinstance(value, bool) or not _VERSION_RE.fullmatch(text):
        raise ValueError(f"Invalid version '{value}': expected 1-4 digits, e.g. '0002'")
    return text.zfill(4)


def _resolve_target(
    target: str | int | None,
    known: Iterable[str],
    *,
    allow_base: bool = False,
) -> str | None:
    """
    Normalise *target* and check that it names a known version.

    Parameters
    ----------
    target : str, int or None
        User-supplied target version; ``None`` passes through.
    known : iterable of str
        Versions the target may name.
    allow_base : bool, optional
        Also accept ``"0000"`` (below every migration).

    Returns
    -------
    str or None
        The normalised target, or ``None`` when *target* is ``None``.

    Raises
    ------
    ValueError
        If *target* is malformed or not a known version.
    """
    if target is None:
        return None
    version = normalize_version(target)
    known_set = set(known)
    if version in known_set or (allow_base and version == _BASE_VERSION):
        return version
    raise ValueError(f"Unknown target version {version!r}; known: {sorted(known_set)}")


class MigrationRunner:
    """
    Orchestrates discovery, application, and rollback of Iceberg
    migrations for a single target namespace.

    Parameters
    ----------
    catalog : Catalog
        A live PyIceberg catalog (from
        :meth:`iceberg_ops.core.catalog.CatalogFactory.build` or BYOC).
    namespace : str
        **Required.** The namespace whose tables this runner manages
        (e.g. ``"sales"``). History is scoped to this namespace so
        runners for different namespaces can share the same catalog
        and history table. Runs for the *same* namespace are not
        guarded against each other: run one job per namespace at a
        time.
    meta_namespace : str
        Namespace used internally to store migration history.
        Default: ``"_iceberg_ops"``. Should not contain user tables.
    migrations_dir : str
        Directory containing ``NNNN_<description>.py`` migration files.
        Default: ``"./migrations"`` (relative to the current working
        directory).
    """

    def __init__(
        self,
        catalog: Any,
        namespace: str,
        meta_namespace: str = DEFAULT_META_NAMESPACE,
        migrations_dir: str = DEFAULT_MIGRATIONS_DIR,
    ) -> None:
        self._catalog = catalog
        self._namespace = namespace
        self._mig_dir = migrations_dir
        self._history = MigrationHistory(catalog, meta_namespace=meta_namespace)
        logger.debug(
            "MigrationRunner ready. namespace=%s meta=%s dir=%s",
            namespace,
            meta_namespace,
            migrations_dir,
        )

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _discover(self) -> list[BaseMigration]:
        """
        Load migration objects from *migrations_dir*, sorted by version.

        Raises
        ------
        FileNotFoundError
            If *migrations_dir* does not exist. An existing but empty
            directory yields an empty list.
        RuntimeError
            If a migration file fails to import or instantiate, its
            ``version`` does not match the filename prefix, or two files
            declare the same version.
        """
        if not os.path.isdir(self._mig_dir):
            raise FileNotFoundError(
                f"Migrations directory not found: {os.path.abspath(self._mig_dir)} "
                "(run `iceberg-ops init` or pass --migrations-dir)"
            )

        ns_dir = os.path.join(self._mig_dir, self._namespace)
        search_dir = ns_dir if os.path.isdir(ns_dir) else self._mig_dir
        logger.debug("Migration discovery directory: %s", search_dir)

        migrations: list[BaseMigration] = []
        seen: dict[str, str] = {}
        for filename in sorted(os.listdir(search_dir)):
            if not _MIGRATION_FILENAME_RE.match(filename):
                continue
            filepath = os.path.join(search_dir, filename)
            mig = self._load_migration(filepath)
            if mig is None:
                continue
            prefix = filename[:4]
            if mig.version != prefix:
                raise RuntimeError(
                    f"{filepath}: Migration.version {mig.version!r} does not match "
                    f"filename prefix {prefix!r}"
                )
            if mig.version in seen:
                raise RuntimeError(
                    f"Duplicate migration version {mig.version!r}: "
                    f"{seen[mig.version]} and {filepath}"
                )
            seen[mig.version] = filepath
            migrations.append(mig)

        return sorted(migrations, key=lambda m: m.version)

    @staticmethod
    def _load_migration(filepath: str) -> BaseMigration | None:
        """
        Import *filepath* and instantiate its ``Migration`` class.

        The module is registered in ``sys.modules`` under a name unique
        to its absolute path (so dataclasses and other import-time
        lookups work, and same-named files in different namespace
        directories do not collide).

        Returns
        -------
        BaseMigration or None
            The migration, or ``None`` when the file defines no
            ``Migration`` class.

        Raises
        ------
        RuntimeError
            If importing the file or instantiating ``Migration`` fails.
        """
        stem = re.sub(r"\W", "_", os.path.basename(filepath)[:-3])
        digest = hashlib.sha1(os.path.abspath(filepath).encode(), usedforsecurity=False)
        module_name = f"_iceberg_ops_migration_{digest.hexdigest()[:10]}_{stem}"
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        if spec is None or spec.loader is None:
            logger.warning("Could not load migration: %s", filepath)
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            sys.modules.pop(module_name, None)
            raise RuntimeError(
                f"Failed to load migration {filepath}: {type(exc).__name__}: {exc}"
            ) from exc

        if not hasattr(module, "Migration"):
            logger.warning("'%s' has no Migration class - skipping.", filepath)
            return None
        try:
            mig: BaseMigration = module.Migration()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to load migration {filepath}: {type(exc).__name__}: {exc}"
            ) from exc
        return mig

    def _ctx(self, mig: BaseMigration) -> MigrationContext:
        return MigrationContext(
            catalog=self._catalog,
            namespace=self._namespace,
            version=mig.version,
            description=mig.description,
        )

    def _check_no_concurrent_migration(self) -> set[str]:
        """
        Read the applied versions that a run starts from.

        This is a baseline read, not a lock: it does not stop another
        runner from starting at the same time.
        """
        return self._history.get_applied_versions(self._namespace)

    def _verify_no_concurrent_modification(
        self,
        baseline: set[str],
        applied_by_us: set[str],
    ) -> None:
        """
        Re-read applied versions; raise if foreign versions appear.

        Only versions this run did not apply are detected here. A
        concurrent write to a version this run also applies is caught by
        :class:`~iceberg_ops.history.MigrationHistory` when it records
        history.
        """
        current = self._history.get_applied_versions(self._namespace)
        expected = baseline | applied_by_us
        unexpected = current - expected
        if unexpected:
            raise ConcurrentMigrationError(
                f"Concurrent modification detected in namespace "
                f"'{self._namespace}'. Unexpected versions appeared in "
                f"history: {sorted(unexpected)}. Another process may have "
                f"run migrations simultaneously."
            )

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def status(self) -> None:
        """
        Print the status of every discovered migration for this
        namespace.

        Legend: ``[applied]`` ``[pending]`` ``[rolled_back]``
        ``[FAILED]``. History rows whose migration file is no longer
        discovered are listed too, marked ``(migration file missing)``.
        Does not write to the catalog (the history table is not
        created), but imports every migration file, which runs its
        module-level code.

        Raises
        ------
        FileNotFoundError
            If the migrations directory does not exist.
        RuntimeError
            If a migration file cannot be loaded.
        """
        all_migs = self._discover()
        history_map = {r["version"]: r for r in self._history.get_all(self._namespace)}
        markers = {
            "applied": "[applied    ]",
            "pending": "[pending    ]",
            "rolled_back": "[rolled_back]",
            "failed": "[FAILED     ]",
        }

        print(f"\nMigration Status  (namespace: {self._namespace})")
        print("=" * 72)

        if not all_migs:
            print(f"  (no migration files found in {self._mig_dir})")

        for mig in all_migs:
            rec = history_map.get(mig.version)
            status = rec["status"] if rec else "pending"
            ts = f"  ({rec['applied_at']})" if rec else ""
            marker = markers.get(status, "[unknown    ]")
            print(f"  {marker} {mig.version} - {mig.description}{ts}")

        discovered = {m.version for m in all_migs}
        for version, rec in sorted(history_map.items()):
            if version in discovered:
                continue
            marker = markers.get(rec["status"], "[unknown    ]")
            print(
                f"  {marker} {version} - {rec['description']}  ({rec['applied_at']})"
                " (migration file missing)"
            )
        print()

    def history(self) -> None:
        """Print all history records for this namespace."""
        records = self._history.get_all(self._namespace)
        if not records:
            print(f"\n  (no migration history for namespace '{self._namespace}')\n")
            return

        # Size each column from its widest value so long versions and
        # full ISO-8601 timestamps never push the next column out of line.
        version_w = max(len("VERSION"), *(len(str(r["version"])) for r in records))
        status_w = max(len("STATUS"), *(len(str(r["status"])) for r in records))
        applied_w = max(len("APPLIED AT"), *(len(str(r["applied_at"])) for r in records))
        header = (
            f"  {'VERSION':<{version_w}} {'STATUS':<{status_w}} "
            f"{'APPLIED AT':<{applied_w}} DESCRIPTION"
        )

        print(f"\nMigration History  (namespace: {self._namespace})")
        print("=" * 72)
        print(header)
        print("  " + "-" * max(68, len(header) - 2))
        error_indent = " " * (2 + version_w + 1)
        for r in records:
            print(
                f"  {r['version']!s:<{version_w}} {r['status']!s:<{status_w}} "
                f"{r['applied_at']!s:<{applied_w}} {r['description']}"
            )
            err = r.get("error")
            if err:
                # One line, so a multi-line message cannot break the table.
                print(f"{error_indent}ERROR: {' '.join(str(err).split())[:80]}")
        print()

    def plan(self, target: str | int | None = None) -> None:
        """
        Preview pending migrations without applying them.

        No tables are created (not even the history table) and no
        history records are written.

        Parameters
        ----------
        target : str or int, optional
            Show only pending migrations whose version is at most this
            one (inclusive). Accepts 1-4 digits (``"2"``, ``"0002"``)
            or an ``int``; it must name a discovered migration.

        Raises
        ------
        ValueError
            If *target* is malformed or names no discovered migration.
        FileNotFoundError
            If the migrations directory does not exist.
        RuntimeError
            If a migration file cannot be loaded.
        """
        all_migs = self._discover()
        target_v = _resolve_target(target, (m.version for m in all_migs))
        applied = self._history.get_applied_versions(self._namespace)
        pending = [
            m
            for m in all_migs
            if m.version not in applied and (target_v is None or m.version <= target_v)
        ]

        print(f"\nMigration Plan  (namespace: {self._namespace})")
        print("=" * 72)

        if not pending:
            print(f"  All migrations are up to date for [{self._namespace}].")
            print()
            return

        for mig in pending:
            print(f"  [pending] {mig.version} - {mig.description}")

        print(f"\n  {len(pending)} migration(s) would be applied to [{self._namespace}].")
        print()

    def migrate(self, target: str | int | None = None) -> None:
        """
        Apply all pending migrations up to *target* (or all if target is
        None).

        Migrations are applied in ascending version order. Any failure
        is recorded as ``"failed"`` and the runner stops immediately.

        Concurrent runs are not guarded. Two ``migrate`` runs for one
        namespace can both execute ``upgrade()``; the second run to
        record history raises
        :class:`~iceberg_ops.history.ConcurrentMigrationError` and
        history keeps one row per ``(version, namespace)``; its
        ``upgrade()`` may already have run, so ``upgrade()`` must be
        idempotent. Run migrations for a namespace from one job at a
        time (CI concurrency group) or use an external lock.

        Parameters
        ----------
        target : str or int, optional
            Apply only up to (and including) this version. Accepts 1-4
            digits (``"2"``, ``"0002"``) or an ``int``; it must name a
            discovered migration.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If *target* is malformed or names no discovered migration.
            Nothing is applied.
        FileNotFoundError
            If the migrations directory does not exist.
        RuntimeError
            If a migration file cannot be loaded, or a migration fails
            (the failure is recorded as ``"failed"``; if that history
            write itself fails, it is logged and the migration's own
            error is still the one raised).
        ConcurrentMigrationError
            If a migration's own commit or its history write conflicted
            with a concurrent commit, nothing is recorded for that
            version. If another runner's versions appear in history
            after this run recorded a version, this run's version stays
            recorded as applied.

        Notes
        -----
        Once a version is recorded as applied, later errors (from the
        history re-read that looks for other runners, for example)
        propagate unchanged and never turn that row into ``"failed"``.
        A failure is recorded as ``"<ExceptionType>: <message>"``; the
        traceback is only logged locally, at ``DEBUG`` level.
        """
        all_migs = self._discover()
        target_v = _resolve_target(target, (m.version for m in all_migs))
        baseline = self._check_no_concurrent_migration()
        pending = [
            m
            for m in all_migs
            if m.version not in baseline and (target_v is None or m.version <= target_v)
        ]

        if not pending:
            print(f"  [{self._namespace}] All migrations are up to date.")
            return

        applied_by_us: set[str] = set()
        for mig in pending:
            ctx = self._ctx(mig)
            logger.info(
                "Applying %s: %s  [ns=%s]",
                mig.version,
                mig.description,
                self._namespace,
            )
            print(f"  Applying {mig.version} - {mig.description} ...")
            try:
                mig.pre_upgrade(ctx)
                mig.upgrade(ctx)
                self._history.record_applied(mig.version, self._namespace, mig.description)
            except ConcurrentMigrationError:
                raise
            except Exception as exc:
                if isinstance(exc, (CommitFailedException, ValidationException)):
                    # Do not record "failed": the other runner's row for
                    # this version must not be clobbered.
                    raise ConcurrentMigrationError(
                        f"Migration {mig.version} hit a concurrent commit in namespace "
                        f"{self._namespace!r}; upgrade() may have partially run: {exc}"
                    ) from exc
                # Everyone with catalog access can read the history table:
                # store the message only and keep the traceback local.
                try:
                    self._history.record_failed(
                        mig.version,
                        self._namespace,
                        mig.description,
                        f"{type(exc).__name__}: {exc}",
                    )
                except ConcurrentMigrationError:
                    raise
                except Exception as rec_exc:
                    # Keep the migration's own error as the one raised:
                    # a failed history write must not hide it.
                    logger.error(
                        "Could not record the failure of %s in history: %s",
                        mig.version,
                        rec_exc,
                    )
                logger.error("Migration %s FAILED: %s", mig.version, exc)
                logger.debug("Traceback of failed migration %s", mig.version, exc_info=True)
                raise RuntimeError(f"Migration {mig.version} failed: {exc}") from exc

            # Recorded as applied: nothing below may record "failed".
            applied_by_us.add(mig.version)
            self._verify_no_concurrent_modification(baseline, applied_by_us)
            try:
                mig.post_upgrade(ctx)
            except Exception as post_err:
                logger.warning(
                    "Post-upgrade hook failed for %s: %s",
                    mig.version,
                    post_err,
                )
            print(f"  [ok] {mig.version} applied.")

        print(f"\n  Done. Applied {len(pending)} migration(s) to [{self._namespace}].")

    def rollback(
        self,
        steps: int = 1,
        target: str | int | None = None,
    ) -> None:
        """
        Roll back applied migrations (highest version first).

        Like :meth:`migrate`, concurrent runs for one namespace are not
        guarded: run one job per namespace at a time.

        Parameters
        ----------
        steps : int
            Number of migrations to undo; must be at least 1. Ignored
            when *target* is set.
        target : str or int, optional
            Roll back until the last applied version is *target* (i.e.
            undo all versions strictly greater than *target*). Accepts
            1-4 digits or an ``int``; ``"0000"`` undoes every applied
            migration. Must name a discovered or applied version.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If *steps* is below 1 (without *target*), or *target* is
            malformed or unknown. Nothing is rolled back.
        FileNotFoundError
            If the migrations directory does not exist.
        RuntimeError
            If a migration file to undo is missing (checked before any
            ``downgrade()`` runs, so nothing is rolled back), a file
            cannot be loaded, or a ``downgrade()`` fails.
        NotImplementedError
            If a migration's ``downgrade()`` is not implemented.
        ConcurrentMigrationError
            If another runner recorded history for this namespace
            during the run, or a migration's own commit conflicted with
            a concurrent commit.

        Notes
        -----
        Once a version is recorded as rolled back, later errors (from
        the history re-read that looks for other runners, for example)
        propagate unchanged rather than as a ``Rollback of ... failed``
        error.
        """
        if target is None and steps < 1:
            raise ValueError(f"steps must be >= 1, got {steps}")
        all_migs = {m.version: m for m in self._discover()}
        baseline = self._check_no_concurrent_migration()
        target_v = _resolve_target(target, set(all_migs) | baseline, allow_base=True)
        applied_vers = sorted(baseline, reverse=True)

        if not applied_vers:
            print(f"  [{self._namespace}] Nothing to roll back.")
            return

        to_undo = (
            [v for v in applied_vers if v > target_v]
            if target_v is not None
            else applied_vers[:steps]
        )

        if not to_undo:
            print(f"  [{self._namespace}] Nothing to roll back.")
            return

        missing = [v for v in to_undo if v not in all_migs]
        if missing:
            raise RuntimeError(
                f"Migration file(s) for {missing} not found in {self._mig_dir}; "
                "nothing rolled back"
            )

        rolled_back_by_us: set[str] = set()
        for version in to_undo:
            mig = all_migs[version]
            ctx = self._ctx(mig)
            logger.info(
                "Rolling back %s: %s  [ns=%s]",
                mig.version,
                mig.description,
                self._namespace,
            )
            print(f"  Rolling back {mig.version} - {mig.description} ...")
            try:
                mig.pre_downgrade(ctx)
                mig.downgrade(ctx)
                self._history.record_rolled_back(
                    mig.version,
                    self._namespace,
                    mig.description,
                )
            except (NotImplementedError, ConcurrentMigrationError):
                raise
            except Exception as exc:
                if isinstance(exc, (CommitFailedException, ValidationException)):
                    raise ConcurrentMigrationError(
                        f"Rollback of {mig.version} hit a concurrent commit in namespace "
                        f"{self._namespace!r}; downgrade() may have partially run: {exc}"
                    ) from exc
                logger.error("Rollback of %s FAILED: %s", mig.version, exc)
                raise RuntimeError(f"Rollback of {mig.version} failed: {exc}") from exc

            # Recorded as rolled back: later errors propagate unchanged.
            rolled_back_by_us.add(version)
            self._verify_no_concurrent_modification(
                baseline - rolled_back_by_us,
                set(),
            )
            try:
                mig.post_downgrade(ctx)
            except Exception as post_err:
                logger.warning(
                    "Post-downgrade hook failed for %s: %s",
                    mig.version,
                    post_err,
                )
            print(f"  [ok] {mig.version} rolled back.")

        print(f"\n  Done. Rolled back {len(to_undo)} migration(s) from [{self._namespace}].")

    def stamp(self, target: str | int | None = None) -> None:
        """
        Mark migrations up to *target* as applied without running
        ``upgrade()``.

        Used to adopt iceberg-ops on a database whose schema was created
        out-of-band (e.g. an existing Iceberg deployment that predates
        the framework). For each migration whose current status is not
        ``applied`` (never run, ``failed`` or ``rolled_back``), an
        ``applied`` row replaces any existing row for that version. A
        ``failed`` version is therefore marked applied without being
        re-run, even if its ``upgrade()`` stopped halfway: check such
        versions before stamping. No ``pre_upgrade`` / ``upgrade`` /
        ``post_upgrade`` hook runs and no user tables are created.
        Idempotent — re-running is a no-op.

        Parameters
        ----------
        target : str or int, optional
            Latest version to stamp (inclusive). ``None`` (default)
            stamps every discovered migration. Accepts 1-4 digits
            (``"2"``, ``"0002"``) or an ``int``; it must name a
            discovered migration.

        Raises
        ------
        ValueError
            If *target* is malformed or names no discovered migration.
        FileNotFoundError
            If the migrations directory does not exist.
        RuntimeError
            If a migration file cannot be loaded.
        ConcurrentMigrationError
            If another runner recorded history for the same version
            concurrently.

        Notes
        -----
        ``stamp`` does not validate that the user's actual schema
        matches the stamped baseline. It simply updates the history. If
        your schema diverges from what migrations 0001...N would have
        produced, a subsequent ``migrate`` may still fail when migration
        ``N+1`` runs against a schema it did not expect.
        """
        all_migs = self._discover()
        target_v = _resolve_target(target, (m.version for m in all_migs))
        applied = self._history.get_applied_versions(self._namespace)

        to_stamp = [
            m
            for m in all_migs
            if m.version not in applied and (target_v is None or m.version <= target_v)
        ]

        if not to_stamp:
            print(f"  [{self._namespace}] Nothing to stamp.")
            return

        for mig in to_stamp:
            self._history.record_applied(mig.version, self._namespace, mig.description)
            print(f"  [ok] Stamped {mig.version} - {mig.description}")

        print(f"\n  Done. Stamped {len(to_stamp)} migration(s) for [{self._namespace}].")
