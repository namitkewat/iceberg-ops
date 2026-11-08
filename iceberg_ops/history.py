"""
iceberg_ops.history
===================
``MigrationHistory`` — stores migration records as a real Iceberg table
inside a dedicated **meta-namespace** within the user's own catalog.

Why Iceberg-backed (not SQLite)?
--------------------------------
* Works with *any* catalog backend — SQLite locally, Glue / Hive / REST
  in prod. The history moves with the data; no extra SQLite file to
  manage or replicate.
* The history table benefits from all Iceberg guarantees: ACID writes,
  time travel, schema evolution.
* A single history table tracks migrations for *all* managed namespaces
  via a ``namespace`` column, so one catalog deployment is all you
  need.

Default layout
--------------
::

    _iceberg_ops/                  <- meta_namespace (configurable)
    `-- migration_history          <- current status per (version, namespace)

Schema of ``migration_history``
-------------------------------
::

    version      string  - zero-padded version string ("0001", ...)
    namespace    string  - the user-managed namespace targeted
    description  string  - human-readable migration summary
    applied_at   string  - ISO-8601 UTC timestamp of last status change
    status       string  - "applied" | "rolled_back" | "failed"
    error        string  - error message excerpt when status="failed"
"""

from __future__ import annotations

import datetime
import logging
import warnings
from typing import TYPE_CHECKING

import pyarrow as pa
from pyiceberg.catalog import Catalog

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.table import Table

logger = logging.getLogger(__name__)

DEFAULT_META_NAMESPACE = "_iceberg_ops"
_HISTORY_TABLE = "migration_history"

# Keep exact (untruncated) column bounds for the key columns, so the
# key-scoped overwrite in ``_upsert`` does not report a false conflict
# between namespaces that share a long common prefix. Raise the commit
# retry budget above PyIceberg's default of 4: runners for many
# namespaces can write history at the same moment, and each lost race
# for a different key is retried. A same-key conflict is still never
# retried (it surfaces as ``ValidationException`` during the retry).
_HISTORY_TABLE_PROPERTIES = {
    "write.metadata.metrics.column.version": "full",
    "write.metadata.metrics.column.namespace": "full",
    "commit.retry.num-retries": "20",
    "commit.retry.max-wait-ms": "5000",
}


class ConcurrentMigrationError(RuntimeError):
    """
    Raised when another process changed the migration history during a
    run.

    Concurrent runs for one namespace are not prevented, only detected
    when history is written: the losing run raises this error and its
    status is not recorded. Run migrations for a namespace from one job
    at a time (for example a CI concurrency group) or put an external
    lock in front of the runner.
    """


def _eq(column: str, value: str) -> BooleanExpression:
    """Return the PyIceberg expression ``column == value``."""
    from pyiceberg.expressions import EqualTo

    # EqualTo's custom positional __init__ is hidden from mypy by the
    # pydantic-synthesised signature.
    return EqualTo(column, value)  # type: ignore[call-arg, arg-type]


def _latest_per_key(rows: list[dict]) -> list[dict]:
    """
    Keep only the newest row per ``(version, namespace)`` key.

    Parameters
    ----------
    rows : list of dict
        Raw history rows.

    Returns
    -------
    list of dict
        One row per key: the one with the greatest ``applied_at``.
    """
    latest: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (row["version"], row["namespace"])
        if key not in latest or row["applied_at"] >= latest[key]["applied_at"]:
            latest[key] = row
    return list(latest.values())


class MigrationHistory:
    """
    Iceberg-backed migration state store.

    The ``<meta_namespace>`` namespace and the ``migration_history``
    table inside it are created on first write; reading an absent table
    returns empty results and creates nothing. No external database
    required.

    Each ``(version, namespace)`` has one row holding its latest status;
    earlier states are not kept as rows. They remain readable via time
    travel on the history table's snapshots until those snapshots
    expire.

    Concurrent writers are not locked out. A write replaces the key's
    row with a key-scoped overwrite; if another process committed a
    change to the same key after this handle last read the table, the
    write raises :class:`ConcurrentMigrationError` instead of landing a
    duplicate row. A write that loses a race against writes for other
    keys (for example runners for other namespaces) is retried, up to
    the history table's ``commit.retry.num-retries`` (20 on a table
    created by iceberg-ops).

    Parameters
    ----------
    catalog : Catalog
        The live PyIceberg catalog (same one the user manages data in).
    meta_namespace : str
        Namespace dedicated to framework metadata. Users should never
        write their own tables here. Default: ``"_iceberg_ops"``.
    """

    SCHEMA = pa.schema(
        [
            pa.field("version", pa.large_utf8(), nullable=False),
            pa.field("namespace", pa.large_utf8(), nullable=False),
            pa.field("description", pa.large_utf8(), nullable=True),
            pa.field("applied_at", pa.large_utf8(), nullable=False),
            pa.field("status", pa.large_utf8(), nullable=False),
            pa.field("error", pa.large_utf8(), nullable=True),
        ]
    )

    def __init__(
        self,
        catalog: Catalog,
        meta_namespace: str = DEFAULT_META_NAMESPACE,
    ) -> None:
        self._catalog = catalog
        self._meta_ns = meta_namespace
        self._table_id = f"{meta_namespace}.{_HISTORY_TABLE}"
        self._table: Table | None = None
        # True once a read found no history table: that read's baseline
        # is "empty", which the first write must still honour.
        self._read_found_no_table = False

    # ------------------------------------------------------------------
    # Table access
    # ------------------------------------------------------------------

    def _load(self) -> Table | None:
        """Return the history table, or ``None`` if it does not exist yet."""
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

        if self._table is None:
            try:
                self._table = self._catalog.load_table(self._table_id)
                logger.debug("Migration history table found: %s", self._table_id)
            except (NoSuchTableError, NoSuchNamespaceError):
                return None
        return self._table

    def _load_or_create(self) -> Table:
        """Return the history table, creating it and its namespace if needed."""
        from pyiceberg.exceptions import TableAlreadyExistsError

        existing = self._load()
        if existing is not None:
            return existing
        self._catalog.create_namespace_if_not_exists(self._meta_ns)
        try:
            tbl = self._catalog.create_table(
                self._table_id,
                schema=self.SCHEMA,
                properties=_HISTORY_TABLE_PROPERTIES,
            )
            logger.info("Created migration history table: %s", self._table_id)
        except TableAlreadyExistsError:
            # Another process created it between our load and create.
            tbl = self._catalog.load_table(self._table_id)
        self._table = tbl
        return tbl

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def _upsert(
        self,
        version: str,
        namespace: str,
        description: str,
        status: str,
        error: str | None = None,
    ) -> None:
        """
        Replace the row for ``(version, namespace)`` with a new status.

        Uses an overwrite scoped to the key rather than ``Table.upsert``:
        the delete half of the overwrite registers the key as a conflict
        filter, so a concurrent write to the same key fails loudly
        instead of being retried into a duplicate row (see
        apache/iceberg-python#4194). The handle is deliberately not
        refreshed first: its last read is the optimistic baseline. If
        that read found no history table, a row for the key that exists
        by the time the table is loaded counts as a concurrent write.

        Raises
        ------
        ConcurrentMigrationError
            If another process committed a change to the same key since
            this handle last read the table, or the commit retries ran
            out because other writers kept committing first.
        """
        from pyiceberg.exceptions import CommitFailedException, ValidationException
        from pyiceberg.expressions import And

        from iceberg_ops.core.utils import create_arrow_table

        key = And(_eq("version", version), _eq("namespace", namespace))
        baseline_empty = self._table is None and self._read_found_no_table
        tbl = self._load_or_create()
        if baseline_empty and tbl.scan(row_filter=key).to_arrow().num_rows:
            raise ConcurrentMigrationError(
                f"History row {version} [{namespace}] was written concurrently after "
                f"this runner found no history; status '{status}' not recorded. "
                f"Another runner is likely migrating this namespace."
            )
        now = datetime.datetime.now(datetime.UTC).isoformat()
        row = create_arrow_table(
            self.SCHEMA,
            [
                {
                    "version": version,
                    "namespace": namespace,
                    "description": description,
                    "applied_at": now,
                    "status": status,
                    "error": error[:2000] if error else None,
                }
            ],
        )
        try:
            with warnings.catch_warnings():
                # The first write of a key deletes nothing; that is expected.
                warnings.filterwarnings(
                    "ignore",
                    message="Delete operation did not match any records",
                )
                tbl.overwrite(row, overwrite_filter=key)
        except ValidationException as exc:
            tbl.refresh()
            raise ConcurrentMigrationError(
                f"History row {version} [{namespace}] was changed concurrently; "
                f"status '{status}' not recorded. Another runner is likely "
                f"migrating this namespace."
            ) from exc
        except CommitFailedException as exc:
            tbl.refresh()
            raise ConcurrentMigrationError(
                f"History table was contended by concurrent writers (retries "
                f"exhausted); status '{status}' of {version} [{namespace}] not "
                f"recorded. Re-run; upgrade() must be idempotent."
            ) from exc

    def record_applied(self, version: str, namespace: str, description: str) -> None:
        """
        Record a migration as successfully applied.

        Parameters
        ----------
        version : str
            Migration version (``"0001"``).
        namespace : str
            Target namespace of the migration.
        description : str
            Human-readable migration summary.

        Raises
        ------
        ConcurrentMigrationError
            If another process changed this key's row concurrently.
        """
        self._upsert(version, namespace, description, "applied")
        logger.info("Migration %s [%s] -> applied.", version, namespace)

    def record_rolled_back(self, version: str, namespace: str, description: str) -> None:
        """
        Record a migration as rolled back.

        Parameters
        ----------
        version : str
            Migration version (``"0001"``).
        namespace : str
            Target namespace of the migration.
        description : str
            Human-readable migration summary.

        Raises
        ------
        ConcurrentMigrationError
            If another process changed this key's row concurrently.
        """
        self._upsert(version, namespace, description, "rolled_back")
        logger.info("Migration %s [%s] -> rolled_back.", version, namespace)

    def record_failed(
        self,
        version: str,
        namespace: str,
        description: str,
        error: str,
    ) -> None:
        """
        Record a migration attempt that raised an exception.

        Parameters
        ----------
        version : str
            Migration version (``"0001"``).
        namespace : str
            Target namespace of the migration.
        description : str
            Human-readable migration summary.
        error : str
            Error text; truncated to 2000 characters.

        Raises
        ------
        ConcurrentMigrationError
            If another process changed this key's row concurrently.
        """
        self._upsert(version, namespace, description, "failed", error)
        logger.warning("Migration %s [%s] -> failed.", version, namespace)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _rows(self, namespace: str) -> list[dict]:
        """Return the latest row per version for *namespace* (unordered)."""
        tbl = self._load()
        if tbl is None:
            self._read_found_no_table = True
            return []
        tbl.refresh()
        rows = tbl.scan(row_filter=_eq("namespace", namespace)).to_arrow().to_pylist()
        return _latest_per_key(rows)

    def get_applied_versions(self, namespace: str) -> set[str]:
        """
        Return the versions whose current status is ``"applied"``.

        Parameters
        ----------
        namespace : str
            Namespace to read history for.

        Returns
        -------
        set of str
            Applied versions. Empty when the history table does not
            exist yet (it is not created by reads).
        """
        return {r["version"] for r in self._rows(namespace) if r["status"] == "applied"}

    def get_all(self, namespace: str) -> list[dict]:
        """
        Return the current history row of every version in *namespace*.

        Parameters
        ----------
        namespace : str
            Namespace to read history for.

        Returns
        -------
        list of dict
            One row per version, ordered by version ascending. Each row
            has the keys ``version``, ``namespace``, ``description``,
            ``applied_at``, ``status`` and ``error``. Empty when the
            history table does not exist yet.
        """
        return sorted(self._rows(namespace), key=lambda r: r["version"])
