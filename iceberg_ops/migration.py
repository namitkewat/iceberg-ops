"""
iceberg_ops.migration
=====================
``BaseMigration`` and ``MigrationContext`` — the two objects every
migration file works with.

Authoring a migration
---------------------
Create ``migrations/NNNN_<description>.py`` and define::

    from iceberg_ops.migration import BaseMigration, MigrationContext
    import pyarrow as pa

    class Migration(BaseMigration):
        version     = "0001"
        description = "Create orders table"

        def upgrade(self, ctx: MigrationContext) -> None:
            ctx.create_table_if_not_exists(
                ctx.qualified("orders"),          # -> "<namespace>.orders"
                pa.schema([pa.field("id", pa.int64())]),
            )

        def downgrade(self, ctx: MigrationContext) -> None:
            ctx.drop_table_if_exists(ctx.qualified("orders"))

Conventions
-----------
* ``version``  — zero-padded 4-digit string matching the file's ``NNNN``
  prefix.
* ``upgrade()``  — **must be idempotent**.
* ``downgrade()`` — exact reverse of ``upgrade``; raise
  ``NotImplementedError`` when a full undo is impossible (e.g. column
  removal requires a table rewrite).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from pyiceberg import types as ice_types
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table
    from pyiceberg.table.sorting import SortOrder

    from iceberg_ops.core.schema import SchemaContract, SchemaEvolutionHelper

logger = logging.getLogger(__name__)


@dataclass
class MigrationContext:
    """
    Runtime environment injected into every ``upgrade()`` / ``downgrade()``
    call.

    Attributes
    ----------
    catalog : Catalog
        The live PyIceberg catalog being managed.
    namespace : str
        The **target namespace** this migration operates on
        (e.g. ``"sales"``, ``"finance"``). Use ``ctx.qualified(name)``
        to build fully-qualified table identifiers.
    version : str
        Version string of the migration currently executing.
    description : str
        Human-readable description of the migration.
    """

    catalog: Any
    namespace: str
    version: str
    description: str

    # ------------------------------------------------------------------
    # Namespace helpers
    # ------------------------------------------------------------------

    def qualified(self, table_name: str) -> str:
        """
        Return a fully-qualified table identifier.

        If *table_name* already contains a dot (``"sales.orders"``), it
        is returned unchanged. Otherwise ``<namespace>.<table_name>`` is
        built.

        Parameters
        ----------
        table_name : str
            Bare (``"orders"``) or qualified (``"sales.orders"``) table
            name.

        Returns
        -------
        str
            The fully-qualified identifier.

        Example
        -------
        ::

            # ctx.namespace = "sales"
            ctx.qualified("orders")         # -> "sales.orders"
            ctx.qualified("sales.orders")   # -> "sales.orders" (unchanged)
        """
        return table_name if "." in table_name else f"{self.namespace}.{table_name}"

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------

    def load_table(self, identifier: str) -> Table:
        """
        Load a table by its (relative or fully-qualified) identifier.

        Parameters
        ----------
        identifier : str
            Table name — relative (``"orders"``, qualified with
            ``ctx.namespace``) or qualified (``"sales.orders"``).

        Returns
        -------
        Table
            The loaded PyIceberg table.

        Raises
        ------
        NoSuchTableError
            If the table does not exist.
        """
        return self.catalog.load_table(self.qualified(identifier))  # type: ignore[no-any-return]

    def create_table_if_not_exists(
        self,
        identifier: str,
        schema: pa.Schema | IcebergSchema,
        *,
        partition_spec: PartitionSpec | None = None,
        sort_order: SortOrder | None = None,
        properties: dict[str, str] | None = None,
        location: str | None = None,
    ) -> Table:
        """
        Create an Iceberg table or silently return the existing one.

        The parent namespace (everything before the last dot, so nested
        namespaces such as ``"sales.eu"`` work) is created automatically
        if it does not exist. If another process creates the table
        between the existence check and the create, the existing table
        is returned.

        Parameters
        ----------
        identifier : str
            Table name — relative (``"orders"``) or qualified
            (``"sales.orders"``). Relative names are qualified with
            ``ctx.namespace``.
        schema : pa.Schema or pyiceberg.schema.Schema
            Schema used when creating a new table.
        partition_spec : PartitionSpec, optional
            Partition spec for a new table. Default: unpartitioned. Its
            fields reference source field ids, so it needs a
            ``pyiceberg.schema.Schema``; with a ``pa.Schema``, create
            the table first and then call :meth:`update_partition_spec`.
        sort_order : SortOrder, optional
            Sort order for a new table. Default: unsorted. Like
            *partition_spec*, it needs a ``pyiceberg.schema.Schema``;
            with a ``pa.Schema``, call :meth:`replace_sort_order` after
            creating the table.
        properties : dict of str to str, optional
            Table properties for a new table.
        location : str, optional
            Table location for a new table. Default: chosen by the
            catalog.

        Returns
        -------
        Table
            The created table, or the existing one unchanged. The
            keyword arguments apply only when the table is created; an
            existing table is never altered to match them.

        Raises
        ------
        ValueError
            If *schema* is a ``pa.Schema`` and *partition_spec* or
            *sort_order* has fields. Raised before anything (namespace
            or table) is created.
        """
        from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError

        if isinstance(schema, pa.Schema) and (
            (partition_spec is not None and partition_spec.fields)
            or (sort_order is not None and sort_order.fields)
        ):
            raise ValueError(
                "partition_spec / sort_order reference source field ids; pass a "
                "pyiceberg.schema.Schema, or create the table with the pyarrow schema "
                "and then call update_partition_spec / replace_sort_order"
            )

        fqn = self.qualified(identifier)
        ns = fqn.rsplit(".", 1)[0] if "." in fqn else "default"

        try:
            tbl = self.catalog.load_table(fqn)
            logger.debug("Table '%s' already exists.", fqn)
            return tbl  # type: ignore[no-any-return]
        except NoSuchTableError:
            pass

        create_kwargs: dict[str, Any] = {
            key: value
            for key, value in {
                "partition_spec": partition_spec,
                "sort_order": sort_order,
                "properties": properties,
                "location": location,
            }.items()
            if value is not None
        }
        self.catalog.create_namespace_if_not_exists(ns)
        try:
            tbl = self.catalog.create_table(fqn, schema=schema, **create_kwargs)
        except TableAlreadyExistsError:
            logger.debug("Table '%s' was created concurrently; loading it.", fqn)
            return self.catalog.load_table(fqn)  # type: ignore[no-any-return]
        logger.info("Created table '%s'.", fqn)
        return tbl  # type: ignore[no-any-return]

    def drop_table_if_exists(self, identifier: str) -> None:
        """
        Drop a table, silently succeeding if it does not exist.

        Parameters
        ----------
        identifier : str
            Table name — relative or fully qualified.
        """
        from pyiceberg.exceptions import NoSuchTableError

        fqn = self.qualified(identifier)
        try:
            self.catalog.drop_table(fqn)
            logger.info("Dropped table '%s'.", fqn)
        except NoSuchTableError:
            logger.debug("Table '%s' not found; skip drop.", fqn)

    def get_helper(self, identifier: str) -> SchemaEvolutionHelper:
        """
        Return a ``SchemaEvolutionHelper`` bound to *identifier*.

        Power-user escape hatch — the most common operations (add,
        enforce, rename, delete columns) are exposed directly on this
        context. Reach for the helper when you need its lower-level
        primitives or the ``contract=`` kwarg.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.

        Returns
        -------
        SchemaEvolutionHelper
            A helper bound to the freshly loaded table.

        Raises
        ------
        NoSuchTableError
            If the table does not exist.
        """
        from iceberg_ops.core.schema import SchemaEvolutionHelper

        return SchemaEvolutionHelper(self.load_table(identifier))

    # ------------------------------------------------------------------
    # Column lifecycle (idempotent / non-idempotent)
    # ------------------------------------------------------------------

    def add_column(
        self,
        identifier: str,
        name: str,
        field_type: ice_types.IcebergType | pa.DataType,
        *,
        doc: str | None = None,
    ) -> None:
        """
        Add a column to a table.

        Non-idempotent — raises if the column already exists. Use
        :meth:`enforce_column` for the idempotent variant.

        Supports dotted paths for nested struct fields (e.g.
        ``"address.zip_code"``); the parent struct must already exist.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        name : str
            New column name. Dotted paths target nested struct fields.
        field_type : ice_types.IcebergType or pa.DataType
            The column's type. A primitive PyArrow type (e.g.
            ``pa.string()``) is translated to its Iceberg equivalent.
        doc : str, optional
            Description stored in the Iceberg schema catalog.

        Raises
        ------
        NoSuchTableError
            If the table does not exist.
        ValueError
            If the column already exists, the parent struct of a dotted
            *name* does not exist, or a PyArrow *field_type* has no
            Iceberg primitive equivalent.
        """
        if isinstance(field_type, pa.DataType):
            from iceberg_ops.core.types import arrow_type_to_iceberg

            field_type = arrow_type_to_iceberg(field_type)
        table = self.load_table(identifier)
        path: str | tuple[str, ...] = tuple(name.split(".")) if "." in name else name
        with table.update_schema() as upd:
            upd.add_column(path, field_type, doc=doc)
        logger.info("Added column '%s' to '%s'.", name, self.qualified(identifier))

    def enforce_column(
        self,
        identifier: str,
        name: str,
        field_type: ice_types.IcebergType | pa.DataType,
        *,
        doc: str | None = None,
        contract: SchemaContract | None = None,
        legacy_archival: bool = False,
    ) -> None:
        """
        Idempotent column enforcement (delegates to
        :meth:`SchemaEvolutionHelper.enforce_column`).

        A column that already has an equivalent type (including an
        identical struct, list or map) is left alone, apart from a doc
        update. A promotion the Iceberg spec allows (``int`` to
        ``long``, ``float`` to ``double``, wider decimal precision at the
        same scale) is applied in place; any other change, including
        ones PyIceberg itself would accept such as ``string`` to
        ``binary``, is incompatible. With ``legacy_archival=False``
        (default), an incompatible type change raises
        ``pyiceberg.exceptions.ValidationError`` and leaves the table
        untouched. With ``legacy_archival=True``, the existing column is
        renamed to ``<name>__legacy_N`` and a fresh column is added; a
        step 2 failure reverses step 1 (compensating transaction).
        Archival happens only on a genuine type incompatibility, never
        because of a commit error or a failing contract.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        name : str
            Canonical column name. Dotted paths target nested struct
            fields.
        field_type : ice_types.IcebergType or pa.DataType
            Intended type. A primitive PyArrow type (e.g.
            ``pa.float64()``) is translated to its Iceberg equivalent.
        doc : str, optional
            Description stored in the Iceberg schema catalog.
        contract : SchemaContract, optional
            If provided, enforced once after the operation completes.
        legacy_archival : bool, optional
            Opt-in to the archive-and-recreate flow on incompatible
            type. Default ``False``.

        Raises
        ------
        pyiceberg.exceptions.ValidationError
            If the type is incompatible and ``legacy_archival`` is
            ``False``.
        ValueError
            If archival is requested for a column referenced by the
            partition spec, sort order or identifier fields.
        SchemaContractViolation
            If *contract* is given and the table does not conform.
        """
        self.get_helper(identifier).enforce_column(
            name,
            field_type,
            doc=doc,
            contract=contract,
            legacy_archival=legacy_archival,
        )

    def enforce_rename(
        self,
        identifier: str,
        old_name: str,
        new_name: str,
        *,
        doc: str | None = None,
        contract: SchemaContract | None = None,
    ) -> None:
        """
        Idempotent rename (delegates to
        :meth:`SchemaEvolutionHelper.enforce_rename`).

        Behaviour depends on which of *old_name* / *new_name* exists in
        the current schema; see the helper docstring for the matrix.
        Supports dotted paths for nested struct fields. When both exist,
        the blocking *new_name* is archived to ``<new_name>__legacy_N``
        (made optional, ``[ARCHIVED]`` doc) unless the partition spec,
        sort order or identifier fields reference it, in which case
        ``ValueError`` is raised and the table is left untouched.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        old_name : str
            Current column name. Dotted paths target nested struct
            fields.
        new_name : str
            Desired column name; must share *old_name*'s parent struct.
        doc : str, optional
            Description to set on the renamed column.
        contract : SchemaContract, optional
            If provided, enforced once after the operation completes
            (also when the rename is a no-op).

        Raises
        ------
        NoSuchTableError
            If the table does not exist.
        ValueError
            If the names do not share a parent struct, the parent
            struct does not exist, or a colliding *new_name* is
            referenced by the partition spec, sort order or identifier
            fields.
        SchemaContractViolation
            If *contract* is given and the table does not conform.
        """
        self.get_helper(identifier).enforce_rename(
            old_name,
            new_name,
            doc=doc,
            contract=contract,
        )

    def delete_column(
        self,
        identifier: str,
        column_name: str,
    ) -> None:
        """
        Delete a column from the table schema (metadata-only).

        Old data files still contain the column but it becomes invisible
        to queries after this operation.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        column_name : str
            Name of the column to delete.

        Raises
        ------
        NoSuchTableError
            If the table does not exist.
        ValueError
            If the column does not exist.
        """
        table = self.load_table(identifier)
        with table.update_schema() as upd:
            upd.delete_column(column_name)
        logger.info(
            "Deleted column '%s' from '%s'.",
            column_name,
            self.qualified(identifier),
        )

    # ------------------------------------------------------------------
    # Partition spec management
    # ------------------------------------------------------------------

    def update_partition_spec(
        self,
        identifier: str,
        add_fields: list[tuple[str, Any, str]] | None = None,
        remove_fields: list[str] | None = None,
    ) -> None:
        """
        Evolve the partition spec of a table.

        Uses PyIceberg's ``table.update_spec()`` context manager to add
        or remove partition fields in a single atomic commit.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        add_fields : list of (column_name, transform, partition_name), optional
            Each tuple contains:

            - ``column_name`` — source column in the table schema.
            - ``transform`` — a PyIceberg ``Transform`` instance
              (e.g. ``DayTransform()``, ``IdentityTransform()``).
            - ``partition_name`` — the name for the new partition field.
        remove_fields : list of str, optional
            Names of partition fields to remove (void-out).

        Example
        -------
        ::

            from pyiceberg.transforms import DayTransform
            ctx.update_partition_spec("orders", add_fields=[
                ("created_at", DayTransform(), "created_day"),
            ])
        """
        table = self.load_table(identifier)
        with table.update_spec() as update:
            for col_name, transform, part_name in add_fields or []:
                update.add_field(col_name, transform, part_name)
            for field_name in remove_fields or []:
                update.remove_field(field_name)
        logger.info("Updated partition spec for '%s'.", self.qualified(identifier))

    # ------------------------------------------------------------------
    # Sort order management
    # ------------------------------------------------------------------

    def replace_sort_order(
        self,
        identifier: str,
        sort_fields: list[tuple[str, str]] | None = None,
    ) -> None:
        """
        Replace the sort order of a table.

        Builds a new sort order from the provided column / direction
        pairs and commits it via ``table.update_sort_order()``.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        sort_fields : list of (column_name, direction), optional
            Each tuple is ``(column_name, "asc" | "desc")``. If *None*
            or empty, the sort order is cleared.

        Raises
        ------
        ValueError
            If *direction* is not ``"asc"`` or ``"desc"``.

        Example
        -------
        ::

            ctx.replace_sort_order("orders", sort_fields=[
                ("order_id", "asc"),
                ("created_at", "desc"),
            ])
        """
        from pyiceberg.table.sorting import NullOrder
        from pyiceberg.transforms import IdentityTransform

        table = self.load_table(identifier)

        if not sort_fields:
            update = table.update_sort_order()
            update.commit()
            logger.info("Cleared sort order for '%s'.", self.qualified(identifier))
            return

        for col_name, direction in sort_fields:
            if direction not in ("asc", "desc"):
                raise ValueError(
                    f"Invalid sort direction '{direction}' for column "
                    f"'{col_name}'. Must be 'asc' or 'desc'."
                )

        update = table.update_sort_order()
        for col_name, direction in sort_fields:
            null_order = NullOrder.NULLS_FIRST if direction == "asc" else NullOrder.NULLS_LAST
            if direction == "asc":
                update.asc(col_name, IdentityTransform(), null_order)
            else:
                update.desc(col_name, IdentityTransform(), null_order)
        update.commit()
        logger.info("Replaced sort order for '%s'.", self.qualified(identifier))

    # ------------------------------------------------------------------
    # Table properties management
    # ------------------------------------------------------------------

    def set_table_properties(
        self,
        identifier: str,
        properties: dict[str, str],
    ) -> None:
        """
        Set one or more Iceberg table properties.

        Uses a PyIceberg transaction so all properties land in a single
        atomic commit.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        properties : dict of str to str
            Properties to set. Any key is allowed, including keys such
            as ``"properties"``.

        Example
        -------
        ::

            ctx.set_table_properties("orders", {
                "write.parquet.compression-codec": "zstd",
            })
        """
        table = self.load_table(identifier)
        with table.transaction() as txn:
            # Positional dict: keys such as "properties" must not collide
            # with set_properties' own parameter names.
            txn.set_properties(properties)
        logger.info(
            "Set %d properties on '%s'.",
            len(properties),
            self.qualified(identifier),
        )

    def unset_table_properties(
        self,
        identifier: str,
        keys: list[str],
    ) -> None:
        """
        Remove one or more Iceberg table properties.

        Uses a PyIceberg transaction so all keys are removed atomically.
        Keys that do not exist are silently ignored.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        keys : list of str
            Property keys to remove.

        Example
        -------
        ::

            ctx.unset_table_properties("orders", [
                "write.parquet.compression-codec",
            ])
        """
        table = self.load_table(identifier)
        with table.transaction() as txn:
            txn.remove_properties(*keys)
        logger.info(
            "Unset %d properties from '%s'.",
            len(keys),
            self.qualified(identifier),
        )

    def set_compaction_target_size(
        self,
        identifier: str,
        target_mb: int,
    ) -> None:
        """
        Set the target data file size for writers.

        Sets the standard Iceberg ``write.target-file-size-bytes``
        property. PyIceberg's writers use it as their bin-packing
        target. iceberg-ops' compaction helpers take an explicit
        ``target_size_mb`` (default 512) and do not read it; managed
        services such as S3 Tables configure compaction targets through
        their own maintenance settings.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        target_mb : int
            Target file size in megabytes. Stored as
            ``target_mb * 1024 * 1024``.

        Example
        -------
        ::

            ctx.set_compaction_target_size("orders", target_mb=256)
        """
        self.set_table_properties(
            identifier,
            {"write.target-file-size-bytes": str(target_mb * 1024 * 1024)},
        )

    def set_snapshot_retention(
        self,
        identifier: str,
        *,
        min_snapshots: int | None = None,
        max_age_hours: int | None = None,
    ) -> None:
        """
        Configure snapshot retention via standard Iceberg properties.

        Sets one or both of:

        * ``history.expire.min-snapshots-to-keep`` — minimum number of
          snapshots Iceberg must retain regardless of age (Iceberg
          default: 1).
        * ``history.expire.max-snapshot-age-ms`` — snapshots older than
          this become eligible for expiration (Iceberg default: 5 days).

        Both conditions must be satisfied for a snapshot to be expired.
        Calling with both arguments ``None`` is a no-op.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        min_snapshots : int, optional
            Minimum snapshot count to retain.
        max_age_hours : int, optional
            Maximum snapshot age in hours. Stored as
            ``max_age_hours * 3600 * 1000`` milliseconds.

        Notes
        -----
        These are the standard Iceberg properties read by engines'
        expire-snapshots procedures. Do not set them on AWS S3 Tables:
        S3 Tables manages retention through its own maintenance
        configuration, and AWS documents that these properties make its
        snapshot management fail; a warning is logged when the catalog
        is configured for S3 Tables.
        :func:`~iceberg_ops.maintenance.snapshots.expire_snapshots` reads these
        properties when its ``max_age_hours`` / ``min_snapshots``
        arguments are omitted.

        Example
        -------
        ::

            ctx.set_snapshot_retention("orders", min_snapshots=5, max_age_hours=72)
        """
        props: dict[str, str] = {}
        if min_snapshots is not None:
            props["history.expire.min-snapshots-to-keep"] = str(min_snapshots)
        if max_age_hours is not None:
            props["history.expire.max-snapshot-age-ms"] = str(max_age_hours * 3600 * 1000)
        if props:
            catalog_props = getattr(self.catalog, "properties", None) or {}
            if catalog_props.get("rest.signing-name") == "s3tables":
                logger.warning(
                    "Setting history.expire.* on '%s', but the catalog is AWS S3 Tables, "
                    "which manages snapshot retention itself; AWS documents that these "
                    "properties make S3 Tables snapshot management fail.",
                    self.qualified(identifier),
                )
            self.set_table_properties(identifier, props)

    def set_clustering_keys(
        self,
        identifier: str,
        columns: list[str],
        *,
        direction: str = "asc",
    ) -> None:
        """
        Set the table's Iceberg sort order on columns with a uniform
        direction.

        The sort order is the only portable, spec-defined clustering
        mechanism; iceberg-ops writes no vendor-specific properties.
        Whether an engine or managed service uses the sort order when
        writing or compacting depends on that engine. This is a single
        :meth:`replace_sort_order` commit.

        Parameters
        ----------
        identifier : str
            Table name — relative or qualified.
        columns : list of str
            One or more column names to cluster by, in priority order.
            Must be non-empty.
        direction : str, optional
            ``"asc"`` (default) or ``"desc"``. Applied uniformly to
            every column. For mixed directions, call
            :meth:`replace_sort_order` directly.

        Raises
        ------
        ValueError
            If *columns* is empty, or if *direction* is not ``"asc"`` /
            ``"desc"``.
        """
        if not columns:
            raise ValueError("set_clustering_keys requires at least one column.")

        self.replace_sort_order(
            identifier,
            sort_fields=[(c, direction) for c in columns],
        )


class BaseMigration:
    """
    Abstract base class for all migration files.

    Subclass this and name the subclass ``Migration`` exactly — the
    runner discovers it by that name.

    The runner calls methods in this order::

        pre_upgrade()    -> upgrade()    -> post_upgrade()
        pre_downgrade()  -> downgrade()  -> post_downgrade()

    All four hook methods (``pre_upgrade``, ``post_upgrade``,
    ``pre_downgrade``, ``post_downgrade``) are optional no-ops by
    default. Override them to add precondition checks, notifications,
    or cleanup logic around the core migration.

    Class attributes
    ----------------
    version : str
        Zero-padded 4-digit string (``"0001"``). Must be unique within
        the project.
    description : str
        Short summary stored in the migration history table.
    """

    version: str = ""
    description: str = ""

    # ------------------------------------------------------------------
    # Core migration methods
    # ------------------------------------------------------------------

    def upgrade(self, ctx: MigrationContext) -> None:
        """
        Apply this migration. Must be idempotent.

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.

        Raises
        ------
        NotImplementedError
            Unless overridden.
        """
        raise NotImplementedError(f"{self.__class__.__name__}.upgrade() not implemented.")

    def downgrade(self, ctx: MigrationContext) -> None:
        """
        Reverse this migration.

        Raise ``NotImplementedError`` when a full undo is not possible
        (e.g. column removal requires a full table rewrite in Iceberg).

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.

        Raises
        ------
        NotImplementedError
            Unless overridden.
        """
        raise NotImplementedError(f"{self.__class__.__name__}.downgrade() not implemented.")

    # ------------------------------------------------------------------
    # Lifecycle hooks (optional, default is no-op)
    # ------------------------------------------------------------------

    def pre_upgrade(self, ctx: MigrationContext) -> None:
        """
        Override to validate preconditions before ``upgrade()``.

        If this method raises, ``upgrade()`` does not run and the
        migration is recorded as failed.

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.
        """

    def post_upgrade(self, ctx: MigrationContext) -> None:
        """
        Override to perform post-migration actions after ``upgrade()``.

        If this method raises, the migration is still recorded as
        applied (the schema change already committed); a warning is
        logged.

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.
        """

    def pre_downgrade(self, ctx: MigrationContext) -> None:
        """
        Override to validate preconditions before ``downgrade()``.

        If this method raises, ``downgrade()`` does not run and the
        rollback fails.

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.
        """

    def post_downgrade(self, ctx: MigrationContext) -> None:
        """
        Override to perform post-rollback cleanup.

        If this method raises, the rollback is still recorded as
        completed; a warning is logged.

        Parameters
        ----------
        ctx : MigrationContext
            Runtime context: catalog, target namespace and table helpers.
        """

    def __repr__(self) -> str:
        return f"<Migration {self.version}: {self.description}>"
