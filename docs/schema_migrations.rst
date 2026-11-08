Schema Migrations
=================

iceberg-ops takes the Alembic mental model -- versioned ``NNNN_*.py``
files driven by a ``MigrationContext`` -- and adapts it to PyIceberg's
metadata-only DDL primitives. Each file has an ``upgrade()`` and a
``downgrade()``; the runner applies pending files in version order and
records each version's status in a history table inside your own
catalog.

Migration file conventions
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Rule
     - Detail
   * - File name
     - ``NNNN_<description>.py`` (4-digit zero-padded version, an
       underscore, then a slug). Files that do not match are ignored.
   * - Class name
     - Must be exactly ``Migration``. A matching file without a
       ``Migration`` class is skipped with a warning.
   * - ``version``
     - String equal to the file's ``NNNN`` prefix. A mismatch, or two
       files with the same version, is an error.
   * - ``description``
     - Human-readable description; surfaces in ``status`` / ``history``.
   * - ``upgrade()``
     - Must be idempotent (see `Concurrent runs`_). Prefer
       ``ctx.create_table_if_not_exists`` / ``enforce_column`` /
       ``enforce_rename`` over raw create / add / rename.
   * - ``downgrade()``
     - Reverses ``upgrade()``. When a step cannot be undone, raise
       ``NotImplementedError`` before changing anything (see
       `Downgrades`_).
   * - Table identifiers
     - Pass bare table names (``"orders"``); the context qualifies them
       with the namespace the runner was given, so one file serves
       every namespace. A dotted name (``"sales.orders"``) is used
       unchanged and pins the migration to that table.
   * - Directory layout
     - Flat (``migrations/0001_*.py``) or one subdirectory per
       namespace (``migrations/sales/0001_*.py``); a subdirectory named
       after the runner's namespace is used when it exists. A missing
       migrations directory is an error (``FileNotFoundError``).

Each ``ctx`` helper call is its own Iceberg commit, so a migration is
not atomic: if ``upgrade()`` fails half-way, the earlier steps stay
committed and the version is recorded as ``failed``. A failed version
is pending again on the next ``migrate``, which is another reason to
keep ``upgrade()`` idempotent.

MigrationContext API
--------------------

The context is injected into every ``upgrade()`` / ``downgrade()`` call.
The core helpers are available as direct methods on the context;
``ctx.get_helper(table)`` returns a ``SchemaEvolutionHelper`` for its
lower-level primitives.

Types: column helpers take a PyIceberg type
(``pyiceberg.types.DoubleType()``, ``StringType()``, ...) or a
primitive PyArrow type (``pa.float64()``, ``pa.string()``), which is
translated to its Iceberg equivalent. Nested types (struct / list /
map) must be given as PyIceberg types. Table schemas take a
``pyarrow.Schema`` or a ``pyiceberg.schema.Schema``.

**Table lifecycle**

* ``ctx.create_table_if_not_exists(identifier, schema, *,
  partition_spec=None, sort_order=None, properties=None,
  location=None)`` -- create the table, or return the existing one
  unchanged. The keyword arguments apply only when the table is
  created. The parent namespace is created if needed (nested
  namespaces such as ``"sales.eu"`` work). ``partition_spec`` and
  ``sort_order`` reference source field ids, so pass them together
  with a ``pyiceberg.schema.Schema``; with a ``pyarrow.Schema``, create
  the table first and then call ``update_partition_spec`` /
  ``replace_sort_order``.
* ``ctx.drop_table_if_exists(identifier)``
* ``ctx.load_table(identifier)``
* ``ctx.qualified(name)`` -- returns ``"<namespace>.<name>"``; a name
  that already contains a dot is returned unchanged.

**Column lifecycle**

* ``ctx.add_column(identifier, name, field_type, *, doc=None)`` -- add
  a new column. Raises if it already exists. Dotted names
  (``"address.zip_code"``) add a field to an existing struct.
* ``ctx.enforce_column(identifier, name, field_type, *, doc=None,
  contract=None, legacy_archival=False)`` -- ensure the column exists
  with the given type; see the `Behaviour matrix`_ below.
* ``ctx.enforce_rename(identifier, old_name, new_name, *, doc=None,
  contract=None)`` -- rename idempotently. Both names must share a
  parent struct. When both names exist, the blocking column is
  archived first (see `Rename collisions`_).
* ``ctx.delete_column(identifier, column_name)`` -- metadata-only
  delete. Data files still hold the values (reachable by time travel).

**Partition spec evolution**

* ``ctx.update_partition_spec(identifier, add_fields=None,
  remove_fields=None)`` -- add / remove partition fields in one
  commit. ``add_fields`` is a list of ``(source_column, transform,
  partition_name)`` tuples, where ``transform`` is a PyIceberg
  transform instance such as ``DayTransform()``; ``remove_fields`` is a
  list of partition field names. Writing to a table partitioned by a
  non-identity transform (bucket, truncate, year, month, day, hour)
  needs ``pyiceberg-core``: ``pip install "iceberg-ops[transforms]"``.

**Sort order management**

* ``ctx.replace_sort_order(identifier, sort_fields=None)`` -- replace
  the table's sort order. Each entry is ``(column, "asc" | "desc")``
  with an identity transform (``asc`` sorts nulls first, ``desc``
  nulls last). ``None`` or an empty list clears the sort order.

**Table properties**

* ``ctx.set_table_properties(identifier, properties)`` -- set one or
  more Iceberg table properties in one commit.
* ``ctx.unset_table_properties(identifier, keys)`` -- remove
  properties by key; missing keys are ignored.

**Convenience accessors**

* ``ctx.set_compaction_target_size(identifier, target_mb)`` -- sets
  ``write.target-file-size-bytes`` (``target_mb`` MiB), which
  PyIceberg's writers use as their target file size. The iceberg-ops
  compaction helper does not read it; it takes an explicit
  ``target_size_mb``.
* ``ctx.set_snapshot_retention(identifier, *, min_snapshots=None,
  max_age_hours=None)`` -- sets the standard
  ``history.expire.min-snapshots-to-keep`` /
  ``history.expire.max-snapshot-age-ms`` properties.
  :py:func:`iceberg_ops.maintenance.expire_snapshots` reads them when
  its own arguments are omitted. Do not use it on AWS S3 Tables, which
  manages snapshot retention itself; a warning is logged when the
  catalog is configured for S3 Tables.
* ``ctx.set_clustering_keys(identifier, columns, *, direction="asc")``
  -- sets the table sort order on ``columns`` (the same as
  ``replace_sort_order`` with one direction for every column). It
  writes no other properties.

Lifecycle hooks
---------------

``BaseMigration`` carries four optional hooks:

.. code-block:: text

   pre_upgrade()    -> upgrade()    -> post_upgrade()
   pre_downgrade()  -> downgrade()  -> post_downgrade()

All four are no-ops by default. Error semantics:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Hook
     - If it raises
   * - ``pre_upgrade``
     - The migration is not applied: it is recorded as ``failed``, the
       run stops, and ``migrate`` raises ``RuntimeError`` (the CLI
       exits 1).
   * - ``post_upgrade``
     - The migration is already recorded as applied -- a warning is
       logged, nothing is rolled back.
   * - ``pre_downgrade``
     - The rollback stops with an error (``RuntimeError``); the version
       stays applied.
   * - ``post_downgrade``
     - The rollback is already recorded -- a warning is logged.

Example: pre_upgrade quality gate
---------------------------------

.. code-block:: python

   # migrations/0010_add_session_duration.py
   from pyiceberg.types import DoubleType

   from iceberg_ops.migration import BaseMigration, MigrationContext


   class Migration(BaseMigration):
       version = "0010"
       description = "Add session_duration with non-empty gate"

       def pre_upgrade(self, ctx: MigrationContext) -> None:
           tbl = ctx.load_table("events")
           if tbl.current_snapshot() is None:
               raise RuntimeError("Cannot migrate empty events table.")

       def upgrade(self, ctx: MigrationContext) -> None:
           ctx.enforce_column("events", "session_duration", DoubleType())

       def downgrade(self, ctx: MigrationContext) -> None:
           ctx.delete_column("events", "session_duration")

Downgrades
----------

``downgrade()`` undoes ``upgrade()`` where the prior state is known: a
column that ``upgrade()`` added can be deleted, a rename can be renamed
back, a property can be restored to its old value. Some steps cannot
be undone:

* a column delete (re-adding it creates a new, empty column; the old
  values are reachable only by time travel);
* a type promotion such as ``int -> long`` (Iceberg only widens types);
* a legacy-archival type change (see below).

For those, raise ``NotImplementedError`` as the first statement of
``downgrade()``, before any reversible step runs, so a rollback leaves
the table untouched. The runner re-raises ``NotImplementedError``
(the CLI exits 1 with one ``Error:`` line), the version stays applied,
and the rollback stops; versions already rolled back earlier in the
same run stay rolled back. Files written by ``revision --autogenerate``
follow this rule (see :doc:`autogenerate`).

Before any ``downgrade()`` runs, ``rollback`` checks that the
migration file of every version it has to undo exists; if one is
missing, nothing is rolled back. ``rollback(target="0000")`` (CLI:
``rollback --target 0000``) rolls back every applied migration.

Legacy archival on incompatible type change
-------------------------------------------

Iceberg's spec only allows *promoting* primitive types in place
(``int -> long``, ``float -> double``, ``decimal`` precision widening
at the same scale). Anything else -- ``int -> string``,
``string -> long``, ``string -> binary``, a decimal scale change,
``timestamp -> date`` -- is not a promotion. ``enforce_column`` checks
this allow-list itself rather than relying on PyIceberg, which accepts
some changes the spec forbids. Because Iceberg has no atomic "drop and
re-add a column with the same name" primitive, the helper implements
an **archive-and-recreate** flow guarded by a compensating
transaction.

Opting in is explicit: pass ``legacy_archival=True`` to
``ctx.enforce_column``. By default an incompatible type change raises
``pyiceberg.exceptions.ValidationError`` and the table is left
untouched, so the migration author sees the conflict and decides what
to do (cast in a backfill, write a data migration, drop the column,
etc.).

Behaviour matrix
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Starting state
     - Action
   * - Column absent
     - Add a new column with the requested type.
   * - Column present, equivalent type (including an identical
       struct / list / map)
     - No-op; only a changed ``doc`` is written.
   * - Column present, promotable primitive type (e.g.
       ``int -> long``)
     - In-place ``update_column``.
   * - Column present, incompatible type, ``legacy_archival=False``
     - Raise ``pyiceberg.exceptions.ValidationError``. Schema
       unchanged.
   * - Column present, incompatible type, ``legacy_archival=True``
     - **Step 1**: rename the existing column to ``<name>__legacy_N``
       (and make it optional if it was required).
       **Step 2**: add a fresh column at the original name with the
       new type and prefix the legacy column's doc with
       ``[ARCHIVED]``. If step 2 fails, the helper renames the legacy
       column back, restores its required flag (compensating
       transaction) and re-raises the step-2 error. If that
       compensation also fails, a ``RuntimeError`` names the stranded
       column.

Archival is refused with ``ValueError`` (table untouched) for a
column that the partition spec, the sort order or the identifier
fields reference, because the archived column keeps its field id and
those references would silently move to it. Archival runs only on a
genuine type incompatibility: commit errors and failing contracts
propagate without archiving anything.

The two steps are separate commits. If the process dies between them,
the column exists only as ``<name>__legacy_N``; re-running
``enforce_column`` then sees the column as absent and adds the fresh
one. The new column starts empty: existing rows read it as null until
you backfill it.

Rename collisions
~~~~~~~~~~~~~~~~~

``ctx.enforce_rename(table, old, new)`` uses the same archival path
when both ``old`` and ``new`` exist: the blocking ``new`` column is
renamed to ``<new>__legacy_N`` (and made optional), then ``old`` is
renamed to ``new`` and the archived column gets an ``[ARCHIVED]`` doc.
The same layout check applies (``ValueError`` if the partition spec,
sort order or identifier fields reference the blocking column). If the
rename fails, the archive is renamed back (compensating transaction)
and the error is re-raised.

The ``__legacy_N`` suffix
~~~~~~~~~~~~~~~~~~~~~~~~~

``N`` starts at ``1`` and increments. Every archival call scans the
current schema (or, for nested fields, the current parent struct's
children) for existing ``<leaf>__legacy_<int>`` siblings and picks
``max(N) + 1``. A column that has been re-archived three times will
therefore have siblings ``v__legacy_1``, ``v__legacy_2``, and
``v__legacy_3`` all coexisting in the schema.

Nested struct fields use dotted paths (``address.zip_code``); the
archival is scoped to the parent struct, so the archived sibling is
``address.zip_code__legacy_1`` and never collides with a top-level
``zip_code`` column or a sibling struct's ``zip_code``.

Example
~~~~~~~

.. code-block:: python

   # migrations/0042_retype_duration.py
   import pyarrow as pa

   from iceberg_ops.migration import BaseMigration, MigrationContext


   class Migration(BaseMigration):
       version = "0042"
       description = "Re-type events.duration from int to string"

       def upgrade(self, ctx: MigrationContext) -> None:
           # int -> string is not a promotion. Opt in to archival so the
           # historical int values are preserved as duration__legacy_1
           # while a fresh string column takes the canonical name.
           ctx.enforce_column(
               "events",
               "duration",
               pa.string(),
               legacy_archival=True,
           )

       def downgrade(self, ctx: MigrationContext) -> None:
           # Raised before any change: the archived column carries the
           # original data; rename it back by hand if needed.
           raise NotImplementedError(
               "Reversing a legacy_archival type change requires a manual "
               "rename of duration__legacy_1 back to duration."
           )

After ``upgrade`` runs, the schema contains both ``duration``
(string) and ``duration__legacy_1`` (int). The legacy column's doc
carries the ``[ARCHIVED]`` prefix so it is obvious at a glance which
fields are historical.

Interaction with autogenerate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``iceberg-ops revision --autogenerate`` (see :doc:`autogenerate`)
emits a type change only when it is an Iceberg promotion
(``ctx.enforce_column(target_table, "<name>", <wider type>)``), and
never adds ``legacy_archival=True``. For any other type change the
generated ``upgrade()`` raises ``NotImplementedError`` before any step
runs, with the generated calls kept as comments. The generated
``downgrade()`` of any type change raises as well, because a type
cannot be narrowed back. To finish such a file, either:

* write the step with ``legacy_archival=True`` to opt into the
  archive-and-recreate flow, or
* replace it with a hand-written data migration (see
  :doc:`data_migration`) that copies the old values into a new column
  before dropping the original.

Migration history
-----------------

The runner records history in the ``_iceberg_ops.migration_history``
Iceberg table of the same catalog (the meta-namespace is
configurable). It holds one row per ``(version, namespace)`` with the
latest status (``applied``, ``rolled_back`` or ``failed``, plus the
error text for a failure). Earlier states are not kept as rows; they
stay readable by time travel on the history table until its snapshots
expire. ``plan``, ``status`` and ``history`` only read the catalog: they
do not create the namespace or the table, which are created on the
first write. ``plan`` and ``status`` still import every migration file
(see :ref:`migration-trust-model`).

``stamp`` records versions as applied without running ``upgrade()``,
to adopt iceberg-ops on tables that already exist. It does not check
that the live schema matches what the stamped migrations would have
produced.

Concurrent runs
---------------

Concurrent runs are not guarded. Two ``migrate`` runs for one
namespace can both execute ``upgrade()`` for the same version; the
second run to record history raises
``iceberg_ops.history.ConcurrentMigrationError`` (also importable from
``iceberg_ops.runner``), and history keeps one row per
``(version, namespace)`` instead of a duplicate. That run's
``upgrade()`` may already have executed, so ``upgrade()`` must be
idempotent.

When a migration's own commit conflicts with a concurrent commit, the
runner raises ``ConcurrentMigrationError`` and records nothing for
that version, so the other run's history row is not overwritten.

Run migrations for a namespace from one job at a time (for example a
CI concurrency group), or put an external lock in front of the
runner. Runners for different namespaces can share the history table.

.. _migration-trust-model:

Trust model
-----------

Migration files are Python code, and the runner executes them.
``status``, ``current``, ``plan``, ``migrate``, ``rollback`` and
``stamp`` (and the matching ``MigrationRunner`` methods) import every
``NNNN_*.py`` file in the migrations directory, or in its
``<namespace>/`` subdirectory when that exists, before they print or
change anything. Module-level code in those files runs with the
caller's catalog credentials, so ``plan`` and ``status`` write nothing
to the catalog but are not safe to run on files you have not reviewed.
``history`` reads the history table without importing the files.
``revision --target-schema-arrow module:attr`` imports the module it
names, looking in the current directory first.

Review migration files, the ``migrations_dir`` setting of a cloned
project's ``iceberg-ops.yaml``, and modules named by
``--target-schema-arrow`` as you would any other code. Do not run these
commands on an untrusted branch with production credentials, for
example in a CI job triggered by ``pull_request_target``.

Running migrations from Python
------------------------------

The CLI (see :doc:`cli`) wraps ``MigrationRunner``. The same flow from
Python, against a throwaway in-memory catalog:

.. code-block:: python

   import tempfile
   from pathlib import Path

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.runner import MigrationRunner

   migrations_dir = Path(tempfile.mkdtemp()) / "migrations"
   migrations_dir.mkdir()
   (migrations_dir / "0001_create_orders.py").write_text(
       '''
   import pyarrow as pa

   from iceberg_ops.migration import BaseMigration, MigrationContext


   class Migration(BaseMigration):
       version = "0001"
       description = "Create orders table"

       def upgrade(self, ctx: MigrationContext) -> None:
           ctx.create_table_if_not_exists(
               "orders",
               pa.schema([pa.field("order_id", pa.int64(), nullable=False)]),
           )

       def downgrade(self, ctx: MigrationContext) -> None:
           ctx.drop_table_if_exists("orders")
   ''',
       encoding="utf-8",
   )

   catalog = CatalogFactory.build("memory")  # use your own catalog in practice
   runner = MigrationRunner(catalog, namespace="sales", migrations_dir=str(migrations_dir))

   runner.plan()  # writes nothing to the catalog, but imports every migration file
   runner.migrate()  # applies 0001 to sales.orders
   runner.status()
   runner.rollback(steps=1)  # runs 0001's downgrade()

Schema contracts
----------------

Schema contracts encode "this is what the table must look like" and
verify the live schema matches. They are useful as a CI gate or as
the target of an autogenerate run (see :doc:`autogenerate`).

Python API:

.. code-block:: python

   import pyarrow as pa
   from pyiceberg import types as ice_types

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.core.schema import ColumnSpec, SchemaContract

   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table(
       "sales.orders",
       pa.schema(
           [
               pa.field("order_id", pa.int64(), nullable=False),
               pa.field("order_total", pa.float64()),
           ]
       ),
   )

   contract = SchemaContract(
       columns=[
           ColumnSpec("order_id", ice_types.LongType(), required=True),
           ColumnSpec("order_total", ice_types.DoubleType()),
       ],
       strict=False,
   )
   contract.enforce(table)  # raises SchemaContractViolation on drift

``required=True`` means the column must be present; it does not check
nullability. Types are compared exactly, so a column that was promoted
(``int -> long``) is reported if the contract still names the old
type. ``strict=True`` also reports columns the contract does not list.

YAML contracts (PyYAML is a core dependency):

.. code-block:: yaml

   # contracts/orders.yaml
   strict: false
   columns:
     - name: order_id
       type: long
       required: true
     - name: order_total
       type: double
     - name: created_at
       type: timestamptz

.. code-block:: python

   # illustrative: needs contracts/orders.yaml and a loaded table
   from iceberg_ops.core.schema import SchemaContract

   contract = SchemaContract.from_yaml("contracts/orders.yaml")
   contract.enforce(table)

The file is read as UTF-8 and validated: ``columns`` must be a
non-empty list with unique names, and ``strict`` / ``required`` must
be booleans. Supported types (case-insensitive): ``boolean``, ``int``
(or ``integer``), ``long``, ``float``, ``double``, ``string``,
``binary``, ``date``, ``time``, ``timestamp``, ``timestamptz``,
``uuid``, ``decimal(P,S)``, ``fixed[N]``, and the other Iceberg
primitive type strings such as ``timestamp_ns``. Nested types are not
supported.

See :doc:`api/migration`, :doc:`api/runner` and :doc:`api/schema` for
the full reference.
