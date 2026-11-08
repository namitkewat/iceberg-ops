CLI Reference
=============

iceberg-ops ships one console script, ``iceberg-ops``. Global options
(catalog, namespace, paths, config file) go before the command; each
command's own options go after it:

.. code-block:: bash

   iceberg-ops [GLOBAL OPTIONS] COMMAND [OPTIONS]
   iceberg-ops --catalog sqlite --namespace sales migrate --target 0002
   iceberg-ops --version

The :ref:`command reference <cli-command-reference>` at the end of this
page is generated from the installed Click commands, so it shows the
same options and help text as ``iceberg-ops COMMAND --help``.

The commands fall into three groups:

* **Project commands** -- ``status``, ``current``, ``migrate``,
  ``rollback``, ``stamp``, ``history``, ``plan``. They need a catalog
  and ``--namespace``, from flags or the config file.
* **Scaffolding** -- ``init`` and ``revision`` need no catalog.
  ``revision --autogenerate`` does, because it loads the existing table
  (see :doc:`autogenerate`).
* **Standalone commands** -- ``migrate-data`` (see
  :doc:`data_migration`) and ``run-policies`` (experimental, see
  :doc:`polaris`) take their own catalog options; the global catalog
  options and the config file's catalog settings do not apply to them.

Every project command except ``history`` imports and executes the
migration files it discovers, with your catalog credentials, so
``plan`` and ``status`` are not safe on files you have not reviewed.
``revision --target-schema-arrow`` imports the module it names. See
:ref:`migration-trust-model`.

Choosing a catalog
------------------

``--catalog sqlite``
   A local SQLite catalog. Without ``--db-path`` / ``--warehouse`` it
   lives in ``local_data/iceberg_catalog.db`` and
   ``local_data/iceberg_warehouse`` next to the config file, or in the
   current directory when there is no config file.

``--catalog-name NAME``
   A catalog defined in ``.pyiceberg.yaml`` or in
   ``PYICEBERG_CATALOG__<NAME>__*`` environment variables, opened with
   :func:`pyiceberg.catalog.load_catalog`.

``--catalog glue|rest|hive|s3tables``
   Loads the catalog named after the kind in the same way, so
   ``PYICEBERG_CATALOG__REST__URI`` configures ``--catalog rest``.
   Combined with ``--catalog-name``, it opens that named catalog as
   that kind.

The CLI does not accept the ``memory`` kind: its state is gone when
the command exits. ``--db-path`` applies to ``sqlite`` only, and a
named catalog cannot use the ``sqlite`` kind.

Configuration file
------------------

By default the CLI reads ``iceberg-ops.yaml`` from the current
directory; ``--config PATH`` reads another file. A ``--config`` path
that does not exist is a usage error, except for ``init``, which
creates it. ``iceberg-ops init`` writes a commented starter file; a
minimal one looks like this:

.. code-block:: yaml

   catalog: sqlite
   namespace: sales

Rules:

* The keys are ``catalog``, ``catalog_name``, ``namespace``,
  ``db_path``, ``warehouse`` and ``migrations_dir``. Every value must
  be a string, so quote numbers (``namespace: "2024"``). Unknown keys
  print a warning and are ignored.
* Command-line flags override the file. A ``--catalog-name`` flag
  replaces the file's ``catalog`` and ``db_path``; a ``--catalog`` flag
  replaces the file's ``catalog_name``. The file's ``db_path`` and
  ``warehouse`` apply only when the flags select the same catalog as
  the file.
* Setting both ``catalog: sqlite`` and ``catalog_name`` in the file is
  a usage error. To use a named catalog, delete the ``catalog: sqlite``
  line that ``init`` writes.
* Relative ``db_path``, ``migrations_dir`` and (for ``sqlite``)
  ``warehouse`` values resolve against the directory that holds the
  file. Relative paths given as flags resolve against the current
  directory. ``migrations/`` defaults to sitting next to the file.
* The file is read as UTF-8; a byte-order mark is allowed.

Exit codes
----------

.. list-table::
   :header-rows: 1
   :widths: 10 90

   * - Code
     - Meaning
   * - ``0``
     - Success.
   * - ``1``
     - A runtime failure, reported as one ``Error:`` line instead of a
       traceback: the catalog cannot be opened (missing extra, missing
       property, connection, permission or server error), a table or
       namespace is missing, a commit conflicts, a migration fails or
       cannot be loaded, a ``downgrade()`` raises
       ``NotImplementedError``, a checkpoint is corrupt or already
       completed, a revision file already exists, or ``migrate-data
       --verify partition_hash`` runs on a destination with
       non-identity partitions (``Verify error:``). ``run-policies``
       prints ``Polaris error:`` when the Polaris request fails. Set
       ``ICEBERG_OPS_TRACEBACK=1`` to see the original traceback
       instead of the ``Error:`` line.
   * - ``2``
     - A usage error: an unknown or malformed option, a bad config file,
       a missing catalog or ``--namespace``, a missing migrations
       directory, a ``--target`` that names no discovered migration,
       ``rollback --steps`` combined with ``--target``, a source and
       destination that are the same table in ``migrate-data``, an
       invalid Polaris token in ``run-policies``, or, for
       ``revision --autogenerate``, a target table that does not exist
       or a target schema that cannot be read or compared. Also
       returned when ``migrate-data --verify`` reports a mismatch and
       when ``run-policies`` has at least one failed policy.

``--target`` takes a migration version with or without leading zeros
(``2`` means ``0002``); ``rollback --target 0`` rolls back every
applied migration. ``rollback`` undoes one migration by default;
``--steps`` must be at least 1.

.. _cli-command-reference:

Command reference
-----------------

.. click:: iceberg_ops.cli:main
   :prog: iceberg-ops
   :nested: full
