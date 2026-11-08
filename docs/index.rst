iceberg-ops
===========

**Alembic-style schema migrations for Apache Iceberg.** Versioned,
reviewable table changes: numbered ``upgrade()`` / ``downgrade()``
revisions, autogenerate, ``stamp``, and a migration history stored as an
Iceberg table in your own catalog. Pure Python on PyIceberg: no Spark,
no JVM.

iceberg-ops brings the migration workflow that application teams use
for relational databases to Apache Iceberg tables. Each schema change is
a numbered Python file that you review in a pull request, apply with
``migrate`` and undo with ``rollback``. The status of each migration is
recorded in a history table that lives next to your data.

The CLI is ``iceberg-ops``; the Python package is ``iceberg_ops``.

60-second demo
--------------

.. code-block:: bash

   pip install "iceberg-ops[sqlite]"
   mkdir demo && cd demo
   iceberg-ops init
   iceberg-ops revision -m "create orders"   # then fill in upgrade() / downgrade()
   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales migrate

   # Describe the next schema in contracts/orders.yaml, then let iceberg-ops write the migration:
   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales \
     revision -m "rename total, add status" --autogenerate \
     --target-table orders --target-schema-yaml contracts/orders.yaml \
     --rename-hint order_total:order_amount

   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales plan
   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales migrate
   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales status
   iceberg-ops --catalog sqlite --db-path demo.db --warehouse warehouse --namespace sales rollback

:doc:`quickstart` walks through the same steps with the contents of the
migration file and the schema contract.

What iceberg-ops is, and isn't
------------------------------

**It is** a migration framework (files, history, rollback, ``stamp``,
autogenerate) for Iceberg schemas, partition specs, sort orders and
table properties, shipped as a Python library and CLI on top of
PyIceberg. The Python API takes any PyIceberg catalog; the CLI opens a
local SQLite catalog or any catalog defined in ``.pyiceberg.yaml`` or
``PYICEBERG_CATALOG__*`` environment variables.

**It isn't:**

* a query engine or a managed maintenance service;
* a merge-on-read writer: the rewrites it performs are copy-on-write,
  and it does not write delete files or deletion vectors. Compaction
  does not support tables that already contain them and does not check
  for them, so do not run it on such tables;
* a lock service: concurrent runs for one namespace are not guarded. A
  second concurrent run is detected when it records history
  (``ConcurrentMigrationError``), but its ``upgrade()`` may already have
  run. Run migrations for a namespace from a single job;
* a partition-rewrite engine: ``migrate-data --write-mode
  overwrite_partition`` is refused unless the whole migration fits in
  one commit.

Also included, secondary to the migration workflow:

* **Cross-table data migration (experimental):** ``iceberg-ops
  migrate-data`` streams rows between Iceberg tables, in one catalog or
  across catalogs. See :doc:`data_migration`.
* **Maintenance helpers (basic):** snapshot expiry, a health report, and
  compaction and orphan removal for tables on the local filesystem. See
  :doc:`maintenance`.
* **Apache Polaris policy executor (experimental):** runs
  ``system.snapshot-expiry`` policies; it does not yet work against a
  released Polaris server. See :doc:`polaris`.

:doc:`compatibility` lists the supported versions, catalogs and limits.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   quickstart
   schema_migrations
   autogenerate
   architecture
   compatibility
   data_migration
   maintenance
   polaris
   cli

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

.. toctree::
   :maxdepth: 1
   :caption: Project

   changelog

Project links
-------------

* `Source code <https://github.com/namitkewat/iceberg-ops>`__ and
  `issues <https://github.com/namitkewat/iceberg-ops/issues>`__
* `Contributing <https://github.com/namitkewat/iceberg-ops/blob/main/CONTRIBUTING.md>`__
* `Roadmap <https://github.com/namitkewat/iceberg-ops/blob/main/ROADMAP.md>`__
* `Security policy <https://github.com/namitkewat/iceberg-ops/blob/main/SECURITY.md>`__
* `Code of conduct <https://github.com/namitkewat/iceberg-ops/blob/main/CODE_OF_CONDUCT.md>`__

For coding agents:

* `AGENTS.md <https://github.com/namitkewat/iceberg-ops/blob/main/AGENTS.md>`__
  is the guide for agents that change the repository.
* `agent-skills/iceberg-ops-migrations/SKILL.md
  <https://github.com/namitkewat/iceberg-ops/blob/main/agent-skills/iceberg-ops-migrations/SKILL.md>`__
  is a skill that teaches a coding agent to write, review and run
  iceberg-ops migrations.
* `llms.txt <https://namitkewat.github.io/iceberg-ops/llms.txt>`__ and
  `llms-full.txt <https://namitkewat.github.io/iceberg-ops/llms-full.txt>`__
  are a plain-text index of this site and the full text of every page.

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

License and trademarks
----------------------

iceberg-ops is released under the `MIT License
<https://github.com/namitkewat/iceberg-ops/blob/main/LICENSE>`__.

Apache Iceberg, Iceberg, Apache Polaris and Apache are trademarks of the
Apache Software Foundation. iceberg-ops is not affiliated with or
endorsed by the ASF.
