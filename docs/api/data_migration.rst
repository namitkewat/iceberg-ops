Data Migration
==============

.. note::

   **Experimental.** The data migrator is a companion to the schema
   migration workflow, with documented limits: the destination table
   must already exist; ``overwrite_partition`` needs an
   identity-partitioned destination and a migration that fits in one
   commit; ``batch_rows`` bounds the rows in each commit, not peak
   memory; and the ``sample`` verify method can report a false
   mismatch on larger tables. The API may change in any release. See
   :doc:`/data_migration` for details.

The ``iceberg_ops.data_migration`` package re-exports its public
classes and functions from the three submodules below. Tests and
downstream code can import the names from either the package
(``from iceberg_ops.data_migration import DataMigrator``) or the
submodule -- both bindings refer to the same object.

Package
-------

.. automodule:: iceberg_ops.data_migration
   :no-members:

Schema Reconciler
-----------------

.. automodule:: iceberg_ops.data_migration.reconciler
   :members:
   :show-inheritance:

Checkpoint Store
----------------

.. automodule:: iceberg_ops.data_migration.checkpoint
   :members:
   :show-inheritance:

Migrator
--------

.. automodule:: iceberg_ops.data_migration.migrator
   :members:
   :show-inheritance:
