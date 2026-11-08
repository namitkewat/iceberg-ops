Maintenance
===========

.. note::

   **Basic, frozen in scope.** These are basic helpers for self-managed
   tables on the local filesystem: compaction and orphan cleanup
   support local (``file://``) table locations only, and delete files
   and v3 deletion vectors are out of scope. The helpers receive bug
   fixes only, and, like the rest of the Alpha API, may change in any
   release before 1.0. See :doc:`/maintenance` for the known
   limitations.

Storage maintenance operations for Iceberg tables. Only needed when
your catalog does not provide managed maintenance. The package
re-exports its public functions and result dataclasses from the four
submodules below; tests and downstream code can import the names from
either the package
(``from iceberg_ops.maintenance import compact_small_files_streaming``)
or the submodule -- both bindings refer to the same object.

Package
-------

.. automodule:: iceberg_ops.maintenance
   :no-members:

Compaction
----------

.. automodule:: iceberg_ops.maintenance.compaction
   :members:
   :undoc-members:

Snapshot Expiry
---------------

.. automodule:: iceberg_ops.maintenance.snapshots
   :members:
   :undoc-members:

Orphan Cleanup
--------------

.. automodule:: iceberg_ops.maintenance.orphans
   :members:
   :undoc-members:

Health Report
-------------

.. automodule:: iceberg_ops.maintenance.advisor
   :members:
   :undoc-members:
