Polaris
=======

.. note::

   **Experimental.** The Apache Polaris policy executor has not been
   verified against a live Polaris server. :class:`PolarisClient
   <iceberg_ops.polaris.client.PolarisClient>` does not yet call the
   Polaris 1.x policy endpoints, so fetching policies from a released
   Polaris server fails. Of the built-in policy types only
   ``system.snapshot-expiry`` runs, and only with ``"enable": true``;
   the others are skipped unless you register a custom handler. The
   API may change in any release.

The ``iceberg_ops.polaris`` package re-exports its public surface
from the three submodules below. Tests and downstream code can
import the names from either the package
(``from iceberg_ops.polaris import PolarisClient``) or the
submodule -- both bindings refer to the same object.

See :doc:`/polaris` for the user-facing guide.

Package
-------

.. automodule:: iceberg_ops.polaris
   :no-members:

Client
------

.. automodule:: iceberg_ops.polaris.client
   :members:
   :show-inheritance:

Policies
--------

.. automodule:: iceberg_ops.polaris.policies
   :members:
   :show-inheritance:

Dispatcher
----------

.. automodule:: iceberg_ops.polaris.dispatcher
   :members:
   :show-inheritance:
