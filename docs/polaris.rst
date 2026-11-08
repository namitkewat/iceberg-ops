Polaris Policy Executor (experimental)
======================================

.. warning::

   Experimental. Not verified against a live Apache Polaris server. In
   this release ``PolarisClient`` does not call the Polaris 1.x policy
   endpoints (``/api/catalog/polaris/v1/{prefix}/...``), so fetching
   from a real server fails with ``PolarisError``. ``dispatch_policy``
   works on ``Policy`` objects you build yourself. Only
   ``system.snapshot-expiry`` is executed. The API may change in any
   release.

Apache Polaris 1.x has a *policy framework*: a way to declare
table-level maintenance rules (snapshot expiry, compaction,
orphan-file removal) at the catalog level via REST endpoints.
Polaris stores and resolves the policies; an external worker has to
fetch the policies attached to a table and run the matching
maintenance operations against it.

The ``iceberg_ops.polaris`` subpackage is a prototype of that worker.
Today it executes ``system.snapshot-expiry`` only, through
:py:func:`iceberg_ops.maintenance.expire_snapshots`.

Pieces
------

* :py:class:`iceberg_ops.polaris.PolarisClient` -- thin HTTP client for
  Polaris policy endpoints (paths unverified, see the warning above).
  Lazy-imports ``requests`` (``pip install "iceberg-ops[polaris]"``);
  supports OAuth2 client-credentials auth or a pre-fetched static
  bearer token.
* :py:exc:`iceberg_ops.polaris.PolarisError` -- raised on HTTP failures
  or unparseable responses.
* :py:class:`iceberg_ops.polaris.Policy` -- frozen dataclass with a
  parsed view of one Polaris policy.
* :py:func:`iceberg_ops.polaris.parse_policy` -- decodes ``content``
  (a JSON string per the Polaris policy API; an already-decoded object
  is accepted too).
* :py:func:`iceberg_ops.polaris.fetch_table_policies` -- convenience
  wrapper around ``list_applicable_policies_for_table`` that parses
  the response into a list of ``Policy``.
* :py:func:`iceberg_ops.polaris.dispatch_policy` /
  :py:func:`iceberg_ops.polaris.dispatch_policies` -- map one or many
  policies to the matching maintenance op against an Iceberg table.
* :py:class:`iceberg_ops.polaris.DispatchResult` -- per-policy outcome
  (``policy_name``, ``policy_type``, ``op``, ``success``, ``skipped``,
  ``skip_reason``, ``details``, ``error``).

Authentication
--------------

* The bearer token -- a static ``token=`` or one fetched with
  ``oauth2_credentials=`` -- is fetched once and cached for the
  client's lifetime. There is no refresh on ``401`` or on
  ``expires_in``.
* Polaris's default token lifetime is one hour (``PT1H``), so create
  one client per run.
* With ``oauth2_credentials=`` the token URL is ``base_url +
  "/api/catalog/v1/oauth/tokens"``. That is Polaris's internal
  authentication endpoint: still served by Polaris 1.8.0, but
  deprecated in the Iceberg REST spec.
* For external / OIDC identity providers, fetch the token yourself and
  pass it via ``token=`` (or ``--polaris-token`` on the CLI).
* Token-endpoint errors never include the response's values: a
  response without ``access_token`` is reported by its key names and
  its ``error`` / ``error_description`` fields only.
* Surrounding whitespace in ``token=`` (for example a trailing CR/LF
  from a token file) is stripped. A token that is empty or still
  contains whitespace or control characters raises ``ValueError``
  without echoing it.
* A request that fails before a response arrives raises
  ``PolarisError`` naming the exception type and URL; the bearer token
  and the client secret are replaced with ``***`` in the message.

Policy types
------------

Every ``system.*`` policy that would run requires ``"enable": true``
(the JSON boolean). A missing ``enable``, ``false``, or a non-boolean
such as ``"true"`` or ``1`` skips the policy.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Policy type
     - Behaviour
   * - ``system.snapshot-expiry``
     - Runs ``expire_snapshots``. See the content keys below.
   * - ``system.metadata-compaction``
     - Skipped: manifest rewrite is not implemented, and iceberg-ops
       no longer runs a data-file rewrite for this type.
   * - ``system.data-compaction``
     - Skipped: not executed from policies in this release
       (compaction supports ``file://`` tables only).
   * - ``system.orphan-file-removal``
     - Skipped: never executed from policy automation in this release.
   * - anything else
     - Skipped as unsupported, unless a custom handler is registered.

Skipped policies are returned with ``DispatchResult(skipped=True,
op=None, skip_reason=...)`` so a batch run can still process the rest.

Snapshot-expiry content
~~~~~~~~~~~~~~~~~~~~~~~

The content follows the Polaris ``system.snapshot-expiry`` schema
(version ``2025-02-03``):

.. code-block:: json

   {
     "version": "2025-02-03",
     "enable": true,
     "config": {
       "max_snapshot_age_days": 2,
       "min_snapshot_to_keep": 1,
       "max_ref_age_days": 3
     }
   }

* Only ``version``, ``enable`` and ``config`` are allowed at the top
  level. Any other key (for example a legacy ``max_snapshot_age_ms``)
  fails the dispatch instead of running on defaults. A ``version``
  other than ``2025-02-03`` fails too.
* ``config.max_snapshot_age_days`` -- maximum snapshot age in days
  (fractional days allowed); passed as ``max_age_hours = days * 24``.
* ``config.min_snapshot_to_keep`` -- integer >= 1; passed as
  ``min_snapshots``.
* Both accept an int, a float, or a numeric string. Booleans, NaN,
  infinities and negative values fail the dispatch.
* ``config.max_ref_age_days`` is ignored with a note: branch and tag
  heads are never expired.
* Other ``config`` keys are ignored with a note in
  ``DispatchResult.details["notes"]``.
* Missing keys are not passed, so ``expire_snapshots`` uses the
  table's ``history.expire.*`` properties, then its defaults (5 days,
  1 snapshot).

Custom policy types
-------------------

Polaris cannot store custom policy types, so the ``handlers`` kwarg is
for other policy sources, or for overriding the built-in behaviour of a
type:

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.polaris import Policy, dispatch_policy

   # A local table for the handler to act on.
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   table.append(pa.table({"order_id": [1, 2, 3]}))


   def handle_row_audit(table, policy, *, dry_run):
       # Do the work; the returned dict becomes DispatchResult.details.
       return {"rows": table.scan().count(), "dry_run": dry_run}


   policy = Policy(name="audit", policy_type="my.org.row-audit", content={})
   result = dispatch_policy(
       table,
       policy,
       handlers={"my.org.row-audit": handle_row_audit},
   )
   print(result.op, result.success, result.details)

The handler receives ``(table, policy, *, dry_run)`` and returns a dict
of details (``None`` counts as ``{}``; any other non-dict fails the
dispatch) or raises on failure. ``handlers`` keys take precedence over
the built-in handling, including the always-skipped system types, but a
handler for a ``system.*`` type still runs only with ``"enable": true``.

CLI usage
---------

.. code-block:: bash

   # illustrative: the policy fetch fails against a released Polaris server
   export ICEBERG_OPS_POLARIS_CLIENT_ID="$POLARIS_ID"
   export ICEBERG_OPS_POLARIS_CLIENT_SECRET="$POLARIS_SECRET"

   iceberg-ops run-policies \
     --polaris-url https://polaris.example.com \
     --polaris-catalog mycat \
     --target-namespace sales --target-table orders \
     --catalog sqlite --db-path catalog.db --warehouse warehouse \
     --dry-run

The command is experimental for the same reason as the client: against
a released Polaris server the policy fetch fails. It prints a warning
saying so on every run.

* Credentials: provide exactly one of a bearer token
  (``ICEBERG_OPS_POLARIS_TOKEN`` or ``--polaris-token``) or the OAuth2
  client-credentials pair (``ICEBERG_OPS_POLARIS_CLIENT_ID`` +
  ``ICEBERG_OPS_POLARIS_CLIENT_SECRET``, or ``--polaris-client-id`` +
  ``--polaris-client-secret``). Prefer the environment variables:
  command-line arguments are visible in process listings and CI logs.
  Auth validation runs before any HTTP or catalog work; a missing or
  doubled credential is a usage error (exit 2).
* A ``--polaris-url`` that uses plain ``http`` to a host other than
  ``localhost`` / ``127.0.0.1`` / ``::1`` prints a warning, because
  credentials are then sent unencrypted.
* The table the policies act on is loaded with ``--catalog`` (plus
  ``--db-path`` / ``--warehouse`` for sqlite) or with
  ``--catalog-name NAME`` for a catalog defined in ``.pyiceberg.yaml``
  or ``PYICEBERG_CATALOG__<NAME>__*`` environment variables. One of
  the two is required.
* ``--dry-run`` forwards ``dry_run=True`` to every maintenance op so
  the table isn't mutated.
* Exit codes: 1 when the Polaris request fails, 2 when any policy
  dispatch fails (skipped policies do not count), 0 otherwise.

Python API
----------

Build the policies yourself and dispatch them:

.. code-block:: python

   import pyarrow as pa

   from iceberg_ops.core.catalog import CatalogFactory
   from iceberg_ops.polaris import Policy, dispatch_policies

   # A local table with a few snapshots.
   catalog = CatalogFactory.build("memory")
   catalog.create_namespace("sales")
   table = catalog.create_table("sales.orders", pa.schema([("order_id", pa.int64())]))
   for i in range(3):
       table.append(pa.table({"order_id": [i]}))

   policies = [
       Policy(
           name="expire",
           policy_type="system.snapshot-expiry",
           content={
               "enable": True,
               "config": {"max_snapshot_age_days": 7, "min_snapshot_to_keep": 5},
           },
       ),
       Policy(
           name="compact",
           policy_type="system.data-compaction",
           content={"enable": True},
       ),  # skipped: not executed in this release
   ]
   results = dispatch_policies(table, policies, dry_run=True)
   for r in results:
       if r.success:
           print(f"[ok] {r.policy_name} -> {r.op}")
       elif r.skipped:
           print(f"[skip] {r.policy_name}: {r.skip_reason}")
       else:
           print(f"[x] {r.policy_name}: {r.error}")

Testing seam
------------

Callers driving custom Polaris workflows can plug a deterministic stub
in via the ``session=`` kwarg:

.. code-block:: python

   from iceberg_ops.polaris import PolarisClient
   from iceberg_ops.testing import FakeResponse, FakeSession

   sess = FakeSession()
   sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))

   client = PolarisClient(
       "https://polaris.example.com",
       "mycat",
       token="t",
       session=sess,
   )
   client.list_policies("sales")  # no real HTTP call
   print(sess.calls[0]["method"], sess.calls[0]["url"])

The ``FakeSession`` records every call in ``sess.calls`` and raises
``AssertionError`` on unmatched requests, so tests fail loudly rather
than silently producing surprising output.

See :doc:`api/polaris` for the full reference and
:doc:`api/testing` for the public test seam.
