"""
iceberg_ops.polaris
===================
Apache Polaris policy executor (experimental).

Polaris 1.0 added a *policy framework*: a way to declare table-level
maintenance rules (snapshot expiry, compaction, orphan-file removal)
at the catalog level via REST endpoints. **Polaris itself does not
execute the policies** - an external worker has to fetch attached
policies and invoke the matching maintenance operations against the
underlying Iceberg tables.

This subpackage is a pure-Python, single-process prototype of that
worker. It dispatches ``system.snapshot-expiry`` policies to
:func:`iceberg_ops.maintenance.expire_snapshots`; every other policy
type is skipped.

Experimental
------------
Not verified against a live Apache Polaris server. In this release
:class:`PolarisClient` does not call the Polaris 1.x policy endpoints
(``/api/catalog/polaris/v1/{prefix}/...``), so fetching policies from
a real server fails with :class:`PolarisError`.
:func:`dispatch_policy` works on :class:`Policy` objects you build
yourself. The API may change in any release.

Public API
----------
:class:`PolarisClient`
    Thin HTTP client for Polaris policy endpoints (paths unverified,
    see above). Lazy-imports ``requests``, supports OAuth2
    client-credentials auth or a pre-fetched static bearer token.

:exc:`PolarisError`
    Raised on HTTP failures or unparseable responses.

:class:`Policy`
    Dataclass representing a parsed Polaris policy (``name``,
    ``policy_type``, ``description``, ``content``, ``inheritable``,
    ``version``).

:func:`parse_policy`
    Parse a raw policy dict into a :class:`Policy`. ``content`` is a
    JSON string per the Polaris policy API; an already-decoded object
    is accepted too.

:func:`fetch_table_policies`
    Convenience wrapper around
    :meth:`PolarisClient.list_applicable_policies_for_table` that
    returns ``list[Policy]`` directly.

:func:`dispatch_policy`
    Map one policy to its matching maintenance call against an
    Iceberg :class:`pyiceberg.table.Table`. Only
    ``system.snapshot-expiry`` runs, and only with ``"enable": true``.
    Returns a :class:`DispatchResult`.

:func:`dispatch_policies`
    Run :func:`dispatch_policy` over a list of policies and collect
    per-policy results. Continues past errors so one bad policy does
    not block the rest of the batch.

:class:`DispatchResult`
    Per-policy outcome (``policy_name``, ``policy_type``, ``op``,
    ``success``, ``skipped``, ``skip_reason``, ``details``,
    ``error``).

CLI surface
-----------
The ``iceberg-ops run-policies`` CLI subcommand consumes this package -
see :func:`iceberg_ops.cli.run_policies` for the entry point.

Lazy imports
------------
The ``requests`` dependency is lazy-imported inside ``PolarisClient``
so installing the base package without ``[polaris]`` extras still
imports ``iceberg_ops.polaris`` cleanly. Calls that need HTTP will
raise a clear :class:`ImportError` pointing at
``pip install 'iceberg-ops[polaris]'``.

Test seam
---------
Tests for downstream integrators can drive the client against
:class:`iceberg_ops.testing.FakeSession` instead of a live HTTP
service - the client accepts any ``request`` / ``post`` -compatible
session via the ``session=`` kwarg.
"""

from __future__ import annotations

from iceberg_ops.polaris.client import PolarisClient, PolarisError
from iceberg_ops.polaris.dispatcher import (
    DispatchResult,
    dispatch_policies,
    dispatch_policy,
)
from iceberg_ops.polaris.policies import (
    Policy,
    fetch_table_policies,
    parse_policy,
)

__all__ = [
    "DispatchResult",
    "PolarisClient",
    "PolarisError",
    "Policy",
    "dispatch_policies",
    "dispatch_policy",
    "fetch_table_policies",
    "parse_policy",
]
