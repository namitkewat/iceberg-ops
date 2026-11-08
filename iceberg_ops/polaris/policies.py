"""
iceberg_ops.polaris.policies
============================
Policy dataclass + parsing helpers for the Polaris policy framework.

This module is the layer between the raw HTTP responses returned by
:class:`~iceberg_ops.polaris.client.PolarisClient` and the dispatcher
(:mod:`iceberg_ops.polaris.dispatcher`). It owns:

* the :class:`Policy` dataclass - a normalised view of a Polaris
  policy that downstream code can rely on;
* :func:`parse_policy` - decodes ``content`` (a JSON string per the
  Polaris policy API; an already-decoded object is accepted too) and
  emits a :class:`Policy`;
* :func:`fetch_table_policies` - convenience wrapper that calls
  :meth:`PolarisClient.list_applicable_policies_for_table` and
  parses the response into a list of :class:`Policy`.

Experimental
------------
Not verified against a live Apache Polaris server. The parser reads
the policy type from a ``type`` key, and :func:`fetch_table_policies`
relies on :class:`~iceberg_ops.polaris.client.PolarisClient`, whose
endpoint paths do not match the Polaris 1.x policy API.

Why a dataclass and not just dicts?
-----------------------------------
The dispatcher reads ``policy.policy_type`` and ``policy.content``
against known schemas. Going through a dataclass:

* gives a single place to decode ``content`` from its JSON-string
  form;
* makes the contract typed at the dispatcher boundary so a
  malformed-policy bug fails at the parser, not deep inside the
  dispatcher;
* lets tests construct fake :class:`Policy` instances directly
  without touching HTTP at all.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from iceberg_ops.polaris.client import PolarisClient, PolarisError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Policy:
    """A parsed Polaris policy (experimental).

    Attributes
    ----------
    name : str
        Policy name within its namespace.
    policy_type : str
        Polaris policy type identifier - e.g.
        ``"system.snapshot-expiry"``,
        ``"system.metadata-compaction"``. Custom (non-``system.*``)
        types are passed through verbatim; the dispatcher will skip
        anything it does not recognise. (Named ``policy_type`` rather
        than ``type`` so the attribute does not shadow the ``type``
        builtin and so Sphinx's cross-references stay unambiguous.)
    content : dict
        Parsed policy parameters. Always a dict - :func:`parse_policy`
        decodes the JSON-string form the Polaris API uses.
    description : str or None
        Free-text description configured on the policy.
    inheritable : bool
        Whether the policy applies to nested namespaces / tables.
        Mostly informational at the executor level - Polaris itself
        resolves inheritance and returns the effective set via the
        applicable-policies endpoints.
    version : int
        Polaris's monotonic version counter for the policy.
    """

    name: str
    policy_type: str
    content: dict[str, Any]
    description: str | None = None
    inheritable: bool = False
    version: int = 0


def parse_policy(raw: dict[str, Any]) -> Policy:
    """Parse a raw Polaris policy dict into a :class:`Policy`.

    The ``content`` field is handled as follows:

    * a string (the form the Polaris policy API uses) - parsed via
      :mod:`json`; it must decode to an object. An empty string
      yields an empty dict.
    * a dict - already decoded (for example a hand-built policy).
      Used as-is.
    * missing - an empty dict.

    Anything else raises :class:`PolarisError` so the caller has a
    single exception type to catch.

    Parameters
    ----------
    raw : dict
        One element of the list returned by
        :meth:`~iceberg_ops.polaris.client.PolarisClient.list_policies`
        or one of the applicable-policies endpoints.

    Returns
    -------
    Policy
        Parsed policy.

    Raises
    ------
    PolarisError
        On missing required fields (``name``, ``type``) or
        unparseable ``content``.
    """
    if not isinstance(raw, dict):
        raise PolarisError(f"parse_policy expected a dict, got {type(raw).__name__}")
    name = raw.get("name")
    policy_type = raw.get("type")
    if not name or not policy_type:
        raise PolarisError(f"Polaris policy missing required field(s): {raw!r}")
    content_raw = raw.get("content", {})
    if isinstance(content_raw, dict):
        content = content_raw
    elif isinstance(content_raw, str):
        # The Polaris policy API carries content as a JSON-encoded
        # string. An empty string yields an empty dict.
        if not content_raw.strip():
            content = {}
        else:
            try:
                parsed = json.loads(content_raw)
            except json.JSONDecodeError as exc:
                raise PolarisError(
                    f"Polaris policy {name!r} has unparseable JSON content: {exc.msg}"
                ) from exc
            if not isinstance(parsed, dict):
                raise PolarisError(
                    f"Polaris policy {name!r} content parsed to "
                    f"{type(parsed).__name__}, expected object."
                )
            content = parsed
    else:
        raise PolarisError(
            f"Polaris policy {name!r} content has unexpected type "
            f"{type(content_raw).__name__}; expected dict or JSON string."
        )

    return Policy(
        name=str(name),
        policy_type=str(policy_type),
        content=content,
        description=raw.get("description"),
        inheritable=bool(raw.get("inheritable", False)),
        version=int(raw.get("version", 0)),
    )


def fetch_table_policies(
    client: PolarisClient,
    namespace: str,
    table: str,
) -> list[Policy]:
    """Fetch and parse all policies applicable to ``namespace.table``.

    Combines :meth:`PolarisClient.list_applicable_policies_for_table`
    with :func:`parse_policy` so the caller gets a list of
    :class:`Policy` ready to feed to the dispatcher.

    Policies that fail to parse are dropped with a warning so a
    single malformed entry doesn't block dispatch of the rest.

    Parameters
    ----------
    client : PolarisClient
        Client connected to the Polaris instance.
    namespace : str
        Namespace name (dot-separated for multi-level).
    table : str
        Table name within the namespace.

    Returns
    -------
    list of Policy
        Parsed policies, with any unparseable entries silently
        dropped (with a logged warning).
    """
    raw = client.list_applicable_policies_for_table(namespace, table)
    policies: list[Policy] = []
    for entry in raw:
        try:
            policies.append(parse_policy(entry))
        except PolarisError as exc:
            logger.warning(
                "Skipping unparseable policy in %s.%s: %s",
                namespace,
                table,
                exc,
            )
    return policies


# Re-export so callers can ``from iceberg_ops.polaris.policies import Policy``
# without also having to import PolarisError from .client.
__all__ = [
    "Policy",
    "fetch_table_policies",
    "parse_policy",
]
