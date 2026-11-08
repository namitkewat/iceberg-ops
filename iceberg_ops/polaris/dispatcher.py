"""
iceberg_ops.polaris.dispatcher
==============================
Maps a Polaris :class:`Policy` to the matching maintenance operation
against an Iceberg :class:`pyiceberg.table.Table`.

Experimental
------------
The dispatcher has not been verified against a live Apache Polaris
server. It works on :class:`Policy` objects you build yourself (or
parse with :func:`~iceberg_ops.polaris.parse_policy`).

Supported policy types
----------------------
One ``system.*`` policy type is executed:

* ``system.snapshot-expiry`` ->
  :func:`iceberg_ops.maintenance.snapshots.expire_snapshots`

The other Polaris 1.x system types are recognised and always skipped:

* ``system.metadata-compaction`` - manifest rewrite is not
  implemented.
* ``system.data-compaction`` - compaction supports ``file://`` tables
  only and is not executed from policies in this release.
* ``system.orphan-file-removal`` - Polaris has defined this type since
  1.0; orphan removal is never executed from policy automation in this
  release.

Every other type, including future ``system.*`` types, is skipped as
unsupported unless a custom handler is registered for it. A skipped
policy is returned as ``DispatchResult(skipped=True, op=None,
skip_reason=...)`` so a batch run can still process the rest.

Enable gate
-----------
Every ``system.*`` policy that would run (built-in or overridden by a
custom handler) runs only when ``content["enable"]`` is the boolean
``True``. A missing ``enable``, ``False``, or a non-boolean value such
as ``"true"`` or ``1`` skips the policy. Non-``system.*`` custom types
are not gated.

Snapshot expiry content
-----------------------
The content follows the Polaris ``system.snapshot-expiry`` schema
(version ``2025-02-03``)::

    {
      "version": "2025-02-03",
      "enable": true,
      "config": {
        "max_snapshot_age_days": 2,
        "min_snapshot_to_keep": 1,
        "max_ref_age_days": 3
      }
    }

* Top-level keys other than ``version``, ``enable`` and ``config`` are
  rejected (the dispatch fails rather than running on defaults). A
  ``version`` other than ``2025-02-03`` is rejected too.
* ``config.max_snapshot_age_days`` -> ``max_age_hours`` (days x 24,
  fractional days allowed).
* ``config.min_snapshot_to_keep`` -> ``min_snapshots`` (integer >= 1).
* Both accept an int, a float, or a numeric string; booleans, NaN,
  infinities and negative values are rejected.
* ``config.max_ref_age_days`` is ignored with a note: branch and tag
  heads are never expired.
* Other ``config`` keys are ignored with a note.
* Missing keys are not passed, so ``expire_snapshots`` falls back to
  the table's ``history.expire.*`` properties and then its defaults.

Custom policy types
-------------------
Polaris cannot store custom policy types, so ``handlers=`` is for
policy sources other than Polaris, or for overriding the built-in
behaviour of a type:

.. code-block:: python

    def handle_my_policy(table, policy, *, dry_run):
        ...
        return {"k": "v"}  # included in DispatchResult.details

    dispatch_policy(
        table,
        policy,
        handlers={"my.org.weird-policy": handle_my_policy},
    )

The handler receives ``(table, policy, *, dry_run)`` and returns a
dict of details (``None`` is treated as ``{}``; any other non-dict
return marks the dispatch as failed) or raises on failure.

Dry-run
-------
``dispatch_policy(..., dry_run=True)`` forwards ``dry_run=True`` to
the maintenance op. The result reports what *would* happen without
mutating the table.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyiceberg.table import Table

from iceberg_ops.polaris.policies import Policy

logger = logging.getLogger(__name__)


_SNAPSHOT_EXPIRY = "system.snapshot-expiry"

# Polaris system types that iceberg-ops recognises but never executes.
_UNSUPPORTED_SYSTEM_TYPES: dict[str, str] = {
    "system.metadata-compaction": (
        "manifest rewrite is not implemented; iceberg-ops no longer runs a "
        "data-file rewrite for this type"
    ),
    "system.data-compaction": (
        "not executed from policies in this release (compaction supports file:// tables only)"
    ),
    "system.orphan-file-removal": "never executed from policy automation in this release",
}

# Polaris system.snapshot-expiry content schema.
_SNAPSHOT_EXPIRY_VERSION = "2025-02-03"
_SNAPSHOT_EXPIRY_TOP_LEVEL_KEYS = frozenset({"version", "enable", "config"})
_SNAPSHOT_EXPIRY_CONFIG_KEYS = frozenset(
    {"max_snapshot_age_days", "min_snapshot_to_keep", "max_ref_age_days"}
)


@dataclass
class DispatchResult:
    """Outcome of dispatching one :class:`Policy` against a table.

    Attributes
    ----------
    policy_name : str
        ``policy.name`` of the policy that was dispatched.
    policy_type : str
        ``policy.policy_type`` of the policy that was dispatched.
    op : str or None
        Name of the maintenance op that was invoked, or ``None`` if
        the dispatch was skipped before any op ran.
    success : bool
        ``True`` when the op ran (or would have run, in dry-run) to
        completion. ``False`` when the op raised or a handler returned
        a non-dict. Always ``False`` when ``skipped`` is ``True``.
    skipped : bool
        ``True`` when the dispatcher chose not to run anything:
        the policy type is unsupported, or a ``system.*`` policy is
        not enabled with ``"enable": true``.
    skip_reason : str or None
        Human-readable reason populated alongside ``skipped=True``.
    details : dict
        Per-op metadata. Includes the parsed kwargs, the maintenance
        helper's result fields, and any notes about ignored options.
    error : str or None
        Stringified exception when ``success=False`` due to a raised
        exception or an invalid handler return. ``None`` for skips.
    """

    policy_name: str
    policy_type: str
    op: str | None = None
    success: bool = False
    skipped: bool = False
    skip_reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


# Type for custom handler functions registered via the ``handlers`` kwarg.
# A ``None`` return is treated as empty details.
HandlerFn = Callable[..., dict[str, Any] | None]


# ===========================================================================
# Public dispatch entry points
# ===========================================================================


def dispatch_policy(
    table: Table,
    policy: Policy,
    *,
    dry_run: bool = False,
    handlers: dict[str, HandlerFn] | None = None,
) -> DispatchResult:
    """Dispatch one :class:`Policy` against ``table``.

    Returns a :class:`DispatchResult` describing the outcome.
    Exceptions raised by the underlying maintenance op are caught
    and surfaced via ``DispatchResult.error`` so a batch run can
    keep going past a single broken policy.

    Parameters
    ----------
    table : pyiceberg.table.Table
        Target Iceberg table.
    policy : Policy
        Parsed policy (from
        :func:`~iceberg_ops.polaris.parse_policy` or
        :func:`~iceberg_ops.polaris.fetch_table_policies`).
    dry_run : bool, optional
        When ``True``, forward ``dry_run=True`` to the underlying
        maintenance op so no mutations occur.
    handlers : dict, optional
        Map of ``policy_type -> handler(table, policy, *, dry_run)``.
        Keys take precedence over built-in handling, including the
        always-skipped system types. A handler for a ``system.*`` type
        still runs only when the policy has ``"enable": true``.

    Returns
    -------
    DispatchResult
        Outcome of the dispatch.
    """
    handlers = handlers or {}
    policy_type = policy.policy_type
    handler = handlers.get(policy_type)

    if handler is not None:
        op_name = f"custom:{policy_type}"
    elif policy_type in _UNSUPPORTED_SYSTEM_TYPES:
        return _skipped(
            policy,
            f"policy type {policy_type!r} is not supported: "
            f"{_UNSUPPORTED_SYSTEM_TYPES[policy_type]}",
        )
    elif policy_type == _SNAPSHOT_EXPIRY:
        handler = _handle_snapshot_expiry
        op_name = "expire_snapshots"
    else:
        return _skipped(
            policy,
            f"unsupported policy type {policy_type!r}; "
            "register a custom handler via the `handlers` kwarg if you need it.",
        )

    if policy_type.startswith("system.") and not _is_enabled(policy.content):
        return _skipped(
            policy,
            "policy disabled: 'enable' must be boolean true "
            f"(got {policy.content.get('enable')!r})",
        )

    return _run_handler(table, policy, handler=handler, op_name=op_name, dry_run=dry_run)


def dispatch_policies(
    table: Table,
    policies: list[Policy],
    *,
    dry_run: bool = False,
    handlers: dict[str, HandlerFn] | None = None,
) -> list[DispatchResult]:
    """Dispatch a batch of policies, returning one result per policy.

    Continues past errors so one bad policy does not block the rest.
    Order in the returned list matches input order.

    Parameters
    ----------
    table : pyiceberg.table.Table
        Target Iceberg table.
    policies : list of Policy
        Parsed policies to dispatch in order.
    dry_run : bool, optional
        When ``True``, forward ``dry_run=True`` to each maintenance op.
    handlers : dict, optional
        Map of ``policy_type -> handler`` for custom policy types.

    Returns
    -------
    list of DispatchResult
        One per input policy, same order.
    """
    return [dispatch_policy(table, p, dry_run=dry_run, handlers=handlers) for p in policies]


# ===========================================================================
# Built-in handlers
# ===========================================================================


def _handle_snapshot_expiry(
    table: Table,
    policy: Policy,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    """``system.snapshot-expiry`` handler -> ``maintenance.expire_snapshots``."""
    # Lazy import to keep importing this module cheap.
    from iceberg_ops.maintenance.snapshots import expire_snapshots

    content = policy.content
    unknown = sorted(set(content) - _SNAPSHOT_EXPIRY_TOP_LEVEL_KEYS)
    if unknown:
        raise ValueError(
            f"unrecognised top-level content keys {unknown}; "
            "expected only 'version', 'enable' and 'config'"
        )
    if "version" in content and content["version"] != _SNAPSHOT_EXPIRY_VERSION:
        raise ValueError(
            f"unsupported content version {content['version']!r}; "
            f"expected {_SNAPSHOT_EXPIRY_VERSION!r}"
        )
    config = content.get("config", {})
    if not isinstance(config, dict):
        raise ValueError(f"'config' must be an object, got {type(config).__name__}")

    kwargs: dict[str, Any] = {"dry_run": dry_run}
    notes: list[str] = []

    if "max_snapshot_age_days" in config:
        days = _as_non_negative_number(config["max_snapshot_age_days"], "max_snapshot_age_days")
        kwargs["max_age_hours"] = days * 24.0
    if "min_snapshot_to_keep" in config:
        count = _as_non_negative_number(config["min_snapshot_to_keep"], "min_snapshot_to_keep")
        if count < 1 or count != int(count):
            raise ValueError(
                f"config 'min_snapshot_to_keep' must be an integer >= 1, "
                f"got {config['min_snapshot_to_keep']!r}"
            )
        kwargs["min_snapshots"] = int(count)
    if "max_ref_age_days" in config:
        notes.append("max_ref_age_days ignored: branch/tag heads are never expired")
    ignored = sorted(set(config) - _SNAPSHOT_EXPIRY_CONFIG_KEYS)
    if ignored:
        notes.append(f"ignored config keys {ignored}")

    result = expire_snapshots(table, **kwargs)
    details: dict[str, Any] = {
        "dry_run": dry_run,
        "kwargs": _stringify_kwargs(kwargs),
        "expired_snapshot_ids": list(result.expired_ids),
        "expired_count": result.expired_count,
        "retained_count": result.retained_count,
        "total_snapshots": result.total_snapshots,
    }
    if notes:
        details["notes"] = notes
    return details


# ===========================================================================
# Internal plumbing
# ===========================================================================


def _is_enabled(content: dict[str, Any]) -> bool:
    """Return ``True`` only when ``content["enable"]`` is the boolean ``True``."""
    return content.get("enable") is True


def _as_non_negative_number(value: Any, key: str) -> float:
    """Coerce a policy config value to a finite, non-negative float.

    Parameters
    ----------
    value : Any
        Raw config value: an int, a float, or a numeric string.
    key : str
        Config key, used in error messages.

    Returns
    -------
    float
        The coerced value.

    Raises
    ------
    ValueError
        If *value* is a bool, not numeric, NaN, infinite, or negative.
    """
    if isinstance(value, bool):
        raise ValueError(f"config {key!r} must be a number, got {value!r}")
    if isinstance(value, int | float):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            raise ValueError(f"config {key!r} must be a number, got {value!r}") from None
    else:
        raise ValueError(f"config {key!r} must be a number, got {type(value).__name__}")
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"config {key!r} must be a finite number >= 0, got {value!r}")
    return number


def _skipped(policy: Policy, reason: str) -> DispatchResult:
    """Build a skipped :class:`DispatchResult` (no op ran)."""
    return DispatchResult(
        policy_name=policy.name,
        policy_type=policy.policy_type,
        skipped=True,
        skip_reason=reason,
    )


def _run_handler(
    table: Table,
    policy: Policy,
    *,
    handler: HandlerFn,
    op_name: str,
    dry_run: bool,
) -> DispatchResult:
    """Invoke ``handler`` and wrap its return / exception in a result.

    Built-in handlers all match the
    ``handler(table, policy, *, dry_run) -> dict`` signature.
    Custom handlers registered via the ``handlers`` kwarg are
    expected to match the same signature. A ``None`` return is
    treated as ``{}``; any other non-dict return fails the dispatch.
    """
    try:
        details = handler(table, policy, dry_run=dry_run)
    except Exception as exc:
        logger.exception(
            "Polaris policy %r (%s) failed against table %s",
            policy.name,
            policy.policy_type,
            table.name(),
        )
        return DispatchResult(
            policy_name=policy.name,
            policy_type=policy.policy_type,
            op=op_name,
            success=False,
            error=f"{type(exc).__name__}: {exc}",
        )
    if details is None:
        details = {}
    elif not isinstance(details, dict):
        return DispatchResult(
            policy_name=policy.name,
            policy_type=policy.policy_type,
            op=op_name,
            success=False,
            error=f"handler returned {type(details).__name__}, expected dict",
        )
    return DispatchResult(
        policy_name=policy.name,
        policy_type=policy.policy_type,
        op=op_name,
        success=True,
        details=details,
    )


def _stringify_kwargs(kwargs: dict[str, Any]) -> dict[str, str]:
    """Render kwargs as strings for inclusion in DispatchResult.details.

    Keeping kwargs as strings here means the result dict is JSON-
    serialisable for the eventual CLI / log output without any
    further coercion.
    """
    return {k: repr(v) for k, v in kwargs.items()}
