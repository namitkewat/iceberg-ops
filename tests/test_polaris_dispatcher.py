"""Tests for :mod:`iceberg_ops.polaris.dispatcher`.

Exercises the policy-to-maintenance-op dispatcher against real
PyIceberg tables in a memory catalog. No HTTP, no Polaris instance -
the tests construct :class:`Policy` objects directly so the full
dispatch path is covered without any external dependencies.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg import types as ice_types
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField

from iceberg_ops.polaris import (
    DispatchResult,
    Policy,
    dispatch_policies,
    dispatch_policy,
)

# ===========================================================================
# Test fixtures / helpers
# ===========================================================================


_BASIC_SCHEMA = IcebergSchema(
    NestedField(1, "id", ice_types.LongType(), required=True),
    NestedField(2, "name", ice_types.StringType()),
)


def _make_table(catalog, ns: str, name: str):
    catalog.create_namespace_if_not_exists(ns)
    return catalog.create_table(f"{ns}.{name}", _BASIC_SCHEMA)


def _seed(table, rows: list[int]) -> None:
    """Append a single batch with the given ids."""
    table.append(
        pa.Table.from_pydict(
            {"id": rows, "name": [f"row_{i}" for i in rows]},
            schema=table.schema().as_arrow(),
        )
    )


def _multi_seed(table, n_appends: int) -> None:
    """Append *n_appends* one-row batches - produces multiple
    snapshots (one per append)."""
    for i in range(n_appends):
        _seed(table, [i])


def _expiry(config: dict[str, Any] | None = None, **top_level: Any) -> Policy:
    """Build an enabled ``system.snapshot-expiry`` policy."""
    content: dict[str, Any] = {"enable": True, **top_level}
    if config is not None:
        content["config"] = config
    return Policy(name="snap-expiry", policy_type="system.snapshot-expiry", content=content)


def _snapshot_count(table) -> int:
    table.refresh()
    return len(table.metadata.snapshots)


# ===========================================================================
# Construction / DispatchResult shape
# ===========================================================================


class TestDispatchResultDataclass:
    def test_defaults(self) -> None:
        r = DispatchResult(policy_name="p", policy_type="t")
        assert r.op is None
        assert r.success is False
        assert r.skipped is False
        assert r.skip_reason is None
        assert r.details == {}
        assert r.error is None

    def test_field_defaults_independent_per_instance(self) -> None:
        # Defensive: confirm `details` uses default_factory so two
        # instances don't share the same dict.
        a = DispatchResult(policy_name="a", policy_type="t")
        b = DispatchResult(policy_name="b", policy_type="t")
        a.details["x"] = 1
        assert b.details == {}


# ===========================================================================
# Snapshot-expiry dispatch
# ===========================================================================


class TestSnapshotExpiryDispatch:
    def test_polaris_schema_example(self, memory_catalog) -> None:
        """The Polaris schema example maps to hours / count plus a note."""
        table = _make_table(memory_catalog, "polaris_test", "t1")
        _multi_seed(table, n_appends=4)
        policy = Policy(
            name="snap-expiry",
            policy_type="system.snapshot-expiry",
            content={
                "version": "2025-02-03",
                "enable": True,
                "config": {
                    "min_snapshot_to_keep": 1,
                    "max_snapshot_age_days": 2,
                    "max_ref_age_days": 3,
                },
            },
        )
        result = dispatch_policy(table, policy, dry_run=True)
        assert result.success is True, result.error
        assert result.op == "expire_snapshots"
        assert result.skipped is False
        kwargs = result.details["kwargs"]
        assert kwargs["max_age_hours"] == repr(48.0)
        assert kwargs["min_snapshots"] == repr(1)
        assert kwargs["dry_run"] == repr(True)
        assert any("max_ref_age_days" in n for n in result.details["notes"])
        assert "expired_count" in result.details

    def test_string_values_are_coerced(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_str")
        _multi_seed(table, n_appends=2)
        policy = _expiry({"max_snapshot_age_days": "2", "min_snapshot_to_keep": "1"})
        result = dispatch_policy(table, policy, dry_run=True)
        assert result.success is True, result.error
        assert result.details["kwargs"]["max_age_hours"] == repr(48.0)
        assert result.details["kwargs"]["min_snapshots"] == repr(1)

    def test_fractional_days_are_not_floored(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_frac")
        _multi_seed(table, n_appends=2)
        result = dispatch_policy(table, _expiry({"max_snapshot_age_days": 1.5}), dry_run=True)
        assert result.success is True, result.error
        assert result.details["kwargs"]["max_age_hours"] == repr(36.0)

    def test_huge_max_age_expires_nothing(self, memory_catalog) -> None:
        """An age far beyond the epoch is "keep everything", not an overflow."""
        table = _make_table(memory_catalog, "polaris_test", "t_huge")
        _multi_seed(table, n_appends=3)
        policy = _expiry({"max_snapshot_age_days": 10**12, "min_snapshot_to_keep": 1})
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.success is True, result.error
        assert result.details["expired_count"] == 0

    @pytest.mark.parametrize(
        "config",
        [
            {"max_snapshot_age_days": True},
            {"max_snapshot_age_days": -1},
            {"max_snapshot_age_days": "abc"},
            {"max_snapshot_age_days": "nan"},
            {"min_snapshot_to_keep": True},
            {"min_snapshot_to_keep": -1},
            {"min_snapshot_to_keep": 0},
            {"min_snapshot_to_keep": 1.5},
            {"min_snapshot_to_keep": "abc"},
        ],
    )
    def test_invalid_config_values_fail(self, memory_catalog, config) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_bad")
        _multi_seed(table, n_appends=3)
        result = dispatch_policy(table, _expiry(config), dry_run=False)
        assert result.success is False
        assert result.skipped is False
        assert "ValueError" in (result.error or "")
        assert _snapshot_count(table) == 3

    def test_legacy_top_level_key_fails_closed(self, memory_catalog) -> None:
        """A top-level ``max_snapshot_age_ms`` fails instead of running on defaults."""
        table = _make_table(memory_catalog, "polaris_test", "t_legacy")
        _multi_seed(table, n_appends=4)
        policy = _expiry(max_snapshot_age_ms=1)
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.success is False
        assert "unrecognised top-level content keys" in (result.error or "")
        assert "max_snapshot_age_ms" in (result.error or "")
        assert _snapshot_count(table) == 4

    def test_unknown_version_fails(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_ver")
        _multi_seed(table, n_appends=2)
        result = dispatch_policy(table, _expiry({}, version="2099-01-01"), dry_run=True)
        assert result.success is False
        assert "2099-01-01" in (result.error or "")

    def test_non_object_config_fails(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_cfg")
        _multi_seed(table, n_appends=2)
        policy = Policy(
            name="p",
            policy_type="system.snapshot-expiry",
            content={"enable": True, "config": [1, 2]},
        )
        result = dispatch_policy(table, policy, dry_run=True)
        assert result.success is False
        assert "'config' must be an object" in (result.error or "")

    def test_unknown_config_keys_are_noted(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_extra")
        _multi_seed(table, n_appends=2)
        result = dispatch_policy(table, _expiry({"future_knob": 1}), dry_run=True)
        assert result.success is True, result.error
        assert any("future_knob" in n for n in result.details["notes"])

    def test_missing_config_uses_helper_defaults(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_def")
        _multi_seed(table, n_appends=2)
        result = dispatch_policy(table, _expiry(), dry_run=True)
        assert result.success is True, result.error
        kwargs = result.details["kwargs"]
        assert "max_age_hours" not in kwargs
        assert "min_snapshots" not in kwargs

    def test_real_run_expires_per_config(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "t_real")
        _multi_seed(table, n_appends=4)
        policy = _expiry({"max_snapshot_age_days": 0, "min_snapshot_to_keep": 2})
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.success is True, result.error
        assert result.details["expired_count"] == 2
        assert _snapshot_count(table) == 2


# ===========================================================================
# Enable gate
# ===========================================================================


class TestEnableGate:
    @pytest.mark.parametrize("enable", [False, "true", "0", 1, None])
    def test_non_true_enable_skips(self, memory_catalog, enable) -> None:
        table = _make_table(memory_catalog, "polaris_test", "g1")
        _multi_seed(table, n_appends=4)
        policy = Policy(
            name="gated",
            policy_type="system.snapshot-expiry",
            content={
                "enable": enable,
                "config": {"max_snapshot_age_days": 0, "min_snapshot_to_keep": 1},
            },
        )
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.skipped is True
        assert result.success is False
        assert result.op is None
        assert "policy disabled" in (result.skip_reason or "")
        assert _snapshot_count(table) == 4

    def test_missing_enable_skips(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "g2")
        _multi_seed(table, n_appends=4)
        policy = Policy(
            name="gated",
            policy_type="system.snapshot-expiry",
            content={"config": {"max_snapshot_age_days": 0, "min_snapshot_to_keep": 1}},
        )
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.skipped is True
        assert result.op is None
        assert _snapshot_count(table) == 4

    def test_enable_true_runs(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "g3")
        _multi_seed(table, n_appends=4)
        policy = _expiry({"max_snapshot_age_days": 0, "min_snapshot_to_keep": 1})
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.success is True, result.error
        assert _snapshot_count(table) == 1

    def test_custom_handler_for_system_type_is_still_gated(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "g4")
        calls: list[str] = []

        def handler(t, p, *, dry_run):
            calls.append(p.name)
            return {}

        policy = Policy(name="snap", policy_type="system.snapshot-expiry", content={})
        result = dispatch_policy(table, policy, handlers={"system.snapshot-expiry": handler})
        assert result.skipped is True
        assert calls == []

    def test_custom_non_system_type_is_not_gated(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "g5")
        policy = Policy(name="mine", policy_type="my.org.thing", content={})
        result = dispatch_policy(
            table,
            policy,
            handlers={"my.org.thing": lambda t, p, *, dry_run: {"ran": True}},
        )
        assert result.success is True
        assert result.details == {"ran": True}


# ===========================================================================
# Recognised but unsupported system types
# ===========================================================================


class TestSkippedSystemTypes:
    @pytest.mark.parametrize(
        "policy_type",
        [
            "system.metadata-compaction",
            "system.data-compaction",
            "system.orphan-file-removal",
        ],
    )
    def test_type_is_skipped(self, memory_catalog, policy_type) -> None:
        table = _make_table(memory_catalog, "polaris_test", "s1")
        policy = Policy(name="p", policy_type=policy_type, content={"enable": True})
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.skipped is True
        assert result.success is False
        assert result.op is None
        assert "not supported" in (result.skip_reason or "")

    def test_metadata_compaction_does_not_rewrite_data_files(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "s2")
        for i in range(6):
            _seed(table, [i])
        table.refresh()
        files_before = table.inspect.files().num_rows
        policy = Policy(
            name="compact",
            policy_type="system.metadata-compaction",
            content={"enable": True, "config": {"target_file_size_bytes": 1}},
        )
        result = dispatch_policy(table, policy, dry_run=False)
        assert result.skipped is True
        table.refresh()
        assert table.inspect.files().num_rows == files_before

    def test_custom_handler_can_take_over_skipped_type(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "s3")
        policy = Policy(name="orphans", policy_type="system.orphan-file-removal", content={})
        handlers = {"system.orphan-file-removal": lambda t, p, *, dry_run: {"ok": 1}}
        # Still gated: no enable -> skipped.
        assert dispatch_policy(table, policy, handlers=handlers).skipped is True
        enabled = Policy(
            name="orphans", policy_type="system.orphan-file-removal", content={"enable": True}
        )
        result = dispatch_policy(table, enabled, handlers=handlers)
        assert result.success is True
        assert result.op == "custom:system.orphan-file-removal"


# ===========================================================================
# Unsupported / unknown policies
# ===========================================================================


class TestUnsupportedPolicy:
    def test_unknown_type_is_skipped(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "u1")
        policy = Policy(name="custom", policy_type="my.org.weird-policy", content={"any": 1})
        result = dispatch_policy(table, policy)
        assert result.skipped is True
        assert result.op is None
        assert "unsupported policy type" in (result.skip_reason or "")
        assert "'my.org.weird-policy'" in (result.skip_reason or "")

    def test_unknown_system_type_is_skipped(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "u1b")
        policy = Policy(name="rls", policy_type="system.row-filter", content={"enable": True})
        result = dispatch_policy(table, policy)
        assert result.skipped is True
        assert "unsupported policy type" in (result.skip_reason or "")

    def test_custom_handler_runs_for_unknown_type(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "u2")
        captured: dict[str, Any] = {}

        def my_handler(t, p, *, dry_run):
            captured["table_name"] = str(t.name())
            captured["policy_name"] = p.name
            captured["dry_run"] = dry_run
            return {"my_metric": 42}

        policy = Policy(
            name="my-custom",
            policy_type="my.org.weird-policy",
            content={"foo": "bar"},
        )
        result = dispatch_policy(
            table,
            policy,
            handlers={"my.org.weird-policy": my_handler},
        )
        assert result.success is True
        assert result.skipped is False
        assert result.op == "custom:my.org.weird-policy"
        assert result.details == {"my_metric": 42}
        assert captured["policy_name"] == "my-custom"
        assert captured["dry_run"] is False

    def test_custom_handler_overrides_builtin(self, memory_catalog) -> None:
        # A custom handler keyed on a built-in policy type takes
        # precedence - useful for downstream users with stricter
        # behaviour requirements.
        table = _make_table(memory_catalog, "polaris_test", "u3")

        def overriding_handler(t, p, *, dry_run):
            return {"used": "override"}

        result = dispatch_policy(
            table,
            _expiry(),
            handlers={"system.snapshot-expiry": overriding_handler},
        )
        assert result.success is True
        assert result.op == "custom:system.snapshot-expiry"
        assert result.details == {"used": "override"}


# ===========================================================================
# Handler return values
# ===========================================================================


class TestHandlerReturnValues:
    def test_none_return_becomes_empty_details(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "h1")
        policy = Policy(name="p", policy_type="custom.none", content={})
        result = dispatch_policy(
            table, policy, handlers={"custom.none": lambda t, p, *, dry_run: None}
        )
        assert result.success is True
        assert result.details == {}

    def test_non_dict_return_fails(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "h2")
        policy = Policy(name="p", policy_type="custom.list", content={})
        result = dispatch_policy(
            table, policy, handlers={"custom.list": lambda t, p, *, dry_run: [1, 2]}
        )
        assert result.success is False
        assert result.skipped is False
        assert result.error == "handler returned list, expected dict"


# ===========================================================================
# Error handling
# ===========================================================================


class TestErrorHandling:
    def test_handler_exception_becomes_dispatch_error(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "e1")

        def bad_handler(t, p, *, dry_run):
            raise RuntimeError("boom from handler")

        policy = Policy(name="bad", policy_type="custom.bad", content={})
        result = dispatch_policy(table, policy, handlers={"custom.bad": bad_handler})
        assert result.success is False
        assert result.skipped is False
        assert result.error is not None
        assert "RuntimeError" in result.error
        assert "boom from handler" in result.error

    def test_builtin_handler_failure_becomes_dispatch_error(
        self, memory_catalog, monkeypatch
    ) -> None:
        # Force expire_snapshots to raise; verify the dispatcher
        # captures it as DispatchResult.error rather than crashing.
        from iceberg_ops.maintenance import snapshots as snap_mod

        def boom(*_a, **_kw):
            raise ValueError("forced failure for test")

        monkeypatch.setattr(snap_mod, "expire_snapshots", boom)

        table = _make_table(memory_catalog, "polaris_test", "e2")
        _seed(table, [1])
        result = dispatch_policy(table, _expiry())
        assert result.success is False
        assert "forced failure" in (result.error or "")


# ===========================================================================
# Batch dispatch
# ===========================================================================


class TestDispatchPolicies:
    def test_batch_dispatch_returns_one_result_per_policy(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "b1")
        _multi_seed(table, n_appends=2)

        policies = [
            _expiry(),
            Policy(name="weird", policy_type="not.a.real.type", content={}),
            Policy(
                name="compact",
                policy_type="system.metadata-compaction",
                content={"enable": True},
            ),
        ]
        results = dispatch_policies(table, policies, dry_run=True)
        assert len(results) == 3
        assert results[0].success is True and results[0].op == "expire_snapshots"
        assert results[1].skipped is True and results[1].op is None
        assert results[2].skipped is True and results[2].op is None

    def test_batch_continues_past_errors(self, memory_catalog, monkeypatch) -> None:
        from iceberg_ops.maintenance import snapshots as snap_mod

        def boom(*_a, **_kw):
            raise RuntimeError("batch test")

        monkeypatch.setattr(snap_mod, "expire_snapshots", boom)

        table = _make_table(memory_catalog, "polaris_test", "b2")
        _seed(table, [1, 2])
        policies = [
            _expiry(),
            Policy(name="will-skip", policy_type="unknown.policy", content={}),
        ]
        results = dispatch_policies(table, policies, dry_run=True)
        assert len(results) == 2
        assert results[0].success is False and results[0].skipped is False
        assert "batch test" in (results[0].error or "")
        assert results[1].skipped is True

    def test_empty_batch(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "b3")
        results = dispatch_policies(table, [], dry_run=True)
        assert results == []


# ===========================================================================
# dry_run propagation
# ===========================================================================


class TestDryRunPropagation:
    def test_dry_run_true_does_not_mutate_table(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "d1")
        _multi_seed(table, n_appends=3)

        # Aggressive expiry that would normally remove old snapshots.
        policy = _expiry({"max_snapshot_age_days": 0, "min_snapshot_to_keep": 1})
        result = dispatch_policy(table, policy, dry_run=True)
        assert result.success is True, result.error
        assert result.details["dry_run"] is True
        assert result.details["expired_count"] == 2
        assert _snapshot_count(table) == 3

    def test_dry_run_false_is_default_in_kwargs(self, memory_catalog) -> None:
        table = _make_table(memory_catalog, "polaris_test", "d2")
        _seed(table, [1])
        result = dispatch_policy(table, _expiry())  # no dry_run kwarg
        assert result.details["dry_run"] is False
        assert result.details["kwargs"]["dry_run"] == repr(False)


# ===========================================================================
# Integration: parse_policy -> dispatch_policy
# ===========================================================================


class TestParsedPolicyEndToEnd:
    """Round-trip from raw API dict -> parse_policy -> dispatch_policy."""

    def test_round_trip_json_string_content(self, memory_catalog) -> None:
        from iceberg_ops.polaris import parse_policy

        table = _make_table(memory_catalog, "polaris_test", "rt1")
        _multi_seed(table, n_appends=2)
        raw = {
            "name": "via-api",
            "type": "system.snapshot-expiry",
            "content": json.dumps(
                {
                    "version": "2025-02-03",
                    "enable": True,
                    "config": {"max_snapshot_age_days": 1, "min_snapshot_to_keep": 3},
                }
            ),
        }
        policy = parse_policy(raw)
        result = dispatch_policy(table, policy, dry_run=True)
        assert result.success is True, result.error
        assert result.details["kwargs"]["max_age_hours"] == repr(24.0)
        assert result.details["kwargs"]["min_snapshots"] == repr(3)

    def test_round_trip_disabled_json_string_content(self, memory_catalog) -> None:
        from iceberg_ops.polaris import parse_policy

        table = _make_table(memory_catalog, "polaris_test", "rt2")
        _multi_seed(table, n_appends=3)
        raw = {
            "name": "off",
            "type": "system.snapshot-expiry",
            "content": '{"enable": false, "config": {"max_snapshot_age_days": 0}}',
        }
        result = dispatch_policy(table, parse_policy(raw), dry_run=False)
        assert result.skipped is True
        assert _snapshot_count(table) == 3
