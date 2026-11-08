"""Tests for :mod:`iceberg_ops.polaris.policies` - the Policy dataclass
and the parse / fetch helpers."""

from __future__ import annotations

import logging

import pytest

from iceberg_ops.polaris import (
    PolarisClient,
    PolarisError,
    Policy,
    fetch_table_policies,
    parse_policy,
)
from iceberg_ops.testing import FakeResponse, FakeSession


class TestParsePolicy:
    def test_dict_content(self) -> None:
        p = parse_policy(
            {
                "name": "snap",
                "type": "system.snapshot-expiry",
                "content": {"max_snapshot_age_ms": 86_400_000},
            }
        )
        assert p == Policy(
            name="snap",
            policy_type="system.snapshot-expiry",
            content={"max_snapshot_age_ms": 86_400_000},
        )

    def test_string_content(self) -> None:
        p = parse_policy(
            {
                "name": "compact",
                "type": "system.metadata-compaction",
                "content": '{"enable": true}',
            }
        )
        assert p.content == {"enable": True}

    def test_empty_string_content_yields_empty_dict(self) -> None:
        p = parse_policy({"name": "p", "type": "t", "content": ""})
        assert p.content == {}

    def test_whitespace_only_string_content_yields_empty_dict(self) -> None:
        p = parse_policy({"name": "p", "type": "t", "content": "   \n\t  "})
        assert p.content == {}

    def test_missing_content_yields_empty_dict(self) -> None:
        p = parse_policy({"name": "p", "type": "t"})
        assert p.content == {}

    def test_optional_fields_default(self) -> None:
        p = parse_policy({"name": "p", "type": "t"})
        assert p.description is None
        assert p.inheritable is False
        assert p.version == 0

    def test_optional_fields_round_trip(self) -> None:
        p = parse_policy(
            {
                "name": "p",
                "type": "t",
                "description": "test",
                "inheritable": True,
                "version": 7,
            }
        )
        assert p.description == "test"
        assert p.inheritable is True
        assert p.version == 7

    def test_missing_name_raises(self) -> None:
        with pytest.raises(PolarisError, match="missing required field"):
            parse_policy({"type": "system.snapshot-expiry"})

    def test_missing_type_raises(self) -> None:
        with pytest.raises(PolarisError, match="missing required field"):
            parse_policy({"name": "p"})

    def test_empty_name_raises(self) -> None:
        with pytest.raises(PolarisError, match="missing required field"):
            parse_policy({"name": "", "type": "t"})

    def test_unparseable_content_string_raises(self) -> None:
        with pytest.raises(PolarisError, match="unparseable JSON"):
            parse_policy({"name": "p", "type": "t", "content": "{not json"})

    def test_content_string_parses_to_non_object_raises(self) -> None:
        with pytest.raises(PolarisError, match="parsed to"):
            parse_policy({"name": "p", "type": "t", "content": '["array"]'})

    def test_non_dict_input_raises(self) -> None:
        with pytest.raises(PolarisError, match="expected a dict"):
            parse_policy("not a dict")  # type: ignore[arg-type]

    def test_unexpected_content_type_raises(self) -> None:
        with pytest.raises(PolarisError, match="unexpected type"):
            parse_policy({"name": "p", "type": "t", "content": 123})

    def test_dataclass_is_frozen(self) -> None:
        # Policy should be immutable - guards against accidental
        # mutation between parse and dispatch.
        p = parse_policy({"name": "p", "type": "t"})
        with pytest.raises(AttributeError):  # frozen dataclass
            p.name = "other"  # type: ignore[misc]


class TestFetchTablePolicies:
    def test_returns_parsed_policies(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/applicable-policies",
            FakeResponse(
                json_body={
                    "applicable-policies": [
                        {
                            "name": "snap",
                            "type": "system.snapshot-expiry",
                            "content": {"max_snapshot_age_ms": 1000},
                        },
                        {
                            "name": "compact",
                            "type": "system.metadata-compaction",
                            "content": '{"enable": true}',
                        },
                    ]
                }
            ),
        )
        client = PolarisClient("https://x", "cat", token="t", session=sess)
        result = fetch_table_policies(client, "ns", "tbl")
        assert len(result) == 2
        assert all(isinstance(p, Policy) for p in result)
        assert result[0].name == "snap"
        assert result[1].content == {"enable": True}

    def test_skips_unparseable_entries(self, caplog) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/applicable-policies",
            FakeResponse(
                json_body={
                    "applicable-policies": [
                        {"name": "good", "type": "system.snapshot-expiry"},
                        # missing 'name' - unparseable
                        {"type": "system.metadata-compaction"},
                    ]
                }
            ),
        )
        client = PolarisClient("https://x", "cat", token="t", session=sess)

        with caplog.at_level(logging.WARNING):
            result = fetch_table_policies(client, "ns", "tbl")
        # The malformed entry was dropped; the good one survived.
        assert len(result) == 1
        assert result[0].name == "good"
        assert any("unparseable" in rec.message.lower() for rec in caplog.records)

    def test_empty_response(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/applicable-policies",
            FakeResponse(json_body={"applicable-policies": []}),
        )
        client = PolarisClient("https://x", "cat", token="t", session=sess)
        assert fetch_table_policies(client, "ns", "tbl") == []
