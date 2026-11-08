"""Tests for iceberg_ops.core.schema."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg import types as ice_types
from pyiceberg.exceptions import CommitFailedException, ValidationError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.table.update.schema import UpdateSchema
from pyiceberg.transforms import IdentityTransform

from iceberg_ops.core.schema import (
    ColumnSpec,
    SchemaContract,
    SchemaContractViolation,
    SchemaEvolutionHelper,
    diff_schemas,
)


def _ice_schema(
    *specs: tuple[int, str, ice_types.IcebergType, str | None],
) -> IcebergSchema:
    """Build an IcebergSchema from (field_id, name, type, doc) tuples."""
    return IcebergSchema(
        *(
            ice_types.NestedField(
                field_id=fid,
                name=name,
                field_type=t,
                required=False,
                doc=doc,
            )
            for fid, name, t, doc in specs
        )
    )


# ===========================================================================
# diff_schemas
# ===========================================================================


class TestDiffSchemas:
    def test_identical(self) -> None:
        a = _ice_schema((1, "id", ice_types.LongType(), None))
        b = _ice_schema((1, "id", ice_types.LongType(), None))
        diff = diff_schemas(a, b)
        assert diff.is_empty
        assert diff.summary() == "Schemas are identical."

    def test_added_column(self) -> None:
        a = _ice_schema((1, "id", ice_types.LongType(), None))
        b = _ice_schema(
            (1, "id", ice_types.LongType(), None),
            (2, "name", ice_types.StringType(), None),
        )
        diff = diff_schemas(a, b)
        assert len(diff.added) == 1
        assert diff.added[0].name == "name"
        assert diff.added[0].field_id == 2
        assert diff.added[0].new_type == "string"

    def test_removed_column(self) -> None:
        a = _ice_schema(
            (1, "id", ice_types.LongType(), None),
            (2, "name", ice_types.StringType(), None),
        )
        b = _ice_schema((1, "id", ice_types.LongType(), None))
        diff = diff_schemas(a, b)
        assert len(diff.removed) == 1
        assert diff.removed[0].name == "name"
        assert diff.removed[0].old_type == "string"

    def test_renamed_keeps_field_id(self) -> None:
        a = _ice_schema((1, "old", ice_types.LongType(), None))
        b = _ice_schema((1, "new", ice_types.LongType(), None))
        diff = diff_schemas(a, b)
        renames = [c for c in diff.changed if c.change_type == "renamed"]
        assert len(renames) == 1
        assert renames[0].old_name == "old"
        assert renames[0].name == "new"
        assert renames[0].field_id == 1

    def test_type_changed(self) -> None:
        a = _ice_schema((1, "id", ice_types.IntegerType(), None))
        b = _ice_schema((1, "id", ice_types.LongType(), None))
        diff = diff_schemas(a, b)
        type_changes = [c for c in diff.changed if c.change_type == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0].old_type == "int"
        assert type_changes[0].new_type == "long"

    def test_doc_changed(self) -> None:
        a = _ice_schema((1, "id", ice_types.LongType(), "old doc"))
        b = _ice_schema((1, "id", ice_types.LongType(), "new doc"))
        diff = diff_schemas(a, b)
        doc_changes = [c for c in diff.changed if c.change_type == "doc_changed"]
        assert len(doc_changes) == 1
        assert doc_changes[0].old_doc == "old doc"
        assert doc_changes[0].new_doc == "new doc"

    def test_summary_groups_changes(self) -> None:
        a = _ice_schema((1, "id", ice_types.LongType(), None))
        b = _ice_schema(
            (1, "id", ice_types.LongType(), None),
            (2, "name", ice_types.StringType(), None),
        )
        summary = diff_schemas(a, b).summary()
        assert "Added (1):" in summary
        assert "name" in summary


# ===========================================================================
# SchemaContract
# ===========================================================================


class TestSchemaContract:
    def test_validate_happy_path(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        contract = SchemaContract(
            [ColumnSpec("id", ice_types.LongType(), required=True)],
        )
        assert contract.validate(tbl) == []
        contract.enforce(tbl)  # should not raise

    def test_missing_required_column_raises(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        contract = SchemaContract(
            [
                ColumnSpec("id", ice_types.LongType(), required=True),
                ColumnSpec("missing", ice_types.StringType(), required=True),
            ]
        )
        with pytest.raises(SchemaContractViolation) as exc_info:
            contract.enforce(tbl)
        assert "missing" in str(exc_info.value)

    def test_type_mismatch_reports(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int32())]),
        )
        contract = SchemaContract(
            [
                ColumnSpec("id", ice_types.LongType(), required=True),
            ]
        )
        violations = contract.validate(tbl)
        assert any("'id'" in v and "int" in v and "long" in v for v in violations)

    def test_strict_mode_flags_extra_columns(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("undeclared", pa.string()),
                ]
            ),
        )
        contract = SchemaContract(
            [ColumnSpec("id", ice_types.LongType(), required=True)],
            strict=True,
        )
        violations = contract.validate(tbl)
        assert any("undeclared" in v for v in violations)

    def test_optional_missing_no_violation(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        contract = SchemaContract(
            [
                ColumnSpec("id", ice_types.LongType(), required=True),
                ColumnSpec("optional_extra", ice_types.StringType(), required=False),
            ]
        )
        assert contract.validate(tbl) == []

    def test_violation_message_includes_count(self) -> None:
        v = SchemaContractViolation(["a", "b", "c"])
        assert "3 issue" in str(v)

    def test_struct_with_other_field_ids_conforms(self, memory_catalog, make_table) -> None:
        """Nested field ids are table-internal; a contract cannot know them."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("address", pa.struct([pa.field("street", pa.string())]))]),
        )
        struct = ice_types.StructType(
            ice_types.NestedField(100, "street", ice_types.StringType(), required=False)
        )
        assert SchemaContract([ColumnSpec("address", struct)]).validate(tbl) == []

    def test_different_struct_still_reports(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("address", pa.struct([pa.field("street", pa.string())]))]),
        )
        struct = ice_types.StructType(
            ice_types.NestedField(100, "city", ice_types.StringType(), required=False)
        )
        violations = SchemaContract([ColumnSpec("address", struct)]).validate(tbl)
        assert len(violations) == 1
        assert "'address'" in violations[0]


class TestSchemaContractFromYaml:
    def test_basic_yaml(self, tmp_path: Path) -> None:
        path = tmp_path / "contract.yaml"
        path.write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: long\n"
            "    required: true\n"
            "  - name: name\n"
            "    type: string\n"
            "    doc: full name\n"
            "strict: false\n"
        )
        contract = SchemaContract.from_yaml(str(path))
        assert len(contract.columns) == 2
        assert contract.columns[0].name == "id"
        assert contract.columns[0].required is True
        assert contract.columns[1].doc == "full name"
        assert contract.strict is False

    def test_unknown_type_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.yaml"
        path.write_text("columns:\n  - name: x\n    type: not_a_type\n")
        with pytest.raises(ValueError, match="Unknown column type"):
            SchemaContract.from_yaml(str(path))

    @pytest.mark.parametrize(
        "type_str",
        [
            "decimal(10,20)",
            "decimal(40,2)",
            "decimal(0,0)",
            "fixed[0]",
            "fixed[-1]",
            "geometry(srid:3857)",
        ],
    )
    def test_invalid_type_parameters_raise_value_error(
        self, tmp_path: Path, type_str: str
    ) -> None:
        """Bad parameters raise ValueError, never PyIceberg's ValidationError."""
        path = tmp_path / "bad.yaml"
        path.write_text(f"columns:\n  - name: x\n    type: '{type_str}'\n")
        with pytest.raises(ValueError, match="column type") as info:
            SchemaContract.from_yaml(str(path))
        assert not isinstance(info.value, ValidationError)

    @pytest.mark.parametrize(
        ("type_str", "expected"),
        [
            ("decimal(38,38)", ice_types.DecimalType(38, 38)),
            ("decimal(10,0)", ice_types.DecimalType(10, 0)),
            ("fixed[1]", ice_types.FixedType(1)),
        ],
    )
    def test_boundary_type_parameters_parse(
        self, tmp_path: Path, type_str: str, expected: ice_types.IcebergType
    ) -> None:
        path = tmp_path / "ok.yaml"
        path.write_text(f"columns:\n  - name: x\n    type: '{type_str}'\n")
        assert SchemaContract.from_yaml(str(path)).columns[0].type == expected


# ===========================================================================
# SchemaEvolutionHelper
# ===========================================================================


class TestEnforceColumnAdd:
    def test_adds_missing_column(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column("name", ice_types.StringType(), doc="user name")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "name" in names
        # doc was applied
        f = tbl.schema().find_field("name")
        assert f.doc == "user name"

    def test_idempotent_on_existing_column(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("name", pa.large_string()),
                ]
            ),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column("name", ice_types.StringType())
        tbl.refresh()
        # Still exactly one "name" column (no legacy archival on a no-op).
        names = [f.name for f in tbl.schema().fields]
        assert names.count("name") == 1
        assert not any(n.startswith("name__legacy") for n in names)


class TestEnforceColumnTypeConflict:
    def test_raises_by_default(self, memory_catalog, make_table) -> None:
        """Without ``legacy_archival=True``, a type conflict surfaces the
        underlying PyIceberg exception and leaves the schema untouched."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        helper = SchemaEvolutionHelper(tbl)
        with pytest.raises(Exception):  # noqa: B017 — pyiceberg raises a varied set
            helper.enforce_column("v", ice_types.StringType())
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        # No legacy column was created.
        assert not any(n.startswith("v__legacy") for n in names)
        # Original column is untouched.
        assert str(tbl.schema().find_field("v").field_type) == "int"

    @pytest.mark.parametrize(
        ("old_type", "new_type"),
        [
            (pa.string(), ice_types.BinaryType()),
            (pa.binary(), ice_types.StringType()),
            (pa.decimal128(10, 2), ice_types.DecimalType(12, 4)),
        ],
    )
    def test_promotions_outside_the_spec_are_rejected(
        self, memory_catalog, make_table, old_type: pa.DataType, new_type
    ) -> None:
        """PyIceberg accepts these changes; the Iceberg spec does not."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", old_type)]),
        )
        schema_before = tbl.schema()
        with pytest.raises(ValidationError, match="Cannot change column type"):
            SchemaEvolutionHelper(tbl).enforce_column("v", new_type)
        tbl.refresh()
        assert tbl.schema() == schema_before

    @pytest.mark.parametrize(
        ("old_type", "new_type"),
        [
            (pa.int32(), ice_types.LongType()),
            (pa.float32(), ice_types.DoubleType()),
            (pa.decimal128(10, 2), ice_types.DecimalType(12, 2)),
        ],
    )
    def test_spec_promotions_apply_in_place(
        self, memory_catalog, make_table, old_type: pa.DataType, new_type
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", old_type)]),
        )
        field_id = tbl.schema().find_field("v").field_id
        SchemaEvolutionHelper(tbl).enforce_column("v", new_type)
        tbl.refresh()
        promoted = tbl.schema().find_field("v")
        assert promoted.field_type == new_type
        assert promoted.field_id == field_id

    def test_archives_to_legacy_when_opt_in(self, memory_catalog, make_table) -> None:
        """With ``legacy_archival=True``, an incompatible type triggers the
        archive-and-recreate flow."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column("v", ice_types.StringType(), legacy_archival=True)
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "v" in names
        assert "v__legacy_1" in names
        assert str(tbl.schema().find_field("v").field_type) == "string"
        assert str(tbl.schema().find_field("v__legacy_1").field_type) == "int"

    def test_increments_legacy_n_on_repeated_archival(
        self,
        memory_catalog,
        make_table,
    ) -> None:
        """Repeated archival picks ``__legacy_2``, ``__legacy_3``, ... so
        every historical type is preserved as a distinct sibling."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        helper = SchemaEvolutionHelper(tbl)

        # int -> string archives to v__legacy_1
        helper.enforce_column("v", ice_types.StringType(), legacy_archival=True)
        # string -> long archives to v__legacy_2
        helper.enforce_column("v", ice_types.LongType(), legacy_archival=True)

        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "v" in names
        assert "v__legacy_1" in names
        assert "v__legacy_2" in names
        assert str(tbl.schema().find_field("v").field_type) == "long"
        assert str(tbl.schema().find_field("v__legacy_1").field_type) == "int"
        assert str(tbl.schema().find_field("v__legacy_2").field_type) == "string"

        # Archived columns carry the [ARCHIVED] doc prefix.
        assert (tbl.schema().find_field("v__legacy_1").doc or "").startswith("[ARCHIVED]")
        assert (tbl.schema().find_field("v__legacy_2").doc or "").startswith("[ARCHIVED]")

    def test_archives_nested_field_to_legacy(self, memory_catalog, make_table) -> None:
        """Dotted paths archive inside the parent struct, so the legacy
        sibling lives at ``<parent>.<leaf>__legacy_N`` rather than at
        the top level."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field(
                        "address",
                        pa.struct(
                            [
                                pa.field("zip_code", pa.int32()),
                                pa.field("state", pa.large_string()),
                            ]
                        ),
                    ),
                ]
            ),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column(
            "address.zip_code",
            ice_types.StringType(),
            legacy_archival=True,
        )
        tbl.refresh()

        # The new column has the requested type at the canonical path.
        new_zip = tbl.schema().find_field("address.zip_code")
        assert str(new_zip.field_type) == "string"

        # The archive lives inside the parent struct, not at the top level.
        legacy = tbl.schema().find_field("address.zip_code__legacy_1")
        assert str(legacy.field_type) == "int"

        top_level_names = {f.name for f in tbl.schema().fields}
        assert "zip_code__legacy_1" not in top_level_names

    def test_compensating_rollback_on_step2_failure(
        self,
        memory_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If step 2 (the recreate) fails, the helper renames the
        archived column back to its original name so the schema is
        restored to its pre-call state."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        helper = SchemaEvolutionHelper(tbl)

        # Force step 2's add_column to fail by routing it through a
        # nonexistent parent struct. Step 1 (the rename) still runs
        # against the real top-level column.
        monkeypatch.setattr(
            SchemaEvolutionHelper,
            "_add_column_path",
            staticmethod(lambda _name: ("never_exists", "child")),
        )

        with pytest.raises(Exception):  # noqa: B017 — pyiceberg raises a varied set
            helper.enforce_column(
                "v",
                ice_types.StringType(),
                legacy_archival=True,
            )

        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        # Compensating rollback restored the original column.
        assert "v" in names
        assert "v__legacy_1" not in names
        # And its type is unchanged, without a leftover [ARCHIVED] doc.
        assert str(tbl.schema().find_field("v").field_type) == "int"
        assert tbl.schema().find_field("v").doc is None


class TestEnforceRename:
    def test_simple_rename(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("old_name", pa.large_string()),
                ]
            ),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_rename("old_name", "new_name")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "new_name" in names
        assert "old_name" not in names

    def test_idempotent_when_already_renamed(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("new_name", pa.large_string()),
                ]
            ),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_rename("old_name", "new_name")  # already done state
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "new_name" in names
        # No legacy column created.
        assert not any(n.startswith("new_name__legacy") for n in names)

    def test_collision_archives_blocking_column(
        self,
        memory_catalog,
        make_table,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("old_name", pa.large_string()),
                    pa.field("new_name", pa.large_string()),
                ]
            ),
        )
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_rename("old_name", "new_name")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "new_name" in names
        assert "old_name" not in names
        assert "new_name__legacy_1" in names

    def test_neither_exists_is_noop(
        self,
        memory_catalog,
        make_table,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        helper = SchemaEvolutionHelper(tbl)
        with caplog.at_level("WARNING"):
            helper.enforce_rename("nope", "also_nope")
        assert "Skipping rename" in caplog.text

    def test_neither_exists_still_checks_contract(self, memory_catalog, make_table) -> None:
        """A misspelled old name must not let a failing contract pass silently."""
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        contract = SchemaContract(
            [ColumnSpec("customer_id", ice_types.StringType(), required=True)]
        )
        with pytest.raises(SchemaContractViolation, match="customer_id"):
            SchemaEvolutionHelper(tbl).enforce_rename(
                "custmer_id", "customer_id", contract=contract
            )


# ===========================================================================
# SchemaEvolutionHelper: idempotency, archival scope, compensation
# ===========================================================================


def _address_table(memory_catalog, make_table):
    return make_table(
        memory_catalog,
        "ns.t",
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field(
                    "address",
                    pa.struct([pa.field("street", pa.string()), pa.field("zip", pa.int32())]),
                ),
                pa.field("tags", pa.list_(pa.string())),
                pa.field("attrs", pa.map_(pa.string(), pa.int64())),
            ]
        ),
    )


def _names(tbl) -> list[str]:
    tbl.refresh()
    return [f.name for f in tbl.schema().fields]


class TestEnforceColumnNestedIdempotent:
    def test_tables_own_struct_type_is_noop(self, memory_catalog, make_table) -> None:
        tbl = _address_table(memory_catalog, make_table)
        schemas_before = len(tbl.metadata.schemas)
        own = tbl.schema().find_field("address").field_type
        SchemaEvolutionHelper(tbl).enforce_column("address", own)
        tbl.refresh()
        assert len(tbl.metadata.schemas) == schemas_before

    def test_struct_with_other_ids_and_order_is_noop(self, memory_catalog, make_table) -> None:
        tbl = _address_table(memory_catalog, make_table)
        schemas_before = len(tbl.metadata.schemas)
        user_built = ice_types.StructType(
            ice_types.NestedField(501, "zip", ice_types.IntegerType(), required=False),
            ice_types.NestedField(500, "street", ice_types.StringType(), required=False),
        )
        SchemaEvolutionHelper(tbl).enforce_column("address", user_built)
        tbl.refresh()
        assert len(tbl.metadata.schemas) == schemas_before

    def test_identical_struct_with_matching_contract_passes(
        self, memory_catalog, make_table
    ) -> None:
        tbl = _address_table(memory_catalog, make_table)
        user_built = ice_types.StructType(
            ice_types.NestedField(500, "street", ice_types.StringType(), required=False),
            ice_types.NestedField(501, "zip", ice_types.IntegerType(), required=False),
        )
        contract = SchemaContract([ColumnSpec("address", user_built)])
        SchemaEvolutionHelper(tbl).enforce_column("address", user_built, contract=contract)

    def test_identical_list_and_map_are_noops(self, memory_catalog, make_table) -> None:
        tbl = _address_table(memory_catalog, make_table)
        schemas_before = len(tbl.metadata.schemas)
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column(
            "tags",
            ice_types.ListType(
                element_id=900, element=ice_types.StringType(), element_required=False
            ),
        )
        helper.enforce_column(
            "attrs",
            ice_types.MapType(
                key_id=901,
                key_type=ice_types.StringType(),
                value_id=902,
                value_type=ice_types.LongType(),
                value_required=False,
            ),
        )
        tbl.refresh()
        assert len(tbl.metadata.schemas) == schemas_before

    def test_archival_twice_on_identical_struct_keeps_data(
        self,
        memory_catalog,
        make_table,
    ) -> None:
        tbl = _address_table(memory_catalog, make_table)
        tbl.append(
            pa.table(
                {
                    "id": pa.array([1], pa.int64()),
                    "address": pa.array(
                        [{"street": "Main", "zip": 1}],
                        pa.struct([pa.field("street", pa.string()), pa.field("zip", pa.int32())]),
                    ),
                    "tags": pa.array([["a"]], pa.list_(pa.string())),
                    "attrs": pa.array([[("k", 1)]], pa.map_(pa.string(), pa.int64())),
                }
            )
        )
        own = tbl.schema().find_field("address").field_type
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column("address", own, legacy_archival=True)
        helper.enforce_column("address", own, legacy_archival=True)
        assert not any("__legacy" in n for n in _names(tbl))
        rows = tbl.scan().to_arrow().to_pylist()
        assert rows[0]["address"] == {"street": "Main", "zip": 1}

    def test_different_struct_raises_validation_error(self, memory_catalog, make_table) -> None:
        tbl = _address_table(memory_catalog, make_table)
        schema_before = tbl.schema()
        other = ice_types.StructType(
            ice_types.NestedField(1, "street", ice_types.StringType(), required=False),
            ice_types.NestedField(2, "country", ice_types.StringType(), required=False),
        )
        with pytest.raises(ValidationError, match="Cannot change column type: address"):
            SchemaEvolutionHelper(tbl).enforce_column("address", other)
        tbl.refresh()
        assert tbl.schema() == schema_before


class TestArchivalOnlyOnTypeConflict:
    def _int_table(self, memory_catalog, make_table):
        return make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )

    def test_commit_failure_propagates_without_archiving(
        self,
        memory_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tbl = self._int_table(memory_catalog, make_table)
        real_commit = UpdateSchema.commit
        calls = {"n": 0}

        def fail_first_commit(self) -> None:
            # Only the promotion commit fails; an archival attempted
            # afterwards would succeed and show up as v__legacy_1.
            calls["n"] += 1
            if calls["n"] == 1:
                raise CommitFailedException("concurrent schema change")
            real_commit(self)

        monkeypatch.setattr(UpdateSchema, "commit", fail_first_commit)
        with pytest.raises(CommitFailedException):
            SchemaEvolutionHelper(tbl).enforce_column(
                "v",
                ice_types.LongType(),
                legacy_archival=True,
            )
        assert calls["n"] == 1
        assert "v__legacy_1" not in _names(tbl)
        assert tbl.schema().find_field("v").field_type == ice_types.IntegerType()

    def test_failing_contract_after_promotion_does_not_archive(
        self,
        memory_catalog,
        make_table,
    ) -> None:
        tbl = self._int_table(memory_catalog, make_table)
        contract = SchemaContract([ColumnSpec("missing", ice_types.LongType(), required=True)])
        with pytest.raises(SchemaContractViolation):
            SchemaEvolutionHelper(tbl).enforce_column(
                "v",
                ice_types.LongType(),
                contract=contract,
                legacy_archival=True,
            )
        assert not any("__legacy" in n for n in _names(tbl))
        assert tbl.schema().find_field("v").field_type == ice_types.LongType()

    def test_doc_only_change_on_identical_type(self, memory_catalog, make_table) -> None:
        tbl = self._int_table(memory_catalog, make_table)
        SchemaEvolutionHelper(tbl).enforce_column("v", ice_types.IntegerType(), doc="a value")
        tbl.refresh()
        field = tbl.schema().find_field("v")
        assert field.doc == "a value"
        assert field.field_type == ice_types.IntegerType()

    def test_arrow_type_is_accepted(self, memory_catalog, make_table) -> None:
        tbl = self._int_table(memory_catalog, make_table)
        helper = SchemaEvolutionHelper(tbl)
        helper.enforce_column("amt", pa.float64())
        tbl.refresh()
        assert tbl.schema().find_field("amt").field_type == ice_types.DoubleType()


class TestArchivalLayoutGuards:
    def _region_table(self, memory_catalog, **create_kwargs):
        memory_catalog.create_namespace("ns")
        schema = IcebergSchema(
            ice_types.NestedField(1, "id", ice_types.LongType(), required=True),
            ice_types.NestedField(2, "region", ice_types.IntegerType(), required=False),
            identifier_field_ids=create_kwargs.pop("identifier_field_ids", []),
        )
        return memory_catalog.create_table("ns.t", schema=schema, **create_kwargs)

    def test_partition_source_is_not_archived(self, memory_catalog) -> None:
        spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="region"
            )
        )
        tbl = self._region_table(memory_catalog, partition_spec=spec)
        schema_before = tbl.schema()
        with pytest.raises(ValueError, match="partition field 'region'"):
            SchemaEvolutionHelper(tbl).enforce_column(
                "region", ice_types.StringType(), legacy_archival=True
            )
        tbl.refresh()
        assert tbl.schema() == schema_before

    def test_sort_order_source_is_not_archived(self, memory_catalog) -> None:
        order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
        tbl = self._region_table(memory_catalog, sort_order=order)
        with pytest.raises(ValueError, match="sort order"):
            SchemaEvolutionHelper(tbl).enforce_column(
                "region", ice_types.StringType(), legacy_archival=True
            )
        assert "region__legacy_1" not in _names(tbl)

    def test_identifier_field_is_not_archived(self, memory_catalog) -> None:
        tbl = self._region_table(memory_catalog, identifier_field_ids=[1])
        with pytest.raises(ValueError, match="identifier fields"):
            SchemaEvolutionHelper(tbl).enforce_column(
                "id", ice_types.StringType(), legacy_archival=True
            )
        assert "id__legacy_1" not in _names(tbl)

    def test_required_column_archived_as_optional(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32(), nullable=False)]),
        )
        SchemaEvolutionHelper(tbl).enforce_column(
            "v", ice_types.StringType(), legacy_archival=True
        )
        tbl.refresh()
        legacy = tbl.schema().find_field("v__legacy_1")
        assert not legacy.required
        assert (legacy.doc or "").startswith("[ARCHIVED]")
        # Writers no longer have to supply the archived column.
        tbl.append(
            pa.table({"id": pa.array([1], pa.int64()), "v": pa.array(["one"], pa.string())})
        )
        assert tbl.scan().to_arrow().num_rows == 1


class TestArchivalCompensation:
    def test_required_column_restored_as_required(
        self,
        memory_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32(), nullable=False)]),
        )
        monkeypatch.setattr(
            SchemaEvolutionHelper,
            "_add_column_path",
            staticmethod(lambda _name: ("never_exists", "child")),
        )
        with pytest.raises(ValueError):
            SchemaEvolutionHelper(tbl).enforce_column(
                "v", ice_types.StringType(), legacy_archival=True
            )
        tbl.refresh()
        restored = tbl.schema().find_field("v")
        assert restored.required
        assert restored.doc is None
        assert restored.field_type == ice_types.IntegerType()

    def test_failed_compensation_raises_runtime_error(
        self,
        memory_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        monkeypatch.setattr(
            SchemaEvolutionHelper,
            "_add_column_path",
            staticmethod(lambda _name: ("never_exists", "child")),
        )
        real_rename = UpdateSchema.rename_column

        def rename_but_not_back(self, path_from, new_name):
            if path_from == "v__legacy_1":
                raise OSError("catalog unreachable")
            return real_rename(self, path_from, new_name)

        monkeypatch.setattr(UpdateSchema, "rename_column", rename_but_not_back)
        with pytest.raises(RuntimeError, match="stranded as 'v__legacy_1'"):
            SchemaEvolutionHelper(tbl).enforce_column(
                "v", ice_types.StringType(), legacy_archival=True
            )


class TestEnforceRenameGuards:
    def test_same_name_is_noop(self, memory_catalog, make_table) -> None:
        tbl = make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        schemas_before = len(tbl.metadata.schemas)
        SchemaEvolutionHelper(tbl).enforce_rename("id", "id")
        assert _names(tbl) == ["id"]
        assert len(tbl.metadata.schemas) == schemas_before

    def test_different_parents_raise(self, memory_catalog, make_table) -> None:
        tbl = _address_table(memory_catalog, make_table)
        schema_before = tbl.schema()
        with pytest.raises(ValueError, match="must share a parent struct"):
            SchemaEvolutionHelper(tbl).enforce_rename("address.zip", "zip")
        tbl.refresh()
        assert tbl.schema() == schema_before

    def test_collision_rename_failure_is_compensated(
        self,
        memory_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("old", pa.string()),
                    pa.field("new", pa.string()),
                ]
            ),
        )
        real_rename = UpdateSchema.rename_column

        def fail_old_to_new(self, path_from, new_name):
            if path_from == "old":
                raise OSError("catalog unreachable")
            return real_rename(self, path_from, new_name)

        monkeypatch.setattr(UpdateSchema, "rename_column", fail_old_to_new)
        with pytest.raises(OSError, match="unreachable"):
            SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        monkeypatch.undo()
        assert _names(tbl) == ["id", "old", "new"]
        assert tbl.schema().find_field("new").doc is None

    def test_collision_archive_doc(self, memory_catalog, make_table) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("old", pa.string()), pa.field("new", pa.string())]),
        )
        SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        tbl.refresh()
        assert (tbl.schema().find_field("new__legacy_1").doc or "").startswith("[ARCHIVED]")

    def _collision_table(self, memory_catalog, **create_kwargs):
        memory_catalog.create_namespace("ns")
        schema = IcebergSchema(
            ice_types.NestedField(1, "id", ice_types.LongType(), required=True),
            ice_types.NestedField(2, "old", ice_types.StringType(), required=False),
            ice_types.NestedField(3, "new", ice_types.StringType(), required=True),
            identifier_field_ids=create_kwargs.pop("identifier_field_ids", []),
        )
        return memory_catalog.create_table("ns.t", schema=schema, **create_kwargs)

    def test_collision_partition_source_is_not_archived(self, memory_catalog) -> None:
        spec = PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="new")
        )
        tbl = self._collision_table(memory_catalog, partition_spec=spec)
        schema_before = tbl.schema()
        with pytest.raises(ValueError, match="partition field 'new'"):
            SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        tbl.refresh()
        assert tbl.schema() == schema_before

    def test_collision_sort_order_source_is_not_archived(self, memory_catalog) -> None:
        order = SortOrder(SortField(source_id=3, transform=IdentityTransform()))
        tbl = self._collision_table(memory_catalog, sort_order=order)
        with pytest.raises(ValueError, match="sort order"):
            SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        assert _names(tbl) == ["id", "old", "new"]

    def test_collision_identifier_field_is_not_archived(self, memory_catalog) -> None:
        tbl = self._collision_table(memory_catalog, identifier_field_ids=[3])
        with pytest.raises(ValueError, match="identifier fields"):
            SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        assert _names(tbl) == ["id", "old", "new"]

    def test_collision_required_blocker_archived_as_optional(self, memory_catalog) -> None:
        tbl = self._collision_table(memory_catalog)
        SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        tbl.refresh()
        assert _names(tbl) == ["id", "new", "new__legacy_1"]
        legacy = tbl.schema().find_field("new__legacy_1")
        assert legacy.field_id == 3
        assert not legacy.required
        assert (legacy.doc or "").startswith("[ARCHIVED]")
        # Writers no longer have to supply the archived column.
        tbl.append(
            pa.table(
                {"id": pa.array([1], pa.int64()), "new": pa.array(["a"], pa.string())},
                schema=pa.schema(
                    [pa.field("id", pa.int64(), nullable=False), pa.field("new", pa.string())]
                ),
            )
        )
        assert tbl.scan().to_arrow().num_rows == 1

    def test_collision_compensation_restores_required(
        self, memory_catalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tbl = self._collision_table(memory_catalog)
        real_rename = UpdateSchema.rename_column

        def fail_old_to_new(self, path_from, new_name):
            if path_from == "old":
                raise OSError("catalog unreachable")
            return real_rename(self, path_from, new_name)

        monkeypatch.setattr(UpdateSchema, "rename_column", fail_old_to_new)
        with pytest.raises(OSError, match="unreachable"):
            SchemaEvolutionHelper(tbl).enforce_rename("old", "new")
        monkeypatch.undo()
        tbl.refresh()
        assert _names(tbl) == ["id", "old", "new"]
        restored = tbl.schema().find_field("new")
        assert restored.field_id == 3
        assert restored.required


class TestHelperLogger:
    def test_logger_name_uses_dotted_identifier(self, memory_catalog, make_table) -> None:
        tbl = make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        assert SchemaEvolutionHelper(tbl)._log.name == "iceberg_ops.core.schema.ns.t"
