"""Tests for iceberg_ops.migration (BaseMigration + MigrationContext)."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg import types as ice_types
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table.sorting import SortDirection
from pyiceberg.transforms import DayTransform, IdentityTransform

from iceberg_ops.core.schema import SchemaEvolutionHelper
from iceberg_ops.migration import BaseMigration, MigrationContext

# ===========================================================================
# Helpers
# ===========================================================================


def _ctx(memory_catalog) -> MigrationContext:
    return MigrationContext(
        catalog=memory_catalog,
        namespace="ns",
        version="0001",
        description="test",
    )


# ===========================================================================
# Namespace + table helpers
# ===========================================================================


class TestQualified:
    def test_relative_name_qualified(self, memory_catalog) -> None:
        assert _ctx(memory_catalog).qualified("orders") == "ns.orders"

    def test_already_qualified_unchanged(self, memory_catalog) -> None:
        assert _ctx(memory_catalog).qualified("sales.orders") == "sales.orders"


class TestCreateTableIfNotExists:
    def test_creates_when_missing(self, memory_catalog) -> None:
        ctx = _ctx(memory_catalog)
        tbl = ctx.create_table_if_not_exists(
            "orders",
            pa.schema([pa.field("id", pa.int64())]),
        )
        assert tbl.name() == ("ns", "orders")

    def test_idempotent_when_present(self, memory_catalog) -> None:
        ctx = _ctx(memory_catalog)
        schema = pa.schema([pa.field("id", pa.int64())])
        tbl1 = ctx.create_table_if_not_exists("orders", schema)
        tbl2 = ctx.create_table_if_not_exists("orders", schema)
        assert tbl1.name() == tbl2.name()


class TestDropTableIfExists:
    def test_drops_existing(self, memory_catalog) -> None:
        ctx = _ctx(memory_catalog)
        ctx.create_table_if_not_exists("orders", pa.schema([pa.field("id", pa.int64())]))
        ctx.drop_table_if_exists("orders")
        with pytest.raises(NoSuchTableError):
            memory_catalog.load_table("ns.orders")

    def test_silent_when_missing(self, memory_catalog) -> None:
        ctx = _ctx(memory_catalog)
        # Should not raise.
        ctx.drop_table_if_exists("never_existed")


class TestGetHelper:
    def test_returns_helper_bound_to_table(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        helper = _ctx(memory_catalog).get_helper("t")
        assert isinstance(helper, SchemaEvolutionHelper)


class TestLoadTable:
    def test_loads_existing(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        tbl = _ctx(memory_catalog).load_table("t")
        assert tbl.name() == ("ns", "t")


# ===========================================================================
# Column lifecycle
# ===========================================================================


class TestAddColumn:
    def test_adds_top_level(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.add_column("t", "name", ice_types.StringType(), doc="user name")
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        f = tbl.schema().find_field("name")
        assert str(f.field_type) == "string"
        assert f.doc == "user name"

    def test_collision_raises(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("dup", pa.large_string())]),
        )
        ctx = _ctx(memory_catalog)
        with pytest.raises(Exception):  # noqa: B017 — pyiceberg exception type varies
            ctx.add_column("t", "dup", ice_types.StringType())


class TestEnforceColumn:
    def test_idempotent_add(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.enforce_column("t", "name", ice_types.StringType())
        ctx.enforce_column("t", "name", ice_types.StringType())
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = [f.name for f in tbl.schema().fields]
        assert names.count("name") == 1

    def test_type_conflict_raises_by_default(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        ctx = _ctx(memory_catalog)
        with pytest.raises(Exception):  # noqa: B017
            ctx.enforce_column("t", "v", ice_types.StringType())
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert not any(n.startswith("v__legacy") for n in names)

    def test_legacy_archival_opt_in(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        ctx = _ctx(memory_catalog)
        ctx.enforce_column(
            "t",
            "v",
            ice_types.StringType(),
            legacy_archival=True,
        )
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "v" in names
        assert "v__legacy_1" in names

    def test_legacy_archival_increments_n(self, memory_catalog, make_table) -> None:
        """Two successive archivals through MigrationContext produce
        ``v__legacy_1`` and ``v__legacy_2`` side-by-side."""
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        ctx = _ctx(memory_catalog)
        ctx.enforce_column("t", "v", ice_types.StringType(), legacy_archival=True)
        ctx.enforce_column("t", "v", ice_types.LongType(), legacy_archival=True)
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "v__legacy_1" in names
        assert "v__legacy_2" in names
        assert str(tbl.schema().find_field("v").field_type) == "long"


class TestEnforceRename:
    def test_simple_rename(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("old", pa.large_string())]),
        )
        ctx = _ctx(memory_catalog)
        ctx.enforce_rename("t", "old", "new")
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "new" in names
        assert "old" not in names


class TestDeleteColumn:
    def test_drops_column(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64()), pa.field("v", pa.int32())]),
        )
        ctx = _ctx(memory_catalog)
        ctx.delete_column("t", "v")
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        names = {f.name for f in tbl.schema().fields}
        assert "v" not in names


# ===========================================================================
# Partition spec / sort order / properties
# ===========================================================================


class TestUpdatePartitionSpec:
    def test_adds_day_partition(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("created_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        ctx = _ctx(memory_catalog)
        ctx.update_partition_spec(
            "t",
            add_fields=[("created_at", DayTransform(), "created_day")],
        )
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        spec_fields = tbl.spec().fields
        assert len(spec_fields) == 1
        assert spec_fields[0].name == "created_day"


class TestReplaceSortOrder:
    def test_invalid_direction_raises(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema([pa.field("id", pa.int64())]),
        )
        ctx = _ctx(memory_catalog)
        with pytest.raises(ValueError, match="Invalid sort direction"):
            ctx.replace_sort_order("t", sort_fields=[("id", "sideways")])

    def test_sets_asc_order(self, memory_catalog, make_table) -> None:
        make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("v", pa.large_string()),
                ]
            ),
        )
        ctx = _ctx(memory_catalog)
        ctx.replace_sort_order("t", sort_fields=[("id", "asc")])
        tbl = memory_catalog.load_table("ns.t")
        tbl.refresh()
        assert len(tbl.sort_order().fields) == 1


class TestProperties:
    def test_set_then_unset(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.set_table_properties("t", {"foo": "bar", "baz": "qux"})
        tbl = memory_catalog.load_table("ns.t")
        assert tbl.properties["foo"] == "bar"
        assert tbl.properties["baz"] == "qux"

        ctx.unset_table_properties("t", ["foo"])
        tbl = memory_catalog.load_table("ns.t")
        assert "foo" not in tbl.properties
        assert tbl.properties["baz"] == "qux"


class TestSetCompactionTargetSize:
    def test_writes_bytes_property(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.set_compaction_target_size("t", target_mb=256)
        tbl = memory_catalog.load_table("ns.t")
        assert tbl.properties["write.target-file-size-bytes"] == str(256 * 1024 * 1024)


class TestSetSnapshotRetention:
    def test_sets_both_when_given(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.set_snapshot_retention("t", min_snapshots=5, max_age_hours=72)
        props = memory_catalog.load_table("ns.t").properties
        assert props["history.expire.min-snapshots-to-keep"] == "5"
        assert props["history.expire.max-snapshot-age-ms"] == str(72 * 3600 * 1000)

    def test_no_args_is_noop(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.set_snapshot_retention("t")  # no kwargs
        props = memory_catalog.load_table("ns.t").properties
        assert "history.expire.min-snapshots-to-keep" not in props
        assert "history.expire.max-snapshot-age-ms" not in props


class TestSetClusteringKeys:
    def test_empty_list_raises(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        with pytest.raises(ValueError, match="at least one column"):
            ctx.set_clustering_keys("t", [])

    def test_sets_sort_order_only(
        self,
        memory_catalog,
        make_table,
    ) -> None:
        tbl = make_table(
            memory_catalog,
            "ns.t",
            pa.schema(
                [
                    pa.field("region", pa.large_string()),
                    pa.field("customer_id", pa.int64()),
                ]
            ),
        )
        commits_before = len(tbl.metadata.metadata_log)
        ctx = _ctx(memory_catalog)
        ctx.set_clustering_keys("t", ["region", "customer_id"], direction="desc")
        tbl = memory_catalog.load_table("ns.t")
        schema = tbl.schema()
        fields = tbl.sort_order().fields
        assert [schema.find_field(f.source_id).name for f in fields] == ["region", "customer_id"]
        assert all(f.direction == SortDirection.DESC for f in fields)
        assert "snowflake.cluster-by" not in tbl.properties
        assert "delta.clustering" not in tbl.properties
        # One replace_sort_order commit, no follow-up property commit.
        assert len(tbl.metadata.metadata_log) == commits_before + 1


# ===========================================================================
# BaseMigration
# ===========================================================================


class TestBaseMigration:
    def test_default_version_and_description_empty(self) -> None:
        m = BaseMigration()
        assert m.version == ""
        assert m.description == ""

    def test_upgrade_not_implemented_by_default(self, memory_catalog) -> None:
        m = BaseMigration()
        with pytest.raises(NotImplementedError):
            m.upgrade(_ctx(memory_catalog))

    def test_downgrade_not_implemented_by_default(self, memory_catalog) -> None:
        m = BaseMigration()
        with pytest.raises(NotImplementedError):
            m.downgrade(_ctx(memory_catalog))

    def test_pre_post_hooks_are_noops(self, memory_catalog) -> None:
        m = BaseMigration()
        ctx = _ctx(memory_catalog)
        # Should not raise.
        m.pre_upgrade(ctx)
        m.post_upgrade(ctx)
        m.pre_downgrade(ctx)
        m.post_downgrade(ctx)

    def test_repr_includes_version_and_description(self) -> None:
        m = BaseMigration()
        m.version = "0001"
        m.description = "test"
        assert "0001" in repr(m)
        assert "test" in repr(m)


# ===========================================================================
# create_table_if_not_exists: races, nested namespaces, create kwargs
# ===========================================================================


class TestCreateTableIfNotExistsHardening:
    def test_create_race_returns_existing_table(
        self,
        sqlite_catalog,
        make_table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Another process creates the table between the load and the create."""
        make_table(sqlite_catalog, "ns.orders", pa.schema([pa.field("id", pa.int64())]))
        real_load = sqlite_catalog.load_table
        calls = {"n": 0}

        def flaky_load(identifier):
            calls["n"] += 1
            if calls["n"] == 1:
                raise NoSuchTableError(identifier)
            return real_load(identifier)

        monkeypatch.setattr(sqlite_catalog, "load_table", flaky_load)
        tbl = _ctx(sqlite_catalog).create_table_if_not_exists(
            "orders",
            pa.schema([pa.field("id", pa.int64())]),
        )
        assert tbl.name() == ("ns", "orders")
        assert calls["n"] == 2

    def test_nested_namespace(self, sqlite_catalog) -> None:
        ctx = MigrationContext(
            catalog=sqlite_catalog,
            namespace="sales.eu",
            version="0001",
            description="test",
        )
        ctx.create_table_if_not_exists("orders", pa.schema([pa.field("id", pa.int64())]))
        tbl = sqlite_catalog.load_table("sales.eu.orders")
        assert tbl.name() == ("sales", "eu", "orders")

    def test_create_kwargs_apply_on_create_only(self, memory_catalog) -> None:
        schema = IcebergSchema(
            ice_types.NestedField(1, "region", ice_types.StringType(), required=False),
            ice_types.NestedField(2, "amount", ice_types.LongType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(
                source_id=1,
                field_id=1000,
                transform=IdentityTransform(),
                name="region",
            )
        )
        ctx = _ctx(memory_catalog)
        tbl = ctx.create_table_if_not_exists(
            "orders",
            schema,
            partition_spec=spec,
            properties={"k": "v"},
        )
        assert tbl.properties["k"] == "v"
        assert [f.name for f in tbl.spec().fields] == ["region"]

        again = ctx.create_table_if_not_exists(
            "orders",
            schema,
            properties={"k": "other", "extra": "x"},
        )
        assert again.properties["k"] == "v"
        assert "extra" not in again.properties
        assert [f.name for f in again.spec().fields] == ["region"]

    @pytest.mark.parametrize("layout", ["partition_spec", "sort_order"])
    def test_arrow_schema_with_layout_raises_before_creating(
        self, memory_catalog, layout: str
    ) -> None:
        """A pa.Schema has no field ids a spec or sort order could reference."""
        from pyiceberg.table.sorting import SortField, SortOrder

        kwargs = {
            "partition_spec": PartitionSpec(
                PartitionField(
                    source_id=1, field_id=1000, transform=IdentityTransform(), name="region"
                )
            ),
            "sort_order": SortOrder(SortField(source_id=1, transform=IdentityTransform())),
        }
        schema = pa.schema([pa.field("region", pa.string()), pa.field("amount", pa.int64())])
        with pytest.raises(ValueError, match=r"pyiceberg\.schema\.Schema"):
            _ctx(memory_catalog).create_table_if_not_exists(
                "orders", schema, **{layout: kwargs[layout]}
            )
        assert memory_catalog.list_namespaces() == []

    def test_arrow_schema_with_empty_layout_creates(self, memory_catalog) -> None:
        from pyiceberg.table.sorting import SortOrder

        tbl = _ctx(memory_catalog).create_table_if_not_exists(
            "orders",
            pa.schema([pa.field("id", pa.int64())]),
            partition_spec=PartitionSpec(),
            sort_order=SortOrder(),
        )
        assert tbl.spec().is_unpartitioned()
        assert tbl.sort_order().is_unsorted


# ===========================================================================
# Column helpers: PyArrow types, nested idempotency
# ===========================================================================


class TestPyArrowTypes:
    def test_enforce_column_accepts_arrow_type(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        ctx = _ctx(memory_catalog)
        ctx.enforce_column("t", "amt", pa.float64())
        tbl = memory_catalog.load_table("ns.t")
        assert tbl.schema().find_field("amt").field_type == ice_types.DoubleType()
        schemas_before = len(tbl.metadata.schemas)

        ctx.enforce_column("t", "amt", pa.float64())
        tbl = memory_catalog.load_table("ns.t")
        assert len(tbl.metadata.schemas) == schemas_before

    def test_add_column_accepts_arrow_type(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        _ctx(memory_catalog).add_column("t", "name", pa.string())
        tbl = memory_catalog.load_table("ns.t")
        assert tbl.schema().find_field("name").field_type == ice_types.StringType()


class TestEnforceColumnStructIdempotent:
    def test_same_struct_twice_is_noop(self, memory_catalog) -> None:
        ctx = _ctx(memory_catalog)
        ctx.create_table_if_not_exists(
            "t",
            pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field(
                        "address",
                        pa.struct([pa.field("street", pa.string()), pa.field("zip", pa.int32())]),
                    ),
                ]
            ),
        )
        struct = ice_types.StructType(
            ice_types.NestedField(100, "street", ice_types.StringType(), required=False),
            ice_types.NestedField(101, "zip", ice_types.IntegerType(), required=False),
        )
        schemas_before = len(memory_catalog.load_table("ns.t").metadata.schemas)
        ctx.enforce_column("t", "address", struct)
        ctx.enforce_column("t", "address", struct)
        tbl = memory_catalog.load_table("ns.t")
        assert len(tbl.metadata.schemas) == schemas_before
        assert [f.name for f in tbl.schema().fields] == ["id", "address"]


# ===========================================================================
# Properties: key names, retention notes
# ===========================================================================


class TestSetTablePropertiesKeys:
    def test_key_named_properties(self, memory_catalog, make_table) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        _ctx(memory_catalog).set_table_properties("t", {"properties": "x", "a": "b"})
        props = memory_catalog.load_table("ns.t").properties
        assert props["properties"] == "x"
        assert props["a"] == "b"


class TestSnapshotRetentionS3TablesWarning:
    def test_warns_on_s3tables_catalog(
        self,
        memory_catalog,
        make_table,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        memory_catalog.properties["rest.signing-name"] = "s3tables"
        with caplog.at_level("WARNING", logger="iceberg_ops.migration"):
            _ctx(memory_catalog).set_snapshot_retention("t", min_snapshots=5)
        assert "S3 Tables" in caplog.text
        props = memory_catalog.load_table("ns.t").properties
        assert props["history.expire.min-snapshots-to-keep"] == "5"

    def test_no_warning_elsewhere(
        self,
        memory_catalog,
        make_table,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        make_table(memory_catalog, "ns.t", pa.schema([pa.field("id", pa.int64())]))
        with caplog.at_level("WARNING", logger="iceberg_ops.migration"):
            _ctx(memory_catalog).set_snapshot_retention("t", min_snapshots=5)
        assert "S3 Tables" not in caplog.text
