"""Test-fixture migration 0002 — add sku column."""

from __future__ import annotations

from pyiceberg import types as ice_types

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = "0002"
    description = "Add sku column"

    def upgrade(self, ctx: MigrationContext) -> None:
        ctx.enforce_column(
            "products",
            "sku",
            ice_types.StringType(),
            doc="Stock keeping unit",
        )

    def downgrade(self, ctx: MigrationContext) -> None:
        ctx.delete_column("products", "sku")
