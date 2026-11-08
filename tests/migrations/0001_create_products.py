"""Test-fixture migration 0001 — create products table."""

from __future__ import annotations

import pyarrow as pa

from iceberg_ops.migration import BaseMigration, MigrationContext


class Migration(BaseMigration):
    version = "0001"
    description = "Create products table"

    def upgrade(self, ctx: MigrationContext) -> None:
        ctx.create_table_if_not_exists(
            "products",
            pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("name", pa.large_string()),
                ]
            ),
        )

    def downgrade(self, ctx: MigrationContext) -> None:
        ctx.drop_table_if_exists("products")
