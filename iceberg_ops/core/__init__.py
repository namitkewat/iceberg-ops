"""
iceberg_ops.core
================
Shared building blocks for PyIceberg schema evolution, catalog
management, type translation, and data operations.

Sub-modules
-----------
catalog
    ``CatalogFactory`` — build local or remote PyIceberg catalogs through
    a single API surface.
schema
    ``diff_schemas``, ``SchemaDiff``, ``FieldChange``, ``SchemaContract``,
    ``ColumnSpec``, ``SchemaContractViolation``, ``SchemaEvolutionHelper``.
types
    ``arrow_type_to_iceberg``, ``iceberg_type_to_arrow`` — bidirectional
    primitive type translation between PyArrow and PyIceberg.
utils
    ``create_arrow_table``, ``upsert_streaming``, ``rollback_to_snapshot``,
    ``get_snapshots``, ``print_time_travel``, ``trace_field_history``.
"""

from __future__ import annotations
