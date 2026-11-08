"""iceberg-ops: Alembic-style schema migrations for Apache Iceberg.

Versioned migration files, autogenerate, and an Iceberg-backed history
table on PyIceberg catalogs, plus a cross-table data mover, basic
maintenance helpers and an experimental Apache Polaris policy executor.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
