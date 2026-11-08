"""
iceberg_ops.core.schema
=======================
Schema evolution, diffing, and contract validation for PyIceberg tables.

Public API
----------
``SchemaEvolutionHelper``
    Idempotent column enforcement and renaming with collision handling.
    Uses Python logging, compensating transactions on step 2 failures,
    and optional per-call ``SchemaContract`` checks.

``diff_schemas(schema_a, schema_b) -> SchemaDiff``
    Compare two Iceberg schemas by stable field ID and return a
    structured diff (added / removed / renamed / type-changed /
    doc-changed fields).

``SchemaContract``
    Declarative column spec. Call ``contract.enforce(table)`` before
    every append to catch schema drift early in the pipeline.

``ColumnSpec``, ``FieldChange``, ``SchemaDiff``, ``SchemaContractViolation``
    Supporting data-classes and exceptions.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa
from pyiceberg import types as ice_types
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table

logger = logging.getLogger(__name__)


# ===========================================================================
# Schema diff primitives
# ===========================================================================


@dataclass
class FieldChange:
    """One field-level difference between two schema versions."""

    field_id: int | None
    name: str
    change_type: Literal["added", "removed", "type_changed", "renamed", "doc_changed"]
    old_name: str | None = None
    old_type: str | None = None
    new_type: str | None = None
    old_doc: str | None = None
    new_doc: str | None = None

    def __str__(self) -> str:
        tag = f"(field_id={self.field_id})"
        if self.change_type == "added":
            return f"  [+] '{self.name}' type={self.new_type} {tag}"
        if self.change_type == "removed":
            return f"  [-] '{self.name}' type={self.old_type} {tag}"
        if self.change_type == "renamed":
            return f"  [~] '{self.old_name}' -> '{self.name}' {tag}"
        if self.change_type == "type_changed":
            return f"  [~] '{self.name}' {self.old_type} -> {self.new_type} {tag}"
        if self.change_type == "doc_changed":
            return f"  [~] '{self.name}' doc updated {tag}"
        return f"  [?] '{self.name}' unknown change {tag}"


@dataclass
class SchemaDiff:
    """
    The result of comparing two Iceberg schemas.

    Attributes
    ----------
    added : list[FieldChange]
        Fields new in *schema_b* (not present in *schema_a*).
    removed : list[FieldChange]
        Fields dropped from *schema_a* (not present in *schema_b*).
    changed : list[FieldChange]
        Fields present in both with a different name, type, or doc.
    """

    added: list[FieldChange] = field(default_factory=list)
    removed: list[FieldChange] = field(default_factory=list)
    changed: list[FieldChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True when both schemas are structurally identical."""
        return not (self.added or self.removed or self.changed)

    def summary(self) -> str:
        """
        Return a multi-line human-readable diff report.

        Returns
        -------
        str
            ``"Schemas are identical."`` for an empty diff, otherwise
            one ``Added`` / ``Removed`` / ``Changed`` section per
            non-empty list, one line per field change.
        """
        if self.is_empty:
            return "Schemas are identical."
        lines: list[str] = []
        if self.added:
            lines.append(f"Added ({len(self.added)}):")
            lines.extend(str(c) for c in self.added)
        if self.removed:
            lines.append(f"Removed ({len(self.removed)}):")
            lines.extend(str(c) for c in self.removed)
        if self.changed:
            lines.append(f"Changed ({len(self.changed)}):")
            lines.extend(str(c) for c in self.changed)
        return "\n".join(lines)


def diff_schemas(
    schema_a: IcebergSchema,
    schema_b: IcebergSchema,
) -> SchemaDiff:
    """
    Compare two Iceberg schemas and return a structured ``SchemaDiff``.

    Comparison uses **Iceberg field IDs** (not column names) as the
    stable identity key. This correctly identifies renames (same ID,
    different name) rather than treating them as a remove + add pair.

    Parameters
    ----------
    schema_a : IcebergSchema
        The "before" / baseline schema.
    schema_b : IcebergSchema
        The "after" / target schema.

    Returns
    -------
    SchemaDiff
        Structured diff with ``added``, ``removed``, and ``changed``
        lists.

    Example
    -------
    ::

        from iceberg_ops.core.schema import diff_schemas
        diff = diff_schemas(table.metadata.schemas[0], table.schema())
        print(diff.summary())
    """
    fields_a = {f.field_id: f for f in schema_a.fields}
    fields_b = {f.field_id: f for f in schema_b.fields}
    ids_a, ids_b = set(fields_a), set(fields_b)
    result = SchemaDiff()

    # Fields only in B → added
    for fid in sorted(ids_b - ids_a):
        f = fields_b[fid]
        result.added.append(
            FieldChange(
                field_id=fid,
                name=f.name,
                change_type="added",
                new_type=str(f.field_type),
            )
        )

    # Fields only in A → removed
    for fid in sorted(ids_a - ids_b):
        f = fields_a[fid]
        result.removed.append(
            FieldChange(
                field_id=fid,
                name=f.name,
                change_type="removed",
                old_type=str(f.field_type),
            )
        )

    # Fields in both → check for name / type / doc changes
    for fid in sorted(ids_a & ids_b):
        fa, fb = fields_a[fid], fields_b[fid]
        if fa.name != fb.name:
            result.changed.append(
                FieldChange(
                    field_id=fid,
                    name=fb.name,
                    change_type="renamed",
                    old_name=fa.name,
                )
            )
        if str(fa.field_type) != str(fb.field_type):
            result.changed.append(
                FieldChange(
                    field_id=fid,
                    name=fb.name,
                    change_type="type_changed",
                    old_type=str(fa.field_type),
                    new_type=str(fb.field_type),
                )
            )
        if fa.doc != fb.doc:
            result.changed.append(
                FieldChange(
                    field_id=fid,
                    name=fb.name,
                    change_type="doc_changed",
                    old_doc=fa.doc,
                    new_doc=fb.doc,
                )
            )

    return result


# ===========================================================================
# Schema contract
# ===========================================================================


@dataclass
class ColumnSpec:
    """Declaration of one column in a ``SchemaContract``.

    Attributes
    ----------
    name : str
        Top-level column name.
    type : ice_types.IcebergType
        Expected Iceberg type.
    required : bool
        For :meth:`SchemaContract.validate`, the column must be present.
        Autogenerate maps it to the field's ``required`` flag for the
        diff, but never adds a NOT NULL constraint to an existing table.
    doc : str or None
        Column description. Autogenerate sets it on newly added columns.
    """

    name: str
    type: ice_types.IcebergType
    required: bool = False
    doc: str | None = None


class SchemaContractViolation(Exception):  # noqa: N818 — established public API name
    """Raised when a table does not conform to its declared contract."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        bullet = "\n".join(f"  * {v}" for v in violations)
        super().__init__(f"Schema contract violated ({len(violations)} issue(s)):\n{bullet}")


_TYPE_MAP: dict[str, ice_types.IcebergType] = {
    "boolean": ice_types.BooleanType(),
    "int": ice_types.IntegerType(),
    "integer": ice_types.IntegerType(),
    "long": ice_types.LongType(),
    "float": ice_types.FloatType(),
    "double": ice_types.DoubleType(),
    "string": ice_types.StringType(),
    "binary": ice_types.BinaryType(),
    "date": ice_types.DateType(),
    "time": ice_types.TimeType(),
    "timestamp": ice_types.TimestampType(),
    "timestamptz": ice_types.TimestamptzType(),
    "uuid": ice_types.UUIDType(),
}


class SchemaContract:
    """
    Declares the expected schema for an Iceberg table.

    Validate *before* every ``table.append()`` to detect schema drift in
    the CI/CD pipeline rather than at query time.

    Parameters
    ----------
    columns : list[ColumnSpec]
        The expected column definitions.
    strict : bool
        ``True`` → the table must have *exactly* these columns and no
        others; used as an autogenerate target, columns missing from
        the contract are dropped (``delete_column``).
        ``False`` (default) → extra columns in the table are silently
        allowed (useful when the schema has legacy-archived columns);
        used as an autogenerate target, columns missing from the
        contract are kept and only the listed columns are reconciled.

    Example
    -------
    ::

        contract = SchemaContract([
            ColumnSpec("order_id",    ice_types.LongType(),   required=True),
            ColumnSpec("order_total", ice_types.DoubleType(), required=False),
        ])
        contract.enforce(table)   # raises SchemaContractViolation on mismatch

    Type comparison
    ---------------
    Types are compared structurally: field ids, field order and nested
    docs are ignored, so a struct, list or map column matches a
    contract built with different field ids. A type that would only be
    reached by a promotion (``int`` where the contract says ``long``)
    is still reported as a violation.
    """

    def __init__(self, columns: list[ColumnSpec], strict: bool = False) -> None:
        self.columns = columns
        self.strict = strict

    def validate(self, table: Table) -> list[str]:
        """
        Check *table*'s current schema against this contract.

        Parameters
        ----------
        table : Table
            The table to check; it is refreshed first.

        Returns
        -------
        list[str]
            Violation messages. Empty list means the table conforms.
        """
        table.refresh()
        existing = {f.name: f for f in table.schema().fields}
        violations: list[str] = []

        for spec in self.columns:
            if spec.name not in existing:
                if spec.required:
                    violations.append(f"Required column '{spec.name}' is missing.")
                continue
            actual_type = existing[spec.name].field_type
            if not _types_equivalent(actual_type, spec.type):
                violations.append(
                    f"Column '{spec.name}' type is '{actual_type}' but contract requires "
                    f"'{spec.type}'."
                )

        if self.strict:
            declared = {s.name for s in self.columns}
            for extra in sorted(set(existing) - declared):
                violations.append(f"Strict mode: undeclared column '{extra}' found.")

        return violations

    def enforce(self, table: Table) -> None:
        """
        Like :meth:`validate`, but raises ``SchemaContractViolation`` on
        failure.

        Call this immediately before ``table.append(data)`` to make
        contract violations a hard error rather than a silent drift.

        Parameters
        ----------
        table : Table
            The table to check; it is refreshed first.

        Raises
        ------
        SchemaContractViolation
            If the table does not conform; its ``violations`` attribute
            lists every issue.
        """
        issues = self.validate(table)
        if issues:
            raise SchemaContractViolation(violations=issues)

    @classmethod
    def from_yaml(cls, path: str) -> SchemaContract:
        """
        Load a schema contract from a YAML file.

        The file must hold a mapping with a non-empty ``columns`` list
        and an optional ``strict`` boolean. Each column entry is a
        mapping with string ``name`` and ``type`` keys, an optional
        boolean ``required`` and an optional string ``doc``. Column
        names must be unique. The file is read as UTF-8 (a leading
        byte-order mark is ignored).

        ``type`` is case-insensitive and accepts ``boolean``, ``int`` /
        ``integer``, ``long``, ``float``, ``double``, ``string``,
        ``binary``, ``date``, ``time``, ``timestamp``, ``timestamptz``,
        ``uuid`` and the parameterised Iceberg primitives
        ``decimal(P,S)`` (``1 <= P <= 38``, ``0 <= S <= P``) and
        ``fixed[N]`` (``N >= 1``), plus the other Iceberg primitive
        type strings such as ``timestamp_ns``. Format-version 3 types
        (``timestamp_ns``, ``timestamptz_ns``, ``unknown``,
        ``geometry``, ``geography``) are parsed here; autogenerate
        refuses them for a format-version 2 table. Nested types are not
        supported.

        Parameters
        ----------
        path : str
            Path to the YAML contract file.

        Returns
        -------
        SchemaContract
            A contract instance ready for ``enforce()`` or
            ``validate()``.

        Raises
        ------
        ImportError
            If pyyaml is not installed.
        OSError
            If the YAML file cannot be read (``FileNotFoundError`` when
            it does not exist).
        ValueError
            If the file is not valid YAML, does not have the layout
            described above, or names an unknown or nested column type.

        Example
        -------
        ::

            contract = SchemaContract.from_yaml("contracts/orders.yaml")
            contract.enforce(table)
        """
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "YAML contracts require the 'contracts' extra. "
                "Install with: pip install 'iceberg-ops[contracts]'"
            ) from None

        with open(path, encoding="utf-8-sig") as fh:
            try:
                data = yaml.safe_load(fh)
            except yaml.YAMLError as exc:
                raise ValueError(f"not valid YAML: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError(
                "expected a mapping with a 'columns' list at the top level, "
                f"got {type(data).__name__}."
            )
        strict = data.get("strict", False)
        if not isinstance(strict, bool):
            raise ValueError(f"'strict' must be true or false, got {strict!r}.")
        raw_columns = data.get("columns")
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError("'columns' must be a non-empty list of column mappings.")

        columns: list[ColumnSpec] = []
        seen: set[str] = set()
        for index, col in enumerate(raw_columns):
            if not isinstance(col, dict):
                raise ValueError(
                    f"columns[{index}] must be a mapping with 'name' and 'type' keys."
                )
            name = col.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"columns[{index}] needs a non-empty string 'name'.")
            if name in seen:
                raise ValueError(f"duplicate column name {name!r}.")
            seen.add(name)
            type_str = col.get("type")
            if not isinstance(type_str, str):
                raise ValueError(f"column {name!r} needs a string 'type'.")
            required = col.get("required", False)
            if not isinstance(required, bool):
                raise ValueError(
                    f"column {name!r}: 'required' must be true or false, got {required!r}."
                )
            doc = col.get("doc")
            if doc is not None and not isinstance(doc, str):
                raise ValueError(f"column {name!r}: 'doc' must be a string, got {doc!r}.")
            columns.append(
                ColumnSpec(
                    name=name,
                    type=_parse_contract_type(type_str, name),
                    required=required,
                    doc=doc,
                )
            )

        return cls(columns, strict=strict)


def _parse_contract_type(type_str: str, column: str) -> ice_types.IcebergType:
    """Parse a YAML contract ``type`` string into an Iceberg primitive type.

    Aliases from ``_TYPE_MAP`` win; anything else goes through
    PyIceberg's own type-string parser and must yield a primitive.
    Decimals need ``1 <= P <= 38`` and ``0 <= S <= P``; fixed types
    need a length of at least 1.

    Parameters
    ----------
    type_str : str
        The YAML ``type`` value (case-insensitive).
    column : str
        Column name, used in the error message.

    Returns
    -------
    ice_types.IcebergType
        The parsed primitive type.

    Raises
    ------
    ValueError
        If the string is not a known primitive type or has invalid
        parameters.
    """
    from pydantic import TypeAdapter, ValidationError
    from pyiceberg.exceptions import ValidationError as IcebergValidationError

    key = type_str.strip().lower()
    if key in _TYPE_MAP:
        return _TYPE_MAP[key]
    parsed: object = None
    try:
        parsed = TypeAdapter(ice_types.IcebergType).validate_python(key)
    except ValidationError:
        parsed = None
    except IcebergValidationError as exc:
        # Raised for a recognised type with bad parameters, such as
        # decimal(40,2); not a ValueError, so translate it.
        raise ValueError(
            f"Invalid column type {type_str!r} for column {column!r}: {exc}"
        ) from None
    if isinstance(parsed, ice_types.DecimalType) and not (0 <= parsed.scale <= parsed.precision):
        raise ValueError(
            f"Invalid column type {type_str!r} for column {column!r}: a decimal "
            "scale must be between 0 and the precision."
        )
    if isinstance(parsed, ice_types.FixedType) and len(parsed) < 1:
        raise ValueError(
            f"Invalid column type {type_str!r} for column {column!r}: a fixed "
            "length must be at least 1."
        )
    if isinstance(parsed, ice_types.PrimitiveType):
        return parsed
    raise ValueError(
        f"Unknown column type {type_str!r} for column {column!r}. Supported types: "
        f"{', '.join(sorted(_TYPE_MAP))}, decimal(P,S), fixed[N]."
    )


# ===========================================================================
# Schema evolution helper
# ===========================================================================


def _types_equivalent(a: ice_types.IcebergType, b: ice_types.IcebergType) -> bool:
    """
    Return True when *a* and *b* describe the same type.

    Primitives compare with ``==``. Structs need the same set of field
    names with, per name, equal ``required`` flags and equivalent
    types; field ids, field order and nested docs are ignored. Lists
    compare ``element_required`` and recurse; maps compare
    ``value_required`` and recurse on key and value.

    Parameters
    ----------
    a, b : ice_types.IcebergType
        The types to compare.

    Returns
    -------
    bool
        Whether the types are equivalent.
    """
    if isinstance(a, ice_types.StructType) and isinstance(b, ice_types.StructType):
        fields_a = {f.name: f for f in a.fields}
        fields_b = {f.name: f for f in b.fields}
        return fields_a.keys() == fields_b.keys() and all(
            fields_a[n].required == fields_b[n].required
            and _types_equivalent(fields_a[n].field_type, fields_b[n].field_type)
            for n in fields_a
        )
    if isinstance(a, ice_types.ListType) and isinstance(b, ice_types.ListType):
        return a.element_required == b.element_required and _types_equivalent(
            a.element_type, b.element_type
        )
    if isinstance(a, ice_types.MapType) and isinstance(b, ice_types.MapType):
        return (
            a.value_required == b.value_required
            and _types_equivalent(a.key_type, b.key_type)
            and _types_equivalent(a.value_type, b.value_type)
        )
    if a.is_primitive and b.is_primitive:
        return a == b
    return False


def _can_promote(a: ice_types.IcebergType, b: ice_types.IcebergType) -> bool:
    """
    Return True when primitive type *a* may be promoted to *b* in place.

    Uses the Iceberg spec allow-list
    (:func:`iceberg_ops.core.types.is_allowed_promotion`: ``int`` to
    ``long``, ``float`` to ``double``, wider decimal precision at the
    same scale) rather than ``pyiceberg.schema.promote``, which also
    accepts changes the spec forbids (e.g. ``string`` to ``binary`` or
    a decimal scale change).

    Parameters
    ----------
    a : ice_types.IcebergType
        The current type.
    b : ice_types.IcebergType
        The requested type.

    Returns
    -------
    bool
        Whether ``update_column(field_type=b)`` is a spec-legal
        promotion. Always False for nested types.
    """
    from iceberg_ops.core.types import is_allowed_promotion

    if not (a.is_primitive and b.is_primitive):
        return False
    return is_allowed_promotion(a, b)


def _parent_path(name: str) -> str:
    """Return the parent struct path of a dotted *name* (``""`` for top level)."""
    return name.rsplit(".", 1)[0] if "." in name else ""


class SchemaEvolutionHelper:
    """
    Safe, idempotent schema-evolution wrapper around a PyIceberg ``Table``.

    Key behaviours
    --------------
    * Uses Python ``logging`` instead of ``print()``.
    * :meth:`enforce_column` adds a **compensating transaction**. If
      step 2 (add new column) fails after step 1 (rename to legacy)
      has committed, the helper attempts to rename back automatically.
    * :meth:`enforce_rename` uses the same compensating-transaction
      pattern when it has to archive a colliding column first.
    * Both methods accept an optional ``contract`` argument to run a
      :meth:`SchemaContract.enforce` check after the operation
      completes.

    Known gap
    ---------
    PyIceberg (as of 0.12) does not support rename + add-with-same-name
    in a single ``update_schema()`` commit, so the two-step approach
    remains. The compensating transaction significantly reduces (but
    cannot eliminate) the risk of a half-migrated table after a hard
    crash.

    Parameters
    ----------
    table : Table
        The active PyIceberg table to evolve.
    """

    def __init__(self, table: Table) -> None:
        self.table = table
        self._log = logging.getLogger(f"{__name__}.{'.'.join(table.name())}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _field_exists(self, col_name: str) -> bool:
        """Return True if *col_name* exists in the current (refreshed) schema."""
        self.table.refresh()
        try:
            self.table.schema().find_field(col_name)
            return True
        except ValueError:
            return False

    def _next_legacy_version(self, col_name: str) -> int:
        """
        Return the next unused ``__legacy_N`` integer suffix for *col_name*.

        Scans existing field names for ``<leaf>__legacy_<N>`` and returns
        ``max(N) + 1`` (or 1 if no legacy columns exist yet). For nested
        fields (dotted paths like ``address.zip_code``), the scan is
        limited to siblings within the parent struct.
        """
        self.table.refresh()
        leaf_name = col_name.rsplit(".", 1)[-1]
        pattern = re.compile(rf"^{re.escape(leaf_name)}__legacy_(\d+)$")

        if "." in col_name:
            parent_path = col_name.rsplit(".", 1)[0]
            try:
                parent_field = self.table.schema().find_field(parent_path)
                fields = (
                    parent_field.field_type.fields
                    if hasattr(parent_field.field_type, "fields")
                    else []
                )
            except ValueError:
                return 1
        else:
            fields = self.table.schema().fields

        max_v = 0
        for f in fields:
            m = pattern.match(f.name)
            if m:
                max_v = max(max_v, int(m.group(1)))
        return max_v + 1

    @staticmethod
    def _add_column_path(col_name: str) -> str | tuple[str, ...]:
        """
        Return the ``path`` argument for ``update_schema().add_column()``.

        PyIceberg requires a **tuple** for nested fields (e.g.
        ``("address", "state")``) but accepts a plain string for
        top-level columns. Dotted strings like ``"address.state"`` raise
        ``ValueError: Cannot add column with ambiguous name``.
        """
        if "." in col_name:
            return tuple(col_name.split("."))
        return col_name

    def _validate_parent_exists(self, col_name: str) -> None:
        """
        Raise ``ValueError`` if the parent struct of a dotted *col_name*
        does not exist in the current schema.
        """
        if "." not in col_name:
            return
        parent_path = col_name.rsplit(".", 1)[0]
        self.table.refresh()
        try:
            parent_field = self.table.schema().find_field(parent_path)
        except ValueError:
            raise ValueError(
                f"Parent struct '{parent_path}' does not exist in the schema. "
                f"Cannot operate on nested field '{col_name}'."
            ) from None
        if not hasattr(parent_field.field_type, "fields"):
            raise ValueError(
                f"Field '{parent_path}' is not a struct type "
                f"({parent_field.field_type}). Cannot operate on nested "
                f"field '{col_name}'."
            )

    def _warn_if_in_spec_or_sort(self, old_name: str, new_name: str) -> None:
        """
        Emit warnings if *old_name* was referenced in the partition spec
        or sort order.

        After a column rename, Iceberg internally tracks fields by ID so
        the spec / sort order still functions. However the *display
        name* in the spec may still show the old source column name
        depending on the catalog. This warning alerts migration authors
        so they can update the spec or sort order in a subsequent step
        if desired.
        """
        self.table.refresh()
        schema = self.table.schema()

        for pf in self.table.spec().fields:
            try:
                source_field = schema.find_field(pf.source_id)
                if source_field.name == new_name:
                    self._log.warning(
                        "Column '%s' was renamed to '%s' but is still referenced "
                        "in the partition spec. The partition spec may need updating.",
                        old_name,
                        new_name,
                    )
            except ValueError:
                pass

        sort_order = self.table.sort_order()
        for sf in sort_order.fields:
            try:
                source_field = schema.find_field(sf.source_id)
                if source_field.name == new_name:
                    self._log.warning(
                        "Column '%s' was renamed to '%s' but is still referenced "
                        "in the sort order. The sort order may need updating.",
                        old_name,
                        new_name,
                    )
            except ValueError:
                pass

    def _layout_references(self, field: ice_types.NestedField) -> list[str]:
        """
        Describe what in the table layout references *field* (or its children).

        Checks the current partition spec, the sort order and the
        schema's identifier fields.
        """
        from pyiceberg.schema import index_by_id

        ids = {field.field_id, *index_by_id(field.field_type)}
        refs = [
            f"partition field '{pf.name}'"
            for pf in self.table.spec().fields
            if pf.source_id in ids
        ]
        if any(sf.source_id in ids for sf in self.table.sort_order().fields):
            refs.append("sort order")
        if ids & set(self.table.schema().identifier_field_ids):
            refs.append("identifier fields")
        return refs

    def get_arrow_schema(self) -> pa.Schema:
        """
        Return the current Iceberg schema as a PyArrow schema.

        Returns
        -------
        pa.Schema
            The table's current schema, read after a refresh.
        """
        self.table.refresh()
        return self.table.schema().as_arrow()

    def get_iceberg_schema(self) -> IcebergSchema:
        """
        Return the current Iceberg schema object.

        Returns
        -------
        IcebergSchema
            The table's current schema, read after a refresh.
        """
        self.table.refresh()
        return self.table.schema()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enforce_column(
        self,
        col_name: str,
        new_type: ice_types.IcebergType | pa.DataType,
        doc: str | None = None,
        contract: SchemaContract | None = None,
        *,
        legacy_archival: bool = False,
    ) -> None:
        """
        Ensure *col_name* exists with *new_type*.

        Supports dotted paths for nested struct fields (e.g.
        ``"address.zip_code"``). The parent struct must already exist.

        Behaviour matrix
        ----------------
        +-----------------------------+--------------------------------------+
        | State                       | Action                               |
        +=============================+======================================+
        | Column absent               | Add new column.                      |
        +-----------------------------+--------------------------------------+
        | Column exists, equivalent   | No-op (only a changed *doc* is       |
        | type (including identical   | written). Struct field ids, field    |
        | struct / list / map types)  | order and nested docs are ignored.   |
        +-----------------------------+--------------------------------------+
        | Column exists, promotable   | ``update_column`` in place (Iceberg  |
        | primitive type              | spec promotions only: ``int`` to     |
        |                             | ``long``, ``float`` to ``double``,   |
        |                             | wider decimal precision).            |
        +-----------------------------+--------------------------------------+
        | Column exists, incompatible | If ``legacy_archival=False``         |
        | type                        | (default), ``ValidationError`` is    |
        |                             | raised and the table is left         |
        |                             | untouched.                           |
        |                             | If ``legacy_archival=True``,         |
        |                             | step 1 renames the existing column   |
        |                             | to ``<col>__legacy_N`` and step 2    |
        |                             | adds a fresh one. A step 2 failure   |
        |                             | reverses step 1 (compensating        |
        |                             | transaction).                        |
        +-----------------------------+--------------------------------------+

        Archival runs only on a genuine type incompatibility: commit
        errors and contract violations propagate without archiving. The
        archived column keeps its field id and data, is made optional
        (so writers no longer have to supply it) and gets an
        ``[ARCHIVED]`` doc. A column referenced by the current partition
        spec, the sort order or the identifier fields is never archived,
        because those references would silently move to the legacy
        column.

        Parameters
        ----------
        col_name : str
            The canonical column name to enforce. Use dotted paths for
            nested struct fields (e.g. ``"address.state"``).
        new_type : ice_types.IcebergType or pa.DataType
            The intended data type. A primitive PyArrow type (e.g.
            ``pa.float64()``) is translated to its Iceberg equivalent.
        doc : str, optional
            Human-readable description stored in the Iceberg schema
            catalog.
        contract : SchemaContract, optional
            If provided, ``contract.enforce(table)`` is called once
            after the operation completes to verify the full schema
            still conforms.
        legacy_archival : bool, optional
            Opt-in to the archive-and-recreate flow on incompatible
            type change. Default ``False``.

        Raises
        ------
        pyiceberg.exceptions.ValidationError
            If the column exists with an incompatible type and
            ``legacy_archival`` is ``False``. Nested types cannot change
            in place; evolve their sub-fields with dotted paths.
        ValueError
            If *col_name* is a dotted path and the parent struct does
            not exist, or archival is requested for a column referenced
            by the partition spec, sort order or identifier fields.
        SchemaContractViolation
            If *contract* is given and the table does not conform.
        RuntimeError
            If the compensating transaction also fails after a step 2
            error (only possible when ``legacy_archival=True``).
        """
        if isinstance(new_type, pa.DataType):
            from iceberg_ops.core.types import arrow_type_to_iceberg

            new_type = arrow_type_to_iceberg(new_type)

        self.table.refresh()
        self._validate_parent_exists(col_name)

        if not self._field_exists(col_name):
            with self.table.update_schema() as upd:
                upd.add_column(self._add_column_path(col_name), new_type, doc=doc)
            self._log.info("Added new column '%s' (%s).", col_name, new_type)
        else:
            existing = self.table.schema().find_field(col_name)
            if _types_equivalent(existing.field_type, new_type):
                if doc is not None and doc != existing.doc:
                    with self.table.update_schema() as upd:
                        upd.update_column(col_name, doc=doc)
                    self._log.info("Updated doc of column '%s'.", col_name)
                else:
                    self._log.info("Column '%s' already matches (%s).", col_name, new_type)
            elif _can_promote(existing.field_type, new_type):
                with self.table.update_schema() as upd:
                    upd.update_column(col_name, field_type=new_type, doc=doc)
                self._log.info(
                    "Promoted column '%s' (%s -> %s).",
                    col_name,
                    existing.field_type,
                    new_type,
                )
            elif not legacy_archival:
                from pyiceberg.exceptions import ValidationError

                raise ValidationError(
                    f"Cannot change column type: {col_name}: {existing.field_type} -> "
                    f"{new_type}. Nested types cannot change in place; evolve sub-fields "
                    f"with dotted paths or pass legacy_archival=True."
                )
            else:
                self._archive_and_recreate(col_name, existing, new_type, doc)

        if contract:
            contract.enforce(self.table)

    def _archive_and_recreate(
        self,
        col_name: str,
        old_field: ice_types.NestedField,
        new_type: ice_types.IcebergType,
        doc: str | None,
    ) -> None:
        """
        Archive *col_name* to ``<col>__legacy_N`` and add a fresh column.

        Step 1 renames the existing column (and makes it optional);
        step 2 adds the new column and marks the legacy one
        ``[ARCHIVED]``. If step 2 fails, step 1 is reversed.
        """
        version = self._next_legacy_version(col_name)
        leaf_name = col_name.rsplit(".", 1)[-1]
        legacy_leaf = f"{leaf_name}__legacy_{version}"
        parent_path = _parent_path(col_name)
        legacy_full = f"{parent_path}.{legacy_leaf}" if parent_path else legacy_leaf

        refs = self._layout_references(old_field)
        if refs:
            raise ValueError(
                f"Cannot archive '{col_name}': referenced by {refs}. Archival keeps the "
                f"field id, so these would move to '{legacy_full}'. Update the partition "
                f"spec / sort order / identifier fields first."
            )

        legacy_doc = (
            f"[ARCHIVED] {old_field.doc}"
            if old_field.doc
            else "[ARCHIVED] Legacy column due to incompatible type change."
        )

        self._log.warning(
            "Type conflict on '%s' (%s -> %s). Archiving to '%s'.",
            col_name,
            old_field.field_type,
            new_type,
            legacy_full,
        )

        # STEP 1: rename the existing column to its legacy name. It is
        # made optional so writers no longer have to supply it.
        with self.table.update_schema() as upd:
            if old_field.required:
                upd.make_column_optional(col_name)
            upd.rename_column(col_name, legacy_leaf)

        # STEP 2: add the fresh column, then mark the legacy one. The add
        # comes first so a failing add stages nothing. Compensate on failure.
        try:
            with self.table.update_schema() as upd:
                upd.add_column(self._add_column_path(col_name), new_type, doc=doc)
                upd.update_column(legacy_full, doc=legacy_doc)
            self._log.info("Recreated '%s' with type %s.", col_name, new_type)
        except Exception as add_err:
            self._log.error(
                "Step 2 failed (%s). Compensating: renaming '%s' back to '%s'.",
                add_err,
                legacy_full,
                col_name,
            )
            try:
                # Restoring "required" is an optional -> required change,
                # which PyIceberg only allows as an incompatible change.
                with self.table.update_schema(
                    allow_incompatible_changes=old_field.required
                ) as upd:
                    upd.rename_column(legacy_full, leaf_name)
                    if old_field.required:
                        upd.update_column(legacy_full, required=True)
            except Exception as rollback_err:
                self._log.critical(
                    "COMPENSATING ROLLBACK FAILED: '%s' is stuck as '%s'. "
                    "Manual intervention required. Error: %s",
                    col_name,
                    legacy_full,
                    rollback_err,
                )
                raise RuntimeError(
                    f"Archival of '{col_name}' failed ({add_err}) and compensation "
                    f"failed; column is stranded as '{legacy_full}'. Rename it back "
                    f"manually."
                ) from rollback_err
            self._log.warning("Compensating rollback succeeded: '%s' restored.", col_name)
            raise

    def enforce_rename(
        self,
        old_name: str,
        new_name: str,
        doc: str | None = None,
        contract: SchemaContract | None = None,
    ) -> None:
        """
        Rename *old_name* → *new_name* idempotently.

        Supports dotted paths for nested struct fields. Both names must
        share the same parent struct (e.g. ``"address.zip_code"`` →
        ``"address.postal_code"``).

        Behaviour matrix
        ----------------
        +----------------------------+--------------------------------------+
        | State                      | Action                               |
        +============================+======================================+
        | *old_name* == *new_name*   | No-op.                               |
        +----------------------------+--------------------------------------+
        | Neither column exists      | No-op (warning logged); *contract*   |
        |                            | is still checked.                    |
        +----------------------------+--------------------------------------+
        | Only *new_name* exists     | Already done — optionally update doc.|
        +----------------------------+--------------------------------------+
        | Only *old_name* exists     | Simple rename.                       |
        +----------------------------+--------------------------------------+
        | Both exist (collision)     | Archive *new_name* → ``__legacy_N``, |
        |                            | then rename *old_name* → *new_name*. |
        |                            | If the rename fails, the archive is  |
        |                            | renamed back (compensating           |
        |                            | transaction).                        |
        +----------------------------+--------------------------------------+

        On a collision the archived column keeps its field id and data,
        is made optional (so writers no longer have to supply it) and
        gets an ``[ARCHIVED]`` doc, as with
        :meth:`enforce_column` archival. A blocking *new_name* that the
        current partition spec, the sort order or the identifier fields
        reference is never archived, because those references would
        silently move to the legacy column.

        Parameters
        ----------
        old_name : str
            Current column name. Use dotted paths for nested struct
            fields.
        new_name : str
            Desired column name. Use dotted paths for nested struct
            fields.
        doc : str, optional
            Documentation string to attach to the renamed column.
        contract : SchemaContract, optional
            Post-operation contract check.

        Raises
        ------
        ValueError
            If the names do not share a parent struct, the parent
            struct of a dotted *old_name* does not exist, or a colliding
            *new_name* is referenced by the partition spec, sort order
            or identifier fields. The table is left untouched.
        RuntimeError
            If the rename fails after a collision archive and renaming
            the archive back also fails.
        SchemaContractViolation
            If *contract* is given and the table does not conform.
        """
        if old_name == new_name:
            self._log.info("enforce_rename: '%s' already has that name; nothing to do.", old_name)
            if contract:
                contract.enforce(self.table)
            return
        if _parent_path(old_name) != _parent_path(new_name):
            raise ValueError(
                f"enforce_rename: '{old_name}' and '{new_name}' must share a parent struct"
            )

        self.table.refresh()
        self._validate_parent_exists(old_name)

        new_leaf = new_name.rsplit(".", 1)[-1]

        old_exists = self._field_exists(old_name)
        new_exists = self._field_exists(new_name)

        if not old_exists and not new_exists:
            self._log.warning(
                "Neither '%s' nor '%s' exists. Skipping rename.",
                old_name,
                new_name,
            )
            if contract:
                contract.enforce(self.table)
            return

        if not old_exists and new_exists:
            if doc:
                with self.table.update_schema() as upd:
                    upd.update_column(new_name, doc=doc)
            self._log.info("'%s' already exists (rename idempotent).", new_name)
            if contract:
                contract.enforce(self.table)
            return

        archived: str | None = None
        blocking_required = False
        if old_exists and new_exists:
            # Collision: archive the blocking column before renaming.
            blocking = self.table.schema().find_field(new_name)
            version = self._next_legacy_version(new_name)
            legacy_leaf = f"{new_leaf}__legacy_{version}"
            parent_path = _parent_path(new_name)
            legacy_full = f"{parent_path}.{legacy_leaf}" if parent_path else legacy_leaf
            refs = self._layout_references(blocking)
            if refs:
                raise ValueError(
                    f"Cannot rename '{old_name}' to '{new_name}': the existing "
                    f"'{new_name}' is referenced by {refs}. Archiving it keeps the field "
                    f"id, so these would move to '{legacy_full}'. Update the partition "
                    f"spec / sort order / identifier fields first."
                )
            archived = legacy_full
            blocking_required = blocking.required
            self._log.warning(
                "Rename collision: '%s' exists. Archiving to '%s'.",
                new_name,
                archived,
            )
            # The [ARCHIVED] doc lands with the rename below, so
            # compensation only has to rename back (and restore
            # "required"). The archive is made optional so writers no
            # longer have to supply it.
            with self.table.update_schema() as upd:
                if blocking_required:
                    upd.make_column_optional(new_name)
                upd.rename_column(new_name, legacy_leaf)

        # Perform the rename (simple path or after collision resolution).
        # NOTE: update_column(new_name) cannot be batched into the same
        # update_schema() block as rename_column(old → new) because
        # PyIceberg resolves column names against the schema state at
        # the START of the transaction, so new_name does not exist yet
        # within that block. The doc update is therefore a separate,
        # subsequent commit.
        try:
            with self.table.update_schema() as upd:
                upd.rename_column(old_name, new_leaf)
                if archived is not None:
                    upd.update_column(
                        archived,
                        doc="[ARCHIVED] Moved to prevent rename collision.",
                    )
        except Exception as rename_err:
            if archived is None:
                raise
            self._log.error(
                "Rename failed (%s). Compensating: renaming '%s' back to '%s'.",
                rename_err,
                archived,
                new_name,
            )
            try:
                # Restoring "required" is an optional -> required change,
                # which PyIceberg only allows as an incompatible change.
                with self.table.update_schema(allow_incompatible_changes=blocking_required) as upd:
                    upd.rename_column(archived, new_leaf)
                    if blocking_required:
                        upd.update_column(archived, required=True)
            except Exception as rollback_err:
                raise RuntimeError(
                    f"Rename of '{old_name}' to '{new_name}' failed ({rename_err}) and "
                    f"compensation failed; '{new_name}' is stranded as '{archived}'. "
                    f"Rename it back manually."
                ) from rollback_err
            raise

        if doc:
            with self.table.update_schema() as upd:
                upd.update_column(new_name, doc=doc)

        self._log.info("Renamed '%s' -> '%s'.", old_name, new_name)

        self._warn_if_in_spec_or_sort(old_name, new_name)

        if contract:
            contract.enforce(self.table)
