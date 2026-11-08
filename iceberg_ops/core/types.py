"""
iceberg_ops.core.types
======================
Bidirectional primitive type translation between PyArrow and PyIceberg,
plus the Iceberg type-promotion rule used by autogenerate.

Public API
----------
``arrow_type_to_iceberg(t)``
    Map a PyArrow ``DataType`` to its Iceberg ``IcebergType`` counterpart.
    Covers booleans, signed integers, floats, strings (including
    ``large_string`` and ``string_view``), binary (including
    ``large_binary`` and ``binary_view``), fixed-size binary, the
    ``arrow.uuid`` extension type, dates, times, timestamps (with and
    without timezone), decimals, and dictionary-encoded columns (mapped
    by their value type). Nested types (list, struct, map) raise
    ``ValueError`` — callers that handle nested shapes must do so before
    delegating here.

``iceberg_type_to_arrow(t)``
    Map an Iceberg type back to a PyArrow ``DataType``. Routes through
    PyIceberg's official ``schema_to_pyarrow`` so the returned type
    matches what PyIceberg readers and writers actually use; casts built
    against this result round-trip correctly.

``is_allowed_promotion(old, new)``
    ``True`` only for the type changes the Iceberg spec allows on an
    existing column: ``int -> long``, ``float -> double`` and decimal
    precision widening at the same scale.

Usage
-----
::

    from iceberg_ops.core.types import arrow_type_to_iceberg, iceberg_type_to_arrow

    iceberg_t = arrow_type_to_iceberg(arrow_field.type)
    arrow_t   = iceberg_type_to_arrow(iceberg_t)
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg import types as ice_types
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema


def arrow_type_to_iceberg(t: pa.DataType) -> ice_types.IcebergType:
    """
    Translate a PyArrow ``DataType`` to an Iceberg ``IcebergType``.

    Parameters
    ----------
    t : pa.DataType
        The PyArrow type to translate.

    Returns
    -------
    ice_types.IcebergType
        The matching Iceberg type. ``int8`` / ``int16`` / ``int32``
        collapse to ``IntegerType`` (32-bit); ``int64`` becomes
        ``LongType``. Timestamps with a timezone become
        ``TimestamptzType`` and naive timestamps become ``TimestampType``.
        ``time32`` / ``time64`` become ``TimeType``,
        ``fixed_size_binary(n)`` becomes ``FixedType(n)``, the
        ``arrow.uuid`` extension type becomes ``UUIDType``, the
        ``string_view`` / ``binary_view`` types map like their plain
        counterparts, and a dictionary-encoded type maps to its value
        type.

    Raises
    ------
    ValueError
        For types with no Iceberg primitive equivalent: nested types
        (lists, structs, maps — the caller must handle these), unsigned
        integers (Iceberg has none), decimals with more than 38 digits
        or a negative scale or one above the precision, and anything
        else unmapped. The message always starts with
        ``"Cannot map Arrow type"``.

    Example
    -------
    >>> arrow_type_to_iceberg(pa.int64())
    LongType()
    >>> arrow_type_to_iceberg(pa.timestamp("us", tz="UTC"))
    TimestamptzType()
    """
    if pa.types.is_dictionary(t):
        return arrow_type_to_iceberg(t.value_type)
    # The uuid extension type must be checked before fixed_size_binary:
    # its storage type is fixed_size_binary(16).
    if getattr(t, "extension_name", None) == "arrow.uuid":
        return ice_types.UUIDType()
    if pa.types.is_boolean(t):
        return ice_types.BooleanType()
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        return ice_types.IntegerType()
    if pa.types.is_int64(t):
        return ice_types.LongType()
    if pa.types.is_float32(t):
        return ice_types.FloatType()
    if pa.types.is_float64(t):
        return ice_types.DoubleType()
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return ice_types.StringType()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t):
        return ice_types.BinaryType()
    if pa.types.is_fixed_size_binary(t):
        return ice_types.FixedType(t.byte_width)
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return ice_types.DateType()
    if pa.types.is_time(t):
        return ice_types.TimeType()
    if pa.types.is_timestamp(t):
        # tz-aware → timestamptz; naive → timestamp
        if t.tz is not None:
            return ice_types.TimestamptzType()
        return ice_types.TimestampType()
    if pa.types.is_decimal(t):
        if t.precision > 38:
            raise ValueError(
                f"Cannot map Arrow type {t} to an Iceberg type: Iceberg decimals allow "
                "at most 38 digits."
            )
        if not 0 <= t.scale <= t.precision:
            raise ValueError(
                f"Cannot map Arrow type {t} to an Iceberg type: an Iceberg decimal "
                "scale must be between 0 and the precision."
            )
        return ice_types.DecimalType(precision=t.precision, scale=t.scale)
    if pa.types.is_nested(t):
        detail = "nested types (list / struct / map) must be handled by the caller"
    elif pa.types.is_unsigned_integer(t):
        detail = "Iceberg has no unsigned integers; cast to int64"
    else:
        detail = "no Iceberg equivalent"
    raise ValueError(f"Cannot map Arrow type {t} to an Iceberg type: {detail}.")


def is_allowed_promotion(old: ice_types.IcebergType, new: ice_types.IcebergType) -> bool:
    """
    Return whether changing a column from *old* to *new* is an Iceberg
    type promotion.

    The allow-list is the Iceberg v2 one: ``int -> long``,
    ``float -> double``, and ``decimal(p, s) -> decimal(q, s)`` with
    ``q > p`` (same scale). Every other change — narrowing, a scale
    change, ``string <-> binary``, ``fixed[16] -> uuid``,
    ``date -> timestamp``, identical types — returns ``False``.

    PyIceberg's own ``pyiceberg.schema.promote`` is deliberately not
    used: it accepts several of the changes above that the spec does
    not allow (for example a decimal scale change), and committing one
    can make existing data files unreadable.

    Parameters
    ----------
    old : ice_types.IcebergType
        The column's current type.
    new : ice_types.IcebergType
        The requested type.

    Returns
    -------
    bool
        ``True`` when the change is an allowed promotion.

    Example
    -------
    >>> is_allowed_promotion(ice_types.IntegerType(), ice_types.LongType())
    True
    >>> is_allowed_promotion(ice_types.DecimalType(10, 2), ice_types.DecimalType(10, 4))
    False
    """
    if type(old) is ice_types.IntegerType and type(new) is ice_types.LongType:
        return True
    if type(old) is ice_types.FloatType and type(new) is ice_types.DoubleType:
        return True
    if isinstance(old, ice_types.DecimalType) and isinstance(new, ice_types.DecimalType):
        return new.scale == old.scale and new.precision > old.precision
    return False


def iceberg_type_to_arrow(t: ice_types.IcebergType) -> pa.DataType:
    """
    Translate an Iceberg ``IcebergType`` to a PyArrow ``DataType``.

    Internally constructs a one-field sentinel schema and routes through
    PyIceberg's :func:`pyiceberg.io.pyarrow.schema_to_pyarrow`, ensuring
    the returned type matches what PyIceberg's own readers and writers
    use. Casts built against this result therefore round-trip correctly
    through the writer.

    Parameters
    ----------
    t : ice_types.IcebergType
        The Iceberg type to translate.

    Returns
    -------
    pa.DataType
        The matching PyArrow type.

    Example
    -------
    >>> iceberg_type_to_arrow(ice_types.LongType())
    DataType(int64)
    """
    sentinel = IcebergSchema(
        ice_types.NestedField(field_id=1, name="x", field_type=t, required=False),
    )
    arrow_schema = schema_to_pyarrow(sentinel)
    return arrow_schema.field(0).type
