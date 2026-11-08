"""Tests for iceberg_ops.core.types."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg import types as ice_types

from iceberg_ops.core.types import (
    arrow_type_to_iceberg,
    iceberg_type_to_arrow,
    is_allowed_promotion,
)


class TestArrowToIceberg:
    @pytest.mark.parametrize(
        ("arrow_t", "iceberg_t"),
        [
            (pa.bool_(), ice_types.BooleanType()),
            (pa.int8(), ice_types.IntegerType()),
            (pa.int16(), ice_types.IntegerType()),
            (pa.int32(), ice_types.IntegerType()),
            (pa.int64(), ice_types.LongType()),
            (pa.float32(), ice_types.FloatType()),
            (pa.float64(), ice_types.DoubleType()),
            (pa.string(), ice_types.StringType()),
            (pa.large_string(), ice_types.StringType()),
            (pa.binary(), ice_types.BinaryType()),
            (pa.large_binary(), ice_types.BinaryType()),
            (pa.date32(), ice_types.DateType()),
            (pa.date64(), ice_types.DateType()),
            (pa.timestamp("us"), ice_types.TimestampType()),
            (pa.timestamp("us", tz="UTC"), ice_types.TimestamptzType()),
            (pa.decimal128(10, 2), ice_types.DecimalType(precision=10, scale=2)),
            (pa.decimal128(38, 10), ice_types.DecimalType(precision=38, scale=10)),
            (pa.time64("us"), ice_types.TimeType()),
            (pa.time32("ms"), ice_types.TimeType()),
            (pa.binary(16), ice_types.FixedType(16)),
            (pa.string_view(), ice_types.StringType()),
            (pa.binary_view(), ice_types.BinaryType()),
            (pa.dictionary(pa.int32(), pa.string()), ice_types.StringType()),
        ],
    )
    def test_primitive_translation(
        self, arrow_t: pa.DataType, iceberg_t: ice_types.IcebergType
    ) -> None:
        assert arrow_type_to_iceberg(arrow_t) == iceberg_t

    @pytest.mark.skipif(not hasattr(pa, "uuid"), reason="pa.uuid() needs a newer pyarrow")
    def test_uuid_extension(self) -> None:
        assert arrow_type_to_iceberg(pa.uuid()) == ice_types.UUIDType()

    def test_unsigned_integer_message(self) -> None:
        with pytest.raises(ValueError, match="Cannot map Arrow type") as excinfo:
            arrow_type_to_iceberg(pa.uint32())
        assert "unsigned" in str(excinfo.value)
        assert "Nested" not in str(excinfo.value)
        assert "nested" not in str(excinfo.value)

    def test_unmapped_type_message(self) -> None:
        with pytest.raises(ValueError, match="no Iceberg equivalent"):
            arrow_type_to_iceberg(pa.null())

    def test_wide_decimal_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"Cannot map Arrow type.*38 digits"):
            arrow_type_to_iceberg(pa.decimal256(40, 2))

    @pytest.mark.parametrize("arrow_t", [pa.decimal128(10, -2), pa.decimal128(5, 10)])
    def test_decimal_scale_outside_precision_raises(self, arrow_t: pa.DataType) -> None:
        with pytest.raises(ValueError, match=r"Cannot map Arrow type.*scale"):
            arrow_type_to_iceberg(arrow_t)

    def test_decimal256_within_38_digits_maps(self) -> None:
        assert arrow_type_to_iceberg(pa.decimal256(38, 2)) == ice_types.DecimalType(38, 2)

    def test_list_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot map Arrow type"):
            arrow_type_to_iceberg(pa.list_(pa.int64()))

    def test_struct_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot map Arrow type"):
            arrow_type_to_iceberg(pa.struct([pa.field("a", pa.int64())]))

    def test_map_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot map Arrow type"):
            arrow_type_to_iceberg(pa.map_(pa.string(), pa.int64()))


class TestIcebergToArrow:
    def test_long_is_int64(self) -> None:
        assert pa.types.is_int64(iceberg_type_to_arrow(ice_types.LongType()))

    def test_integer_is_int32(self) -> None:
        assert pa.types.is_int32(iceberg_type_to_arrow(ice_types.IntegerType()))

    def test_double_is_float64(self) -> None:
        assert pa.types.is_float64(iceberg_type_to_arrow(ice_types.DoubleType()))

    def test_float_is_float32(self) -> None:
        assert pa.types.is_float32(iceberg_type_to_arrow(ice_types.FloatType()))

    def test_string_is_string_or_large(self) -> None:
        result = iceberg_type_to_arrow(ice_types.StringType())
        assert pa.types.is_string(result) or pa.types.is_large_string(result)

    def test_boolean(self) -> None:
        assert pa.types.is_boolean(iceberg_type_to_arrow(ice_types.BooleanType()))

    def test_date(self) -> None:
        result = iceberg_type_to_arrow(ice_types.DateType())
        assert pa.types.is_date32(result) or pa.types.is_date64(result)

    def test_timestamp_naive(self) -> None:
        result = iceberg_type_to_arrow(ice_types.TimestampType())
        assert pa.types.is_timestamp(result)
        assert result.tz is None

    def test_timestamp_with_tz(self) -> None:
        result = iceberg_type_to_arrow(ice_types.TimestamptzType())
        assert pa.types.is_timestamp(result)
        assert result.tz is not None

    def test_decimal(self) -> None:
        result = iceberg_type_to_arrow(ice_types.DecimalType(precision=18, scale=4))
        assert pa.types.is_decimal(result)
        assert result.precision == 18
        assert result.scale == 4


class TestRoundTrip:
    """arrow_type_to_iceberg(iceberg_type_to_arrow(t)) == t for primitives."""

    @pytest.mark.parametrize(
        "iceberg_t",
        [
            ice_types.BooleanType(),
            ice_types.IntegerType(),
            ice_types.LongType(),
            ice_types.FloatType(),
            ice_types.DoubleType(),
            ice_types.StringType(),
            ice_types.BinaryType(),
            ice_types.DateType(),
            ice_types.TimestampType(),
            ice_types.TimestamptzType(),
            ice_types.DecimalType(precision=10, scale=2),
            ice_types.DecimalType(precision=38, scale=10),
            ice_types.TimeType(),
            ice_types.UUIDType(),
            ice_types.FixedType(16),
        ],
    )
    def test_round_trip(self, iceberg_t: ice_types.IcebergType) -> None:
        assert arrow_type_to_iceberg(iceberg_type_to_arrow(iceberg_t)) == iceberg_t


class TestIsAllowedPromotion:
    @pytest.mark.parametrize(
        ("old", "new", "allowed"),
        [
            (ice_types.IntegerType(), ice_types.LongType(), True),
            (ice_types.FloatType(), ice_types.DoubleType(), True),
            (ice_types.DecimalType(10, 2), ice_types.DecimalType(12, 2), True),
            (ice_types.DecimalType(10, 2), ice_types.DecimalType(12, 4), False),
            (ice_types.DecimalType(10, 2), ice_types.DecimalType(10, 4), False),
            (ice_types.DecimalType(12, 2), ice_types.DecimalType(10, 2), False),
            (ice_types.DecimalType(10, 2), ice_types.DecimalType(10, 2), False),
            (ice_types.LongType(), ice_types.IntegerType(), False),
            (ice_types.DoubleType(), ice_types.FloatType(), False),
            (ice_types.StringType(), ice_types.LongType(), False),
            (ice_types.BinaryType(), ice_types.StringType(), False),
            (ice_types.StringType(), ice_types.BinaryType(), False),
            (ice_types.FixedType(16), ice_types.UUIDType(), False),
            (ice_types.DateType(), ice_types.TimestampType(), False),
            (ice_types.IntegerType(), ice_types.IntegerType(), False),
        ],
    )
    def test_allow_list(
        self, old: ice_types.IcebergType, new: ice_types.IcebergType, allowed: bool
    ) -> None:
        assert is_allowed_promotion(old, new) is allowed
