"""Unit tests for swarmq.serialization."""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

from swarmq.exceptions import ResultSerializationError
from swarmq.serialization import (
    encode_json,
    decode_json,
    encode_result,
    decode_result,
)


class TestJsonPrimitives:
    """Test 1: JSON roundtrip with primitives."""

    @pytest.mark.parametrize(
        "value",
        [
            "hello",
            42,
            3.14,
            True,
            False,
            None,
            [1, 2, 3],
            {"key": "value"},
        ],
    )
    def test_json_roundtrip_primitives(self, value):
        encoded = encode_json(value)
        assert isinstance(encoded, bytes)
        decoded = decode_json(encoded)
        assert decoded == value


class TestJsonDatetime:
    """Test 2: JSON roundtrip with datetime."""

    def test_json_roundtrip_datetime(self):
        dt = datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        encoded = encode_json(dt)
        # Should be ISO 8601 string in JSON
        assert b"2025-06-15" in encoded
        decoded = decode_json(encoded, type_hint=datetime)
        assert decoded == dt


class TestJsonUuid:
    """Test 3: JSON roundtrip with UUID."""

    def test_json_roundtrip_uuid(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        encoded = encode_json(uid)
        assert b"12345678-1234-5678-1234-567812345678" in encoded
        decoded = decode_json(encoded, type_hint=UUID)
        assert decoded == uid


class TestJsonDecimal:
    """Test 4: JSON roundtrip with Decimal."""

    def test_json_roundtrip_decimal(self):
        d = Decimal("3.14159")
        encoded = encode_json(d)
        assert b"3.14159" in encoded
        decoded = decode_json(encoded, type_hint=Decimal)
        assert decoded == d


class TestJsonBytes:
    """Test 5: JSON roundtrip with bytes (base64)."""

    def test_json_roundtrip_bytes(self):
        b = b"hello world"
        encoded = encode_json(b)
        assert isinstance(encoded, bytes)
        decoded = decode_json(encoded, type_hint=bytes)
        assert decoded == b


class Color(Enum):
    RED = "red"
    GREEN = "green"


class TestJsonEnum:
    """Test 6: JSON roundtrip with Enum."""

    def test_json_roundtrip_enum(self):
        c = Color.RED
        encoded = encode_json(c)
        assert b'"red"' in encoded
        decoded = decode_json(encoded, type_hint=Color)
        assert decoded == c
        assert decoded is Color.RED


class TestMsgpackPrimitives:
    """Test 7: msgpack roundtrip with primitives."""

    @pytest.mark.parametrize(
        "value",
        [
            "hello",
            42,
            3.14,
            True,
            False,
            None,
            [1, 2, 3],
            {"key": "value"},
        ],
    )
    def test_msgpack_roundtrip_primitives(self, value):
        encoded = encode_result(value)
        assert isinstance(encoded, bytes)
        decoded = decode_result(encoded)
        assert decoded == value


class TestMsgpackDatetime:
    """Test 8: msgpack roundtrip with datetime (Ext-Type 1)."""

    def test_msgpack_roundtrip_datetime(self):
        dt = datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        encoded = encode_result(dt)
        decoded = decode_result(encoded)
        assert decoded == dt
        assert isinstance(decoded, datetime)


class TestMsgpackUuid:
    """Test 9: msgpack roundtrip with UUID (Ext-Type 5)."""

    def test_msgpack_roundtrip_uuid(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        encoded = encode_result(uid)
        decoded = decode_result(encoded)
        assert decoded == uid
        assert isinstance(decoded, UUID)


class TestMsgpackDecimal:
    """Test 10: msgpack roundtrip with Decimal (Ext-Type 4)."""

    def test_msgpack_roundtrip_decimal(self):
        d = Decimal("3.14159")
        encoded = encode_result(d)
        decoded = decode_result(encoded)
        assert decoded == d
        assert isinstance(decoded, Decimal)


class TestMsgpackSetFrozenset:
    """Test 11: msgpack roundtrip with set/frozenset (Ext-Types 6/7)."""

    def test_msgpack_roundtrip_set(self):
        s = {1, 2, 3}
        encoded = encode_result(s)
        decoded = decode_result(encoded)
        assert decoded == s
        assert isinstance(decoded, set)

    def test_msgpack_roundtrip_frozenset(self):
        fs = frozenset({4, 5, 6})
        encoded = encode_result(fs)
        decoded = decode_result(encoded)
        assert decoded == fs
        assert isinstance(decoded, frozenset)


class TestMsgpackNone:
    """Test 12: msgpack None result -> nil -> None."""

    def test_msgpack_none_roundtrip(self):
        encoded = encode_result(None)
        decoded = decode_result(encoded)
        assert decoded is None


class TestSerializationError:
    """Test 13: Non-serializable type -> ResultSerializationError."""

    def test_encode_json_non_serializable(self):
        with pytest.raises(ResultSerializationError):
            encode_json(object())

    def test_encode_result_non_serializable(self):
        with pytest.raises(ResultSerializationError):
            encode_result(object())

    def test_decode_json_invalid_data(self):
        with pytest.raises(ResultSerializationError):
            decode_json(b"not valid json")

    def test_decode_result_invalid_data(self):
        with pytest.raises(ResultSerializationError):
            decode_result(b"\xff\xff\xff")


class TestEmptyContainers:
    """Test 14: Empty dict/list serialize correctly."""

    def test_json_empty_dict(self):
        assert decode_json(encode_json({})) == {}

    def test_json_empty_list(self):
        assert decode_json(encode_json([])) == []

    def test_msgpack_empty_dict(self):
        assert decode_result(encode_result({})) == {}

    def test_msgpack_empty_list(self):
        assert decode_result(encode_result([])) == []


class TestNestedStructures:
    """Test 15: Nested structures (dict with datetime values)."""

    def test_msgpack_nested_datetime(self):
        dt = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        data = {
            "task_id": "abc-123",
            "created_at": dt,
            "tags": [1, 2, 3],
            "metadata": {"nested": True},
        }
        encoded = encode_result(data)
        decoded = decode_result(encoded)
        assert decoded["task_id"] == "abc-123"
        assert decoded["created_at"] == dt
        assert isinstance(decoded["created_at"], datetime)
        assert decoded["tags"] == [1, 2, 3]
        assert decoded["metadata"] == {"nested": True}

    def test_msgpack_nested_uuid_and_decimal(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        data = {
            "id": uid,
            "amount": Decimal("99.99"),
            "items": [Decimal("10.00"), Decimal("20.00")],
        }
        encoded = encode_result(data)
        decoded = decode_result(encoded)
        assert decoded["id"] == uid
        assert decoded["amount"] == Decimal("99.99")
        assert decoded["items"] == [Decimal("10.00"), Decimal("20.00")]
