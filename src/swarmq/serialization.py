"""SwarmQ serialization: JSON (msgspec) and msgpack for results."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgpack
import msgspec.json

from swarmq.exceptions import ResultSerializationError


_json_encoder = msgspec.json.Encoder()
_json_decoder = msgspec.json.Decoder()


def encode_json(obj: Any) -> bytes:
    """Serialize obj to JSON bytes."""
    try:
        return _json_encoder.encode(obj)
    except (TypeError, msgspec.EncodeError) as exc:
        raise ResultSerializationError(
            f"Failed to JSON-encode object of type {type(obj).__name__}: {exc}"
        ) from exc


def decode_json(data: bytes, type_hint: type | None = None) -> Any:
    """Deserialize JSON bytes."""
    try:
        if type_hint is not None:
            decoder = msgspec.json.Decoder(type_hint)
            return decoder.decode(data)
        return _json_decoder.decode(data)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise ResultSerializationError(
            f"Failed to JSON-decode data: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# msgpack serialization (for Results)
# ---------------------------------------------------------------------------

# Ext-type codes
_EXT_DATETIME = 1
_EXT_DATE = 2
_EXT_TIME = 3
_EXT_DECIMAL = 4
_EXT_UUID = 5
_EXT_SET = 6
_EXT_FROZENSET = 7


def _msgpack_default(obj: Any) -> msgpack.ExtType:
    """Encode non-native types as msgpack Ext types."""
    if isinstance(obj, datetime):
        return msgpack.ExtType(_EXT_DATETIME, obj.isoformat().encode())
    if isinstance(obj, date):
        return msgpack.ExtType(_EXT_DATE, obj.isoformat().encode())
    if isinstance(obj, time):
        return msgpack.ExtType(_EXT_TIME, obj.isoformat().encode())
    if isinstance(obj, Decimal):
        return msgpack.ExtType(_EXT_DECIMAL, str(obj).encode())
    if isinstance(obj, UUID):
        return msgpack.ExtType(_EXT_UUID, str(obj).encode())
    if isinstance(obj, set):
        return msgpack.ExtType(
            _EXT_SET, msgpack.packb(list(obj), default=_msgpack_default)
        )
    if isinstance(obj, frozenset):
        return msgpack.ExtType(
            _EXT_FROZENSET, msgpack.packb(list(obj), default=_msgpack_default)
        )
    raise TypeError(f"Unknown type: {type(obj)}")


def _msgpack_ext_hook(code: int, data: bytes) -> Any:
    """Decode msgpack Ext types back to Python objects."""
    if code == _EXT_DATETIME:
        return datetime.fromisoformat(data.decode())
    if code == _EXT_DATE:
        return date.fromisoformat(data.decode())
    if code == _EXT_TIME:
        return time.fromisoformat(data.decode())
    if code == _EXT_DECIMAL:
        return Decimal(data.decode())
    if code == _EXT_UUID:
        return UUID(data.decode())
    if code == _EXT_SET:
        return set(msgpack.unpackb(data, raw=False, ext_hook=_msgpack_ext_hook))
    if code == _EXT_FROZENSET:
        return frozenset(
            msgpack.unpackb(data, raw=False, ext_hook=_msgpack_ext_hook)
        )
    return msgpack.ExtType(code, data)


def encode_result(obj: Any) -> bytes:
    """Serialize obj to msgpack bytes."""
    try:
        return msgpack.packb(obj, default=_msgpack_default)
    except (TypeError, ValueError) as exc:
        raise ResultSerializationError(
            f"Failed to msgpack-encode object of type {type(obj).__name__}: {exc}"
        ) from exc


def decode_result(data: bytes) -> Any:
    """Deserialize msgpack bytes."""
    try:
        return msgpack.unpackb(data, raw=False, ext_hook=_msgpack_ext_hook)
    except Exception as exc:
        raise ResultSerializationError(
            f"Failed to msgpack-decode data: {exc}"
        ) from exc
