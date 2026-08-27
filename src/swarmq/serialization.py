"""SwarmQ serialization: JSON (msgspec) and msgpack for results."""

from __future__ import annotations

import hashlib
import math
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
            decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(type_hint)
            return decoder.decode(data)
        return _json_decoder.decode(data)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise ResultSerializationError(
            f"Failed to JSON-decode data: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# canonical_encode: deterministic bytes for dedup hashing
# ---------------------------------------------------------------------------


def _canonical_float(obj: float) -> float:
    """Finite floats pass through; NaN / ±Inf have no stable JSON form
    (NaN → null would collide with None in the hash)."""
    if math.isnan(obj) or math.isinf(obj):
        raise TypeError(
            "canonical_encode does not support NaN / +Inf / -Inf — "
            "these have no stable JSON representation (NaN would "
            "collide with None). Normalize upstream or disable dedup."
        )
    return obj


def _canonical_dict(obj: dict[Any, Any]) -> dict[str, Any]:
    """Sort keys; reject non-str keys (JSON constraint, makes sort stable)."""
    result = {}
    for key in sorted(obj.keys()):
        if not isinstance(key, str):
            raise TypeError(
                "canonical_encode requires str dict keys; "
                f"got {type(key).__name__!r}"
            )
        result[key] = _canonicalize(obj[key])
    return result


def _canonical_set(obj: set[Any] | frozenset[Any]) -> list[Any]:
    """Sort; raises TypeError for mixed/unsortable elements."""
    try:
        items = sorted(obj)
    except TypeError as exc:
        raise TypeError(
            f"canonical_encode cannot sort {type(obj).__name__} "
            f"with mixed element types: {exc}"
        ) from exc
    return [_canonicalize(item) for item in items]


def _canonicalize(obj: Any) -> Any:
    """Pre-normalize a value into a form that JSON-encodes deterministically.

    Contract: identical semantic inputs produce identical bytes under
    msgspec.json.Encoder, regardless of call-site key order or type
    variation between UUID vs str, datetime vs iso-string, etc.

    Reject types with no single canonical form (bytes, dataclass,
    msgspec.Struct, custom classes) — caller must normalize them.
    """
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    # datetime IS date (isinstance-wise) — check datetime first.
    if isinstance(obj, float):
        return _canonical_float(obj)
    if isinstance(obj, (UUID, Decimal)):
        return str(obj)
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return _canonical_dict(obj)
    if isinstance(obj, (list, tuple)):
        return [_canonicalize(item) for item in obj]
    if isinstance(obj, (set, frozenset)):
        return _canonical_set(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raise TypeError(
            "canonical_encode does not support bytes; "
            "dedup requires JSON-native args. Convert to str before scheduling, "
            "or disable dedup for this task."
        )
    raise TypeError(
        f"canonical_encode does not support {type(obj).__name__}; "
        "dedup requires JSON-native or pre-normalizable args. "
        "Convert to str/dict before scheduling, or disable dedup for this task."
    )


def canonical_encode(args: tuple[Any, ...], kwargs: dict[str, Any]) -> bytes:
    """Produce deterministic JSON bytes for (args, kwargs) dedup hashing.

    Pre-normalizes values (UUID→str, datetime→iso, Decimal→str, set→sorted list)
    and sorts dict keys recursively before msgspec.json encoding. Guarantees
    hash stability across call-site kwarg-order and supported-type variation.

    Raises TypeError for unsupported types (bytes, dataclass, custom classes).
    Raises TypeError for non-str dict keys and mixed-type sets.
    """
    canonical = {
        "args": _canonicalize(list(args)),
        "kwargs": _canonicalize(dict(kwargs)),
    }
    return _json_encoder.encode(canonical)


def compute_dedup_hash(task_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """sha256-hex of task_name + canonical_encode(args, kwargs).

    Shared helper for R3.5 producer-side SETNX: Client.schedule,
    Client._schedule_deferred, Client.schedule_many, and
    Scheduler._fire_one all derive the ``sq:unique:{hash}`` key with
    this function so the same (task_name, args, kwargs) triple
    always hashes to the same Valkey cell regardless of entry point.
    """
    payload = task_name.encode("utf-8") + b"\x00" + canonical_encode(args, kwargs)
    return hashlib.sha256(payload).hexdigest()


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


# Explicit unpack limits, defensive against a
# compromised (or foreign-written) backend. msgpack's defaults derive
# from len(data), which already blocks length-prefix bombs, but pinning
# explicit ceilings makes the contract independent of msgpack-version
# default drift and bounds the recursive set/frozenset ext hook with
# the same numbers. Far above any sane task result; not configurable on
# purpose — a result this large is a bug, not a tuning knob.
_UNPACK_LIMITS: dict[str, int] = {
    "max_array_len": 1_000_000,
    "max_map_len": 1_000_000,
    "max_str_len": 64 * 1024 * 1024,
    "max_bin_len": 64 * 1024 * 1024,
    "max_ext_len": 64 * 1024 * 1024,
}


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
        return set(
            msgpack.unpackb(
                data, raw=False, ext_hook=_msgpack_ext_hook, **_UNPACK_LIMITS
            )
        )
    if code == _EXT_FROZENSET:
        return frozenset(
            msgpack.unpackb(
                data, raw=False, ext_hook=_msgpack_ext_hook, **_UNPACK_LIMITS
            )
        )
    # Fail loud on an unknown ext code instead of
    # returning a raw, inert ExtType. An unknown code almost always means
    # schema drift -- a result written by a newer SwarmQ version with an
    # ext type this version does not understand. Silently handing back an
    # ExtType produces a confusingly-wrong object far from the cause;
    # raising here surfaces it as a clean ResultSerializationError
    # (decode_result wraps it).
    raise ValueError(
        f"unknown msgpack ext code {code} ({len(data)} bytes) — "
        f"likely a result encoded by a newer SwarmQ version (schema drift)"
    )


def encode_result(obj: Any) -> bytes:
    """Serialize obj to msgpack bytes."""
    try:
        return msgpack.packb(obj, default=_msgpack_default)  # type: ignore[no-any-return]  # msgpack ist untypisiert
    except (TypeError, ValueError) as exc:
        raise ResultSerializationError(
            f"Failed to msgpack-encode object of type {type(obj).__name__}: {exc}"
        ) from exc


def decode_result(data: bytes) -> Any:
    """Deserialize msgpack bytes (bounded — see ``_UNPACK_LIMITS``)."""
    try:
        return msgpack.unpackb(
            data, raw=False, ext_hook=_msgpack_ext_hook, **_UNPACK_LIMITS
        )
    except Exception as exc:
        raise ResultSerializationError(
            f"Failed to msgpack-decode data: {exc}"
        ) from exc
