"""Property-based tests for swarmq.serialization.

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U3.3

Three invariants:

1. ``decode_json(encode_json(x)) == x`` for JSON-compatible ``x``.
2. ``decode_result(encode_result(x)) == x`` for typical result payload
   dicts (task_id, queue, value, error, duration_ms, …).
3. ``canonical_encode`` is stable under dict-key insertion-order
   permutations: the same semantic mapping always hashes to the same
   bytes.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from swarmq.serialization import (
    canonical_encode,
    decode_json,
    decode_result,
    encode_json,
    encode_result,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Avoid surrogates: msgspec's JSON encoder treats them as invalid UTF-8.
_SAFE_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    max_size=100,
)

# JSON cannot represent NaN / Inf stably across implementations; our
# encoder (msgspec) rejects them. Stay finite.
_SAFE_FLOAT = st.floats(allow_nan=False, allow_infinity=False, width=64)

_JSON_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    _SAFE_FLOAT,
    _SAFE_TEXT,
)


def _json_value(max_leaves: int = 20) -> st.SearchStrategy:
    """JSON-compatible values: scalars plus nested list/dict."""
    return st.recursive(
        _JSON_SCALARS,
        lambda children: st.one_of(
            st.lists(children, max_size=5),
            st.dictionaries(_SAFE_TEXT, children, max_size=5),
        ),
        max_leaves=max_leaves,
    )


def _result_dict() -> st.SearchStrategy:
    """Worker-side result payload shape."""
    return st.fixed_dictionaries(
        {
            "task_id": _SAFE_TEXT,
            "queue": _SAFE_TEXT,
            "task_name": _SAFE_TEXT,
            "value": _json_value(max_leaves=10),
            "error": st.one_of(st.none(), _SAFE_TEXT),
            "duration_ms": st.floats(
                min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False
            ),
            "attempt": st.integers(min_value=0, max_value=100),
        }
    )


def _equal_modulo_float(a, b, *, tol: float = 1e-9) -> bool:
    """Deep comparison that is tolerant of float drift across encodings."""
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
        return math.isclose(a, b, rel_tol=tol, abs_tol=tol)
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            return False
        return all(_equal_modulo_float(a[k], b[k], tol=tol) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(_equal_modulo_float(x, y, tol=tol) for x, y in zip(a, b))
    return a == b


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@given(value=_json_value())
def test_encode_decode_json_roundtrip(value) -> None:
    """JSON roundtrip is identity for JSON-compatible values (modulo float repr)."""
    encoded = encode_json(value)
    assert isinstance(encoded, bytes)
    decoded = decode_json(encoded)
    assert _equal_modulo_float(decoded, value)


@given(result=_result_dict())
def test_encode_result_roundtrip(result: dict) -> None:
    """msgpack encode/decode preserves a standard worker result shape."""
    encoded = encode_result(result)
    assert isinstance(encoded, bytes)
    decoded = decode_result(encoded)
    assert isinstance(decoded, dict)
    assert decoded.keys() == result.keys()
    for key in result:
        assert _equal_modulo_float(decoded[key], result[key])


@given(
    entries=st.lists(
        st.tuples(_SAFE_TEXT, _JSON_SCALARS),
        min_size=0,
        max_size=10,
        unique_by=lambda pair: pair[0],
    )
)
def test_canonical_encode_stable_under_dict_key_order(entries: list) -> None:
    """Two dicts built via different key orders encode to identical bytes.

    Also check kwargs ordering at the top level, which is the actual
    dedup-hash attack surface in Client.schedule().
    """
    # Ascending vs. descending insertion order.
    forward = dict(entries)
    reverse = dict(reversed(entries))
    # Sanity: equal mappings, possibly different insertion order.
    assume(forward == reverse)

    # As top-level kwargs.
    assert canonical_encode((), forward) == canonical_encode((), reverse)

    # As a nested dict inside args (wrapped to dodge non-str-key edges).
    assert canonical_encode((forward,), {}) == canonical_encode((reverse,), {})


def test_canonical_encode_rejects_nan() -> None:
    """Regression pin: NaN has no canonical JSON form and must raise."""
    with pytest.raises(TypeError, match="NaN"):
        canonical_encode((), {"x": float("nan")})


def test_canonical_encode_rejects_inf() -> None:
    """Regression pin: +/-Inf has no canonical JSON form and must raise."""
    with pytest.raises(TypeError, match="Inf"):
        canonical_encode((), {"x": float("inf")})
    with pytest.raises(TypeError, match="Inf"):
        canonical_encode((), {"x": float("-inf")})
