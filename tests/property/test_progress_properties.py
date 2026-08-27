"""Property-based tests for ProgressInfo.from_hash roundtrip.

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U3.3

Verifies that the ProgressInfo decoding path is bijective — whatever a
worker writes via HSET, a client reads back via from_hash. Uses the
hypothesis-registered ``quick``/``thorough`` profiles from
``tests/property/conftest.py``.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from swarmq.progress import ProgressInfo

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# No surrogate code points and finite-only: ProgressInfo stores strings
# verbatim but the Valkey hash is text (UTF-8). Surrogates cannot be
# UTF-8-encoded, so they're out-of-contract for the writer.
_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    max_size=200,
)


def _percent_strategy() -> st.SearchStrategy[float]:
    # The writer stores percent with 2-decimal rounding and 0–100 range.
    # Use finite floats; avoid NaN/Inf (they'd break str/float roundtrip).
    return st.floats(
        min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
    ).map(lambda x: round(x, 2))


# Non-empty text — ``from_hash`` collapses empty-string ``started_at``
# to None via ``_decode(...) or None`` (see ``progress.py::from_hash``).
# The writer only populates ``started_at`` at execution-start, so the
# empty-string input is out-of-contract. Pinning the collapse with a
# separate regression test below.
_NONEMPTY_TEXT = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    min_size=1,
    max_size=200,
)


@st.composite
def progress_infos(draw) -> ProgressInfo:
    current = draw(st.integers(min_value=0, max_value=10**9))
    total = draw(st.integers(min_value=0, max_value=10**9))
    percent = draw(_percent_strategy())
    message = draw(_TEXT)
    updated_at = draw(_TEXT)
    started_at = draw(st.one_of(st.none(), _NONEMPTY_TEXT))
    eta_seconds = draw(
        st.one_of(st.none(), st.integers(min_value=0, max_value=10**7))
    )
    return ProgressInfo(
        current=current,
        total=total,
        percent=percent,
        message=message,
        updated_at=updated_at,
        started_at=started_at,
        eta_seconds=eta_seconds,
    )


def _to_hash(info: ProgressInfo, as_bytes: bool = False) -> dict:
    """Mimic the worker-side HSET: every field stringified."""
    raw: dict[str, str] = {
        "current": str(info.current),
        "total": str(info.total),
        "percent": str(info.percent),
        "message": info.message,
        "updated_at": info.updated_at,
    }
    if info.started_at is not None:
        raw["started_at"] = info.started_at
    if info.eta_seconds is not None:
        raw["eta_seconds"] = str(info.eta_seconds)
    if as_bytes:
        return {
            k.encode("utf-8"): v.encode("utf-8") for k, v in raw.items()
        }
    return raw


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@given(info=progress_infos())
def test_progress_info_from_hash_roundtrip(info: ProgressInfo) -> None:
    """String-keyed hash decodes losslessly."""
    raw = _to_hash(info, as_bytes=False)
    decoded = ProgressInfo.from_hash(raw)
    assert decoded is not None
    assert decoded.current == info.current
    assert decoded.total == info.total
    # Percent roundtrips via str(); compare as floats with small tolerance
    # to survive float repr quirks on tiny values.
    assert math.isclose(decoded.percent, info.percent, abs_tol=1e-9)
    assert decoded.message == info.message
    assert decoded.updated_at == info.updated_at
    assert decoded.started_at == info.started_at
    assert decoded.eta_seconds == info.eta_seconds


@given(info=progress_infos())
def test_progress_info_from_hash_handles_bytes_keys(info: ProgressInfo) -> None:
    """Raw valkey HGETALL output (``dict[bytes, bytes]``) decodes identically."""
    raw = _to_hash(info, as_bytes=True)
    decoded = ProgressInfo.from_hash(raw)
    assert decoded is not None
    assert decoded.current == info.current
    assert decoded.total == info.total
    assert math.isclose(decoded.percent, info.percent, abs_tol=1e-9)
    assert decoded.message == info.message
    assert decoded.updated_at == info.updated_at
    assert decoded.started_at == info.started_at
    assert decoded.eta_seconds == info.eta_seconds


@pytest.mark.parametrize("missing_field", ["current", "total", "percent"])
@given(info=progress_infos())
def test_progress_info_missing_required_field_raises(
    missing_field: str, info: ProgressInfo
) -> None:
    """Stripping any required field surfaces ValueError (contract pin)."""
    raw = _to_hash(info, as_bytes=False)
    del raw[missing_field]
    # Avoid the empty-dict case, which returns None instead of raising.
    assume(len(raw) > 0)
    with pytest.raises(ValueError, match="missing required fields"):
        ProgressInfo.from_hash(raw)


def test_progress_info_empty_started_at_collapses_to_none() -> None:
    """Contract pin: ``started_at=""`` decodes to ``None``.

    ``ProgressInfo.from_hash`` uses ``_decode("started_at") or None``,
    so an empty-string value is indistinguishable from a missing key.
    The writer never emits an empty ``started_at`` (it's set at
    execution-start), so the collision is acceptable — document it
    here so a future refactor doesn't silently break the contract.
    """
    raw = {
        "current": "0",
        "total": "0",
        "percent": "0.0",
        "message": "",
        "updated_at": "",
        "started_at": "",
    }
    decoded = ProgressInfo.from_hash(raw)
    assert decoded is not None
    assert decoded.started_at is None
