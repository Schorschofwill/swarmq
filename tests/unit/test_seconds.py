"""Ganzzahlige Sekundenfelder (E-055, Plan 0.12.3 KTD8).

Testliste:
# [x] None bleibt None
# [x] int bleibt unverändert (auch bool, E-055 regelt bool nicht)
# [x] ganzzahliger float wird int
# [x] echter Bruch wird aufgerundet (ceil), Typ int
# [x] inf / -inf / nan -> ValueError mit Feldname (kein OverflowError)
"""

from __future__ import annotations

import pytest

from swarmq.seconds import whole_seconds


def test_none_stays_none() -> None:
    assert whole_seconds(None, "result_ttl") is None


@pytest.mark.parametrize("value", [0, 1, 3600, True])
def test_int_stays_unchanged(value: int) -> None:
    result = whole_seconds(value, "result_ttl")

    assert result is value


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(86400.0, 86400, id="ganzzahlig"),
        pytest.param(0.0, 0, id="null"),
        pytest.param(1.5, 2, id="bruch"),
        pytest.param(0.2, 1, id="kleiner-bruch"),
    ],
)
def test_float_becomes_int_rounded_up(value: float, expected: int) -> None:
    result = whole_seconds(value, "result_ttl")

    assert type(result) is int
    assert result == expected


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_non_finite_float_raises_value_error_naming_the_field(value: float) -> None:
    with pytest.raises(ValueError, match="result_ttl must be a finite number"):
        whole_seconds(value, "result_ttl")
