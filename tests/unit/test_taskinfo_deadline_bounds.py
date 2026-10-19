"""Obergrenze für die Fristen in ``TaskInfo`` (E-063, Nachtrag 2026-10-19).

``MAX_DEADLINE_S`` (zehn Jahre, ``config_validation.py``) galt bis 0.13.0rc1
nur für ``SwarmQConfig``. ``result_ttl=sys.maxsize`` als „für immer“ ergab mal
1000 mehr als int64; Valkey lehnte das ``PX`` ab, der Worker stellte neu zu
und der Task lief bei jeder Zustellung erneut bis zum Zustelllimit
(Entscheidungsvorlage 0.13.0 vom 2026-10-19, Punkt 2). Seit 0.13.0rc2 lehnt
``TaskInfo`` Werte über der Grenze beim Import mit ``ValueError`` ab, wie die
übrigen ``TaskInfo``-Prüfungen (E-065, E-057: Exit 1).

Testliste:
# [x] result_ttl knapp über der Grenze -> ValueError mit Feldnamen
# [x] result_ttl genau an der Grenze gültig
# [x] max_age, unique_for, hard_timeout: an der Grenze gültig, knapp darüber ValueError
# [x] float knapp über der Grenze (nach dem Aufrunden) wird abgelehnt
# [x] max_age, hard_timeout: float knapp über der Grenze (ohne Aufrunden) abgelehnt
# [x] max_age, hard_timeout: nan und -inf abgelehnt (nan bestand ``> MAX_DEADLINE_S``)
# [x] soft_timeout=nan neben hard_timeout abgelehnt (``hard > soft`` gilt für nan nicht)
# [x] Client: schedule(..., unique_for=) über der Grenze -> ValueError vor jedem Claim
# [x] CLI: schedule add --unique-for über der Grenze ist ein Argumentfehler

Nachtrag 2026-10-19 (0.13.1, Untergrenze und Endlichkeit für die Laufzeitfristen):
# [x] soft_timeout allein: nan, -inf, 0, -1 -> ValueError mit Feldnamen
# [x] hard_timeout: 0, -1, -30, -31 -> ValueError mit Feldnamen (nan/-inf siehe oben)
# [x] soft_timeout, hard_timeout: timedelta -> TypeError mit Feldnamen
# [x] False ist 0 und wird abgelehnt (wie unique_for=False, max_age=False); True bleibt 1 s
# [x] soft_timeout=inf und soft_timeout=10**30 allein bleiben erlaubt
# [x] kleinster positiver Wert (5e-324) ist für beide Felder erlaubt
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any

import pytest

from swarmq.config_validation import MAX_DEADLINE_S
from swarmq.task import TaskInfo

pytestmark = pytest.mark.entscheidung("E-063")


def _info(**fields: Any) -> TaskInfo:
    return TaskInfo(name="t", queues=["default"], **fields)


def test_result_ttl_just_above_the_bound_names_the_field() -> None:
    with pytest.raises(ValueError, match=rf"^result_ttl must be at most {MAX_DEADLINE_S} seconds"):
        TaskInfo(name="t", queues=["default"], result_ttl=MAX_DEADLINE_S + 1)


def test_max_age_just_above_the_bound_names_the_field() -> None:
    with pytest.raises(ValueError, match=rf"^max_age must be at most {MAX_DEADLINE_S} seconds"):
        TaskInfo(name="t", queues=["default"], max_age=MAX_DEADLINE_S + 1)


def test_hard_timeout_just_above_the_bound_names_the_field() -> None:
    with pytest.raises(ValueError, match=rf"^hard_timeout must be at most {MAX_DEADLINE_S} seconds"):
        TaskInfo(name="t", queues=["default"], hard_timeout=MAX_DEADLINE_S + 1)


def test_unique_for_just_above_the_bound_names_the_field() -> None:
    with pytest.raises(ValueError, match=rf"^unique_for must be at most {MAX_DEADLINE_S} seconds"):
        TaskInfo(name="t", queues=["default"], unique_for=MAX_DEADLINE_S + 1)


@pytest.mark.parametrize("field_name", ["result_ttl", "max_age", "unique_for", "hard_timeout"])
def test_deadline_exactly_at_the_bound_is_valid(field_name: str) -> None:
    info = _info(**{field_name: MAX_DEADLINE_S})

    assert getattr(info, field_name) == MAX_DEADLINE_S


@pytest.mark.parametrize("field_name", ["result_ttl", "unique_for"])
def test_float_just_above_the_bound_is_rejected_after_rounding_up(field_name: str) -> None:
    with pytest.raises(ValueError, match=rf"^{field_name} must be at most {MAX_DEADLINE_S} seconds"):
        _info(**{field_name: MAX_DEADLINE_S + 0.5})


@pytest.mark.parametrize("field_name", ["max_age", "hard_timeout"])
def test_float_just_above_the_bound_is_rejected_without_rounding(field_name: str) -> None:
    with pytest.raises(ValueError, match=rf"^{field_name} must be at most {MAX_DEADLINE_S} seconds"):
        _info(**{field_name: MAX_DEADLINE_S + 0.5})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        pytest.param("max_age", math.nan, id="max_age-nan"),
        pytest.param("hard_timeout", math.nan, id="hard_timeout-nan"),
        pytest.param("hard_timeout", -math.inf, id="hard_timeout-neg-inf"),
    ],
)
def test_non_finite_deadline_is_rejected(field_name: str, value: float) -> None:
    """``nan`` besteht ``value > MAX_DEADLINE_S``; ``max_age=nan`` liesse jede
    Zustellung als abgelaufen in die DLQ laufen (``expiry.py``)."""
    with pytest.raises(ValueError, match=rf"^{field_name} must be a finite number of seconds, got"):
        _info(**{field_name: value})


def test_nan_soft_timeout_next_to_hard_timeout_is_rejected() -> None:
    """E-063: ``soft_timeout`` hat keine eigene Grenze, neben ``hard_timeout``
    begrenzt es ``hard_timeout > soft_timeout``. Für ``nan`` ist jeder
    Vergleich falsch; ``hard_timeout <= nan`` liess den Wert durch."""
    with pytest.raises(ValueError, match="^hard_timeout must be greater than soft_timeout"):
        _info(soft_timeout=math.nan, hard_timeout=60)


# --- Nachtrag 2026-10-19 (0.13.1): Untergrenze und Endlichkeit -------------


@pytest.mark.parametrize("value", [math.nan, -math.inf, 0, -1], ids=["nan", "neg-inf", "zero", "minus-one"])
def test_soft_timeout_alone_must_be_a_positive_number(value: float) -> None:
    """``soft_timeout=nan`` zerstört die Heap-Ordnung der Timer und kann den
    Event-Loop des Workers abstürzen lassen (``select(nan)``); ``<= 0`` bricht
    jeden Lauf sofort ab."""
    with pytest.raises(ValueError, match=r"^soft_timeout must be > 0 seconds, got"):
        _info(soft_timeout=value)


@pytest.mark.parametrize("value", [0, -1, -30, -31], ids=["zero", "minus-one", "minus-30", "minus-31"])
def test_hard_timeout_must_be_positive(value: float) -> None:
    """``hard_timeout <= 0`` bricht jeden Lauf sofort ab; ab ``-30`` ergibt
    sich mit ``lock`` eine Lock-TTL ``<= 0`` (``hard_timeout + 30 s``), und
    die Nachricht kreiste bis zum Zustelllimit."""
    with pytest.raises(ValueError, match=r"^hard_timeout must be > 0 seconds, got"):
        _info(hard_timeout=value)


@pytest.mark.parametrize("field_name", ["soft_timeout", "hard_timeout"])
def test_timedelta_timeout_names_the_field(field_name: str) -> None:
    """Die Felder nehmen Sekunden, keine ``timedelta``; der Fehler nennt beim
    Import das Feld (E-057: Exit 1) statt eines nackten Vergleichsfehlers."""
    with pytest.raises(TypeError, match=rf"^{field_name} must be a number of seconds, got datetime.timedelta"):
        _info(**{field_name: timedelta(seconds=60)})


@pytest.mark.parametrize("field_name", ["soft_timeout", "hard_timeout"])
def test_false_timeout_is_zero_and_rejected(field_name: str) -> None:
    """``bool`` bleibt unverändert (E-055): ``True`` wirkt als 1 s, ``False``
    als 0 und fällt wie ``unique_for=False`` und ``max_age=False`` unter die
    Untergrenze. „Kein Timeout“ heißt ``None``."""
    with pytest.raises(ValueError, match=rf"^{field_name} must be > 0 seconds, got False"):
        _info(**{field_name: False})


def test_true_timeout_stays_one_second() -> None:
    info = _info(soft_timeout=True, hard_timeout=2)

    assert info.soft_timeout is True


@pytest.mark.parametrize("value", [math.inf, 10**30], ids=["inf", "ten-to-the-30"])
def test_soft_timeout_alone_has_no_upper_bound(value: float) -> None:
    """``soft_timeout`` endet nur in ``asyncio.wait_for``; ``inf`` und große
    Werte heißen dort „nie“ und bleiben erlaubt."""
    assert _info(soft_timeout=value).soft_timeout == value


@pytest.mark.parametrize("field_name", ["soft_timeout", "hard_timeout"])
def test_smallest_positive_timeout_is_valid(field_name: str) -> None:
    smallest = math.ulp(0.0)

    assert getattr(_info(**{field_name: smallest}), field_name) == smallest
