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
# [x] Client: schedule(..., unique_for=) über der Grenze -> ValueError vor jedem Claim
# [x] CLI: schedule add --unique-for über der Grenze ist ein Argumentfehler
"""

from __future__ import annotations

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
