"""Nachweis F3-31: ``float`` in ``result_ttl``/``unique_for`` wird nicht
normalisiert und legt zur Laufzeit den Worker lahm.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-31, R3C-01).

``result_ttl=timedelta(days=1).total_seconds()`` (``86400.0``) besteht die
Pruefungen in ``TaskInfo`` und ``SwarmQConfig``. Spaeter wirft valkey-py beim
Speichern ``DataError: px must be datetime.timedelta or int``, jede Nachricht
bleibt unbestaetigt; ein Cron mit ``unique_for=3600.0`` wird vom Leader nie
uebernommen.

Behoben in 0.12.3 (E-055, ``swarmq.seconds.whole_seconds``).

Festlegung (Entscheidung 2, Option a): normalisieren -- ganzzahlige Floats
still zu ``int``, echte Brueche aufrunden (``ceil``). Gilt fuer ``TaskInfo``
(``result_ttl``, ``unique_for``) und ``SwarmQConfig`` (``result_ttl``). Der
Aufruf-Parameter ``unique_for`` an ``schedule`` gehoert zu F3-07.

Testliste:
# [x] TaskInfo.result_ttl: 86400.0 -> 86400 (int), 1.5 -> 2 (int)
# [x] TaskInfo.unique_for: 3600.0 -> 3600 (int), 0.5 -> 1 (int)
# [x] SwarmQConfig.result_ttl: 86400.0 -> 86400 (int), 1.5 -> 2 (int)
"""

from __future__ import annotations

from typing import cast

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo

_CASES = [
    pytest.param(86400.0, 86400, id="ganzzahlig"),
    pytest.param(1.5, 2, id="bruch-aufgerundet"),
]


@pytest.mark.befund("F3-31")
@pytest.mark.entscheidung("E-055")
@pytest.mark.parametrize(("given", "expected"), _CASES)
def test_task_info_result_ttl_is_normalized_to_int(given: float, expected: int) -> None:
    info = TaskInfo(
        name="f331_result_ttl", queues=["default"], result_ttl=cast("int", given)
    )  # float absichtlich (Befund)

    assert type(info.result_ttl) is int, f"result_ttl bleibt {info.result_ttl!r}"
    assert info.result_ttl == expected


@pytest.mark.befund("F3-31")
@pytest.mark.entscheidung("E-055")
@pytest.mark.parametrize(
    ("given", "expected"), [pytest.param(3600.0, 3600, id="ganzzahlig"), pytest.param(0.5, 1, id="bruch-aufgerundet")]
)
def test_task_info_unique_for_is_normalized_to_int(given: float, expected: int) -> None:
    info = TaskInfo(
        name="f331_unique_for", queues=["default"], unique_for=cast("int", given)
    )  # float absichtlich (Befund)

    assert type(info.unique_for) is int, f"unique_for bleibt {info.unique_for!r}"
    assert info.unique_for == expected


@pytest.mark.befund("F3-31")
@pytest.mark.entscheidung("E-055")
@pytest.mark.parametrize(("given", "expected"), _CASES)
def test_config_result_ttl_is_normalized_to_int(given: float, expected: int) -> None:
    config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", result_ttl=cast("int", given)
    )  # float absichtlich (Befund)

    assert type(config.result_ttl) is int, f"result_ttl bleibt {config.result_ttl!r}"
    assert config.result_ttl == expected
