"""Nachweis F3-16: ``fixed``-Backoff wird nicht durch ``retry_max_delay``
gedeckelt.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-16, R3D-15).

``calculate_backoff(0, "fixed", 7200, 3600)`` liefert heute 7200, obwohl der
Docstring ``max_delay`` als "Maximum delay cap" beschreibt und der Guide sagt,
``retry_max_delay`` "bounds any computed delay". Damit ist auch die Invariante
``scheduler_dedup_ttl >= retry_max_delay`` (Schutz fuer Cron-Retries)
ausgehebelt.

Festlegung (Entscheidung 11, Praezisierung E-023): ``fixed`` liefert genau
``base_delay``, hoechstens ``retry_max_delay``, ohne Jitter, also
``min(base_delay, max_delay)``. Der E-023-Test
``test_backoff.py::test_fixed_strategy_returns_base_delay`` (``base_delay`` <
``max_delay``) bleibt mit dem Fix gruen.

Testliste:
# [x] fixed mit base_delay > max_delay liefert genau max_delay (ohne Jitter),
#     fuer jeden Versuch
"""

from __future__ import annotations

import pytest

from swarmq.backoff import calculate_backoff


@pytest.mark.befund("F3-16")
@pytest.mark.xfail(strict=True, reason="F3-16: offen - fixed-Backoff ignoriert retry_max_delay")
@pytest.mark.parametrize("attempt", [0, 3, 100])
def test_fixed_backoff_is_capped_by_max_delay(attempt: int) -> None:
    delay = calculate_backoff(attempt=attempt, strategy="fixed", base_delay=7200.0, max_delay=3600.0)

    assert delay == 3600.0, f"fixed-Backoff ueberschreitet retry_max_delay: {delay} > 3600.0"
