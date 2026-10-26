"""Nachweis DP-01, Teil 2: Abgewiesene Tasks eines Rate-Limits kommen ohne
Jitter gleichzeitig zurück.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-01, Verdacht V1).

Mit dem korrigierten ``retry_after_ms`` (Teil 1, Integrationstest
``test_befund_dp_01_rate_limit_retry_after.py``) wachen alle in einem
Moment Abgewiesenen zur selben Zeit auf; nur einer bekommt den freien Platz,
die anderen werden erneut abgewiesen. Die Probe (echtes Lua-Skript,
``100/m``, Rückstand 2000) zeigte dafür etwa 16-mal so viele Prüfungen wie
bisher. Der Jitter verteilt die Rückkehr: gleichverteilt bis zu
``(Fenster / Limit) · 2^n`` zusätzlich, höchstens ein Fenster; ``n`` ist die
Zahl der bisherigen Abweisungen dieser Nachricht
(``x-swarmq-rate-limit-count``). Probe: 100 je Minute bei etwa doppelt so
vielen Prüfungen wie vor dem Fix.

Testliste:
# [x] ohne Zufall (unten) genau retry_after
# [x] erste Abweisung streut über zwei Platzabstände (100/m: 1200 ms)
# [x] Streuung verdoppelt sich je Abweisung, höchstens ein Fenster
# [x] Limit 0: Streuung ein Fenster; sehr großer Zähler ohne Überlauf
"""

from __future__ import annotations

import pytest

from swarmq.backoff import rate_limit_requeue_delay_ms

pytestmark = pytest.mark.befund("DP-01")


def _low(lo: float, hi: float) -> float:
    return lo


def _high(lo: float, hi: float) -> float:
    return hi


def test_without_jitter_the_delay_is_retry_after() -> None:
    assert rate_limit_requeue_delay_ms(250, window_ms=60_000, max_requests=100, bounces=1, uniform=_low) == 250


def test_the_first_bounce_spreads_over_two_slot_intervals() -> None:
    assert rate_limit_requeue_delay_ms(250, window_ms=60_000, max_requests=100, bounces=1, uniform=_high) == 1_450


def test_the_spread_doubles_per_bounce_up_to_one_window() -> None:
    delays = [
        rate_limit_requeue_delay_ms(0, window_ms=60_000, max_requests=100, bounces=n, uniform=_high)
        for n in (1, 2, 3, 7, 50)
    ]

    assert delays == [1_200, 2_400, 4_800, 60_000, 60_000]


def test_limit_zero_spreads_over_one_window_and_huge_counters_do_not_overflow() -> None:
    assert rate_limit_requeue_delay_ms(10, window_ms=1_000, max_requests=0, bounces=1, uniform=_high) == 1_010
    assert rate_limit_requeue_delay_ms(10, window_ms=1_000, max_requests=5, bounces=10**6, uniform=_high) == 1_010
