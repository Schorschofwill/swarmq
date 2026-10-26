"""Nachweis DP-03 und Entscheidung E-087: ``TaskInfo`` lehnt negative
``result_ttl`` und ``max_retries`` ab.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-03, V3a; LÜCKE V3b,
entschieden als E-087).

DP-03: ``TaskInfo(result_ttl=-1)`` wurde angenommen und scheiterte erst beim
Speichern (``SET … PX -1000``), ``-0.5`` wurde über ``ceil`` still zu ``0``
(„Ergebnis nicht speichern“). ``SwarmQConfig.result_ttl`` prüft den Rohwert
vor dem Aufrunden (E-055, Nachtrag 2026-10-15); ``TaskInfo.result_ttl`` ist
dessen Override je Task.

E-087: ``max_retries=-1`` wirkte wie ``0``; Georg hat am 2026-10-26
entschieden, es wie die Nachbarfelder ``retry_base_delay`` und ``max_age``
abzulehnen.

Testliste:
# [x] result_ttl=-1 und -0.5 -> ValueError mit Feldname
# [x] max_retries=-1 -> ValueError mit Feldname
# [x] max_retries=0 bleibt gültig (keine Wiederholung)
"""

from __future__ import annotations

import pytest

from swarmq import TaskInfo


@pytest.mark.befund("DP-03")
@pytest.mark.parametrize("value", [-1, -0.5])
def test_negative_result_ttl_is_rejected_before_rounding(value: float) -> None:
    with pytest.raises(ValueError, match="result_ttl must be >= 0"):
        TaskInfo(name="t", queues=["q"], result_ttl=value)


@pytest.mark.entscheidung("E-087")
def test_negative_max_retries_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_retries must be >= 0"):
        TaskInfo(name="t", queues=["q"], max_retries=-1)


@pytest.mark.entscheidung("E-087")
def test_zero_max_retries_stays_valid() -> None:
    assert TaskInfo(name="t", queues=["q"], max_retries=0).max_retries == 0
