"""Wächter der Basislinie (Plan 2026-10-24-1118, KTD7).

Jeder Anstieg in der Historie von ``basislinie.toml`` trägt einen
auflösbaren Verweis: ``E-nnn`` mit Datei im Pack, eine Befund-ID aus einem
Register unter ``docs/befunde/`` oder einen existierenden Planpfad. So bleibt
schleichendes Wachstum sichtbar, auch wenn jemand die Datei von Hand ändert.

Test-Liste:
- [x] kein Anstieg der Historie ohne auflösbaren Verweis
- [x] die Historie, von leer nachgespielt, ergibt genau ``werte`` (keine Handänderung)
"""

from __future__ import annotations

from pathlib import Path

from tests.kosten.conftest import basislinie_skript, lies_basislinie

_WURZEL = Path(__file__).resolve().parents[2]


def test_every_rise_in_the_history_carries_a_resolvable_reference() -> None:
    assert basislinie_skript.unbelegte_anstiege(lies_basislinie(), _WURZEL) == []


def test_replaying_the_history_yields_exactly_the_values() -> None:
    assert basislinie_skript.handaenderungen(lies_basislinie()) == []
