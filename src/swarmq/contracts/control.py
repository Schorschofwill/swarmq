"""Werte des Kontrollkanals (Plan 2026-09-16 Kontrollkanal, R9/R10/R17).

Reine Datentypen, die Backend-Implementierung und Prozessverwaltung teilen,
ohne dass die eine die andere importiert: der Sollwert einer Worker-Gruppe
und das Ergebnis „unlesbar", das der Speicher statt einer Ausnahme
zurückgibt, damit der Supervisor es wie eine Ablehnung behandeln kann.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Setpoint:
    """Unter- und Obergrenze einer Worker-Gruppe; ``low == high`` ist die feste Form."""

    low: int
    high: int

    @property
    def fixed(self) -> bool:
        return self.low == self.high


@dataclass(frozen=True, slots=True)
class SetpointUnreadable:
    """Der Schlüssel existiert, sein Inhalt lässt sich aber nicht als Sollwert deuten."""

    reason: str


SetpointRead = Setpoint | SetpointUnreadable | None
