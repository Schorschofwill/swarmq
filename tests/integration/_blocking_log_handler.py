"""Log-Ziel, das nie fertig wird (Plan 2026-10-06-1811, U4).

Wie ein Log-Versand, dessen Gegenstelle nicht mehr antwortet: ``emit`` wartet
auf ein Event, das niemand setzt. Fuer dictConfigs in Kindprozessen, die nur
importierbare Klassen kennen.
"""

from __future__ import annotations

import logging
import threading

_NEVER = threading.Event()


class NeverReturns(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        _NEVER.wait()


class AlwaysRaises(logging.Handler):
    """Log-Ziel, dessen ``emit`` wirft, ohne ``handleError`` (Analyse S2.1)."""

    def emit(self, record: logging.LogRecord) -> None:
        raise ConnectionError("log shipper down")
