"""Erfassen von swarmq-Logs ohne globale structlog-Konfiguration.

Plan 2026-10-06-0831, U4. Die Bibliotheks-Logger (``swarmq.logging.get_logger``)
uebergeben ans Standard-``logging``: Der Event-Name wird zu ``record.msg``,
alle Felder werden zu LogRecord-Attributen. ``structlog.testing.capture_logs``
sieht davon nichts mehr. Dieser Helfer haengt deshalb einen sammelnden
stdlib-Handler an den Logger ``swarmq`` und liefert die Records als Dicts in
der Form von ``capture_logs`` (``event``, ``log_level``, Felder) -- plus
``logger``. Damit bleibt die Migration bestehender Tests mechanisch.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

# Attribute, die jeder LogRecord mitbringt -- alles andere ist ein Feld.
_STANDARD_RECORD_ATTRS = frozenset(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", (), None))
) | {"message", "asctime"}


class _CollectingHandler(logging.Handler):
    """Sammelt jeden Record als ``capture_logs``-artiges Dict."""

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        super().__init__(logging.DEBUG)
        self._entries = entries

    def emit(self, record: logging.LogRecord) -> None:
        entry: dict[str, Any] = {
            "event": record.msg,
            "log_level": record.levelname.lower(),
            "logger": record.name,
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_RECORD_ATTRS:
                entry[key] = value
        self._entries.append(entry)


@contextmanager
def capture_swarmq_logs() -> Iterator[list[dict[str, Any]]]:
    """Erfasst alle Records unter ``swarmq`` ab DEBUG.

    Level und ``disabled`` des Loggers ``swarmq`` werden fuer die Dauer
    umgestellt und danach -- wie die Handler-Liste -- wiederhergestellt.
    """
    logger = logging.getLogger("swarmq")
    entries: list[dict[str, Any]] = []
    handler = _CollectingHandler(entries)
    old_level = logger.level
    old_disabled = logger.disabled
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    logger.addHandler(handler)
    try:
        yield entries
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
        logger.disabled = old_disabled
