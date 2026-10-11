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
from typing import Any, override

# Attribute, die jeder LogRecord mitbringt -- alles andere ist ein Feld.
_STANDARD_RECORD_ATTRS = frozenset(vars(logging.LogRecord("", logging.INFO, "", 0, "", (), None))) | {
    "message",
    "asctime",
}


class _CollectingHandler(logging.Handler):
    """Sammelt jeden Record als ``capture_logs``-artiges Dict."""

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        super().__init__(logging.DEBUG)
        self._entries = entries

    @override
    def emit(self, record: logging.LogRecord) -> None:
        entry: dict[str, Any] = {
            "event": record.msg,
            "log_level": record.levelname.lower(),
            "logger": record.name,
        }
        entry.update({key: value for key, value in vars(record).items() if key not in _STANDARD_RECORD_ATTRS})
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


def _is_pytest_handler(handler: logging.Handler) -> bool:
    return type(handler).__module__ == "_pytest.logging"


@contextmanager
def preserved_root_logger() -> Iterator[None]:
    """Stellt Root-Handler und Root-Level -- sowie Handler, Level und
    ``propagate`` des Loggers ``swarmq`` -- nach dem Block wieder her und gibt
    das Logging per ``shutdown_logging()`` frei (Plan 2026-10-06-0831, U4).

    Ein ``configure_logging`` im Block ersetzt die Root-Handler; danach baut
    ein neues ``configure_logging`` wieder auf. pytests eigene Capture-Handler
    wechseln je Testphase und bleiben, wie pytest sie gerade gesetzt hat --
    zurueckgelegte Handler einer beendeten Phase sammelten sonst weiter.
    """
    import swarmq.logging_queue as _queue_core
    from swarmq.logging import shutdown_logging
    from swarmq.logging_queue import _reset_log_queue
    from swarmq.logging_settings import LoggingSettings

    root = logging.getLogger()
    own = [h for h in root.handlers if not _is_pytest_handler(h)]
    level = root.level
    # Der Opt-in (U8, KTD10) haengt am Logger ``swarmq``.
    swarmq_logger = logging.getLogger("swarmq")
    swarmq_state = (list(swarmq_logger.handlers), swarmq_logger.level, swarmq_logger.propagate)
    try:
        yield
    finally:
        _reset_log_queue()
        shutdown_logging()
        # Eine im Test eingerichtete Frist (Plan 2026-10-06-1811, U4) gilt
        # nicht fuer den naechsten.
        _queue_core._DRAIN_TIMEOUT = LoggingSettings().drain_timeout
        # Ein im Test als Fork-Quelle markierter Prozess (U6, KTD6) startet
        # sonst keinen Listener mehr.
        # Ebenso ein im Test angehaengter Forkserver-Sentinel (U9, KTD9).
        from swarmq.logging_forksource import FORK_SOURCE

        FORK_SOURCE.active = False
        FORK_SOURCE.sentinel = None
        FORK_SOURCE.detached_at_fork = False
        root.handlers[:] = own + [h for h in root.handlers if _is_pytest_handler(h)]
        root.setLevel(level)
        swarmq_logger.handlers[:], swarmq_level, swarmq_logger.propagate = swarmq_state
        swarmq_logger.setLevel(swarmq_level)
