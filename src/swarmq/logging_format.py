"""Formatter fuer swarmq- und Fremd-Records (Plan 2026-10-06-0831, U2, KTD3).

Ausgelagert aus ``swarmq.logging`` (800-Zeilen-Guard). ``build_formatter``
bleibt dort als ``swarmq.logging.build_formatter`` erreichbar, auch fuer
dictConfig-Dateien.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Any

import structlog


class _SafeProcessorFormatter(structlog.stdlib.ProcessorFormatter):
    """ProcessorFormatter, der an fremden Records nicht scheitert.

    Plan 2026-09-08-001, U7 (R27, KTD9). ``format`` ruft fuer fremde
    Records ``record.getMessage()`` — ``msg % args``. Wirft dabei ein
    ``__repr__`` (so ``aio_pika.tools.OneShotCallback`` in aio-pika 10),
    landete die Ausnahme in ``Handler.handleError`` und damit als
    Traceback auf stderr, mitten im Logstrom. Stattdessen rendert dieser
    Formatter ein Ersatz-Event ``log_record_unformattable`` mit
    ``logger``, ``level`` und ``error`` durch dieselbe Prozessorkette und
    denselben Renderer wie jede andere Zeile.
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except Exception as exc:  # noqa: BLE001 - Formatfehler faellt auf Ersatz-Record zurueck
            return super().format(self._substitute_record(record, exc))

    def _substitute_record(self, record: logging.LogRecord, exc: Exception) -> logging.LogRecord:
        """Baut einen structlog-nativen Record mit dem Ersatz-Event.

        Die ``foreign_pre_chain`` (Level, Zeitstempel) laeuft hier von
        Hand, weil der Ersatz-Record den structlog-Pfad von ``format``
        nimmt — dort laeuft sie nicht mehr.
        """
        logger = logging.getLogger(record.name)
        meth_name = record.levelname.lower()
        ed: Any = {
            "event": "log_record_unformattable",
            "logger": record.name,
            "error": f"{type(exc).__name__}: {exc}",
        }
        for proc in self.foreign_pre_chain or ():
            ed = proc(logger, meth_name, ed)
        args, kwargs = structlog.stdlib.ProcessorFormatter.wrap_for_formatter(logger, meth_name, ed)
        substitute = logging.LogRecord(
            record.name,
            record.levelno,
            record.pathname,
            record.lineno,
            args[0],
            None,
            None,
        )
        substitute.__dict__.update(kwargs["extra"])
        return substitute


def _add_record_timestamp(_: Any, __: str, event_dict: structlog.typing.EventDict) -> structlog.typing.EventDict:
    """Zeitstempel aus ``record.created`` statt aus dem Formatierungszeitpunkt.

    Gerendert wird im Listener-Thread; staut sich die Queue, laege die Zeit
    sonst um den Rueckstau daneben (KTD3). Format wie
    ``TimeStamper(fmt="iso")``: ISO 8601 in UTC mit Suffix ``Z``. Ohne
    ``_record`` (Ersatz-Event aus ``_SafeProcessorFormatter``) gilt die
    aktuelle Zeit.
    """
    record = event_dict.get("_record")
    created = record.created if record is not None else time.time()
    event_dict["timestamp"] = iso_utc(created)
    return event_dict


def iso_utc(created: float) -> str:
    """``created`` (Epochensekunden) als ISO 8601 in UTC mit Suffix ``Z``."""
    return dt.datetime.fromtimestamp(created, tz=dt.UTC).isoformat().replace("+00:00", "Z")


_EXTRA_ADDER = structlog.stdlib.ExtraAdder()

_FORMATTER_RECORD_ATTRS = ("message", "asctime")
"""Setzt ``logging.Formatter.format`` auf dem Record; ein frueherer Handler
hinterlaesst sie. Felder eines swarmq-Events sind sie nie: ``get_logger``
benennt gleichnamige Felder in ``swarmq_<key>`` um."""


def _add_record_extras(
    logger: Any, method_name: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """``ExtraAdder`` ohne die Streufelder ``message``/``asctime`` (KTD3)."""
    event_dict = _EXTRA_ADDER(logger, method_name, event_dict)
    for key in _FORMATTER_RECORD_ATTRS:
        event_dict.pop(key, None)
    return event_dict


def build_formatter(json: bool, colors: bool) -> structlog.stdlib.ProcessorFormatter:
    """Liefert den Formatter fuer swarmq- und Fremd-Records (Plan
    2026-10-06-0831, U2, KTD3).

    ``json`` waehlt den ``JSONRenderer``, sonst rendert der
    ``ConsoleRenderer`` mit ANSI-Farben nur bei ``colors``. Records der
    Bibliotheks-Logger (``get_logger``) tragen ihre Felder als Attribute und
    laufen wie Fremd-Records durch die ``foreign_pre_chain``; structlog-native
    Records (``wrap_for_formatter``) bringen Level und Zeitstempel schon mit.
    Scheitert die Formatierung eines Records, erscheint
    ``log_record_unformattable`` (R8). In einer dictConfig nutzbar per
    ``{"()": "swarmq.logging.build_formatter", "json": true, "colors": false}``.
    """
    renderer: Any = structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer(colors=colors)
    return _SafeProcessorFormatter(
        # Ohne ``remove_processors_meta`` rendert der Formatter seine
        # eigenen Arbeitsschluessel mit (B6): ``_record`` traegt den
        # LogRecord, die Nutzlast stand damit zweimal in jeder Zeile. Ueber
        # einen 24h-Lauf die doppelte Logmenge, und von Hand kaum lesbar
        # genau im Diagnosefall. Muss VOR dem Renderer stehen; beide
        # Renderer teilen diese Kette.
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=[
            structlog.processors.add_log_level,
            structlog.stdlib.add_logger_name,
            _add_record_timestamp,
            _add_record_extras,
            structlog.processors.format_exc_info,
        ],
    )
