"""Letzte Fehlergrenze einer Zustellung (B22).

``Worker._on_message`` startet jede Zustellung per ``create_task``. Eine dort
entweichende Exception meldete asyncio als „Task exception was never
retrieved“ -- auf ERROR, ohne Task-Kontext und ohne Fehlerfelder. Diese
Grenze loggt stattdessen genau eine Zeile im gebundenen Kontext:

- ``BrokerConnectionError`` (Kanal oder Verbindung weg, meist beim Ack):
  WARNING ``delivery_settle_failed``. Selbstheilend (KTD11), denn der Broker
  stellt die unbestaetigte Nachricht neu zu. Das deckt bis RabbitMQ 4.2 auch
  das spaete Ack nach einem Consumer-Timeout ab (Channel zu, U7). Ab 4.3
  haelt der Broker-Adapter ein solches Settle zurueck und loggt selbst
  (``delivery_settle_deferred``/``delivery_settle_withheld``); es kommt nicht
  als Fehler hier an.
- jede andere ``Exception``: ERROR ``delivery_handler_failed`` mit Traceback.

``CancelledError`` (Shutdown, ``_abort_teardown``) ist keine ``Exception``
und geht unveraendert durch.
"""

from __future__ import annotations

import contextlib
from collections.abc import Generator

from swarmq.exceptions import BrokerConnectionError
from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)


@contextlib.contextmanager
def contain_delivery_errors() -> Generator[None]:
    try:
        yield
    except BrokerConnectionError as exc:
        log_settle_failed(exc)
    except Exception as exc:
        logger.error("delivery_handler_failed", **_error_fields(exc), exc_info=True)  # noqa: G201 - exception() reicht anderen Methodennamen an stdlib


def log_settle_failed(exc: BrokerConnectionError) -> None:
    """Die eine WARNING ``delivery_settle_failed``: Kanal oder Verbindung weg,
    die Zustellung bleibt unbestaetigt und kommt neu (KTD11). Auch fuer ein
    gescheitertes ``nack`` von ``redeliver_in_place`` (F4-18)."""
    logger.warning(
        "delivery_settle_failed",
        **_error_fields(exc),
        hint="delivery not settled; the broker redelivers the unacknowledged message",
    )
