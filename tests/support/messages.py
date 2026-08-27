"""Gemeinsamer Message-Helfer der Worker-Unit-Tests (Plan U4).

Bis U6 bleibt der Helfer aio_pika-förmig (``body``, ``headers``, ``ack``,
``nack``), weil der noch ungeteilte Worker genau das konsumiert. Ersetzt die
fünf Kopien von ``_make_message`` (test_worker, test_worker_timeouts,
test_graceful_shutdown, test_worker_dishka, test_task_expiry).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json


def make_message(
    payload: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    *,
    body: bytes | None = None,
    enqueued_at: int | None = None,
) -> MagicMock:
    """Mock einer AMQP-Nachricht.

    ``payload`` wird JSON-kodiert; alternativ ``body`` als Rohbytes.
    ``enqueued_at`` setzt ``x-swarmq-enqueued-at``.
    """
    if payload is not None and body is not None:
        raise ValueError("payload und body sind exklusiv")
    msg = MagicMock()
    msg.body = body if body is not None else encode_json(payload if payload is not None else {})
    msg.headers = dict(headers or {})
    if enqueued_at is not None:
        msg.headers[HEADER_ENQUEUED_AT] = enqueued_at
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg
