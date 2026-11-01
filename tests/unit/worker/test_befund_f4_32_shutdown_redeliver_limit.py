"""Nachweis F4-32 (Loadsuite B43): Ein Worker im Graceful Shutdown, dessen Consumer
noch angemeldet ist, verbraucht das Zustelllimit einer nie gestarteten
Nachricht und schickt sie als ``RedeliveryLimitError`` in die DLQ.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-32; Gegenprüfung 2026-11-01).

Quelle der Zusage: ``website/explanation/delivery-semantics.md``
- :113 „prefetched messages that have not started go back to the queue“,
- :297-304 (Szenario 3) „Messages the broker delivered but the worker had not
  started yet go back to the queue and are delivered to a sibling worker …
  how you roll a deploy without losing work“, Mitigation „Nothing for
  messages that had not started“,
- :726-729 Zustelllimit als „guard against endless loops … from a buggy worker“.

Auslöser (main, ohne Docker-Pause): ``_request_shutdown`` setzt
``_shutdown_event`` sofort; ``_execute_shutdown`` ruft erst den
``WORKER_SHUTDOWN``-Dispatch (bis 10 s), dann ``_stop_http_surfaces``, dann
``cancel_consume``. Bis zum ``CancelOk`` ist der Consumer angemeldet. Jede
Zustellung in diesem Fenster gibt Schritt 3b (``pipeline.py:611-617``) per
``redeliver_in_place`` zurück, ohne Pause (``wait_unless_shutdown`` kehrt
bei gesetztem Shutdown sofort zurück, ``redelivery.py:45-59``). RabbitMQ 4.3
stellt sie dem freien Consumer sofort wieder zu und zählt nur
``x-acquired-count`` (Spike S4). Das Delivery-Count-Gate läuft vor 3b und
schickt sie bei ``limit - 1`` in die DLQ.

Der Test spielt den Broker auf einer virtuellen Uhr nach: Solange der
Consumer angemeldet ist (hier 10 s, die Obergrenze des
``WORKER_SHUTDOWN``-Dispatchs, ``_LIFECYCLE_DISPATCH_TIMEOUT_S``), kommt jede
Rückgabe nach einer Rundreise von 1 ms mit ``x-acquired-count + 1`` zurück
(4.3-Zählerbild aus BZ-29). Die Rückgabe-Pause läuft auf derselben Uhr. Ein
Fix, der die Pause im Shutdown nicht aufhebt (rund 20 Runden > 1 min,
``delivery-semantics.md:169-171``), ein Fix, der späte Zustellungen nicht
zurückgibt (E-050), oder einer, der 3b-Rückgaben nicht gegen das Limit zählt,
macht den Test grün.

Testliste:
# [x] Shutdown mit noch angemeldetem Consumer -> eine nie gestartete Nachricht
#     landet nicht per Zustelllimit in der DLQ
"""

from __future__ import annotations

import pytest

from swarmq.worker import redelivery
from swarmq.worker.lifecycle import WorkerState
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_ROUNDS = 25  # mehr als das Default-Limit 20
_CONSUMER_REGISTERED_S = 10.0  # bis cancel_consume greift (Dispatch-Obergrenze)
_ROUND_TRIP_S = 0.001


@pytest.mark.befund("F4-32")
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="F4-32: Shutdown-Rückgabe ohne Pause verbraucht das Zustelllimit"
)
async def test_a_message_not_started_during_shutdown_is_not_dead_lettered(
    worker, broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [0.0]

    async def virtual_sleep(delay_s: float) -> None:
        now[0] += delay_s

    monkeypatch.setattr(redelivery, "_sleep", virtual_sleep)
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()  # SIGTERM: Event gesetzt, cancel_consume noch nicht gelaufen

    acquired = 0
    for _ in range(_ROUNDS):
        headers = {"x-acquired-count": acquired} if acquired else {}
        message = make_message(_default_payload(task_id="b43-task"), headers=headers)
        handed_back = broker.redeliver.await_count
        await worker._handle_message(message)
        if broker.redeliver.await_count == handed_back:
            break  # nicht zurückgegeben: Schleife zu Ende
        now[0] += _ROUND_TRIP_S
        if now[0] >= _CONSUMER_REGISTERED_S:
            break  # Consumer abgemeldet: die Nachricht geht an einen Geschwister-Worker
        acquired += 1  # Broker stellt dem noch angemeldeten Consumer sofort wieder zu

    dead_lettered = [call.kwargs.get("headers", {}) for call in broker.dead_letter.await_args_list]
    assert not dead_lettered, (
        f"nie gestartete Nachricht nach {acquired} Rückgaben in {now[0]:.3f} s Shutdown in die DLQ geschickt: "
        f"{[h.get('x-swarmq-error-type', h) for h in dead_lettered]}"
    )
