"""Nachweis F3-02: ``schedule(delay=|eta=)`` umgeht ``pre_enqueue`` und
``TASK_ENQUEUED``.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-02; Quelle R3D-04).

``Client.schedule`` kehrt bei ``delay>0`` oder ``eta`` ueber
``_schedule_deferred`` zurueck (``client/schedule.py``), bevor
``run_pre_enqueue`` und der ``TASK_ENQUEUED``-Dispatch erreicht werden. Ein
Veto der Middleware (Auth, Tenant, Validierung) laesst sich so mit jedem
``delay>0`` umgehen; der Oneshot wird trotzdem gespeichert.

Erwartet laut E-059 (Georg, 2026-10-15): ``pre_enqueue`` laeuft im
Producer VOR dem Speichern und vor dem Unique-Claim (ein Veto wirkt),
``TASK_ENQUEUED`` feuert nach dem Speichern, und Aenderungen der Middleware
an der Nachricht werden mit der Definition gespeichert, damit der Leader sie
publiziert. Cron-Fires (F3-36) bleiben offen und sind hier nicht Thema.

Middleware-Aenderungen: ``MessageContext`` traegt keine Header; im sofortigen
Pfad wirkt eine In-place-Aenderung an ``message["kwargs"]`` auf die
publizierte Nachricht (dasselbe ``dict``). Der Test prueft dasselbe fuer die
gespeicherte Definition.

Testliste:
# [x] Veto bei delay=30 bzw. eta: Ausnahme propagiert, nichts gespeichert
# [x] Veto mit unique_for: kein Unique-Claim (pre_enqueue vor dem Claim)
# [x] TASK_ENQUEUED nach dem Speichern, mit der gelieferten task_id
# [x] kwargs-Ergaenzung der Middleware landet in der gespeicherten Definition
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.signals import Signal

pytestmark = pytest.mark.timeout(5)


class _F302Veto(Middleware):
    @override
    async def pre_enqueue(self, message: MessageContext) -> None:
        raise PermissionError("f302 tenant missing")


class _F302Correlation(Middleware):
    @override
    async def pre_enqueue(self, message: MessageContext) -> None:
        message["kwargs"]["correlation_id"] = "f302-corr"


@pytest.fixture
def schedule_store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from swarmq.client import schedule as client_mod

    store = MagicMock()
    store.add_schedule = AsyncMock()

    def _factory(_backend: object) -> MagicMock:
        return store

    monkeypatch.setattr(client_mod, "ScheduleStore", _factory)
    return store


@pytest.fixture
def dispatcher() -> MagicMock:
    d = MagicMock()
    d.dispatch = AsyncMock()
    return d


def _client(broker: AsyncMock, backend: AsyncMock, middleware: Middleware, dispatcher: MagicMock) -> Client:
    return Client(
        broker=broker,
        backend=backend,
        middleware_chain=MiddlewareChain([middleware]),
        signal_dispatcher=dispatcher,
    )


def _deferral(kind: str) -> dict[str, Any]:
    if kind == "delay":
        return {"delay": 30}
    return {"eta": datetime.now(UTC) + timedelta(minutes=5)}


@pytest.mark.befund("F3-02")
@pytest.mark.entscheidung("E-059")
@pytest.mark.parametrize("kind", ["delay", "eta"])
async def test_pre_enqueue_veto_blocks_deferred_schedule(
    broker: AsyncMock, backend: AsyncMock, dispatcher: MagicMock, schedule_store: MagicMock, kind: str
) -> None:
    client = _client(broker, backend, _F302Veto(), dispatcher)

    with pytest.raises(PermissionError, match="f302 tenant missing"):
        await client.schedule("f302.task", 1, queue="f302", **_deferral(kind))

    schedule_store.add_schedule.assert_not_awaited()
    dispatcher.dispatch.assert_not_awaited()


@pytest.mark.befund("F3-02")
@pytest.mark.entscheidung("E-059")
async def test_pre_enqueue_veto_precedes_unique_claim_on_deferred_schedule(
    broker: AsyncMock, backend: AsyncMock, dispatcher: MagicMock, schedule_store: MagicMock
) -> None:
    backend.claim_unique = AsyncMock(return_value=None)
    client = _client(broker, backend, _F302Veto(), dispatcher)

    with pytest.raises(PermissionError, match="f302 tenant missing"):
        await client.schedule("f302.task", 1, queue="f302", delay=30, unique_for=60)

    backend.claim_unique.assert_not_awaited()
    schedule_store.add_schedule.assert_not_awaited()


@pytest.mark.befund("F3-02")
@pytest.mark.entscheidung("E-059")
async def test_task_enqueued_fires_after_store_on_deferred_schedule(
    broker: AsyncMock, backend: AsyncMock, dispatcher: MagicMock, schedule_store: MagicMock
) -> None:
    order: list[str] = []

    def _stored(_definition: object) -> None:
        order.append("store")

    def _dispatched(signal: Signal, **_fields: object) -> None:
        order.append(signal.name)

    schedule_store.add_schedule = AsyncMock(side_effect=_stored)
    dispatcher.dispatch = AsyncMock(side_effect=_dispatched)
    client = _client(broker, backend, Middleware(), dispatcher)

    task_id = await client.schedule("f302.task", 1, queue="f302", delay=30)

    assert order == ["store", Signal.TASK_ENQUEUED.name], "TASK_ENQUEUED fehlt oder kommt vor dem Speichern"
    kwargs = dispatcher.dispatch.await_args.kwargs
    assert (kwargs["task_id"], kwargs["task_name"], kwargs["queue"]) == (task_id, "f302.task", "f302")


@pytest.mark.befund("F3-02")
@pytest.mark.entscheidung("E-059")
async def test_middleware_kwargs_change_is_stored_with_deferred_definition(
    broker: AsyncMock, backend: AsyncMock, dispatcher: MagicMock, schedule_store: MagicMock
) -> None:
    client = _client(broker, backend, _F302Correlation(), dispatcher)

    await client.schedule("f302.task", 1, queue="f302", delay=30, user="u-1")

    definition = schedule_store.add_schedule.await_args.args[0]
    assert definition.kwargs == {"user": "u-1", "correlation_id": "f302-corr"}, (
        "Leader publiziert den Oneshot ohne die Ergaenzung der Middleware"
    )
