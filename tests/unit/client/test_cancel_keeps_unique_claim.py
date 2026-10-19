"""Ein Abbruch beim Senden oder Speichern hält den Unique-Claim (0.13.1).

Entscheidungsvorlage 0.13.0, Punkt 6, Option C (Georg 2026-10-19); Nachtrag
E-059 und E-041. Wird ``schedule()`` oder ``schedule_many()`` abgebrochen
(``CancelledError``: Timeout des Aufrufers, ``task.cancel()``), während der
Publish bzw. ``sched_add.lua`` läuft, ist der Ausgang unbestimmt: valkey-py
sendet ``EVALSHA`` und liest danach, aio-pika kann die Frames schon
geschrieben haben. Der Claim bleibt deshalb wie nach einem Backend-Ausfall
oder ``PublishIndeterminate`` stehen und läuft über seine TTL ab; der
``CancelledError`` geht unverändert an den Aufrufer.

Test-Liste:
- [x] sofortiger Pfad: Abbruch im Publish -> Claim bleibt, ``CancelledError``
- [x] sofortiger Pfad: Abbruch in ``pre_enqueue`` (noch nichts gesendet) ->
      Claim wird freigegeben, ``CancelledError``
- [x] ``schedule_many``: Abbruch des Aufrufers im Publish -> Claims bleiben
- [x] ``schedule_many``: ein Publish endet mit ``CancelledError`` -> sein
      Claim bleibt, ``CancelledError`` erreicht den Aufrufer
- [x] ``schedule_many``: Index 0 ``RuntimeError``, Index 1 ``CancelledError``
      -> ``CancelledError``; Claim von 1 bleibt, der von 0 wird frei
      (Review 0.13.1, P3-2)
- [x] verzögerter Pfad: Abbruch beim Speichern -> Claim bleibt,
      ``CancelledError``, kein ``post_enqueue``/``TASK_ENQUEUED``
- [x] verzögerter Pfad: ``RuntimeError`` beim Speichern gibt weiter frei
      (bestehender E-059-Test ``other-error``)

Nachtrag 0.14 (Plan U4, E-041): Meldet der Broker den Abbruch als
``PublishCancelledUnsent`` (nachweislich nichts gesendet), gibt der Client
den Claim frei; der Abbruch geht weiter.
- [x] sofortiger Pfad: markierter Abbruch -> Claim frei, ``CancelledError``,
      kein ``post_enqueue``/``TASK_ENQUEUED``
- [x] ``schedule_many``: markierter Eintrag gibt frei, unmarkierter hält,
      der Batch wirft ``CancelledError``
- [x] ``schedule_many``: Abbruch des Aufrufers, alle Publishes markiert ->
      Claims frei
- [x] Aufrufer mit ``asyncio.timeout`` um ``schedule()``: ``TimeoutError``,
      Claim frei
- [x] zweiter Abbruch während der Freigabe: der Abbruch geht weiter, die
      Freigabe endet unvollständig (Claim bleibt konservativ stehen)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client
from swarmq.exceptions import PublishCancelledUnsent
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.signals import Signal

pytestmark = pytest.mark.timeout(5)


def _hanging(started: asyncio.Event) -> Callable[..., Awaitable[Any]]:
    """Seiteneffekt, der den Start meldet und dann hängt, bis abgebrochen wird."""

    async def hang(*_args: Any, **_kwargs: Any) -> Any:
        started.set()
        await asyncio.Event().wait()

    return hang


async def _cancel_when(started: asyncio.Event, call: Awaitable[Any]) -> None:
    task = asyncio.ensure_future(call)
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


def _hanging_then_unsent(started: asyncio.Event) -> Callable[..., Awaitable[Any]]:
    """Publish wie der RabbitMQ-Broker im Blocked-Warten: Er hängt, und ein
    Abbruch endet als ``PublishCancelledUnsent`` (nichts gesendet)."""

    async def hang(*_args: Any, **_kwargs: Any) -> Any:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            raise PublishCancelledUnsent(*exc.args) from exc

    return hang


@pytest.mark.entscheidung("E-041")
async def test_cancel_marked_unsent_by_the_broker_releases_the_unique_claim(broker, backend, registry) -> None:
    started = asyncio.Event()
    middleware = MagicMock(spec=Middleware)
    middleware.pre_enqueue = AsyncMock()
    middleware.post_enqueue = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.dispatch = AsyncMock()
    client = Client(
        broker=broker,
        backend=backend,
        registry=registry,
        middleware_chain=MiddlewareChain([middleware]),
        signal_dispatcher=dispatcher,
    )
    broker.publish = AsyncMock(side_effect=_hanging_then_unsent(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule("add", 1, 2, queue="math", unique_for=60))

    backend.release_unique.assert_awaited_once()
    middleware.post_enqueue.assert_not_awaited()
    assert [c for c in dispatcher.dispatch.await_args_list if c.args[0] is Signal.TASK_ENQUEUED] == []


@pytest.mark.entscheidung("E-041")
async def test_caller_timeout_around_schedule_raises_timeout_and_releases_the_claim(client, broker, backend) -> None:
    """``asyncio.timeout`` des Aufrufers erkennt ``PublishCancelledUnsent``
    als seinen Abbruch: Der Aufrufer sieht ``TimeoutError``, der Claim ist frei."""
    started = asyncio.Event()
    broker.publish = AsyncMock(side_effect=_hanging_then_unsent(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()
    deadline: list[asyncio.Timeout] = []

    async def caller() -> object:
        async with asyncio.timeout(None) as cm:
            deadline.append(cm)
            return await client.schedule("add", 1, 2, queue="math", unique_for=60)

    task = asyncio.create_task(caller())
    await started.wait()
    deadline[0].reschedule(asyncio.get_running_loop().time())

    with pytest.raises(TimeoutError):
        await task
    backend.release_unique.assert_awaited_once()


@pytest.mark.entscheidung("E-041")
async def test_second_cancel_during_the_release_is_not_swallowed(client, broker, backend) -> None:
    started, releasing = asyncio.Event(), asyncio.Event()
    broker.publish = AsyncMock(side_effect=_hanging_then_unsent(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock(side_effect=_hanging(releasing))

    task = asyncio.ensure_future(client.schedule("add", 1, 2, queue="math", unique_for=60))
    await started.wait()
    task.cancel()
    await releasing.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


@pytest.mark.entscheidung("E-041")
async def test_cancel_during_the_publish_keeps_the_unique_claim(client, broker, backend) -> None:
    started = asyncio.Event()
    broker.publish = AsyncMock(side_effect=_hanging(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule("add", 1, 2, queue="math", unique_for=60))

    broker.publish.assert_awaited_once()
    backend.release_unique.assert_not_awaited()


class _HangingPreEnqueue(Middleware):
    def __init__(self, started: asyncio.Event) -> None:
        self._hang = _hanging(started)

    @override
    async def pre_enqueue(self, message: MessageContext) -> None:
        await self._hang()


@pytest.mark.entscheidung("E-041")
async def test_cancel_in_pre_enqueue_before_the_publish_releases_the_unique_claim(broker, backend, registry) -> None:
    """Vor dem Publish ist sicher nichts gesendet: der Abbruch gibt frei wie
    jeder andere Fehler, ``CancelledError`` erreicht den Aufrufer."""
    started = asyncio.Event()
    client = Client(
        broker=broker,
        backend=backend,
        registry=registry,
        middleware_chain=MiddlewareChain([_HangingPreEnqueue(started)]),
    )
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule("add", 1, 2, queue="math", unique_for=60))

    broker.publish.assert_not_awaited()
    backend.release_unique.assert_awaited_once()


@pytest.mark.entscheidung("E-041")
async def test_cancel_of_schedule_many_during_the_publish_keeps_the_unique_claims(client, broker, backend) -> None:
    """Hält Bestand fest, kein Nachweis des Fixes 0.13.1: Der Abbruch des
    Aufrufers trifft ``asyncio.gather``, das die Publishes abbricht und den
    ``CancelledError`` sofort weiterreicht; der Rückbau der Claims läuft gar
    nicht erst. Der Test ist deshalb auch ohne den Fix grün; den Fix belegt
    ``test_publish_of_schedule_many_ending_in_cancelled_error_keeps_its_claim``."""
    started = asyncio.Event()
    broker.publish = AsyncMock(side_effect=_hanging(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule_many([("add", (1, 2), {}), ("add", (3, 4), {})], unique_for=60))

    backend.release_unique.assert_not_awaited()


@pytest.mark.entscheidung("E-041")
async def test_publish_of_schedule_many_ending_in_cancelled_error_keeps_its_claim(client, broker, backend) -> None:
    """Endet ein einzelner Publish mit ``CancelledError`` (etwa ein Abbruch im
    Treiber), ist sein Ausgang unbestimmt: sein Claim bleibt, und der
    ``CancelledError`` erreicht den Aufrufer."""
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()
    broker.publish = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await client.schedule_many([("add", (1, 2), {})], unique_for=60)

    backend.release_unique.assert_not_awaited()


@pytest.mark.entscheidung("E-041")
async def test_schedule_many_with_a_failed_and_a_cancelled_publish_raises_cancelled_and_keeps_only_that_claim(
    client, broker, backend
) -> None:
    """Review 0.13.1 (P3-2): Index 0 scheitert bestimmt (``RuntimeError``),
    Index 1 endet mit ``CancelledError``. Ein unbestimmter Eintrag macht den
    Batch unbestimmt (B24): der Aufrufer sieht ``CancelledError``, nicht den
    bestimmten Fehler an kleinerem Index. Der Claim von 0 wird frei, der von
    1 bleibt."""
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()
    broker.publish = AsyncMock(side_effect=[RuntimeError("routing failed"), asyncio.CancelledError()])

    with pytest.raises(asyncio.CancelledError):
        await client.schedule_many([("add", (1, 2), {}), ("add", (3, 4), {})], unique_for=60)

    first_hash = backend.claim_unique.await_args_list[0].args[0]
    backend.release_unique.assert_awaited_once()
    assert backend.release_unique.await_args.args[0] == first_hash


@pytest.mark.entscheidung("E-041")
async def test_schedule_many_releases_only_the_claim_of_a_publish_marked_unsent(client, broker, backend) -> None:
    """Index 0 endet als ``PublishCancelledUnsent`` (nichts gesendet), Index 1
    mit einem unmarkierten ``CancelledError`` (unbestimmt). Nur der Claim von
    0 wird frei; der Batch wirft ``CancelledError`` (B24). ``gather`` ersetzt
    den Abbruch eines Kinds durch einen neuen ``CancelledError``; die
    Markierung muss das überstehen."""
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()
    broker.publish = AsyncMock(side_effect=[PublishCancelledUnsent(), asyncio.CancelledError()])

    with pytest.raises(asyncio.CancelledError):
        await client.schedule_many([("add", (1, 2), {}), ("add", (3, 4), {})], unique_for=60)

    first_hash = backend.claim_unique.await_args_list[0].args[0]
    backend.release_unique.assert_awaited_once()
    assert backend.release_unique.await_args.args[0] == first_hash


@pytest.mark.entscheidung("E-041")
async def test_cancel_of_schedule_many_with_publishes_marked_unsent_releases_their_claims(
    client, broker, backend
) -> None:
    """Der Abbruch des Aufrufers trifft ``gather``, das die Publishes abbricht.
    Melden beide ``PublishCancelledUnsent``, werden beide Claims frei; der
    Abbruch geht weiter."""
    started = asyncio.Event()
    broker.publish = AsyncMock(side_effect=_hanging_then_unsent(started))
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule_many([("add", (1, 2), {}), ("add", (3, 4), {})], unique_for=60))

    claimed = sorted(c.args[0] for c in backend.claim_unique.await_args_list)
    assert sorted(c.args[0] for c in backend.release_unique.await_args_list) == claimed


@pytest.mark.entscheidung("E-059")
async def test_cancel_while_storing_a_deferred_schedule_keeps_the_unique_claim(
    broker, backend, registry, monkeypatch
) -> None:
    """``sched_add.lua`` kann gelaufen sein: der Eintrag feuert womöglich. Kein
    ``post_enqueue``, kein ``TASK_ENQUEUED``; der Claim bleibt bis
    ``unique_for`` plus Verzögerung."""
    from swarmq.client import schedule as client_mod

    started = asyncio.Event()
    store = MagicMock()
    store.add_schedule = AsyncMock(side_effect=_hanging(started))
    monkeypatch.setattr(client_mod, "ScheduleStore", lambda _backend: store)
    middleware = MagicMock(spec=Middleware)
    middleware.pre_enqueue = AsyncMock()
    middleware.post_enqueue = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.dispatch = AsyncMock()
    client = Client(
        broker=broker,
        backend=backend,
        registry=registry,
        middleware_chain=MiddlewareChain([middleware]),
        signal_dispatcher=dispatcher,
    )
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock()

    await _cancel_when(started, client.schedule("add", 1, queue="math", delay=30, unique_for=60))

    store.add_schedule.assert_awaited_once()
    backend.release_unique.assert_not_awaited()
    middleware.post_enqueue.assert_not_awaited()
    assert [c for c in dispatcher.dispatch.await_args_list if c.args[0] is Signal.TASK_ENQUEUED] == []
