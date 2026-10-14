"""Aufraeumen einer Zustellung in jedem Abbruch- und Fehlerpfad (Plan
2026-10-14-0748, U4; FS-04, FS-03 Teil 2; KTD6).

Der ``finally`` von ``_process_message`` laeuft als Kaskade: Settle/Finalize,
DI-Teardown, Lock-Freigabe, Semaphor. Jeder spaetere Schritt laeuft auch,
wenn ein frueherer scheitert oder abgebrochen wird. Der Semaphor-Erwerb liegt
vor dem ``try``; ein Abbruch dort gibt nur den Schedfire-Claim frei.

Absichtlich ohne ``from __future__ import annotations``: ``FromDishka`` muss
bei der Klassendefinition aufloesbar sein (wie in ``test_worker_dishka.py``).

Testliste:
# [x] Abbruch am Semaphor mit Schedfire-Header -> Claim frei, keine
#     finally-Schritte (kein Completed-Marker, kein Broker-Verb, Semaphor
#     bleibt beim Halter)
# [x] Abbruch am Semaphor ohne Schedfire-Header -> keine Freigabe-Aufrufe
# [x] aeusserer Abbruch trifft den DI-Teardown -> Lock und Semaphor bleiben
#     gehalten, bis der Finalizer endet; danach frei (KTD6)
# [x] Normalpfad ohne Abbruch, blockierender Finalizer -> Lock gehalten, bis
#     der Finalizer endet
# [x] Abbruch im DI-Teardown und Shutdown-Abraeumen gleichzeitig -> genau
#     eine Freigabe, kein task_lock_lost
# [x] Finalizer wirft BaseException (CancelledError) -> Semaphor
#     zurueckgegeben, Lock frei
# [x] concurrency=1, scheiternder Finalizer -> die zweite Zustellung laeuft
#     durch
# [x] Unique-Freigabe (maybe_release_unique) wirft eine fremde Ausnahme ->
#     Lock und Semaphor frei
# [x] Kombination: Abbruch am Semaphor, danach zweite Zustellung mit
#     scheiterndem Finalizer bei concurrency=1 -> eine dritte laeuft durch
# [x] Lock-Freigabe des Handlers und _release_leftover_locks gleichzeitig ->
#     kein task_lock_lost
#
# ``execution_guard.settle`` faengt ``Exception`` selbst (``_mark``); nur ein
# Abbruch kaeme dort durch -- derselbe Pfad wie der Abbruch im DI-Teardown.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Literal, override
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("dishka")

from dishka import (
    FromDishka,
    Provider,
    Scope,
    from_context,
    make_async_container,
    provide,  # pyright: ignore[reportUnknownVariableType] - provide-Overload von dishka nur teilweise typisiert
)

from swarmq import dishka as di
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.headers import HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory.backend import InMemoryLockStore
from tests.support.inmemory.clock import FakeClock
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_LOCK = "finally-res"
_SCHEDFIRE = "cron:finally:1"
_HASH = "b" * 64


class _Session:
    """Steht fuer eine DB-Session mit Commit im Finalizer."""


class _FailingCommitProvider(Provider):
    tc = from_context(di.TaskContext, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    async def session(self, ctx: di.TaskContext) -> AsyncGenerator[_Session, BaseException | None]:
        yield _Session()
        raise RuntimeError(f"commit failed for {ctx.task_id}")


class _CancelledFinalizerProvider(Provider):
    tc = from_context(di.TaskContext, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    async def session(self, ctx: di.TaskContext) -> AsyncGenerator[_Session, BaseException | None]:
        yield _Session()
        raise asyncio.CancelledError(f"finalizer aborted for {ctx.task_id}")


class _BlockingFinalizerProvider(Provider):
    """Finalizer haelt an, bis der Test ihn freigibt."""

    tc = from_context(di.TaskContext, scope=Scope.REQUEST)

    def __init__(self, entered: asyncio.Event, proceed: asyncio.Event) -> None:
        super().__init__()
        self._entered = entered
        self._proceed = proceed

    @provide(scope=Scope.REQUEST)
    async def session(self, ctx: di.TaskContext) -> AsyncGenerator[_Session, BaseException | None]:
        yield _Session()
        self._entered.set()
        await self._proceed.wait()


class _LockedDiTask(Task):
    task_info = TaskInfo(name="finally_locked_di", queues=["default"], lock=_LOCK, hard_timeout=60)

    @override
    async def run(self, session: FromDishka[_Session]) -> None:
        return None


class _DiTask(Task):
    task_info = TaskInfo(name="finally_di", queues=["default"])

    @override
    async def run(self, session: FromDishka[_Session]) -> None:
        return None


class _LockedTask(Task):
    task_info = TaskInfo(name="finally_locked", queues=["default"], lock=_LOCK, hard_timeout=60)

    @override
    async def run(self) -> None:
        return None


class _PlainTask(Task):
    task_info = TaskInfo(name="finally_plain", queues=["default"])

    @override
    async def run(self) -> None:
        return None


class _SignallingSemaphore(asyncio.Semaphore):
    """Meldet, sobald eine Zustellung auf einen Platz wartet."""

    def __init__(self, value: int) -> None:
        super().__init__(value)
        self.waiting = asyncio.Event()

    @override
    async def acquire(self) -> Literal[True]:
        if self.locked():
            self.waiting.set()
        return await super().acquire()


def _worker(broker: Any, backend: Any, *tasks: type[Task], store: InMemoryLockStore | None = None) -> Worker:
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=1,
        queues=["default"],
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    if store is not None:
        worker._lock_manager = store
    return worker


def _message(task_id: str, name: str, headers: dict[str, Any] | None = None) -> MagicMock:
    return make_message(_default_payload(task_id=task_id, name=name, args=[], kwargs={}), headers=headers)


async def _cancel_while_waiting_for_slot(worker: Worker, message: MagicMock) -> _SignallingSemaphore:
    """Belegt den einzigen Platz, laesst ``message`` am Semaphor warten und
    bricht sie dort ab. Der Platz bleibt belegt (Halter ist der Test)."""
    semaphore = _SignallingSemaphore(1)
    worker._semaphore = semaphore
    await semaphore.acquire()
    handler = asyncio.ensure_future(worker._handle_message(message))
    await semaphore.waiting.wait()
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    return semaphore


# --- FS-03 Teil 2: Abbruch am Semaphor -------------------------------------


async def test_cancel_at_semaphore_releases_the_fire_claim_without_finally_steps(
    broker: MagicMock, backend: MagicMock
) -> None:
    worker = _worker(broker, backend, _PlainTask)

    semaphore = await _cancel_while_waiting_for_slot(
        worker, _message("fin-k5", "finally_plain", {"x-swarmq-schedfire": _SCHEDFIRE})
    )

    released = [c.args[0] for c in backend.schedules.release_fire_claim.await_args_list]
    assert released == [backend.schedules.claim_fire.await_args.args[0]]
    backend.schedules.mark_fire_completed.assert_not_awaited()
    for verb in (broker.ack, broker.nack, broker.redeliver, broker.retry_later, broker.dead_letter):
        verb.assert_not_awaited()
    assert semaphore.locked(), "der finally gab einen nie erworbenen Platz zurueck"


async def test_cancel_at_semaphore_without_schedfire_header_releases_nothing(
    broker: MagicMock, backend: MagicMock
) -> None:
    backend.release_unique = AsyncMock()
    worker = _worker(broker, backend, _PlainTask)

    await _cancel_while_waiting_for_slot(worker, _message("fin-k5b", "finally_plain"))

    backend.schedules.release_fire_claim.assert_not_awaited()
    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()


# --- FS-04: Kaskade im finally ----------------------------------------------


async def test_outer_cancel_during_di_teardown_still_returns_the_slot_and_lock(
    broker: MagicMock, backend: MagicMock
) -> None:
    """KTD6: Finalizer vor Lock-Freigabe, auch nach aeusserem Abbruch. Der
    geshieldete Finalizer (z. B. DB-Commit) laeuft weiter; Lock und
    Semaphor werden erst nach seinem Ende frei, sonst liefe der Commit ohne
    Mutex."""
    entered, proceed = asyncio.Event(), asyncio.Event()
    di.setup_dishka(make_async_container(_BlockingFinalizerProvider(entered, proceed)))
    store = InMemoryLockStore(FakeClock())
    worker = _worker(broker, backend, _LockedDiTask, store=store)

    handler = asyncio.ensure_future(worker._handle_message(_message("fin-c2", "finally_locked_di")))
    await entered.wait()
    handler.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await handler

        assert await store.current_owner(_LOCK) is not None, "Lock frei, waehrend der Finalizer noch laeuft"
        assert worker._semaphore.locked(), "Semaphor frei, waehrend der Finalizer noch laeuft"
    finally:
        proceed.set()
        while worker._detached_releases:
            await asyncio.gather(*worker._detached_releases)

    assert not worker._semaphore.locked(), "Semaphor-Platz nach Abbruch im DI-Teardown verloren"
    assert await store.current_owner(_LOCK) is None, "Lock nach Abbruch im DI-Teardown gehalten"


async def test_blocking_finalizer_without_cancel_keeps_the_lock_until_it_ends(
    broker: MagicMock, backend: MagicMock
) -> None:
    """Normalpfad (KTD6): der Finalizer laeuft vor der Lock-Freigabe."""
    entered, proceed = asyncio.Event(), asyncio.Event()
    di.setup_dishka(make_async_container(_BlockingFinalizerProvider(entered, proceed)))
    store = InMemoryLockStore(FakeClock())
    worker = _worker(broker, backend, _LockedDiTask, store=store)

    handler = asyncio.ensure_future(worker._handle_message(_message("fin-c3", "finally_locked_di")))
    await entered.wait()
    try:
        assert await store.current_owner(_LOCK) is not None, "Lock frei, waehrend der Finalizer noch laeuft"
        assert worker._semaphore.locked()
    finally:
        proceed.set()
        await handler

    assert await store.current_owner(_LOCK) is None
    assert not worker._semaphore.locked()


async def test_finalizer_raising_base_exception_still_returns_the_slot_and_lock(
    broker: MagicMock, backend: MagicMock
) -> None:
    """Dishka sammelt nur ``Exception``; eine ``BaseException`` aus dem
    Finalizer bricht ``__aexit__`` sofort ab und kommt beim Handler an."""
    di.setup_dishka(make_async_container(_CancelledFinalizerProvider()))
    store = InMemoryLockStore(FakeClock())
    worker = _worker(broker, backend, _LockedDiTask, store=store)

    handler = asyncio.ensure_future(worker._handle_message(_message("fin-c1", "finally_locked_di")))
    with contextlib.suppress(asyncio.CancelledError):
        await handler

    assert not worker._semaphore.locked()
    assert await store.current_owner(_LOCK) is None


async def test_second_delivery_runs_after_a_failing_finalizer_at_concurrency_one(
    broker: MagicMock, backend: MagicMock
) -> None:
    di.setup_dishka(make_async_container(_FailingCommitProvider()))
    worker = _worker(broker, backend, _DiTask)

    await worker._handle_message(_message("fin-a1", "finally_di"))
    await worker._handle_message(_message("fin-a2", "finally_di"))

    stored = [c.args[0] for c in backend.store_result.await_args_list]
    assert stored == ["fin-a1", "fin-a2"]
    assert broker.ack.await_count == 2


async def test_failing_unique_release_still_frees_lock_and_slot(broker: MagicMock, backend: MagicMock) -> None:
    """Ein Dritt-Backend wirft beim Unique-Release eine fremde Ausnahme
    (``release_unique`` schluckt nur ``BackendConnectionError``)."""
    backend.release_unique = AsyncMock(side_effect=RuntimeError("foreign backend failure"))
    store = InMemoryLockStore(FakeClock())
    worker = _worker(broker, backend, _LockedTask, store=store)

    await worker._handle_message(_message("fin-b1", "finally_locked", {HEADER_UNIQUE_HASH: _HASH}))

    backend.release_unique.assert_awaited_once()  # Voraussetzung: die Freigabe lief und scheiterte
    assert await store.current_owner(_LOCK) is None
    assert not worker._semaphore.locked()


async def test_cancel_at_semaphore_then_failing_finalizer_leaves_the_worker_serving(
    broker: MagicMock, backend: MagicMock
) -> None:
    """Kombination K-5 + FS-04 im selben Lauf (concurrency=1)."""
    di.setup_dishka(make_async_container(_FailingCommitProvider()))
    worker = _worker(broker, backend, _DiTask)
    semaphore = await _cancel_while_waiting_for_slot(
        worker, _message("fin-k1", "finally_di", {"x-swarmq-schedfire": _SCHEDFIRE})
    )
    semaphore.release()  # der Test gibt seinen Platz zurueck

    await worker._handle_message(_message("fin-k2", "finally_di"))
    await worker._handle_message(_message("fin-k3", "finally_di"))

    backend.schedules.release_fire_claim.assert_awaited_once()
    stored = [c.args[0] for c in backend.store_result.await_args_list]
    assert stored == ["fin-k2", "fin-k3"]


class _GatedReleaseStore(InMemoryLockStore):
    """Haelt die erste Freigabe an, bis der Test sie loslaesst."""

    def __init__(self) -> None:
        super().__init__(FakeClock())
        self.first_release_started = asyncio.Event()
        self.proceed = asyncio.Event()
        self._gated = False

    @override
    async def release(self, token: LockToken) -> bool:
        if not self._gated:
            self._gated = True
            self.first_release_started.set()
            await self.proceed.wait()
        return await super().release(token)


async def test_handler_release_racing_shutdown_cleanup_reports_no_lost_lock(
    broker: MagicMock, backend: MagicMock, swarmq_logs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Die Lock-Freigabe des Handlers laeuft noch, waehrend der Shutdown die
    Registry liegengebliebener Locks abraeumt. Derselbe Token darf nicht
    zweimal freigegeben werden (sonst Fehlalarm ``task_lock_lost``)."""
    from swarmq.worker import lifecycle

    # Kurzes Mindestbudget je Schritt: das Warten auf die (absichtlich
    # angehaltene) Freigabe des Handlers laeuft ab -- der Fall, in dem der
    # Shutdown die Registry selbst abraeumt --, die eigene In-Memory-Freigabe
    # des Shutdowns passt aber hinein.
    monkeypatch.setattr(lifecycle, "_SHUTDOWN_STEP_MIN_S", 0.01)
    store = _GatedReleaseStore()
    worker = _worker(broker, backend, _LockedTask, store=store)
    worker._config.shutdown_timeout = 0

    handler = asyncio.ensure_future(worker._handle_message(_message("fin-r1", "finally_locked")))
    await store.first_release_started.wait()
    worker._request_shutdown()
    await worker._execute_shutdown()
    store.proceed.set()
    await handler

    assert not [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert await store.current_owner(_LOCK) is None


async def test_cancel_during_di_teardown_racing_shutdown_cleanup_releases_once(
    broker: MagicMock, backend: MagicMock, swarmq_logs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Der Nachlauf eines abgebrochenen DI-Teardowns wartet noch auf den
    Finalizer, als der Shutdown die liegengebliebenen Locks abraeumt. Der
    Shutdown gibt den Lock frei; der Nachlauf darf es danach nicht noch
    einmal tun (sonst Fehlalarm ``task_lock_lost``)."""
    from swarmq.worker import lifecycle

    # Kurzes Mindestbudget je Schritt: das Warten auf den (absichtlich
    # angehaltenen) Nachlauf laeuft ab, der Shutdown raeumt selbst ab.
    monkeypatch.setattr(lifecycle, "_SHUTDOWN_STEP_MIN_S", 0.01)
    entered, proceed = asyncio.Event(), asyncio.Event()
    di.setup_dishka(make_async_container(_BlockingFinalizerProvider(entered, proceed)))
    store = InMemoryLockStore(FakeClock())
    releases: list[str] = []
    original_release = store.release

    async def counting_release(token: LockToken) -> bool:
        releases.append(token.name)
        return await original_release(token)

    monkeypatch.setattr(store, "release", counting_release)
    worker = _worker(broker, backend, _LockedDiTask, store=store)
    worker._config.shutdown_timeout = 0

    handler = asyncio.ensure_future(worker._handle_message(_message("fin-c4", "finally_locked_di")))
    await entered.wait()
    handler.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handler
    worker._request_shutdown()
    await worker._execute_shutdown()
    proceed.set()
    while worker._detached_releases:
        await asyncio.gather(*worker._detached_releases)

    assert releases == [_LOCK]
    assert not [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert await store.current_owner(_LOCK) is None
    assert not worker._semaphore.locked()
