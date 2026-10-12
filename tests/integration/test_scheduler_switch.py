"""Scheduler je Worker abschaltbar (Plan 2026-10-12-1105, MR 5, P1, E2 A).

Zwei echte Worker an einem Valkey (eigener Namensraum je Test) und am
Test-Broker. Gewartet wird auf Ereignisse (Signale, Spione an den
Meldungen), nicht auf feste Zeiten -- außer in Test 17, siehe dort.

Test-Liste:
16. A mit ``off``, B mit ``on``: B wird Leader; A meldet seinen statischen
    Cron, B übernimmt und feuert ihn (A führt ihn aus). Nach dem Stopp von
    B wird A nicht Leader, auch nach mehreren Melde-Takten.
17. Nur ein Worker mit ``off``: ein ``delay``-Task feuert nicht innerhalb von
    2 x ``scheduler_poll_interval``. Danach startet ein Worker mit ``on``,
    und der Task läuft.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_POLL_S = 0.2


def _uid() -> str:
    return uuid.uuid4().hex[:8]


def _config(rabbitmq_url: str, valkey_url: str, *, queue: str, namespace: str, scheduler: str) -> SwarmQConfig:
    return SwarmQConfig(
        broker_url=rabbitmq_url,
        backend_url=valkey_url,
        queues=[queue],
        concurrency=1,
        namespace=namespace,
        scheduler=scheduler,
        scheduler_poll_interval=_POLL_S,
        scheduler_heartbeat_interval=_POLL_S,
        scheduler_lock_ttl=1.0,
        scheduler_seen_interval_s=_POLL_S,
    )


class _Counter:
    """Zählt Aufrufe und setzt ein Event, sobald ``wanted`` erreicht ist."""

    def __init__(self) -> None:
        self.calls = 0
        self._waiters: list[tuple[int, asyncio.Event]] = []

    def hit(self) -> None:
        self.calls += 1
        for wanted, event in self._waiters:
            if self.calls >= wanted:
                event.set()

    async def wait_for(self, more: int, timeout: float) -> None:
        event = asyncio.Event()
        self._waiters.append((self.calls + more, event))
        await asyncio.wait_for(event.wait(), timeout=timeout)


def _spy_reports(backend: ValkeyBackend) -> _Counter:
    """Spion an den Versionsmeldungen (eine je Melde-Takt)."""
    counter = _Counter()
    schedules = backend.schedules
    original = schedules.report_worker_version

    async def _report(*args: Any, **kwargs: Any) -> None:
        await original(*args, **kwargs)
        counter.hit()

    schedules.report_worker_version = _report  # type: ignore[method-assign]  # Spion am Exemplar
    return counter


@contextlib.asynccontextmanager
async def _running(worker: Worker) -> AsyncIterator[asyncio.Task[Any]]:
    task = asyncio.create_task(worker.start())
    try:
        yield task
    finally:
        worker._shutdown_event.set()
        try:
            await asyncio.wait_for(task, timeout=10)
        except BaseException:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


def _leader_event(dispatcher: SignalDispatcher, sink: list[str] | None = None) -> asyncio.Event:
    became = asyncio.Event()

    @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
    async def _leader(**_kw: Any) -> None:
        if sink is not None:
            sink.append("leader")
        became.set()

    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
    async def _failover(**_kw: Any) -> None:
        if sink is not None:
            sink.append("failover")
        became.set()

    return became


async def test_observer_reports_its_cron_and_never_becomes_leader(rabbitmq_url: str, valkey_url: str) -> None:
    namespace = f"pk5-{_uid()}"
    queue_a, queue_b = f"pk5-a-{_uid()}", f"pk5-b-{_uid()}"
    ran = asyncio.Event()
    cron_name = f"pk5_cron_{_uid()}"

    class CronTask(Task):
        task_info = TaskInfo(name=cron_name, queues=[queue_a], cron="* * * * *")

        @override
        async def run(self) -> None:
            ran.set()

    class Noop(Task):
        task_info = TaskInfo(name=f"pk5_noop_{_uid()}", queues=[queue_b])

        @override
        async def run(self) -> None:
            return None

    registry_a, registry_b = TaskRegistry(), TaskRegistry()
    registry_a.register(CronTask)
    registry_b.register(Noop)

    backend_a = ValkeyBackend(valkey_url, namespace=namespace)
    backend_b = ValkeyBackend(valkey_url, namespace=namespace)
    await backend_a.connect()
    await backend_b.connect()
    reports_a = _spy_reports(backend_a)
    dispatcher_a, dispatcher_b = SignalDispatcher(), SignalDispatcher()
    leader_a: list[str] = []
    _leader_event(dispatcher_a, leader_a)
    b_is_leader = _leader_event(dispatcher_b)

    worker_a = Worker(
        broker=RabbitMQBroker(rabbitmq_url),
        backend=backend_a,
        registry=registry_a,
        config=_config(rabbitmq_url, valkey_url, queue=queue_a, namespace=namespace, scheduler="off"),
        signal_dispatcher=dispatcher_a,
    )
    worker_b = Worker(
        broker=RabbitMQBroker(rabbitmq_url),
        backend=backend_b,
        registry=registry_b,
        config=_config(rabbitmq_url, valkey_url, queue=queue_b, namespace=namespace, scheduler="on"),
        signal_dispatcher=dispatcher_b,
    )

    async with _running(worker_a):
        async with _running(worker_b):
            await asyncio.wait_for(b_is_leader.wait(), timeout=5)
            # A meldet seinen Cron; B übernimmt ihn in seinen Bestand.
            await reports_a.wait_for(1, timeout=5)
            names = {r.name for r in (await backend_a.schedules.cron_reports()).reports}
            assert cron_name in names
            payload = await _adopted_payload(backend_b, cron_name, reports_a)
            # Den nächsten Termin auf jetzt ziehen, statt bis zur vollen
            # Minute zu warten; feuern muss der Leader B.
            await backend_b.schedules.put(cron_name, payload, int(time.time() * 1000))
            await asyncio.wait_for(ran.wait(), timeout=5)
        # B ist gestoppt (Lock freigegeben). A bewirbt sich trotzdem nicht.
        await reports_a.wait_for(5, timeout=5)
        assert leader_a == []
        assert (await backend_a.schedules.leader_info()).owner is None
    await backend_a.close()
    await backend_b.close()


async def _adopted_payload(backend: ValkeyBackend, name: str, reports: _Counter) -> bytes:
    """Wartet Melde-Takt für Melde-Takt, bis der Leader die Definition
    übernommen hat (Übernahme im selben Takt wie die Meldungen)."""
    for _ in range(20):
        payload = await backend.schedules.get(name)
        if payload is not None:
            return payload
        await reports.wait_for(1, timeout=5)
    raise AssertionError(f"leader never adopted {name!r}")


async def test_delay_task_waits_for_a_worker_with_the_scheduler_on(rabbitmq_url: str, valkey_url: str) -> None:
    namespace = f"pk5-{_uid()}"
    queue = f"pk5-d-{_uid()}"
    queue_b = f"pk5-db-{_uid()}"
    ran = asyncio.Event()
    task_name = f"pk5_delay_{_uid()}"

    class DelayTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> None:
            ran.set()

    registry_a, registry_b = TaskRegistry(), TaskRegistry()
    registry_a.register(DelayTask)
    dispatcher_b = SignalDispatcher()
    b_is_leader = _leader_event(dispatcher_b)

    worker_a = Worker(
        broker=RabbitMQBroker(rabbitmq_url),
        backend=ValkeyBackend(valkey_url, namespace=namespace),
        registry=registry_a,
        config=_config(rabbitmq_url, valkey_url, queue=queue, namespace=namespace, scheduler="off"),
    )
    app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url, namespace=namespace),
        namespace=namespace,
        queues=[queue],
    )
    async with _running(worker_a), app:
        await app.broker.declare_queue(queue)
        await app.schedule(task_name, delay=0.1)
        # Bewusste Wartezeit (Plan MR 5, Test 17): Nachweis, dass ohne
        # Leader nichts feuert. Zwei Poll-Intervalle reichten einem Leader
        # sicher; ohne Leader darf der Task darin nicht laufen.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(ran.wait(), timeout=2 * _POLL_S + 0.1)

        worker_b = Worker(
            broker=RabbitMQBroker(rabbitmq_url),
            backend=ValkeyBackend(valkey_url, namespace=namespace),
            registry=registry_b,
            config=_config(rabbitmq_url, valkey_url, queue=queue_b, namespace=namespace, scheduler="on"),
            signal_dispatcher=dispatcher_b,
        )
        async with _running(worker_b):
            await asyncio.wait_for(b_is_leader.wait(), timeout=5)
            await asyncio.wait_for(ran.wait(), timeout=5)
