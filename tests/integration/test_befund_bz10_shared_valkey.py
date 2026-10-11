"""Befund BZ-10: mehrere Apps auf einem Valkey haben keinen Namensraum.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-10; D-10, Z-7).

Die README wirbt damit, dass sich mehrere Apps einen Valkey teilen können.
swarmq hat aber kein App-Präfix: Rate-Limit-Keys heißen
``sq:rl:{<task_name>}:<window>`` (``backend/valkey/rate_limit.py``), Locks
``sq:lock:<name>`` (``locking.py``), und es gibt genau einen Scheduler-Leader
(``sq:lock:{sched}:leader``) für alle Apps. Der Leader publiziert jeden Cron
-- auch die der anderen App -- über **seinen** Broker
(``scheduler/loop.py`` ``_fire_one``), und seine Wahl hängt nur an Valkey:
Ein Leader mit defektem Broker bleibt Leader und hält alle Crons aller Apps an.

Die Tests bilden „zwei Apps“ als zwei getrennte Registries/Queues mit je eigener
Valkey-Verbindung und eigenem (gemocktem) Broker auf **demselben** Test-Valkey
nach. Erwartet ist jeweils, dass die Apps sich nicht gegenseitig beeinflussen.
Entscheidung (Register, Plan 0.12.0, MR 15): ein optionaler Namensraum je App
(``SWARMQ_NAMESPACE``) trennt Keys, Kanäle und Leader; ohne Namensraum tritt ein
Leader mit defektem Broker zurück. Die Tests 1, 2 und 4 geben den Apps darum
je einen Namensraum (Plan Abschnitt 6, vor dem Fix umgeschrieben); Test 3
läuft bewusst ohne Namensraum (geteilter Leader, Rücktritt).

Test-Liste:
- [x] gleichnamiger Task mit Rate-Limit 1/m in App A und App B (je eigener
      Namensraum): B läuft, nachdem A sein Budget verbraucht hat
- [x] gleichnamiger Lock in App A und App B (je eigener Namensraum): B läuft,
      während A den Lock hält
- [x] Leader (App A, ohne Namensraum) mit defektem Broker: er tritt zurück, und
      der Cron von App B feuert über den Broker von B
- [x] gesunder Leader (App A), je eigener Namensraum: der Cron von App B wird
      über den Broker von App B publiziert, nicht über den von A
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import BrokerConnectionError
from swarmq.registry import TaskRegistry
from swarmq.scheduler import ScheduleDefinition, SchedulerLoop, ScheduleStore
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


def _broker_mock() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    for name in (
        "connect",
        "declare_queue",
        "consume",
        "ack",
        "nack",
        "redeliver",
        "retry_later",
        "dead_letter",
        "route_via_dlx",
        "close",
        "cancel_consume",
        "publish",
    ):
        setattr(b, name, AsyncMock())
    return b


async def _app_worker(
    valkey_url: str, task_cls: type[Task], queue: str, namespace: str | None = None
) -> tuple[Worker, MagicMock, ValkeyBackend]:
    """Eine „App“: eigene Registry, eigene Queue, eigene Valkey-Verbindung."""
    backend = ValkeyBackend(valkey_url, namespace=namespace)
    await backend.connect()
    broker = _broker_mock()
    registry = TaskRegistry()
    registry.register(task_cls)
    config = SwarmQConfig(broker_url="amqp://unused", backend_url=valkey_url, queues=[queue], namespace=namespace)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    # Die Koordinationsvertraege wie in ``_wire_coordination`` -- ohne Scheduler.
    worker._rate_limiter = backend.rate_limit
    worker._lock_manager = backend.locks
    return worker, broker, backend


def _message(task_name: str, queue: str) -> Any:
    msg = make_message({"task": {"id": uuid.uuid4().hex, "queue": queue, "name": task_name}, "args": [], "kwargs": {}})
    msg.queue = queue
    return msg


# ---------------------------------------------------------------------------
# Rate-Limit-Teilung
# ---------------------------------------------------------------------------


def _report_tasks() -> tuple[type[Task], type[Task]]:
    """Zwei Apps mit gleichnamigem Task.

    Erst im Test definiert: Auf Modulebene stuenden beide Klassen unter
    demselben Namen in der globalen Registry, und jede ``SwarmQ``-App in
    anderen Tests der Sitzung bricht dann mit "already registered" ab. Das
    autouse-Fixture ``_reset_task_registry`` raeumt nach dem Test auf.
    """

    class _ReportA(Task):
        task_info = TaskInfo(name="bz10_send_report", queues=["app_a"], rate_limit="1/m")
        runs: ClassVar[list[str]] = []

        async def run(self) -> None:
            type(self).runs.append("a")

    class _ReportB(Task):
        task_info = TaskInfo(name="bz10_send_report", queues=["app_b"], rate_limit="1/m")
        runs: ClassVar[list[str]] = []

        async def run(self) -> None:
            type(self).runs.append("b")

    return _ReportA, _ReportB


async def test_same_task_name_in_two_apps_does_not_share_the_rate_limit(valkey_url: str) -> None:
    _ReportA, _ReportB = _report_tasks()
    worker_a, _broker_a, backend_a = await _app_worker(valkey_url, _ReportA, "app_a", "bz10-a")
    worker_b, broker_b, backend_b = await _app_worker(valkey_url, _ReportB, "app_b", "bz10-b")
    try:
        await worker_a._handle_message(_message("bz10_send_report", "app_a"))
        assert _ReportA.runs == ["a"]

        await worker_b._handle_message(_message("bz10_send_report", "app_b"))

        assert _ReportB.runs == ["b"], (
            "App B hat ihren ersten Aufruf nicht ausgefuehrt: das Budget von App A "
            f"(1/m) gilt auch fuer B (retry_later aufgerufen: {broker_b.retry_later.await_count}x)"
        )
    finally:
        await backend_a.close()
        await backend_b.close()


# ---------------------------------------------------------------------------
# Lock-Teilung
# ---------------------------------------------------------------------------


class _ExportA(Task):
    task_info = TaskInfo(name="bz10_export_a", queues=["app_a"], lock="export", hard_timeout=30)
    started: ClassVar[asyncio.Event]
    finish: ClassVar[asyncio.Event]

    async def run(self) -> None:
        type(self).started.set()
        await type(self).finish.wait()


class _ExportB(Task):
    task_info = TaskInfo(name="bz10_export_b", queues=["app_b"], lock="export", hard_timeout=30)
    runs: ClassVar[list[str]] = []

    async def run(self) -> None:
        type(self).runs.append("b")


async def test_same_lock_name_in_two_apps_does_not_block_the_other_app(valkey_url: str) -> None:
    _ExportA.started, _ExportA.finish = asyncio.Event(), asyncio.Event()
    _ExportB.runs = []
    worker_a, _broker_a, backend_a = await _app_worker(valkey_url, _ExportA, "app_a", "bz10-a")
    worker_b, broker_b, backend_b = await _app_worker(valkey_url, _ExportB, "app_b", "bz10-b")
    running_a = asyncio.create_task(worker_a._handle_message(_message("bz10_export_a", "app_a")))
    try:
        await asyncio.wait_for(_ExportA.started.wait(), timeout=3)

        await worker_b._handle_message(_message("bz10_export_b", "app_b"))

        assert _ExportB.runs == ["b"], (
            "App B wurde vom Lock 'export' der App A blockiert "
            f"(zurueckgestellt: retry_later {broker_b.retry_later.await_count}x)"
        )
    finally:
        _ExportA.finish.set()
        await asyncio.wait_for(running_a, timeout=5)
        await backend_a.close()
        await backend_b.close()


# ---------------------------------------------------------------------------
# Ein Leader fuer alle Apps
# ---------------------------------------------------------------------------


async def _scheduler(
    valkey_url: str, broker: MagicMock, owner: str, dispatcher: SignalDispatcher, namespace: str | None = None
) -> tuple[SchedulerLoop, ValkeyBackend]:
    backend = ValkeyBackend(valkey_url, namespace=namespace)
    await backend.connect()
    config = SwarmQConfig(
        broker_url="amqp://unused",
        backend_url=valkey_url,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=1.0,
        namespace=namespace,
    )
    loop = SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=backend.locks,
        store=ScheduleStore(backend),
        signal_dispatcher=dispatcher,
        owner_token=owner,
    )
    return loop, backend


def _app_b_cron() -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name="bz10_app_b_billing",
        task_name="bz10_app_b_billing",
        args=(),
        kwargs={},
        queue="app_b",
        cron="* * * * *",
        fire_ts_ms=int(time.time() * 1000) - 1_000,
        source="cli",
    )


async def _run_two_apps(
    valkey_url: str,
    broker_a: MagicMock,
    broker_b: MagicMock,
    published_b: asyncio.Event,
    namespaces: tuple[str | None, str | None] = (None, None),
) -> None:
    """App A wird Leader, danach startet App B; App B hat einen faelligen Cron.

    Wartet ohne ``suppress`` darauf, dass der Cron von B publiziert wird
    (Plan 0.12.0, MR 15): ein Timeout laesst den Test scheitern, statt ihn
    still weiterlaufen zu lassen. Zeit genug fuer einen Ruecktritt von A
    (2 Fehler ueber 1 s) und die Uebernahme durch B."""
    d_a = SignalDispatcher()
    a_leader = asyncio.Event()

    @d_a.on_signal(Signal.SCHEDULER_LEADER)
    async def _on_leader(**_kw: Any) -> None:
        a_leader.set()

    ns_a, ns_b = namespaces
    loop_a, backend_a = await _scheduler(valkey_url, broker_a, f"A-{uuid.uuid4().hex[:6]}", d_a, ns_a)
    loop_b, backend_b = await _scheduler(valkey_url, broker_b, f"B-{uuid.uuid4().hex[:6]}", SignalDispatcher(), ns_b)
    try:
        await loop_a.start()
        await asyncio.wait_for(a_leader.wait(), timeout=3)
        await loop_b.start()
        assert await ScheduleStore(backend_b).add_schedule(_app_b_cron())

        await asyncio.wait_for(published_b.wait(), timeout=8)
    finally:
        await loop_a.stop()
        await loop_b.stop()
        await backend_a.close()
        await backend_b.close()


async def test_leader_with_broken_broker_does_not_stop_the_other_apps_crons(valkey_url: str) -> None:
    attempted_a, published_b = asyncio.Event(), asyncio.Event()
    broker_a = _broker_mock()

    async def _broken(*_a: Any, **_kw: Any) -> None:
        attempted_a.set()
        raise BrokerConnectionError("ACCESS_REFUSED - vhost app_a not reachable")

    broker_a.publish = AsyncMock(side_effect=_broken)
    broker_b = _broker_mock()
    broker_b.publish = AsyncMock(side_effect=lambda *_a, **_kw: published_b.set())

    await _run_two_apps(valkey_url, broker_a, broker_b, published_b)

    assert attempted_a.is_set(), "App A war Leader und haette den Cron von B zuerst versuchen muessen"
    assert published_b.is_set(), (
        f"der Cron von App B feuerte nicht: App A ist Leader, ihr Broker scheitert "
        f"({broker_a.publish.await_count} Publish-Versuche), und sie bleibt Leader"
    )


async def test_other_apps_cron_is_published_through_that_apps_broker(valkey_url: str) -> None:
    attempted_a, published_b = asyncio.Event(), asyncio.Event()
    broker_a = _broker_mock()
    broker_a.publish = AsyncMock(side_effect=lambda *_a, **_kw: attempted_a.set())
    broker_b = _broker_mock()
    broker_b.publish = AsyncMock(side_effect=lambda *_a, **_kw: published_b.set())

    await _run_two_apps(valkey_url, broker_a, broker_b, published_b, ("bz10-a", "bz10-b"))

    queues_a = [c.args[0] for c in broker_a.publish.await_args_list]
    msg = (
        f"der Cron von App B ging ueber den Broker von App A (Queues: {queues_a}); "
        "liegen die Apps auf getrennten Brokern/vhosts, landet er dort ohne Consumer"
    )
    assert published_b.is_set(), msg
    assert "app_b" not in queues_a, msg
