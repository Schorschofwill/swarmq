"""e2e: Supervisor-Abbau waehrend eines Recycling-Drains (FS-30, Plan 2026-10-14-0748, U9, KTD4).

Ein recycelnder Worker wartet seine eigene Frist ab (hier 630 s). Kommt in
dieser Zeit ein Deploy (SIGTERM an den Supervisor), muss der Worker im
Shutdown-Budget ab diesem Signal enden: vor dem SIGKILL des Supervisors, mit
freigegebener Task-Sperre und sauberer Leader-Uebergabe (Marker, also
``SCHEDULER_LEADER`` statt ``SCHEDULER_LEADER_FAILOVER`` beim Nachfolger).
Vor dem Fix lief der Drain weiter, der Supervisor eskalierte bis SIGKILL, die
Sperre blieb bis zu ihrer TTL stehen und der Nachfolger meldete FAILOVER.

Testliste:
- [x] SIGTERM an den Supervisor im Recycling-Drain: Exit vor ``sigkill_at``,
      keine SIGKILL-Eskalation, Sperre frei, Leader-Uebergabe mit Marker
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import threading
import time
import uuid
from subprocess import Popen

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.locking import SCHEDULER_LOCK_KEY, lock_key
from swarmq.shutdown_budget import shutdown_budget
from swarmq.signals import Signal
from swarmq.task import Task
from tests.support.services import flush_valkey_db
from tests.support.worker_subprocess import ensure_fixture_registered, kill_subprocess, spawn_worker

pytestmark = [pytest.mark.timeout(120)]

_FIXTURE_MODULE = "tests.e2e._recycling_fixture_tasks"
_SHUTDOWN_TIMEOUT_S = 8  # unter dem Supervisor: Drain bis 3 s, SIGKILL bei 6 s
_EVENT_TIMEOUT = 30.0


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


def _collect(proc: Popen[str], sink: list[str]) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        sink.append(line)  # noqa: PERF402 - jede Zeile muss sofort sichtbar sein, waehrend der Test parallel wartet


async def _until(pred, what: str, lines: list[str], timeout: float = _EVENT_TIMEOUT) -> None:
    """Pollt *pred* (Prozessgrenze: kein Event moeglich)."""
    deadline = time.monotonic() + timeout
    while not await pred():
        if time.monotonic() > deadline:
            raise AssertionError(f"nicht eingetreten: {what}\n{''.join(lines)[-4000:]}")
        await asyncio.sleep(0.1)


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_supervisor_shutdown_during_a_recycling_drain_ends_in_budget(rabbitmq_url, valkey_url) -> None:
    await flush_valkey_db(valkey_url)  # Leader-Lock frueherer Tests weg (nur Testport)
    queue = f"e2e-recycle-{uuid.uuid4().hex[:10]}"
    ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)
    from tests.e2e._recycling_fixture_tasks import RECYCLE_LOCK, STARTED_KEY

    proc = spawn_worker(
        queue,
        fixture_module=_FIXTURE_MODULE,
        broker_url=rabbitmq_url,
        backend_url=valkey_url,
        concurrency=2,
        env_extra={
            "SWARMQ_RECYCLE_MAX_TASKS": "1",
            "SWARMQ_SHUTDOWN_TIMEOUT": str(_SHUTDOWN_TIMEOUT_S),
            "SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL": "0.2",
            "SWARMQ_SCHEDULER_LOCK_TTL": "30",
        },
    )
    lines: list[str] = []
    reader = threading.Thread(target=_collect, args=(proc, lines), daemon=True)
    reader.start()
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    app_b = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[f"{queue}-b"],
        concurrency=1,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=30.0,
    )
    try:

        async def _started(n: int) -> bool:
            return sum("worker_started" in line for line in lines) >= n

        await _until(lambda: _started(1), "Worker gestartet", lines)
        await _until(lambda: client.exists(SCHEDULER_LOCK_KEY), "Worker ist Scheduler-Leader", lines)

        async with app_b:
            b_leader = asyncio.Event()
            b_failover: list[str] = []

            @app_b.on_signal(Signal.SCHEDULER_LEADER)
            async def _leader(**_kw) -> None:
                b_leader.set()

            @app_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
            async def _failover(**_kw) -> None:
                b_failover.append("failover")

            b_task = asyncio.create_task(app_b.start_worker())
            try:
                # B muss den Leader mindestens einmal gesehen haben (Wahltakt
                # 0,2 s); dafuer gibt es kein Signal, deshalb ein fester Abstand.
                await asyncio.sleep(1.0)
                assert not b_leader.is_set(), "Voraussetzung: der CLI-Worker ist Leader, B steht bereit"

                await app_b.schedule("recycle_long_locked", queue=queue)
                await _until(lambda: client.exists(STARTED_KEY), "Langlaeufer laeuft", lines)
                await app_b.schedule("recycle_quick", queue=queue)
                # Ersatz gestartet -> der alte Worker ist im Recycling-Drain (Frist 630 s).
                await _until(lambda: _started(2), "Ersatzworker gestartet", lines)
                assert await client.exists(lock_key(RECYCLE_LOCK)), "Voraussetzung: Sperre gehalten"

                sigterm_at = time.monotonic()
                proc.send_signal(signal.SIGTERM)
                rc = await asyncio.to_thread(proc.wait, 30)
                elapsed = time.monotonic() - sigterm_at

                output = "".join(lines)
                budget = shutdown_budget(_SHUTDOWN_TIMEOUT_S, supervised=True)
                assert "shutdown_sigkill_escalation" not in output, output[-4000:]
                assert elapsed < budget.sigkill_at, f"Abbau dauerte {elapsed:.1f} s"
                assert rc == 0, output[-4000:]
                assert not await client.exists(lock_key(RECYCLE_LOCK)), "Task-Sperre nicht freigegeben"

                await asyncio.wait_for(b_leader.wait(), _EVENT_TIMEOUT)
                assert not b_failover, "Nachfolger meldet FAILOVER: keine Leader-Uebergabe mit Marker"
            finally:
                b_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await b_task
    finally:
        kill_subprocess(proc)
        reader.join(timeout=5)
        await client.aclose()
        await flush_valkey_db(valkey_url)
