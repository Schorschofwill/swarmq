"""U5/U8: end-to-end workflow roundtrips driven by a real worker.

Verifies the full path: apply() enqueues the frontier, the worker runs each
step and advances the workflow (chain result-injection, chord fan-in), and
handle.get() returns the final result. Against real RabbitMQ + Valkey.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError, TaskError
from swarmq.middleware import Middleware
from swarmq.serialization import decode_json
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(30)]


def _q() -> str:
    return f"wfe2e-{uuid.uuid4().hex[:12]}"


@contextlib.asynccontextmanager
async def _worker(rabbitmq_url, valkey_url, queue, *, middleware=None, on_signal=None):
    """Run a worker consuming *queue* for the duration of the context.

    ``middleware``/``on_signal`` (U11) haengen die Enqueue-Beobachtung des
    Workers an: Folgeschritte laufen durch dieselbe Kette und dieselben
    Signale wie ``Client.schedule``."""
    wb = RabbitMQBroker(rabbitmq_url)
    wbk = ValkeyBackend(valkey_url)
    wapp = SwarmQ(wb, wbk, queues=[queue])
    for mw in middleware or []:
        wapp.add_middleware(mw)
    for signal, handler in (on_signal or {}).items():
        wapp.on_signal(signal)(handler)
    task = asyncio.create_task(wapp.start_worker())
    try:
        yield wapp
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await wb.close()
        await wbk.close()


async def test_chain_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Step1(Task):
        task_info = TaskInfo(name=f"s1_{queue}", queues=[queue])

        @override
        async def run(self, seed: str) -> str:
            return f"{seed}-1"

    class Step2(Task):
        task_info = TaskInfo(name=f"s2_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-2"

    class Step3(Task):
        task_info = TaskInfo(name=f"s3_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-3"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chain(Step1.s("seed"), Step2.s(), Step3.s()))
            result = await handle.get(timeout=20)
            assert result == "seed-1-2-3"


async def test_chord_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Dl(Task):
        task_info = TaskInfo(name=f"dl_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n * 10

    class Merge(Task):
        task_info = TaskInfo(name=f"merge_{queue}", queues=[queue])

        @override
        async def run(self, results: list) -> int:
            return sum(results)

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chord(group(Dl.s(1), Dl.s(2), Dl.s(3)), Merge.s()))
            result = await handle.get(timeout=20)
            # Merge receives [10, 20, 30] in definition order → 60.
            assert result == 60


async def test_workflow_cancel_stops_chain(rabbitmq_url, valkey_url):
    """Härtung R2 B6/P1-10: der Test prüfte nur, dass ``get()`` das
    Cancel-Flag sieht — NICHT, dass Steps tatsächlich gestoppt werden
    (das Versprechen im Testnamen). Jetzt: cancel VOR dem Worker-Start
    → kein einziger Step darf laufen (Pickup-Check greift)."""
    queue = _q()
    ran: list[str] = []

    class Slow1(Task):
        task_info = TaskInfo(name=f"slow1_{queue}", queues=[queue])

        @override
        async def run(self) -> str:
            ran.append("slow1")
            return "1"

    class Slow2(Task):
        task_info = TaskInfo(name=f"slow2_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            ran.append("slow2")
            return "2"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        # Cancel BEFORE any worker exists: the first step is already
        # enqueued; only the pickup-time workflow-flag check can stop it.
        handle = await app.apply(chain(Slow1.s(), Slow2.s()))
        await handle.cancel()
        async with _worker(rabbitmq_url, valkey_url, queue):
            with pytest.raises(TaskCancelledError):
                await handle.get(timeout=20)
            # Give the worker a moment to (wrongly) run anything queued.
            await asyncio.sleep(1.0)

    assert ran == [], f"cancelled workflow still executed steps: {ran}"


async def test_workflow_cancel_stops_enqueued_group_members(
    rabbitmq_url,
    valkey_url,
):
    """Härtung R2 B6/P1-10: group/chord-Member werden alle bei apply()
    eingereiht — cancel() musste sie am Pickup stoppen, vorher liefen
    sie ungebremst durch."""
    queue = _q()
    ran: list[int] = []

    class Member(Task):
        task_info = TaskInfo(name=f"member_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            ran.append(n)
            return n

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        # All three members are enqueued by apply(); cancel before any
        # worker can pick them up.
        handle = await app.apply(group(Member.s(1), Member.s(2), Member.s(3)))
        await handle.cancel()
        async with _worker(rabbitmq_url, valkey_url, queue):
            with pytest.raises(TaskCancelledError):
                await handle.get(timeout=20)
            await asyncio.sleep(1.0)

    assert ran == [], f"cancelled group still executed members: {ran}"


async def test_chain_step_failure_fails_workflow_with_liveness(rabbitmq_url, valkey_url):
    queue = _q()
    step2_ran = asyncio.Event()

    class Boom(Task):
        task_info = TaskInfo(name=f"boom_{queue}", queues=[queue], max_retries=0)

        @override
        async def run(self) -> str:
            raise ValueError("boom")

    class Never(Task):
        task_info = TaskInfo(name=f"never_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            step2_ran.set()
            return "never"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chain(Boom.s(), Never.s()))
            # Fail-fast + liveness: get() raises within bounded time (the
            # worker marks the workflow failed at the DLQ seam and wakes us) —
            # it does NOT hang to the timeout.
            with pytest.raises(TaskError):
                await handle.get(timeout=20)
            assert not step2_ran.is_set()  # successor never enqueued


async def test_chord_header_failure_does_not_fire_body(rabbitmq_url, valkey_url):
    # AE5 / R16: a chord header member that fails terminally must fail the
    # whole workflow with bounded liveness, and the body must NEVER run.
    queue = _q()
    body_ran = asyncio.Event()

    class Ok(Task):
        task_info = TaskInfo(name=f"ok_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n

    class HeaderBoom(Task):
        task_info = TaskInfo(name=f"hboom_{queue}", queues=[queue], max_retries=0)

        @override
        async def run(self, n: int) -> int:
            raise ValueError("header member failed")

    class Body(Task):
        task_info = TaskInfo(name=f"body_{queue}", queues=[queue])

        @override
        async def run(self, results: list) -> int:
            body_ran.set()
            return sum(results)

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chord(group(Ok.s(1), HeaderBoom.s(2), Ok.s(3)), Body.s()))
            with pytest.raises(TaskError):
                await handle.get(timeout=20)
            # Once get() raises, the workflow is terminal. The failed header
            # member never enters the chord completion set, so the set can
            # never reach N=3 and the body is never enqueued — assertable
            # immediately, no wait needed.
            assert not body_ran.is_set()


# --------------------------------------------------------------------------
# B10 — zusammengesetzte Gruppenglieder (Plan 2026-08-30-002, U4)
#
# Die Nutzersicht: was `handle.get()` zurueckgibt. Bis 0.3.0 fehlte hier das
# zusammengesetzte Glied stillschweigend.
# --------------------------------------------------------------------------


async def test_group_with_composite_member_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Double(Task):
        task_info = TaskInfo(name=f"double_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n * 2

    class AddOne(Task):
        task_info = TaskInfo(name=f"addone_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n + 1

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(group(Double.s(1), chain(Double.s(2), AddOne.s())))
            result = await handle.get(timeout=20)
            # Glied 0: 1*2 = 2. Glied 1: 2*2 = 4, dann +1 = 5.
            # Bis 0.3.0 kam hier [2] zurueck — das Kettenglied fehlte.
            assert result == [2, 5]


async def test_chord_over_composite_header_member_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Double(Task):
        task_info = TaskInfo(name=f"cdouble_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n * 2

    class AddOne(Task):
        task_info = TaskInfo(name=f"caddone_{queue}", queues=[queue])

        @override
        async def run(self, n: int) -> int:
            return n + 1

    class Total(Task):
        task_info = TaskInfo(name=f"ctotal_{queue}", queues=[queue])

        @override
        async def run(self, results: list) -> int:
            return sum(results)

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(
                chord(
                    group(Double.s(1), chain(Double.s(2), AddOne.s())),
                    Total.s(),
                )
            )
            result = await handle.get(timeout=20)
            # Der gefaehrlichere Fall: die gekuerzte Liste waere die EINGABE
            # des Bodys gewesen. Bis 0.3.0 haette Total([2]) = 2 gerechnet.
            assert result == 7


async def test_workflow_steps_run_through_middleware_and_signals(rabbitmq_url, valkey_url):
    """AE6 (R15, R16): Eine Kette aus drei Tasks — die zaehlende Middleware des
    WORKERS sieht die beiden Folgeschritte in ``pre_enqueue``, und
    ``WORKFLOW_STEP_ENQUEUED`` feuert mit ihren Schrittpfaden. Vor U11 lief
    kein Folgeschritt durch die Kette und kein Signal trug den Schrittpfad."""
    queue = _q()

    class M1(Task):
        task_info = TaskInfo(name=f"m1_{queue}", queues=[queue])

        @override
        async def run(self, seed: str) -> str:
            return f"{seed}-1"

    class M2(Task):
        task_info = TaskInfo(name=f"m2_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-2"

    class M3(Task):
        task_info = TaskInfo(name=f"m3_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-3"

    pre_seen: list[tuple[str, str]] = []
    post_seen: list[str] = []
    step_signals: list[tuple[str, str]] = []
    task_enqueued: list[str] = []

    class Counting(Middleware):
        @override
        async def pre_enqueue(self, message) -> None:
            pre_seen.append((message["step_path"], message["delivery_mode"]))

        @override
        async def post_enqueue(self, message) -> None:
            post_seen.append(message["step_path"])

    async def _on_step(**kwargs) -> None:
        step_signals.append((kwargs["step_path"], kwargs["task_name"]))

    async def _on_enqueued(**kwargs) -> None:
        task_enqueued.append(kwargs["task_name"])

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(
            rabbitmq_url,
            valkey_url,
            queue,
            middleware=[Counting()],
            on_signal={
                Signal.WORKFLOW_STEP_ENQUEUED: _on_step,
                Signal.TASK_ENQUEUED: _on_enqueued,
            },
        ):
            handle = await app.apply(chain(M1.s("seed"), M2.s(), M3.s()))
            assert await handle.get(timeout=20) == "seed-1-2-3"

    # Die Anfangsfront (/0) laeuft ueber den Client, die Folgeschritte /1 und
    # /2 ueber den Advancer im Worker.
    assert [p for p, _ in pre_seen] == ["/1", "/2"]
    assert {mode for _, mode in pre_seen} == {"at_least_once"}
    assert post_seen == ["/1", "/2"]
    assert step_signals == [("/1", f"m2_{queue}"), ("/2", f"m3_{queue}")]
    assert task_enqueued == [f"m2_{queue}", f"m3_{queue}"]


async def test_middleware_rejecting_a_group_member_fails_the_workflow(rabbitmq_url, valkey_url):
    """AE7 (R15, R17): Lehnt die Middleware des Workers das zweite Glied einer
    Gruppen-Front ab, wird kein Glied publiziert, der Workflow ist ``FAILED``
    mit ``error.type``, ``handle.get()`` wirft schnell statt in den Timeout zu
    laufen, und die Nachricht des Vorgaengers ist bestaetigt."""
    queue = _q()

    class Head(Task):
        task_info = TaskInfo(name=f"head_{queue}", queues=[queue])

        @override
        async def run(self, seed: str) -> str:
            return seed

    class Leg(Task):
        task_info = TaskInfo(name=f"leg_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-leg"

    published: list[str] = []

    class RejectSecond(Middleware):
        @override
        async def pre_enqueue(self, message) -> None:
            published.append(message["step_path"])
            if message["step_path"].endswith("g1"):
                raise PermissionError("quota exceeded")

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue, middleware=[RejectSecond()]):
            handle = await app.apply(chain(Head.s("seed"), group(Leg.s(), Leg.s())))
            with pytest.raises(TaskError, match="quota exceeded"):
                await handle.get(timeout=10)
            assert await handle.status() == "failed"
            raw = await backend.get_workflow_state(handle.workflow_id)
            assert decode_json(raw)["error"]["type"] == "PermissionError"
            # Kein Glied der abgelehnten Front hat einen Schritt-Marker.
            assert not [p for p in await handle.children() if p.startswith("/1/")]
        # Der Vorgaenger wurde bestaetigt: die Queue ist leer, die Nachricht
        # dreht nicht bis zum Delivery-Limit.
        assert await broker.queue_depth(queue) == 0
