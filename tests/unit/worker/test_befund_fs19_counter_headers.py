"""Nachweis FS-19: Ein nicht ganzzahliger Zaehler-Header laesst die
Nachricht unquittiert.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-19, Quelle MA-6).

Ein manuelles Neu-Publizieren ueber die RabbitMQ-UI setzt Header als
Zeichenketten. ``x-swarmq-retry-count: "1"`` liess den Retry-Pfad an
``retry_count >= max_retries`` scheitern (``TypeError``), ein nicht
numerischer ``x-swarmq-rate-limit-count`` das Rate-Limit-Gate an ``int()``.
Beide Ausnahmen liefen ins ``finally`` der Pipeline: kein Broker-Verb, nur
``delivery_handler_failed``; die Nachricht hing bis ``consumer_timeout``.

KTD15 (Plan 2026-10-14-0748): Der Retry-Zaehler wird streng gelesen -- ein
nicht ganzzahliger Wert zaehlt wie kein Header (wie ``retry_count_of`` im
Ausfuehrungsschutz und wie ``swarmq dlq retry``). Der Rate-Limit-Zaehler
dient nur der Diagnose und wird nachsichtig gelesen.

Testliste:
# [x] IO: Task scheitert mit ``x-swarmq-retry-count: "1"`` -> genau ein
#     Broker-Verb (``retry_later``), kein ``delivery_handler_failed``
# [x] Rate-Limit gesperrt mit ``x-swarmq-rate-limit-count: "abc"`` ->
#     ``retry_later`` mit Zaehler 1, kein ``delivery_handler_failed``
# [x] Rate-Limit mit ``"4"`` zaehlt nachsichtig weiter (5)
# [x] CPU-Executor mit nicht numerischem Retry-Header: Job wird gebaut, Zaehler 0
# [x] Middleware und ``TaskContext.retry_count`` sehen ein ``int`` (0)
# [x] Ganzzahliger Header bleibt: Middleware sieht 2, Retry-Kopie traegt 3
# [x] Review U17: Rate-Limit-Zaehler ``float("inf")`` (OverflowError) -> Zaehler 1
# [x] Review U17: Broker-Zaehler ``float("inf")`` -> Pause wie ohne Zaehler
# [x] Review U17: Retry-Zaehler ``float("inf")`` -> Lock-Generation ``"0:"``
# [x] Review U17: Retry-Zaehler ``True`` (bool) zaehlt wie kein Header
# [x] Review U17: negativer Retry-Zaehler (-2) -> Retry-Kopie traegt 1
"""

from __future__ import annotations

from typing import Any, ClassVar, cast, override
from unittest.mock import AsyncMock, MagicMock

import pytest
from dishka import FromDishka, Provider, Scope, from_context, make_async_container

from swarmq import dishka as di
from swarmq.contracts.coordination import RateLimitDecision
from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope, success_envelope
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.pool import SlotPool
from swarmq.worker.gates.lock import wait_generation
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


class Fs19Fails(Task):
    task_info = TaskInfo(name="fs19_fails", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise RuntimeError("fs19 boom")


class Fs19RateLimited(Task):
    task_info = TaskInfo(name="fs19_rate_limited", queues=["default"], rate_limit="10/m")

    @override
    async def run(self) -> None:
        raise AssertionError("gesperrt, darf nicht laufen")


class Fs19ContextProvider(Provider):
    task_context = from_context(di.TaskContext, scope=Scope.REQUEST)


class Fs19SeesContext(Task):
    task_info = TaskInfo(name="fs19_sees_context", queues=["default"])
    seen: ClassVar[list[Any]] = []

    @override
    async def run(self, task_context: FromDishka[di.TaskContext]) -> None:
        self.seen.append(task_context.retry_count)


class Fs19RecordingMiddleware(Middleware):
    def __init__(self) -> None:
        self.seen: list[Any] = []

    @override
    async def pre_execute(self, message: MessageContext) -> None:
        self.seen.append(message.get("retry_count"))


def _fs19_worker(
    broker: Any, backend: Any, config: Any, *tasks: type[Task], middleware: Middleware | None = None
) -> Worker:
    registry = TaskRegistry()
    for task in tasks or (Fs19Fails,):
        registry.register(task)
    chain = MiddlewareChain([middleware] if middleware is not None else [])
    return Worker(broker=broker, backend=backend, registry=registry, config=config, middleware_chain=chain)


def _fs19_message(name: str, **headers: Any) -> MagicMock:
    return make_message(_default_payload(task_id=f"{name}-id", name=name, args=[], kwargs={}), headers=headers)


def _fs19_broker_verbs(broker: Any) -> list[str]:
    verbs = ("ack", "nack", "redeliver", "retry_later", "dead_letter", "route_via_dlx")
    return [verb for verb in verbs for _ in range(getattr(broker, verb).await_count)]


def _fs19_handler_failures(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry["event"] == "delivery_handler_failed"]


@pytest.mark.befund("FS-19")
async def test_fs19_string_retry_count_still_settles_the_failed_task(
    broker: Any, backend: Any, config: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    worker = _fs19_worker(broker, backend, config)

    await worker._handle_message(_fs19_message("fs19_fails", **{HEADER_RETRY_COUNT: "1"}))

    assert _fs19_handler_failures(swarmq_logs) == []
    assert _fs19_broker_verbs(broker) == ["retry_later"]
    # Streng gelesen (KTD15): "1" zaehlt wie kein Header, die Kopie ist Versuch 1.
    assert broker.retry_later.await_args.kwargs["headers"][HEADER_RETRY_COUNT] == 1


def _fs19_rate_limited_worker(broker: Any, backend: Any, config: Any) -> Worker:
    worker = _fs19_worker(broker, backend, config, Fs19RateLimited)
    worker._rate_limiter = MagicMock()
    worker._rate_limiter.check = AsyncMock(
        return_value=RateLimitDecision(allowed=False, remaining=0, retry_after_ms=1000)
    )
    return worker


@pytest.mark.befund("FS-19")
async def test_fs19_non_numeric_rate_limit_count_still_defers_the_message(
    broker: Any, backend: Any, config: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    worker = _fs19_rate_limited_worker(broker, backend, config)

    await worker._handle_message(_fs19_message("fs19_rate_limited", **{"x-swarmq-rate-limit-count": "abc"}))

    assert _fs19_handler_failures(swarmq_logs) == []
    assert _fs19_broker_verbs(broker) == ["retry_later"]
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-rate-limit-count"] == 1


@pytest.mark.befund("FS-19")
async def test_fs19_middleware_and_task_context_see_an_int_retry_count(broker: Any, backend: Any, config: Any) -> None:
    Fs19SeesContext.seen.clear()
    middleware = Fs19RecordingMiddleware()
    container = make_async_container(Fs19ContextProvider())
    di.setup_dishka(container)
    try:
        worker = _fs19_worker(broker, backend, config, Fs19SeesContext, middleware=middleware)
        await worker._handle_message(_fs19_message("fs19_sees_context", **{HEADER_RETRY_COUNT: "3"}))
    finally:
        await container.close()

    assert middleware.seen == [0]
    assert Fs19SeesContext.seen == [0]
    assert all(type(value) is int for value in (*middleware.seen, *Fs19SeesContext.seen))


class Fs19Plain(Task):
    task_info = TaskInfo(name="fs19_plain", queues=["default"])

    @override
    async def run(self) -> int:
        return 1


class Fs19RecordingPool:
    """Rechenpool ohne Kind: haelt den gebauten Job fest und meldet Erfolg."""

    def __init__(self) -> None:
        self.jobs: list[ComputeJob] = []

    async def run(self, job: ComputeJob, *, use_di: bool) -> ResultEnvelope:
        self.jobs.append(job)
        return success_envelope(job.task_id, 1, slot=0)


@pytest.mark.befund("FS-19")
async def test_fs19_compute_executor_builds_the_job_despite_a_non_numeric_retry_count(
    broker: Any, backend: Any, config: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    worker = _fs19_worker(broker, backend, config, Fs19Plain)
    pool = Fs19RecordingPool()
    worker._executor = ComputeExecutor(worker._gate_deps, cast("SlotPool", pool), worker_id="w1")

    await worker._handle_message(_fs19_message("fs19_plain", **{HEADER_RETRY_COUNT: "abc"}))

    assert _fs19_handler_failures(swarmq_logs) == []
    assert [job.retry_count for job in pool.jobs] == [0]
    assert _fs19_broker_verbs(broker) == ["ack"]


async def test_fs19_numeric_string_rate_limit_count_is_read_leniently(broker: Any, backend: Any, config: Any) -> None:
    """KTD15: der Rate-Limit-Zaehler dient nur der Diagnose und zaehlt ``"4"`` weiter."""
    worker = _fs19_rate_limited_worker(broker, backend, config)

    await worker._handle_message(_fs19_message("fs19_rate_limited", **{"x-swarmq-rate-limit-count": "4"}))

    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-rate-limit-count"] == 5


async def test_fs19_integer_retry_count_keeps_counting(broker: Any, backend: Any, config: Any) -> None:
    """Gegenstueck: ein ganzzahliger Zaehler bleibt -- Middleware sieht 2, die Kopie traegt 3."""
    middleware = Fs19RecordingMiddleware()
    worker = _fs19_worker(broker, backend, config, Fs19Fails, middleware=middleware)

    await worker._handle_message(_fs19_message("fs19_fails", **{HEADER_RETRY_COUNT: 2}))

    assert middleware.seen == [2]
    assert broker.retry_later.await_args.kwargs["headers"][HEADER_RETRY_COUNT] == 3


async def test_fs19_infinite_rate_limit_count_counts_like_no_header(
    broker: Any, backend: Any, config: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    """Review U17: ``int(float("inf"))`` wirft ``OverflowError`` -- zaehlt wie kein Header."""
    worker = _fs19_rate_limited_worker(broker, backend, config)

    await worker._handle_message(_fs19_message("fs19_rate_limited", **{"x-swarmq-rate-limit-count": float("inf")}))

    assert _fs19_handler_failures(swarmq_logs) == []
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-rate-limit-count"] == 1


def test_fs19_infinite_retry_count_gives_the_first_lock_generation() -> None:
    """Review U17: die Lock-Wartegeneration liest ``float("inf")`` wie keinen Header."""
    assert wait_generation({HEADER_RETRY_COUNT: float("inf")}) == "0:"


async def test_fs19_bool_retry_count_counts_like_no_header(broker: Any, backend: Any, config: Any) -> None:
    """Review U17: ``bool`` ist eine ``int``-Unterklasse; ``True`` ist trotzdem
    kein Zaehler -- Middleware sieht 0, die Kopie ist Versuch 1."""
    middleware = Fs19RecordingMiddleware()
    worker = _fs19_worker(broker, backend, config, Fs19Fails, middleware=middleware)

    await worker._handle_message(_fs19_message("fs19_fails", **{HEADER_RETRY_COUNT: True}))

    assert middleware.seen == [0]
    assert type(middleware.seen[0]) is int
    assert broker.retry_later.await_args.kwargs["headers"][HEADER_RETRY_COUNT] == 1


async def test_fs19_negative_retry_count_is_clamped_to_the_first_attempt(
    broker: Any, backend: Any, config: Any
) -> None:
    """Review U17: ein negativer Zaehler zaehlt wie keiner -- die Kopie traegt 1."""
    worker = _fs19_worker(broker, backend, config, Fs19Fails)

    await worker._handle_message(_fs19_message("fs19_fails", **{HEADER_RETRY_COUNT: -2}))

    assert broker.retry_later.await_args.kwargs["headers"][HEADER_RETRY_COUNT] == 1
