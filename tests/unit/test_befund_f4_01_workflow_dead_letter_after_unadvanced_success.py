"""Nachweis F4-01: Ein Dead-Letter nach gespeichertem, aber nicht
weitergeschaltetem Erfolg lässt den Workflow auf ``running``.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-01; Verdacht
R4Z-02; Gegenprüfung ``6-gegenpruefung-Z.md``). Abgeleitet aus dem
geschrumpften Ablauf der Workflow-Maschine
(``tests/property/stateful/workflow_scenarios.py``,
``dead_letter_after_unadvanced_success``), hier ohne Hypothesis mit
``InMemoryBackend`` und der Worker-Naht aus ``ResultTail`` (Ergebnis,
Advance) und ``RetryHandler`` (Dead-Letter, Fail-Mark).

Ablauf: Ein Workflow aus einem Schritt. Die erste Zustellung speichert das
Erfolgsergebnis, das Compare-and-Set des Advance scheitert (Valkey kurz weg),
die Zustellung geht per ``redeliver`` zurück. Die Neuzustellung endet im
Dead-Letter (der zweite Lauf scheitert terminal). ``fail_workflow_step`` sieht
das gespeicherte Erfolgsergebnis und markiert nicht (``_step_succeeded``);
kein Advance läuft mehr, der Workflow bleibt ``running`` bis zur TTL.

Erwartet: E-040 („Ein Workflow läuft nie still in den ``get()``-Timeout“);
die Bewusste Folge von E-040 („Ein Schritt, der schon erfolgreich war,
markiert seinen Workflow bei einem späteren Dead-Letter nicht als
gescheitert“) verbietet nur das Fail-Mark und gilt laut BZ-13/a570112 für
einen weitergelaufenen Workflow; ``workflows.md:232``. Der Nachweis verlangt
nur einen terminalen Status (``completed`` über einen Neu-Advance oder
``failed``), kein Fail-Mark; er widerspricht damit nicht
``test_terminal.py::test_step_without_a_success_result_fails_its_workflow`` (E-040).

Testliste:
# [x] Erfolg gespeichert, Advance scheitert (Neuzustellung), Neuzustellung im Dead-Letter -> Workflow terminal
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Mapping
from typing import Any, cast, override

import pytest

from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery, PublishReceipt
from swarmq.exceptions import BackendConnectionError
from swarmq.middleware import MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, decode_result
from swarmq.signals import SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker.decode import valid_workflow_id
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.results import ResultTail
from swarmq.worker.retry import RetryHandler
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    WorkflowAdvancer,
    WorkflowStepEnqueuer,
    workflow_step_task_id,
)
from tests.support.inmemory import FakeClock, InMemoryBackend, InMemoryBroker

pytestmark = pytest.mark.timeout(5)

_QUEUE = "default"
_STATE_TTL_MS = 86_400_000
_TERMINAL = {STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED}


class F401Step(Task):
    task_info = TaskInfo(name="f4_01_step", queues=[_QUEUE])

    @override
    async def run(self, *args: Any) -> list[Any]:
        return list(args)


class _RecordingBroker(InMemoryBroker):
    """Publiziert in den Speicher, hält Veröffentlichungen und Verben fest."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.published: list[tuple[bytes, dict[str, Any]]] = []
        self.verbs: list[str] = []

    @override
    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        self.published.append((body, dict(headers or {})))
        return await super().publish(queue, body, headers)

    @override
    async def ack(self, delivery: Delivery) -> None:
        self.verbs.append("ack")

    @override
    async def redeliver(self, delivery: Delivery) -> None:
        self.verbs.append("redeliver")

    @override
    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        self.verbs.append("retry_later")

    @override
    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        self.verbs.append("dead_letter")

    @override
    async def route_via_dlx(self, delivery: Delivery) -> None:
        self.verbs.append("route_via_dlx")


class _CasOutage:
    """Backend-Sicht des Workers: das nächste Compare-and-Set des
    Workflow-Zustands scheitert (Valkey kurz weg, nach dem Publish)."""

    def __init__(self, inner: InMemoryBackend) -> None:
        self._inner = inner
        self.outages = 0

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name != "compare_and_store_workflow_state" or not inspect.iscoroutinefunction(attr):
            return attr

        async def call(*args: Any, **kwargs: Any) -> Any:
            if self.outages > 0:
                self.outages -= 1
                raise BackendConnectionError("valkey unreachable (injected)")
            return await attr(*args, **kwargs)

        return call


async def _finalized(_ctx: GateContext, _terminal: bool) -> bool:
    return True


async def _no_unique(_delivery: Any, _ctx: GateContext, _info: TaskInfo | None) -> None:
    return None


@pytest.fixture
async def backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


@pytest.mark.befund("F4-01")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-01: offen - Dead-Letter nach gespeichertem, nicht weitergeschaltetem Erfolg laesst den Workflow running",
)
async def test_dead_letter_after_an_unadvanced_success_does_not_leave_the_workflow_running(
    backend: InMemoryBackend,
) -> None:
    clock = FakeClock()
    broker = _RecordingBroker(clock)
    await broker.connect()
    registry = TaskRegistry()
    registry.register(F401Step)
    signals = SignalDispatcher()
    config = SwarmQConfig(broker_url="amqp://unused", backend_url="valkey://unused")
    client = Client(broker=broker, backend=cast("Any", backend), registry=registry)

    handle = await client.apply(F401Step.s("a1"), state_ttl_ms=_STATE_TTL_MS)
    [(body, headers)] = broker.published
    task_id = str(decode_json(body)["task"]["id"])

    view = _CasOutage(backend)
    advancer = WorkflowAdvancer(
        cast("Any", view),
        cast("Any", backend.chords),
        publisher=WorkflowStepEnqueuer(
            publish=broker.publish, dispatch=signals.dispatch, get_chain=lambda: MiddlewareChain([])
        ),
        resolve_queue=lambda _name: _QUEUE,
        make_task_id=workflow_step_task_id,
        unpack=lambda raw: decode_result(raw)["result"],
        ttl_ms=_STATE_TTL_MS,
    )
    deps = GateDeps(
        broker=cast("Any", broker),
        backend=cast("Any", view),
        config=config,
        signals=signals,
        registry=registry,
        rate_limiter=lambda: None,
        lock_manager=lambda: None,
        finalize_schedfire=_finalized,
        maybe_release_unique=_no_unique,
        extract_valid_workflow_id=valid_workflow_id,
        register_lock=lambda _handler, _entry: None,
    )
    tail = ResultTail(deps, workflow_advancer=lambda: advancer)
    retry = RetryHandler(deps, workflow_advancer=lambda: advancer, finish=tail.finish)
    deps.fail_workflow = retry.fail_workflow
    ctx = GateContext(
        task_id=task_id,
        task_name="f4_01_step",
        queue=_QUEUE,
        task_info=F401Step.task_info,
        task_class=F401Step,
        args=["a1"],
        kwargs={},
        workflow_id=handle.workflow_id,
    )

    # Erste Zustellung: Erfolg gespeichert, der Advance scheitert vorübergehend.
    view.outages = 1
    success = ExecutionResult(
        result_value=["a1"], is_error=False, is_cancelled=False, error_message="", task_exception=None
    )
    await tail.finish(Delivery(body=body, headers=dict(headers), queue=_QUEUE, handle=object()), ctx, success)
    assert broker.verbs == ["redeliver"]

    # Die Neuzustellung endet im Dead-Letter.
    await retry.dead_letter(
        Delivery(body=body, headers=dict(headers), queue=_QUEUE, handle=object()), ctx, "boom", "RuntimeError"
    )
    assert broker.verbs[1:] == ["dead_letter"]

    raw = await backend.get_workflow_state(handle.workflow_id)
    assert raw is not None
    status = decode_json(raw)["status"]
    assert status in _TERMINAL, f"Workflow nach dem Dead-Letter {status!r} (E-040: nie still in den get()-Timeout)"
