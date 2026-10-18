"""Workflow-Maschine (Fehlersuche Runde 4, Plan 2026-10-17-0941, U8; R6-R8, KTD1-KTD3).

Subjekte: der echte ``WorkflowAdvancer`` (``workflow_orchestration.py``) mit
dem ``ChordStore`` der Umgebung (``InMemoryChordStore`` oder ``ChordManager``
über ``ValkeyBackend.chords``) und dem Compare-and-Set des Backends
(``compare_and_store_workflow_state``); der Start läuft über den echten
``Client.apply``, die Worker-Naht über die echten ``CancelGate``,
``ResultTail.finish`` (Erfolg, Fehlerergebnis, Advance-Fehler) und
``RetryHandler.dead_letter`` (terminales Scheitern mit Fail-Mark). Der Broker
ist in beiden Stufen ein ``InMemoryBroker``; er nimmt Publishes an und hält
die Verben (ack, redeliver, dead_letter, retry_later) je Zustellung fest, die
Maschine stellt selbst zu. Eine Queue ``sm-missing`` gibt es nicht: ein
Publish dorthin wirft ``UnroutableMessage`` wie ein RabbitMQ-``basic.return``.

Topologien: zufällig aus Chain, Group und Chord bis Tiefe 3, Blätter sind
``sm_echo.s``/``.si`` (Ergebnis = eigene Argumente, also mit injiziertem
Vorgänger) und selten ``sm_ghost`` (Queue ohne Ziel). Ein Referenz-Auswerter
(``reference``) berechnet das Endergebnis aus der Topologie.

Modell je Workflow: Topologie, Referenz-Ergebnis, Publish-Zähler je Pfad,
Pfade mit erlaubtem Doppel-Publish, Ursachen eines ``failed``, Cancel-Wunsch,
zuletzt beobachteter Status. Dazu offene Zustellungen (``pending``) und
erledigte Schritte (``done``, Erfolg bestätigt). Der Ausführungsschutz
(E-009) wird nachgebildet: eine Kopie eines bestätigt erledigten Schritts
wird verworfen; die Regel ``redeliver_done`` umgeht ihn (Absturz vor dem Ack).

Invarianten (Quelle):

- W1 Nie vor dem letzten Vorgänger: Ein Schritt wird erst publiziert, wenn
  jedes Blatt, von dem er abhängt (frühere Kettenglieder, der ganze
  Chord-Header), ein Erfolgsergebnis gespeichert hat. Für den Chord-Body
  heißt das: nie vor dem letzten Mitglied. Quelle: Plan U8, R12 des
  Workflow-Plans (2026-05-30-001), ``chord_complete.lua``.
- W2 Callback genau einmal: Jeder Pfad wird höchstens einmal publiziert,
  außer im Publish-vor-Persistenz-Fenster (siehe erlaubte Abweichungen).
  Quelle: Plan U8; ``tests/chaos/test_workflow_chord_idempotency.py``.
- W3 publish-then-mark: Jeder Pfad in ``step_tasks`` des gespeicherten
  Zustands wurde vorher erfolgreich publiziert (``mark_enqueued`` nur mit
  ``PublishReceipt``). Ausgenommen ist die Anfangsfront: ``Client.apply``
  speichert den Zustand samt Front bewusst vor dem Publish (R18 des
  Workflow-Plans; scheitert der Publish, markiert apply ``failed``).
  Quelle: AE5, ``WorkflowAdvancer.mark_enqueued``.
- W4 Terminales Scheitern ist synchron: Nach einem Fehlerergebnis oder
  Dead-Letter eines Schritts, der nicht neu zugestellt wurde, ist der
  Workflow terminal. Quelle: E-040.
- W5 Unroutbarer Advance: Wirft ein Folge-Publish ``UnroutableMessage`` und
  wird die Zustellung bestätigt, ist der Workflow ``failed`` (Fehlertyp
  ``UnroutableMessage``); ein unroutbarer Start (``apply``) hinterlässt
  ``failed``. Quelle: E-002.
- W6 FAF beim Bau abgelehnt: ``chain``/``group``/``chord`` mit einem
  FIRE_AND_FORGET-Schritt werfen ``WorkflowValidationError``, nichts wird
  gespeichert oder publiziert. Quelle: E-039.
- W7 Terminal ist endgültig und begründet: ein terminaler Status ändert sich
  nie; ``completed`` nur mit Erfolgsergebnis jedes Blatts, ohne vorherigen
  Cancel-Wunsch und mit dem Referenz-Ergebnis; ``cancelled`` nur nach
  Cancel; ``failed`` nur mit einer Ursache aus W4/W5 und Fehlertyp
  ``TaskError``/``UnroutableMessage``. Endet ein Schritt, der vor dem
  Cancel das Gate passiert hatte, schaltet sein Advance nicht weiter und
  setzt ``cancelled``. Quelle: R13-R17 des Workflow-Plans,
  ``WorkflowHandle.cancel``, ``WorkflowHandle._resolve_terminal``.
- W8 Jede Zustellung endet mit genau einem Broker-Verb. Quelle:
  ``docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md``.
- W9 Lebendigkeit (Regel ``drain``): sind alle Zustellungen abgearbeitet,
  ist jeder Workflow terminal, außer nach ``cancel`` (FS-32).

Erlaubte Abweichungen (KTD3):

- E-040: Scheitert das Fail-Mark vorübergehend (Backend weg, 25 verlorene
  CAS-Runden), wird die Zustellung neu zugestellt statt bestätigt.
- FS-32 (LÜCKE, offen): Nach ``handle.cancel()`` bleibt ``status()``
  ``running``, wenn kein Schritt mehr fertig wird; die Schritte enden im
  Cancel-Gate.
- Doppel-Publish im Publish-vor-Persistenz-Fenster (keine E-ID; dokumentiert
  im Workflow-Plan 2026-05-30-001 Z. 103 und 465, ``chords.py``,
  ``chord_complete.lua``): Publiziert ein Advance und endet seine Zustellung
  danach mit Neuzustellung (WorkflowStateConflict nach 25 Runden), oder
  laufen zwei Advances überlappend (Regel ``concurrent``), darf ein Pfad
  mehrfach publiziert werden. Die Schritt-Kennung ist deterministisch, der
  Ausführungsschutz verwirft die Kopie. Quelle auch Commit 4f8820d
  („at worst a rare idempotent double-enqueue“); R4Z-03 ist danach NICHT
  HALTBAR (Register Runde 4).

Nicht nachgebildet: Ablauf von Ergebnissen und Zustand (TTL 24 h,
``WorkflowResultExpired``), Middleware-Ablehnung, FIRE_AND_FORGET per
``sig()`` zur Laufzeit, die Zähler im Chord-Store (nur über ihre Wirkung, W1
und W2, beobachtet; der Store hat keine Lese-API).

Bekannte Befunde und Entscheidungen (Register
``docs/befunde/fehlersuche-runde-4-2026-10.md``), ausgeklammert an der
auslösenden Regel (KTD3, R8):

- F4-01 (R4Z-02): Ein Dead-Letter nach gespeichertem, nicht
  weitergeschaltetem Erfolg lässt den Workflow ``running``. In
  ``deliver_pending`` wird ``dlq`` nach gespeichertem Erfolg zu ``ok``;
  Nachweis ``test_dead_letter_after_unadvanced_success``.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, override

from hypothesis import strategies as st
from hypothesis.stateful import invariant, precondition, rule

from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery, PublishReceipt
from swarmq.exceptions import BackendConnectionError, UnroutableMessage, WorkflowValidationError
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.middleware import MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, decode_result, encode_json
from swarmq.signals import SignalDispatcher
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker.decode import valid_workflow_id
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.gates.cancel import CancelGate
from swarmq.worker.outcome import Continue
from swarmq.worker.results import ResultTail
from swarmq.worker.retry import RetryHandler
from swarmq.workflow import Chain, Chord, Group, Signature, WorkflowNode, chain, chord, group
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    WorkflowAdvancer,
    WorkflowHandle,
    WorkflowStepEnqueuer,
    initial_frontier,
    workflow_step_task_id,
)
from tests.support.inmemory import FakeClock, InMemoryBroker
from tests.support.stateful import AsyncStateMachine

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from tests.support.inmemory import InMemoryBackend

TERMINAL = frozenset({STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED})
QUEUE = "default"
MISSING_QUEUE = "sm-missing"
STATE_TTL_MS = 86_400_000
MAX_WORKFLOWS = 3
Kind = Literal["ok", "error", "dlq"]
_NO_INJECT: Any = object()


class SmEcho(Task):
    task_info = TaskInfo(name="sm_echo", queues=[QUEUE])

    @override
    async def run(self, *args: Any) -> list[Any]:
        """Läuft nie: die Maschine rechnet das Ergebnis selbst (``list(args)``)."""
        return list(args)


class SmGhost(Task):
    """Ein Schritt, dessen Queue es nicht gibt (Publish wirft ``UnroutableMessage``)."""

    task_info = TaskInfo(name="sm_ghost", queues=[MISSING_QUEUE])

    @override
    async def run(self, *args: Any) -> list[Any]:
        return list(args)


class SmFaf(Task):
    task_info = TaskInfo(name="sm_faf", queues=[QUEUE], delivery=DeliveryMode.FIRE_AND_FORGET)

    @override
    async def run(self, *args: Any) -> None:
        return None


TASKS: dict[str, type[Task]] = {"sm_echo": SmEcho, "sm_ghost": SmGhost, "sm_faf": SmFaf}


class WorkflowEnv(Protocol):
    """Store-Factory der Workflow-Maschine (KTD2)."""

    @property
    def clock(self) -> FakeClock: ...

    @property
    def backend(self) -> InMemoryBackend | ValkeyBackend: ...

    async def advance(self, seconds: float) -> None: ...

    async def close(self) -> None: ...


# -- Topologie ------------------------------------------------------------------

# Spezifikation als Tupel, damit Hypothesis sie schrumpfen und anzeigen kann.
# Blätter sind Paare aus Art (s, si, ghost, faf) und Label; Container sind
# Paare aus chain oder group und Kinderliste; ein Chord trägt zusätzlich den
# Body als drittes Element.
Spec = tuple[Any, ...]


def _leaf() -> st.SearchStrategy[Spec]:
    kinds = st.sampled_from(("s", "s", "s", "s", "si", "s", "s", "s", "s", "s", "s", "ghost"))
    return st.tuples(kinds, st.integers(min_value=0, max_value=9).map(lambda n: f"a{n}"))


def specs(depth: int = 3) -> st.SearchStrategy[Spec]:
    """Topologie bis Tiefe *depth* (Container-Ebenen)."""
    if depth == 0:
        return _leaf()
    inner = specs(depth - 1)
    width = 3 if depth == 3 else 2
    return st.one_of(
        _leaf(),
        st.tuples(st.just("chain"), st.lists(inner, min_size=1, max_size=width)),
        st.tuples(st.just("group"), st.lists(inner, min_size=1, max_size=width)),
        st.tuples(st.just("chord"), st.lists(inner, min_size=1, max_size=width), inner),
    )


def container_specs() -> st.SearchStrategy[Spec]:
    """Topologie mit einem Container an der Wurzel (für E-039)."""
    return specs(3).filter(lambda spec: spec[0] in {"chain", "group", "chord"})


def build(spec: Spec) -> WorkflowNode:
    kind = spec[0]
    if kind == "s":
        return SmEcho.s(spec[1])
    if kind == "si":
        return SmEcho.si(spec[1])
    if kind == "ghost":
        return SmGhost.s(spec[1])
    if kind == "faf":
        return SmFaf.s(spec[1])
    if kind == "chain":
        return chain(*(build(child) for child in spec[1]))
    if kind == "group":
        return group(*(build(child) for child in spec[1]))
    return chord(group(*(build(child) for child in spec[1])), build(spec[2]))


def leaf_count(spec: Spec) -> int:
    if spec[0] in {"chain", "group"}:
        return sum(leaf_count(child) for child in spec[1])
    if spec[0] == "chord":
        return sum(leaf_count(child) for child in spec[1]) + leaf_count(spec[2])
    return 1


def with_faf(spec: Spec, index: int) -> Spec:
    """*spec* mit dem Blatt Nummer *index* (Tiefensuche) als FAF-Schritt."""
    counter = itertools.count()

    def walk(node: Spec) -> Spec:
        if node[0] == "chain" or node[0] == "group":
            return (node[0], [walk(child) for child in node[1]])
        if node[0] == "chord":
            return ("chord", [walk(child) for child in node[1]], walk(node[2]))
        return ("faf", node[1]) if next(counter) == index else node

    return walk(spec)


def reference(node: WorkflowNode, inject: Any = _NO_INJECT) -> Any:
    """Endergebnis laut Topologie, wenn jeder Schritt seine Argumente liefert."""
    if isinstance(node, Signature):
        args = list(node.args)
        if inject is not _NO_INJECT and not node.immutable:
            args = [inject, *args]
        return args
    if isinstance(node, Chain):
        value = reference(node.steps[0], inject)
        for step in node.steps[1:]:
            value = reference(step, value)
        return value
    if isinstance(node, Group):
        return [reference(member, inject) for member in node.members]
    assert isinstance(node, Chord)
    return reference(node.body, reference(node.header, inject))


def leaf_paths(node: WorkflowNode, prefix: str = "") -> list[str]:
    if isinstance(node, Signature):
        return [prefix]
    if isinstance(node, Chain):
        return [p for i, step in enumerate(node.steps) for p in leaf_paths(step, f"{prefix}/{i}")]
    if isinstance(node, Group):
        return [p for i, member in enumerate(node.members) for p in leaf_paths(member, f"{prefix}/g{i}")]
    assert isinstance(node, Chord)
    return [*leaf_paths(node.header, f"{prefix}/h"), *leaf_paths(node.body, f"{prefix}/b")]


def predecessors(root: WorkflowNode, path: str) -> list[str]:
    """Blätter, die fertig sein müssen, bevor der Schritt an *path* läuft (W1)."""
    out: list[str] = []
    node, prefix = root, ""
    for seg in [s for s in path.split("/") if s]:
        if isinstance(node, Chain):
            index = int(seg)
            for j in range(index):
                out.extend(leaf_paths(node.steps[j], f"{prefix}/{j}"))
            node = node.steps[index]
        elif isinstance(node, Group):
            node = node.members[int(seg[1:])]
        else:
            assert isinstance(node, Chord)
            if seg == "b":
                out.extend(leaf_paths(node.header, f"{prefix}/h"))
                node = node.body
            else:
                node = node.header
        prefix = f"{prefix}/{seg}"
    return out


# -- Modell --------------------------------------------------------------------


@dataclass(eq=False)
class Wf:
    seq: int
    spec: Spec
    node: WorkflowNode
    wf_id: str
    handle: WorkflowHandle | None
    expected: Any
    publishes: dict[str, int] = field(default_factory=dict)
    overlap_paths: set[str] = field(default_factory=set)
    fail_causes: list[str] = field(default_factory=list)
    cancel_requested: bool = False
    last_status: str = STATUS_RUNNING
    violations: list[str] = field(default_factory=list)

    @override
    def __repr__(self) -> str:
        return f"W{self.seq}({self.spec!r})"


@dataclass(eq=False)
class Msg:
    seq: int
    wf: Wf
    path: str
    task_id: str
    body: bytes
    headers: dict[str, Any]

    @override
    def __repr__(self) -> str:
        return f"M{self.seq}(W{self.wf.seq}{self.path or '/'})"


@dataclass
class Faults:
    """Eingespielte Störungen, geteilt von allen Sichten auf das Backend."""

    cas_conflicts: int = 0
    state_read_outages: int = 0
    state_write_outages: int = 0


class MachineBroker(InMemoryBroker):
    """``InMemoryBroker`` als Publisher; die Maschine stellt selbst zu und
    hält die Verben je Zustellung fest. Eine Queue ``sm-missing`` gibt es nicht."""

    def __init__(
        self, clock: FakeClock, on_publish: Callable[[str, bytes, dict[str, Any]], Coroutine[Any, Any, None]]
    ) -> None:
        super().__init__(clock)
        self._on_publish = on_publish
        self.verbs: dict[int, list[str]] = {}
        self.unroutable: list[str] = []

    @override
    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        headers = dict(headers or {})
        if queue == MISSING_QUEUE:
            self.unroutable.append(str(headers.get(HEADER_WORKFLOW_ID)))
            raise UnroutableMessage(f"no queue {queue!r} (basic.return)")
        receipt = await super().publish(queue, body, headers)
        await self._on_publish(queue, body, headers)
        return receipt

    def _verb(self, delivery: Delivery, verb: str) -> None:
        self.verbs.setdefault(id(delivery.handle), []).append(verb)

    @override
    async def ack(self, delivery: Delivery) -> None:
        self._verb(delivery, "ack")

    @override
    async def redeliver(self, delivery: Delivery) -> None:
        self._verb(delivery, "redeliver")

    @override
    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        self._verb(delivery, "retry_later")

    @override
    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        self._verb(delivery, "dead_letter")

    @override
    async def route_via_dlx(self, delivery: Delivery) -> None:
        self._verb(delivery, "route_via_dlx")


class _ChordView:
    def __init__(self, inner: Any, view: BackendView) -> None:
        self._inner = inner
        self._view = view

    async def record_member(self, wf_id: str, chord_id: str, member_id: str, *, expected: int, ttl_ms: int) -> bool:
        await self._view.pause()
        return bool(await self._inner.record_member(wf_id, chord_id, member_id, expected=expected, ttl_ms=ttl_ms))


class BackendView:
    """Sicht eines Worker-Prozesses auf das Backend: reicht durch, spielt
    Störungen ein (``Faults``) und gibt vor jedem Aufruf den Loop so oft ab,
    wie ``yields`` vorgibt (Verschränkung zweier Advancer, Regel ``concurrent``)."""

    def __init__(self, inner: Any, faults: Faults, yields: Sequence[int] = ()) -> None:
        self._inner = inner
        self._faults = faults
        self._yields: Iterator[int] = iter(yields)
        self.chords = _ChordView(inner.chords, self)

    async def pause(self) -> None:
        for _ in range(next(self._yields, 0)):
            await asyncio.sleep(0)

    async def _bump(self, wf_id: str) -> None:
        """Ein nebenläufiger Schreiber: gleicher Inhalt, Version + 1."""
        raw = await self._inner.get_workflow_state(wf_id)
        if raw is None:
            return
        doc = decode_json(raw)
        version = int(doc.get("version", 0))
        doc["version"] = version + 1
        await self._inner.compare_and_store_workflow_state(
            wf_id, encode_json(doc), STATE_TTL_MS, expected_version=version
        )

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not inspect.iscoroutinefunction(attr):
            return attr

        async def call(*args: Any, **kwargs: Any) -> Any:
            await self.pause()
            if name == "get_workflow_state" and self._faults.state_read_outages > 0:
                self._faults.state_read_outages -= 1
                raise BackendConnectionError("valkey unreachable (injected)")
            if name == "compare_and_store_workflow_state" and self._faults.state_write_outages > 0:
                self._faults.state_write_outages -= 1
                raise BackendConnectionError("valkey unreachable (injected)")
            if name == "compare_and_store_workflow_state" and self._faults.cas_conflicts > 0:
                self._faults.cas_conflicts -= 1
                await self._bump(args[0])
            return await attr(*args, **kwargs)

        return call


class Worker:
    """Die Worker-Naht eines Prozesses: Cancel-Gate, Ergebnis-Tail,
    Dead-Letter mit Fail-Mark, ein eigener Advancer."""

    def __init__(self, machine: WorkflowMachine, view: BackendView) -> None:
        self.view = view
        backend = cast("Any", view)
        self.advancer = WorkflowAdvancer(
            backend,
            cast("Any", view.chords),
            publisher=WorkflowStepEnqueuer(
                publish=machine.broker.publish,
                dispatch=machine.signals.dispatch,
                get_chain=lambda: MiddlewareChain([]),
            ),
            resolve_queue=lambda name: TASKS[name].task_info.queues[0],
            make_task_id=workflow_step_task_id,
            unpack=lambda raw: decode_result(raw)["result"],
            ttl_ms=STATE_TTL_MS,
        )
        self.deps = GateDeps(
            broker=cast("Any", machine.broker),
            backend=backend,
            config=machine.config,
            signals=machine.signals,
            registry=machine.registry,
            rate_limiter=lambda: None,
            lock_manager=lambda: None,
            finalize_schedfire=_finalized,
            maybe_release_unique=_no_unique,
            extract_valid_workflow_id=valid_workflow_id,
            register_lock=lambda _handler, _entry: None,
        )
        self.tail = ResultTail(self.deps, workflow_advancer=lambda: self.advancer)
        self.retry = RetryHandler(self.deps, workflow_advancer=lambda: self.advancer, finish=self.tail.finish)
        self.deps.fail_workflow = self.retry.fail_workflow
        self.gate = CancelGate(self.deps)

    async def process(
        self, msg: Msg, kind: Kind, midway: Callable[[], Awaitable[object]] | None = None
    ) -> tuple[list[str], bool]:
        """Eine Zustellung; Rückgabe: Verben und ob das Cancel-Gate sie beendete.
        *midway* läuft nach dem Cancel-Gate, während der Task „läuft“."""
        delivery = Delivery(body=msg.body, headers=dict(msg.headers), queue=QUEUE, handle=object())
        payload = decode_json(msg.body)
        name = payload["task"]["name"]
        ctx = GateContext(
            task_id=msg.task_id,
            task_name=name,
            queue=QUEUE,
            task_info=TASKS[name].task_info,
            task_class=TASKS[name],
            args=list(payload["args"]),
            kwargs=dict(payload["kwargs"]),
            workflow_id=msg.wf.wf_id,
        )
        broker = cast("MachineBroker", self.deps.broker)
        try:
            outcome = await self.gate.apply(delivery, ctx)
            if outcome is not Continue:
                return broker.verbs.pop(id(delivery.handle), []), True
            if midway is not None:
                await midway()
            if kind == "dlq":
                await self.retry.dead_letter(delivery, ctx, "boom", "RuntimeError")
            else:
                await self.tail.finish(delivery, ctx, _execution(list(payload["args"]), failed=kind == "error"))
            return broker.verbs.pop(id(delivery.handle), []), False
        finally:
            broker.verbs.pop(id(delivery.handle), None)


async def _finalized(_ctx: GateContext, _terminal: bool) -> bool:
    return True


async def _no_unique(_delivery: Any, _ctx: GateContext, _info: TaskInfo | None) -> None:
    return None


def _execution(args: list[Any], *, failed: bool) -> ExecutionResult:
    if failed:
        return ExecutionResult(
            result_value=None, is_error=True, is_cancelled=False, error_message="boom", task_exception=None
        )
    return ExecutionResult(result_value=args, is_error=False, is_cancelled=False, error_message="", task_exception=None)


# -- Maschine ------------------------------------------------------------------


class WorkflowMachine(AsyncStateMachine[WorkflowEnv]):
    # Die Valkey-Stufe sperrt ``concurrent_advance``: dort ändert die echte
    # Antwortreihenfolge die erlaubten Regeln (``FlakyStrategyDefinition``).
    allow_concurrent = True

    def __init__(self, open_env: Callable[[], Coroutine[Any, Any, WorkflowEnv]]) -> None:
        super().__init__(open_env)
        try:
            self.wfs: list[Wf] = []
            self.pending: list[Msg] = []
            self.done: list[Msg] = []
            self.done_ids: set[str] = set()
            self.discarded = 0
            self.faults = Faults()
            self._seq = itertools.count(1)
            self._overlap = False
            self._applying: Wf | None = None
            self._unroutable_now: set[str] = set()
            self.signals = SignalDispatcher()
            self.config = SwarmQConfig(broker_url="amqp://unused", backend_url="valkey://unused")
            self.registry = TaskRegistry()
            for task in TASKS.values():
                self.registry.register(task)
            self.broker = MachineBroker(self.env.clock, self._on_publish)
            self.run(self.broker.connect())
            self.client = Client(broker=self.broker, backend=cast("Any", self.env.backend), registry=self.registry)
        except BaseException:
            self.teardown()
            raise

    # -- Hilfen ----------------------------------------------------------------

    def wf_of(self, wf_id: str) -> Wf | None:
        return next((wf for wf in self.wfs if wf.wf_id == wf_id), None)

    async def _on_publish(self, _queue: str, body: bytes, headers: dict[str, Any]) -> None:
        """Jeder erfolgreiche Publish: W1 prüfen, Zähler, Zustellung anlegen."""
        wf_id = str(headers.get(HEADER_WORKFLOW_ID))
        path = str(headers.get(HEADER_WORKFLOW_STEP))
        wf = self.wf_of(wf_id)
        if wf is None:  # während apply(): die Maschine kennt den Workflow noch nicht
            wf = self._applying
            assert wf is not None, f"Publish für unbekannten Workflow {wf_id}"
        backend = self.env.backend
        for leaf in predecessors(wf.node, path):
            raw = await backend.get_result(workflow_step_task_id(wf_id, leaf))
            ok = (
                raw is not None
                and not decode_result(raw).get("is_error")
                and not decode_result(raw).get("is_cancelled")
            )
            if not ok:
                wf.violations.append(f"{path or '/'} publiziert vor dem Erfolg von {leaf} (W1)")
        wf.publishes[path] = wf.publishes.get(path, 0) + 1
        if self._overlap:
            wf.overlap_paths.add(path)
        task_id = str(decode_json(body)["task"]["id"])
        self.pending.append(Msg(next(self._seq), wf, path, task_id, body, dict(headers)))

    def state_of(self, wf: Wf) -> dict[str, Any] | None:
        raw = self.run(self.env.backend.get_workflow_state(wf.wf_id))
        return None if raw is None else cast("dict[str, Any]", decode_json(raw))

    def status_of(self, wf: Wf) -> str:
        state = self.state_of(wf)
        assert state is not None, f"{wf}: Zustand fehlt"
        return str(state["status"])

    def worker(self, yields: Sequence[int] = ()) -> Worker:
        return Worker(self, BackendView(self.env.backend, self.faults, yields))

    # -- Bau -------------------------------------------------------------------

    def start(self, spec: Spec) -> Wf:
        node = build(spec)
        seq = next(self._seq)
        wf = Wf(seq, spec, node, wf_id="", handle=None, expected=reference(node))
        self._applying = wf
        unroutable = len(self.broker.unroutable)
        try:
            handle = self.run(self.client.apply(cast("Any", node), state_ttl_ms=STATE_TTL_MS))
        except UnroutableMessage:
            handle = None
        finally:
            self._applying = None
        if handle is None:
            # E-002 beim Start: der Zustand liegt schon, apply() markiert ihn failed.
            wf.wf_id = self.broker.unroutable[-1]
            wf.fail_causes.append("apply unroutable")
        else:
            assert len(self.broker.unroutable) == unroutable
            wf.wf_id = handle.workflow_id
            wf.handle = handle
        for msg in self.pending:
            if msg.wf is wf:
                assert msg.headers[HEADER_WORKFLOW_ID] == wf.wf_id
        self.wfs.append(wf)
        if handle is None:
            status = self.status_of(wf)
            assert status == STATUS_FAILED, f"{wf}: unroutbarer Start hinterlässt {status} (W5, E-002)"
        return wf

    @precondition(lambda self: len(self.wfs) < MAX_WORKFLOWS)
    @rule(spec=specs())
    def start_workflow(self, spec: Spec) -> None:
        self.start(spec)

    def start_with_faf(self, spec: Spec, index: int) -> None:
        """E-039: ein FAF-Schritt im Container wird beim Bau abgelehnt."""
        published = len(self.pending)
        try:
            build(with_faf(spec, index))
        except WorkflowValidationError:
            pass
        else:
            raise AssertionError(f"FAF-Schritt in {spec!r} beim Bau nicht abgelehnt (W6, E-039)")
        assert len(self.pending) == published, "abgelehnter Bau hat publiziert (W6)"

    @rule(spec=container_specs(), data=st.data())
    def start_workflow_with_faf(self, spec: Spec, data: st.DataObject) -> None:
        index = data.draw(st.integers(min_value=0, max_value=leaf_count(spec) - 1), label="faf_leaf")
        self.start_with_faf(spec, index)

    # -- Zustellung ------------------------------------------------------------

    def _settle(self, msg: Msg, kind: Kind, verbs: list[str], gate: bool, status_before: str) -> None:
        """Verben auswerten, Modell nachziehen, W4/W5/W8 prüfen."""
        assert len(verbs) == 1, f"{msg}: Verben {verbs} statt genau einem (W8)"
        verb = verbs[0]
        if msg in self.pending:
            self.pending.remove(msg)
        if verb in {"redeliver", "retry_later"}:
            self.pending.append(msg)
            return
        assert verb in {"ack", "dead_letter"}, f"{msg}: unerwartetes Verb {verb}"
        if gate:
            return
        wf = msg.wf
        if kind == "ok":
            if msg.task_id not in self.done_ids:
                self.done.append(msg)
                self.done_ids.add(msg.task_id)
            if wf.wf_id in self._unroutable_now:
                wf.fail_causes.append(f"advance unroutable at {msg.path or '/'}")
                status = self.status_of(wf)
                assert status in TERMINAL, f"{wf}: bestätigter unroutbarer Advance, Status {status} (W5, E-002)"
            return
        if status_before == STATUS_RUNNING:
            wf.fail_causes.append(f"{kind} at {msg.path or '/'}")
        status = self.status_of(wf)
        assert status in TERMINAL, f"{wf}: {kind} von {msg} bestätigt, Status {status} (W4, E-040)"

    def succeeded(self, msg: Msg) -> bool:
        """Liegt schon ein Erfolgsergebnis des Schritts (früherer Lauf)?"""
        raw = self.run(self.env.backend.get_result(msg.task_id))
        if raw is None:
            return False
        result = decode_result(raw)
        return not result.get("is_error") and not result.get("is_cancelled")

    def deliver(self, msg: Msg, kind: Kind = "ok") -> None:
        if msg.task_id in self.done_ids:
            # Ausführungsschutz (E-009): eine Kopie eines erledigten Schritts wird verworfen.
            self.pending.remove(msg)
            self.discarded += 1
            return
        self._attempt(msg, kind)

    def _attempt(self, msg: Msg, kind: Kind, midway: Callable[[], Awaitable[object]] | None = None) -> None:
        status_before = self.status_of(msg.wf)
        unroutable = len(self.broker.unroutable)
        published = {wf.wf_id: dict(wf.publishes) for wf in self.wfs}
        verbs, gate = self.run(self.worker().process(msg, kind, midway))
        self._unroutable_now = set(self.broker.unroutable[unroutable:])
        if verbs and verbs[0] in {"redeliver", "retry_later"}:
            # Publish-vor-Persistenz-Fenster: was diese Zustellung publiziert
            # hat, darf ihre Neuzustellung noch einmal publizieren.
            for wf in self.wfs:
                for path, count in wf.publishes.items():
                    if count > published.get(wf.wf_id, {}).get(path, 0):
                        wf.overlap_paths.add(path)
        self._settle(msg, kind, verbs, gate, status_before)

    @precondition(lambda self: bool(self.pending))
    @rule(data=st.data(), kind=st.sampled_from(("ok", "ok", "ok", "ok", "ok", "ok", "error", "dlq")))
    def deliver_pending(self, data: st.DataObject, kind: Kind) -> None:
        msg = data.draw(st.sampled_from(self.pending), label="msg")
        if kind == "dlq" and self.succeeded(msg):
            # F4-01 (R4Z-02): ein Dead-Letter nach gespeichertem Erfolg
            # markiert nicht (E-040, Bewusste Folgen) -- auch dann nicht, wenn
            # der Advance dieses Erfolgs nie durchlief; der Workflow hängt.
            # Nachweis ``test_dead_letter_after_unadvanced_success``.
            kind = "ok"
        self.deliver(msg, kind)

    def redeliver(self, msg: Msg) -> None:
        """Absturz vor dem Ack: der erledigte Schritt läuft erneut, gleiches Ergebnis."""
        copy = Msg(next(self._seq), msg.wf, msg.path, msg.task_id, msg.body, dict(msg.headers))
        self._attempt(copy, "ok")

    @precondition(lambda self: bool(self.done))
    @rule(data=st.data())
    def redeliver_done(self, data: st.DataObject) -> None:
        self.redeliver(data.draw(st.sampled_from(self.done), label="msg"))

    def concurrent(self, first: Msg, second: Msg, yields: tuple[list[int], list[int]]) -> None:
        """Zwei Worker zugleich, Verschränkung an jedem Backend-Aufruf."""
        msgs = (first, second)
        before = {msg.seq: self.status_of(msg.wf) for msg in msgs}

        async def both() -> list[tuple[list[str], bool]]:
            workers = [self.worker(y) for y in yields]
            return list(await asyncio.gather(*(w.process(m, "ok") for w, m in zip(workers, msgs, strict=True))))

        unroutable = len(self.broker.unroutable)
        self._overlap = True
        try:
            results = self.run(both())
        finally:
            self._overlap = False
        # Welche der beiden Zustellungen unroutbar advancte, ist nicht
        # zuzuordnen: W5 nur über die Ursache in W7, nicht synchron.
        self._unroutable_now = set()
        for wf in self.wfs:
            if wf.wf_id in self.broker.unroutable[unroutable:]:
                wf.fail_causes.append("advance unroutable (concurrent)")
        for msg, (verbs, gate) in zip(msgs, results, strict=True):
            if msg is second and second is first:
                assert len(verbs) == 1, f"{msg}: Verben {verbs} statt genau einem (W8)"
                if msg not in self.pending:
                    continue  # die erste Zustellung hat schon bestätigt
                if verbs[0] in {"redeliver", "retry_later"}:
                    continue  # schon wieder eingereiht
            self._settle(msg, "ok", verbs, gate, before[msg.seq])

    @precondition(lambda self: self.allow_concurrent and (bool(self.done) or bool(self.pending)))
    @rule(data=st.data())
    def concurrent_advance(self, data: st.DataObject) -> None:
        candidates = [*self.pending, *self.done]
        first = data.draw(st.sampled_from(candidates), label="first")
        second = data.draw(st.sampled_from(candidates), label="second")
        lanes = st.lists(st.integers(min_value=0, max_value=2), max_size=12)
        yields = (data.draw(lanes, label="yields_first"), data.draw(lanes, label="yields_second"))
        if first in self.done:
            first = Msg(next(self._seq), first.wf, first.path, first.task_id, first.body, dict(first.headers))
        if second in self.done:
            second = Msg(next(self._seq), second.wf, second.path, second.task_id, second.body, dict(second.headers))
        self.concurrent(first, second, yields)

    # -- Cancel und Störungen --------------------------------------------------

    def cancel(self, wf: Wf) -> None:
        assert wf.handle is not None
        self.run(wf.handle.cancel())
        wf.cancel_requested = True
        status = self.run(wf.handle.status())
        # FS-32 (LÜCKE): bleibt running, bis ein Schritt fertig wird.
        assert status in {STATUS_RUNNING, *TERMINAL}, status

    def cancel_midway(self, msg: Msg) -> None:
        """``handle.cancel()``, während der Schritt läuft (nach dem Cancel-Gate):
        der Schritt läuft zu Ende, sein Advance schaltet nicht weiter und setzt
        ``cancelled`` (``WorkflowHandle.cancel``, R17 des Workflow-Plans)."""
        handle = msg.wf.handle
        assert handle is not None
        msg.wf.cancel_requested = True
        status_before = self.status_of(msg.wf)
        published = sum(msg.wf.publishes.values())
        self._attempt(msg, "ok", handle.cancel)
        if status_before == STATUS_RUNNING and msg in self.done:
            status = self.status_of(msg.wf)
            assert status == STATUS_CANCELLED, f"{msg.wf}: Schritt nach cancel fertig, Status {status} (W7)"
            assert sum(msg.wf.publishes.values()) == published, f"{msg.wf}: nach cancel weitergeschaltet (W7)"

    @precondition(lambda self: any(msg.wf.handle is not None for msg in self.pending))
    @rule(data=st.data())
    def cancel_while_running(self, data: st.DataObject) -> None:
        candidates = [msg for msg in self.pending if msg.wf.handle is not None]
        msg = data.draw(st.sampled_from(candidates), label="msg")
        if msg.task_id in self.done_ids:
            self.deliver(msg)  # Kopie eines erledigten Schritts: der Ausführungsschutz verwirft sie
            return
        self.cancel_midway(msg)

    @precondition(lambda self: any(wf.handle is not None for wf in self.wfs))
    @rule(data=st.data())
    def cancel_workflow(self, data: st.DataObject) -> None:
        self.cancel(data.draw(st.sampled_from([wf for wf in self.wfs if wf.handle is not None]), label="wf"))

    @rule(rounds=st.sampled_from((1, 2, 24, 25)))
    def cas_conflict(self, rounds: int) -> None:
        """Die nächsten *rounds* Compare-and-Sets verlieren gegen einen Schreiber."""
        self.faults.cas_conflicts = rounds

    @rule(reads=st.sampled_from((1, 2)))
    def backend_outage(self, reads: int) -> None:
        """Die nächsten *reads* Lesungen des Workflow-Zustands scheitern."""
        self.faults.state_read_outages = reads

    @rule()
    def backend_write_outage(self) -> None:
        """Das nächste Compare-and-Set scheitert (Valkey kurz weg, nach dem Publish)."""
        self.faults.state_write_outages = 1

    def drain(self) -> None:
        """Alles zustellen (Erfolg), dann W9."""
        for _ in range(2_000):
            if not self.pending:
                break
            self.deliver(self.pending[0])
        assert not self.pending, f"Zustellungen enden nicht: {self.pending[:5]}"
        for wf in self.wfs:
            status = self.status_of(wf)
            if status == STATUS_RUNNING:
                assert wf.cancel_requested, f"{wf}: hängt in running ohne Cancel (W9)"
                wf.last_status = status  # FS-32 (LÜCKE): erlaubt

    @rule()
    def drain_all(self) -> None:
        self.drain()

    # -- Invarianten -----------------------------------------------------------

    @invariant()
    def published_after_predecessors(self) -> None:
        """W1."""
        for wf in self.wfs:
            assert not wf.violations, f"{wf}: {wf.violations}"

    @invariant()
    def published_at_most_once(self) -> None:
        """W2."""
        for wf in self.wfs:
            for path, count in wf.publishes.items():
                if path not in wf.overlap_paths:
                    assert count == 1, f"{wf}: {path or '/'} {count}-mal publiziert (W2)"

    @invariant()
    def marked_only_after_publish(self) -> None:
        """W3."""
        for wf in self.wfs:
            state = self.state_of(wf)
            assert state is not None, f"{wf}: Zustand fehlt"
            # apply() trägt die Anfangsfront vor dem Publish ein (Zustand vor
            # Front, damit ein schneller erster Schritt die Topologie findet).
            seeded = {path for path, _sig in initial_frontier(wf.node)}
            unpublished = set(state["step_tasks"]) - set(wf.publishes) - seeded
            assert not unpublished, f"{wf}: markiert ohne Publish {sorted(unpublished)} (W3)"
            for path, task_id in state["step_tasks"].items():
                assert task_id == workflow_step_task_id(wf.wf_id, path), f"{wf}: {path} -> {task_id}"

    @invariant()
    def terminal_status_is_final_and_explained(self) -> None:
        """W7."""
        for wf in self.wfs:
            state = self.state_of(wf)
            assert state is not None, f"{wf}: Zustand fehlt"
            status = str(state["status"])
            if wf.last_status in TERMINAL:
                assert status == wf.last_status, f"{wf}: {wf.last_status} -> {status} (W7)"
                continue
            if status == STATUS_COMPLETED:
                assert not wf.cancel_requested, f"{wf}: completed nach cancel (W7)"
                for leaf in leaf_paths(wf.node):
                    raw = self.run(self.env.backend.get_result(workflow_step_task_id(wf.wf_id, leaf)))
                    assert raw is not None, f"{wf}: completed ohne Ergebnis von {leaf} (W7)"
                    assert not decode_result(raw).get("is_error"), f"{wf}: completed trotz Fehler in {leaf} (W7)"
                final_id = state["final_result_task_id"]
                raw = self.run(self.env.backend.get_result(final_id)) if final_id is not None else None
                assert raw is not None, f"{wf}: completed ohne Endergebnis (W7)"
                value = decode_result(raw)["result"]
                assert value == wf.expected, f"{wf}: Endergebnis {value!r}, Referenz {wf.expected!r} (W7)"
            elif status == STATUS_CANCELLED:
                assert wf.cancel_requested, f"{wf}: cancelled ohne cancel (W7)"
            elif status == STATUS_FAILED:
                assert wf.fail_causes, f"{wf}: failed ohne Ursache, error={state['error']} (W7)"
                error = cast("dict[str, Any]", state["error"] or {})
                error_type = error.get("type")
                assert error_type in {"TaskError", "UnroutableMessage"}, f"{wf}: failed mit {state['error']} (W7)"
            wf.last_status = status

    def verify(self) -> None:
        """Alle Invarianten auf einmal (für deterministische Abläufe)."""
        self.published_after_predecessors()
        self.published_at_most_once()
        self.marked_only_after_publish()
        self.terminal_status_is_final_and_explained()

    # -- Abbau -----------------------------------------------------------------

    @override
    async def close_subjects(self) -> None:
        broker = getattr(self, "broker", None)
        if broker is not None:
            await broker.close()


__all__ = ["Msg", "Wf", "WorkflowEnv", "WorkflowMachine", "build", "specs"]
