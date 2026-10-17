"""Retry- und DLQ-Ketten-Maschine (Fehlersuche Runde 4, Plan 2026-10-17-0941,
U9; R6-R8, KTD1-KTD3).

Subjekt: die echte Worker-Pipeline über ``Worker._handle_message``. Sie
verdrahtet genau die Klassen aus dem Plan: ``GateDeps``, das
``DeliveryCountGate``, den ``ExecutionGuard`` (``retry_count_of``, Claim,
Finish über den ``GuardedBroker``), das ``RateLimitGate`` und den
``RetryHandler`` (``evaluate``, ``dead_letter``). Die Stores kommen aus der
Umgebung (KTD2): ``InMemoryExecutionGuardStore``/``InMemoryRateLimitStore``
oder ``ValkeyExecutionGuardStore``/``ValkeyRateLimitStore``
(``guard_claim.lua``, ``guard_finish.lua``, ``sliding_window_counter.lua``).
Der Broker ist in beiden Stufen ein ``InMemoryBroker`` (``ChainBroker``, der
die Verben je Zustellung festhält), die Kette Retry -> Verzögerung -> DLQ ->
Replay liegt in ihm.

Drei Worker teilen Broker und Stores: ``io`` (IO-Modus, echte Soft- und
Hard-Timeouts mit 5 ms), ``cpu`` (CPU-Modus mit einem Pool ohne Kind, der
``ChildDied`` wirft) und ``doomed`` (stirbt in der Ausführung: ab dem
Tod erreicht keiner seiner Aufrufe Broker oder Stores mehr; danach stellt der
Broker die Nachricht mit erhöhten Zählern neu zu, wie eine Quorum-Queue nach
Kanalverlust).

Modell: Ausführungszustand je Identität (``done``, ``sup``, Lease eines
toten Halters, Waisen, Running-Marker bis), Rate-Limit-Zähler je Fenster
(dieselbe Formel wie ``sliding_window_counter.lua``), abgeschlossene Läufe je
(Identität, Retry-Zähler), DLQ-Kopien je Identität. Für jede Zustellung sagt
das Modell das Vertragsverb samt Kopie voraus (``predict``); die Regel
vergleicht mit dem, was der Broker gesehen hat, und schreibt erst dann den
Modellzustand fort.

Invarianten (Quelle):

- K1 Genau ein Vertragsverb je Zustellung (``ack``, ``redeliver``,
  ``retry_later``, ``dead_letter``), kein Verb nach einem Absturz.
  Quelle: ``docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md``
  Abschnitt 1.
- K2 Das Verb und seine Kopie stimmen mit dem Modell: Zustelllimit vor dem
  Ausführungsschutz (``gates/delivery_count.py``), ``done``/``superseded`` ->
  bestätigt ohne Lauf, ``running`` -> Zurückstellung ohne Retry-Verbrauch,
  Waisengrenze -> DLQ (E-046 Nachtrag F3-27), Rate-Limit -> Rückstellung
  mit ``retry_after_ms`` (E-070), Fehler -> Retry oder DLQ.
- K3 Hard-Timeout und Kindtod nehmen den Retry-Pfad (Retry-Kopie oder DLQ),
  nie ``redeliver``. Quelle: E-037.
- K4 ``Retry(delay)`` zählt gegen ``max_retries``, eine negative Wartezeit
  wird 0, bei erschöpftem Budget DLQ. Quelle: E-060.
- K5 Der Retry-Zähler wird streng gelesen: ``"abc"``, ``"3"``, ``True``,
  ``2.5`` und negative Werte zählen wie kein Header. Quelle: E-051.
- K6 ``fixed`` wartet genau ``min(base, retry_max_delay)`` ohne Jitter;
  ``linear``/``exponential`` liegen in ``[nominal, min(2 nominal, max)]`` oder
  am Deckel in ``[0,6 max, 0,8 max]``. Quelle: E-023, ``backoff.py``.
- K7 Nach ``max_retries`` landet genau eine Kopie in der DLQ, mit
  Fehlerergebnis im Backend (auch wenn danach der DLQ-Publish scheitert).
  Quelle: E-031. Je Identität höchstens eine DLQ-Kopie aus einem Pfad hinter
  dem Ausführungsschutz.
- K8 Keine Kopie, die swarmq publiziert (Retry, Zurückstellung, DLQ-Stempel,
  ``dlq retry``), trägt ``x-delivery-count``/``x-acquired-count``.
  Quelle: E-054.
- K9 Ein Replay ist eine neue Identität und läuft; eine Zustellung mit
  derselben Identität (zweites ``dlq retry`` derselben XQ-Nachricht,
  doppelte Kopie, UI-Move ohne Replay-ID) ist ein Duplikat. Je (Identität,
  Retry-Zähler) höchstens ein abgeschlossener Lauf. Quelle: E-009.
- K10 Geht beim Bestätigen der Kanal verloren (``ack`` scheitert bzw.
  ``SettleFailedAfterPublish`` nach der Kopie), setzt der Ausführungsschutz
  trotzdem Done bzw. Superseded; die Neuzustellung des Originals (Broker-
  Zähler + 1) ist ein Duplikat. Quelle: E-043, E-009,
  ``worker/guarded_broker.py`` (Docstrings von ``ack`` und ``retry_later``).

Erlaubte Abweichungen (KTD3):

- E-038: Recycling verbraucht keinen Retry. Nicht nachgebildet: das
  Recycling wartet ohne Abbruch, die Kette sieht davon nichts.
- E-051: Ein UI-Replay mit ``x-swarmq-retry-count: "3"`` bekommt das volle
  Budget (Regel ``replay`` mit ``ui_move_str``).
- E-041: Scheitert der Retry- oder DLQ-Publish, wird das Original neu
  zugestellt; es läuft erneut, beim DLQ-Pfad liegt das Fehlerergebnis schon
  im Backend. Zählt nicht als abgeschlossener Lauf (K9).
- E-046/E-010: Nach einem Absturz läuft dieselbe Ausführung erneut, sobald
  der Running-Marker abgelaufen ist (Waise). Zustands-TTLs sind auf 1 d
  gesetzt; eine Beispielkette dauert höchstens 45 min, Marker laufen also
  nie ab (E-010 nicht nachgebildet).
- E-009 (Bewusste Folgen): Ein UI-Move ohne Replay-ID (0.11-Altbestand)
  wird als Duplikat verworfen.
- Ein Zustelllimit-Dead-Letter liegt vor dem Ausführungsschutz und setzt
  keinen Done-Marker (``gates/delivery_count.py``, kein Claim); eine weitere
  Kopie derselben Identität läuft danach. Keine E-ID, Ablauf laut Code.

Nicht nachgebildet: Locks (Lock-Maschine U6), ``max_age``, Workflows,
Unique-Flags, Drain-Abbruch (E-001), Backend-Ausfall.

Verdachte (Bericht ``1-zustandsmodelle.md``, Abschnitt U9):

- R4Z-04 verletzt K10: Am Zustelllimit schickt das Delivery-Count-Gate, das
  vor dem Ausführungsschutz liegt, die Neuzustellung einer abgeschlossenen
  oder überholten Ausführung in die DLQ und speichert ein Fehlerergebnis.
  Ausgeklammert an ``deliver_one`` über ``deliverable`` und die
  Vorbedingung (Kommentar ``F4-offen (R4Z-04)``); Nachweis ``r4z_04_*`` in
  ``retry_chain_scenarios.py``.
- R4Z-05 ist kein Widerspruch zum Modell, sondern zur Erwartung des Plans:
  Das Modell folgt dem Code (eine per UI mit Text-Zähler neu publizierte
  Retry-Kopie gilt als überholt und wird still bestätigt); Nachweis
  ``r4z_05_*``.

Grenzen gegenüber dem Plan: ``Worker._handle_message`` und das Setzen von
``_rate_limiter``/``_guard_store``/``_executor`` sind privat; sie sind der
etablierte Einstieg der Unit-Tests (z. B.
``tests/unit/worker/test_befund_fs19_counter_headers.py``), ``start()``
brächte Scheduler, Heartbeats und echte Wartezeiten mit.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import math
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, override

from hypothesis import strategies as st
from hypothesis.stateful import initialize, invariant, precondition, rule

from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, NoRetry, Retry, SettleFailedAfterPublish
from swarmq.headers import (
    BROKER_DELIVERY_COUNTERS,
    HEADER_DEAD_LETTER_TTL_MS,
    HEADER_DEAD_LETTERED_AT,
    HEADER_DLQ_GENERATION,
    HEADER_GUARD_DEFER_COUNT,
    HEADER_REPLAY_ID,
    HEADER_RETRY_COUNT,
    HEADER_XDEATH_BASE,
    LOOP_STATE_HEADERS,
)
from swarmq.identity import dead_letter_generation
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.pool import ChildDied, SlotPool
from tests.support.inmemory import FakeClock, InMemoryBroker
from tests.support.stateful import AsyncStateMachine

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.contracts.coordination import ExecutionGuardStore, RateLimitStore
    from swarmq.worker.gates import WorkerBackend, WorkerBroker
    from tests.support.inmemory import InMemoryBackend

QUEUE = "default"
TASK_NAME = "stateful_retry_chain"
# Echte Fristen: der Watchdog bzw. ``wait_for`` laufen real ab (5 ms).
TINY_TIMEOUT_S = 0.005
RATE_LIMIT = "2/m"
RATE_MAX, RATE_WINDOW_MS = 2, 60_000
RETRY_MAX_DELAY_S = 60.0
DELIVERY_LIMIT = 4  # Schwelle des Gates: ab dem vorletzten Versuch (3)
GUARD_TTL_MS = 60_000
MAX_ORPHANS = 3
STATE_TTL_S = 86_400.0
MAX_CHAINS = 3
RETRY_COUNT_VALUES: tuple[object, ...] = ("abc", "3", -2, True, 2.5, 1, 7, None)
RETRY_DELAYS = (-5.0, 0.0, 2.5, 30.0)

Kind = Literal["ok", "error", "noretry", "retry", "hang", "child_death", "crash"]
Backoff = Literal["fixed", "linear", "exponential"]
Timeouts = Literal["soft", "hard"]
ReplayMode = Literal["dlq_retry", "dlq_retry_twice", "ui_move", "ui_move_str", "ui_move_without_replay_id"]


class ChainEnv(Protocol):
    """Store-Factory der Retry-Maschine (KTD2)."""

    @property
    def clock(self) -> FakeClock: ...

    @property
    def backend(self) -> InMemoryBackend | ValkeyBackend: ...

    async def advance(self, seconds: float) -> None: ...

    async def close(self) -> None: ...


RateLimitFactory = Callable[[ChainEnv], "RateLimitStore"]


@dataclass(frozen=True)
class Act:
    """Was der Task bei dieser Zustellung tut."""

    kind: Kind
    delay: float | None = None

    @override
    def __repr__(self) -> str:
        return f"Act({self.kind})" if self.delay is None else f"Act(retry, delay={self.delay})"


@dataclass(frozen=True)
class ChainCfg:
    """Die TaskInfo des Tasks; ``reconfigure`` tauscht sie wie ein Deploy."""

    max_retries: int = 2
    backoff: Backoff = "fixed"
    base_delay: float = 1.0
    timeouts: Timeouts = "soft"
    rate_limited: bool = False

    def task_info(self) -> TaskInfo:
        tiny: Any = TINY_TIMEOUT_S  # TaskInfo annotiert int, nimmt aber Sekunden als float
        return TaskInfo(
            name=TASK_NAME,
            queues=[QUEUE],
            max_retries=self.max_retries,
            retry_backoff=self.backoff,
            retry_base_delay=self.base_delay,
            rate_limit=RATE_LIMIT if self.rate_limited else None,
            soft_timeout=tiny if self.timeouts == "soft" else None,
            hard_timeout=tiny if self.timeouts == "hard" else None,
        )


# -- Modellsicht auf Header (aus den Entscheidungen, nicht aus dem Code) -----


def model_retry_count(headers: Mapping[str, Any]) -> int:
    """E-051: nur ein echtes ``int`` größer 0 zählt."""
    raw = headers.get(HEADER_RETRY_COUNT, 0)
    return raw if type(raw) is int and raw > 0 else 0


def lenient_count(raw: Any) -> int | None:
    """Broker-Zähler wie im Delivery-Count-Gate: ``int()``, unlesbar -> None."""
    if raw is None:
        return None
    try:
        return int(raw)
    except TypeError, ValueError, OverflowError:
        return None


def model_identity(headers: Mapping[str, Any], task_id: str) -> str:
    """E-009: ``task_id|H#n``; die Maschine erzeugt kein ``x-death``."""
    replay = headers.get(HEADER_REPLAY_ID)
    suffix = f"|{replay}" if isinstance(replay, str) and 0 < len(replay) <= 64 else ""
    base = headers.get(HEADER_XDEATH_BASE)
    if type(base) is int and base > 0:
        suffix += f"#{base}"
    return f"{task_id}{suffix}"


def broker_counted(headers: Mapping[str, Any]) -> bool:
    return any((lenient_count(headers.get(name)) or 0) > 0 for name in BROKER_DELIVERY_COUNTERS)


def backoff_range_ms(cfg: ChainCfg, attempt: int) -> tuple[int, int]:
    """E-023 und ``calculate_backoff``: erlaubte Wartezeit der Retry-Kopie (ms)."""
    cap = RETRY_MAX_DELAY_S
    if cfg.backoff == "fixed":
        exact = int(min(cfg.base_delay, cap) * 1000)
        return exact, exact
    linear = cfg.backoff == "linear"
    nominal = cfg.base_delay * (attempt + 1) if linear else cfg.base_delay * 2 ** min(attempt, 64)
    if 2 * nominal <= cap:
        return int(nominal * 1000), int(2 * nominal * 1000)
    if nominal > cap:
        return int(0.6 * cap * 1000), int(0.8 * cap * 1000)
    # Jitter entscheidet: unterhalb des Deckels [nominal, cap], darüber [0,6; 0,8] cap.
    return int(min(nominal, 0.6 * cap) * 1000), int(cap * 1000)


def guard_defer_range_ms(attempt: int) -> tuple[int, int]:
    """``backend_defer_delay`` mit Basis 1 s, Deckel 60 s, Jitter 0,8 bis 1,2."""
    nominal = min(1.0 * 2 ** (max(attempt, 1) - 1), 60.0)
    return int(0.8 * nominal * 1000), int(1.2 * nominal * 1000)


# -- Modellzustand -----------------------------------------------------------


@dataclass
class GuardState:
    done: bool = False
    sup: int | None = None
    dead_lease: bool = False
    orphans: int = 0
    running_until_ms: int | None = None


@dataclass
class Expect:
    """Vorhersage für eine Zustellung."""

    verb: Literal["ack", "retry_later", "dead_letter", "redeliver"] | None
    reason: str
    runs: bool = False
    copy_retry_count: int | None = None
    delay_ms: tuple[int, int] | None = None
    stores_error: bool = False
    guard_after: GuardState | None = None
    rate_consumed: int | None = None  # Fenster, dessen Zähler steigt
    guard_defer_attempt: int | None = None
    rate_limit_count: int | None = None
    settle_lost: bool = False
    suspect: str | None = None


@dataclass(eq=False)
class Pending:
    seq: int
    delivery: Delivery

    @property
    def headers(self) -> Mapping[str, Any]:
        return self.delivery.headers

    @property
    def task_id(self) -> str:
        return cast("str", json.loads(self.delivery.body)["task"]["id"])

    @override
    def __repr__(self) -> str:
        rc = self.headers.get(HEADER_RETRY_COUNT, "-")
        counters = {k: v for k, v in self.headers.items() if k in BROKER_DELIVERY_COUNTERS}
        return f"D{self.seq}({self.task_id}, rc={rc!r}{', ' + str(counters) if counters else ''})"


@dataclass
class VerbCall:
    verb: str
    ok: bool
    delay_ms: int | None = None
    headers: dict[str, Any] | None = None
    settle_lost: bool = False  # Kopie liegt, die Bestätigung des Originals ging verloren


@dataclass
class Copy:
    """Eine Kopie, die swarmq publiziert hat (K8)."""

    via: str
    headers: dict[str, Any]


class ChainBroker(InMemoryBroker):
    """``InMemoryBroker`` mit Verbprotokoll je Zustellung.

    Zwei Lücken des Fakes gegenüber ``RabbitMQBroker`` schließt er hier:
    ``dead_letter`` stempelt die Generation wie
    ``broker/rabbitmq/delivery.py`` (``dead_letter_generation``,
    ``x-swarmq-dead-lettered-at``, ohne Zustands-Header der
    Rückstellschleifen), und ``redeliver`` zählt ``x-acquired-count`` hoch
    wie RabbitMQ 4.3 (BZ-29).

    Störungen: ``copy_failure`` lässt ``retry_later`` und ``dead_letter`` mit
    ``BrokerConnectionError`` scheitern (nichts gesendet). ``lose_settle``
    lässt die nächste Bestätigung scheitern, weil der Kanal weg ist: ``ack``
    wirft ``BrokerConnectionError``, ``retry_later``/``dead_letter``
    publizieren die Kopie und werfen ``SettleFailedAfterPublish`` wie
    ``_ack_after_publish``; jedes weitere Verb auf dieser Zustellung wirft
    ``BrokerConnectionError``. Die Maschine stellt das Original danach mit
    erhöhten Broker-Zählern neu zu (``requeue_lost``).
    """

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.calls: dict[object, list[VerbCall]] = {}
        self.copies: list[Copy] = []
        self.dead: list[tuple[str, dict[str, Any]]] = []  # (Task-ID, gestempelte Header)
        self.copy_failure = False
        self.lose_settle = False
        self.lost: set[object] = set()
        self.clock = clock

    def _record(self, delivery: Delivery, call: VerbCall) -> None:
        self.calls.setdefault(delivery.handle, []).append(call)

    def _closed(self, delivery: Delivery, verb: str) -> None:
        """Kanal der Zustellung ist weg: jedes weitere Verb scheitert."""
        if delivery.handle in self.lost:
            self._record(delivery, VerbCall(verb, ok=False))
            raise BrokerConnectionError(f"{verb}: channel closed (injected)")

    def _lose_now(self, delivery: Delivery) -> bool:
        if not self.lose_settle:
            return False
        self.lose_settle = False
        self.lost.add(delivery.handle)
        return True

    @override
    async def ack(self, delivery: Delivery) -> None:
        self._closed(delivery, "ack")
        await super().ack(delivery)
        if self._lose_now(delivery):
            self._record(delivery, VerbCall("ack", ok=False, settle_lost=True))
            raise BrokerConnectionError("ack: channel closed (injected)")
        self._record(delivery, VerbCall("ack", ok=True))

    @override
    async def redeliver(self, delivery: Delivery) -> None:
        self._closed(delivery, "redeliver")
        await InMemoryBroker.ack(self, delivery)  # settle, dann neu einreihen
        self._record(delivery, VerbCall("redeliver", ok=True))
        headers = dict(delivery.headers)
        headers["x-acquired-count"] = (lenient_count(headers.get("x-acquired-count")) or 0) + 1
        await self.publish(delivery.queue, delivery.body, headers)

    @override
    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        self._closed(delivery, "retry_later")
        if self.copy_failure:
            self._record(delivery, VerbCall("retry_later", ok=False, delay_ms=delay_ms, headers=dict(headers)))
            raise BrokerConnectionError("delay publish failed (injected, nothing sent)")
        await super().retry_later(delivery, delay_ms=delay_ms, headers=headers)
        lost = self._lose_now(delivery)
        self._record(
            delivery, VerbCall("retry_later", ok=True, delay_ms=delay_ms, headers=dict(headers), settle_lost=lost)
        )
        self.copies.append(Copy("retry_later", dict(headers)))
        if lost:
            raise SettleFailedAfterPublish("ack after delay publish: channel closed (injected)")

    @override
    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        self._closed(delivery, "dead_letter")
        if self.copy_failure:
            self._record(delivery, VerbCall("dead_letter", ok=False, headers=dict(headers)))
            raise BrokerConnectionError("dead-letter publish failed (injected, nothing sent)")
        at_ms = self.clock.now_ms()
        stamped = {
            k: v
            for k, v in dead_letter_generation(headers, delivery.body, at_ms).items()
            if k not in LOOP_STATE_HEADERS
        }
        stamped[HEADER_DEAD_LETTERED_AT] = at_ms
        stamped[HEADER_DEAD_LETTER_TTL_MS] = int(STATE_TTL_S * 1000)
        await super().dead_letter(delivery, headers=stamped)
        lost = self._lose_now(delivery)
        self._record(delivery, VerbCall("dead_letter", ok=True, headers=stamped, settle_lost=lost))
        self.copies.append(Copy("dead_letter", stamped))
        self.dead.append((cast("str", json.loads(delivery.body)["task"]["id"]), stamped))
        if lost:
            raise SettleFailedAfterPublish("ack after dead-letter publish: channel closed (injected)")

    @override
    async def route_via_dlx(self, delivery: Delivery) -> None:
        self._closed(delivery, "route_via_dlx")
        self._record(delivery, VerbCall("route_via_dlx", ok=True))
        await super().route_via_dlx(delivery)

    async def requeue_lost(self, delivery: Delivery) -> None:
        """Der Kanal ist weg, die Bestätigung kam nie an: die Quorum-Queue
        stellt dieselbe Nachricht mit erhöhten Zählern neu zu (im Fake ist
        die Zustellung schon erledigt)."""
        headers = dict(delivery.headers)
        for name in BROKER_DELIVERY_COUNTERS:
            headers[name] = (lenient_count(headers.get(name)) or 0) + 1
        await self.publish(delivery.queue, delivery.body, headers)

    async def crash_requeue(self, delivery: Delivery) -> None:
        """Kanalverlust nach dem Tod des Workers: die Quorum-Queue stellt
        dieselbe Nachricht mit erhöhten Zählern neu zu (kein Vertragsverb)."""
        await InMemoryBroker.ack(self, delivery)
        headers = dict(delivery.headers)
        for name in BROKER_DELIVERY_COUNTERS:
            headers[name] = (lenient_count(headers.get(name)) or 0) + 1
        await self.publish(delivery.queue, delivery.body, headers)

    async def operator_move(self, delivery: Delivery, headers: Mapping[str, Any]) -> None:
        """RabbitMQ-UI: Nachricht mit geänderten Headern neu publizieren und
        das Original entfernen (kein Vertragsverb)."""
        await InMemoryBroker.ack(self, delivery)
        await self.publish(delivery.queue, delivery.body, headers)


class Life:
    dead = False


class Doomed:
    """Hülle um Broker, Backend oder Store des sterbenden Workers: nach dem
    Tod kommt kein Aufruf mehr an (Coroutinen werden zu No-ops)."""

    def __init__(self, inner: Any, life: Life) -> None:
        self._inner = inner
        self._life = life

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not inspect.iscoroutinefunction(attr):
            return attr

        async def call(*args: Any, **kwargs: Any) -> Any:
            if self._life.dead:
                return None
            return await attr(*args, **kwargs)

        return call


class DyingPool:
    """Rechenpool ohne Kind: das Kind stirbt ohne Ergebnis (OOM-Kill, SIGKILL)."""

    def __init__(self, on_run: Callable[[], None]) -> None:
        self._on_run = on_run

    async def run(self, job: ComputeJob, *, use_di: bool) -> ResultEnvelope:
        del job, use_di
        self._on_run()
        raise ChildDied("compute child pid=0 exited without result")


_seq = itertools.count(1)


async def _ignore_pause(_delay_s: float) -> None:
    """Pause vor ``redeliver`` (BZ-29): das Modell braucht keine Wartezeit."""


class RetryChainMachine(AsyncStateMachine[ChainEnv]):
    def __init__(
        self,
        open_env: Callable[[], Coroutine[Any, Any, ChainEnv]],
        rate_limit: RateLimitFactory | None = None,
    ) -> None:
        super().__init__(open_env)
        try:
            self.cfg = ChainCfg()
            self.task_class = self._make_task_class()
            self.registry = TaskRegistry()
            self.registry.register(self.task_class)
            self.config = SwarmQConfig(
                broker_url="amqp://unused",
                backend_url="valkey://unused",
                queues=[QUEUE],
                retry_max_delay=RETRY_MAX_DELAY_S,
                delivery_limit=DELIVERY_LIMIT,
                execution_guard_ttl_s=GUARD_TTL_MS / 1000,
                execution_guard_done_ttl_s=STATE_TTL_S,
                execution_guard_max_orphans=MAX_ORPHANS,
                dead_letter_ttl_s=STATE_TTL_S,
            )
            self.broker = ChainBroker(self.env.clock)
            self.pending: list[Pending] = []
            self.run(self.broker.connect())
            self.run(self.broker.consume(QUEUE, self._on_delivery, concurrency=1_000_000))
            backend = self.env.backend
            self.rate_store: RateLimitStore = rate_limit(self.env) if rate_limit is not None else backend.rate_limit
            self.guard_store: ExecutionGuardStore = backend.execution_guard
            self.io = self._worker(self.broker, backend, self.rate_store, self.guard_store)
            self.cpu = self._worker(self.broker, backend, self.rate_store, self.guard_store)
            self.cpu._executor = ComputeExecutor(
                self.cpu._gate_deps,
                cast("SlotPool", DyingPool(self._record_run)),
                worker_id="cpu",
            )
            self.life = Life()
            self.doomed = self._worker(
                cast("Any", Doomed(self.broker, self.life)),
                cast("Any", Doomed(backend, self.life)),
                self.rate_store,
                cast("Any", Doomed(self.guard_store, self.life)),
            )
            # Modell
            self.guard: dict[str, GuardState] = {}
            self.rate_counts: dict[int, int] = {}
            self.settled_runs: dict[tuple[str, int], int] = {}
            self.dlq_by_identity: dict[str, list[str]] = {}
            self.task_ids: list[str] = []
            self.runs: list[tuple[str, int]] = []
            self.current: Pending | None = None
            self.act = Act("ok")
        except BaseException:
            self.teardown()
            raise

    # -- Aufbau ----------------------------------------------------------------

    def _make_task_class(self) -> type[Task]:
        machine = self

        async def run(_task: Task) -> str:
            return await machine._perform()

        task_class = cast(
            "type[Task]",
            type(
                "StatefulRetryChain", (Task,), {"task_info": self.cfg.task_info(), "run": run, "__module__": __name__}
            ),
        )
        # Nicht global registriert lassen (Muster test_befund_fs19_counter_headers.py).
        Task._registered_tasks.remove(task_class)
        return task_class

    def _worker(self, broker: Any, backend: Any, rate: Any, guard: Any) -> Worker:
        worker = Worker(
            broker=cast("WorkerBroker", broker),
            backend=cast("WorkerBackend", backend),
            registry=self.registry,
            config=self.config,
        )
        worker._rate_limiter = rate
        worker._lock_manager = self.env.backend.locks
        worker._guard_store = guard
        worker._gate_deps.redeliver_pause = _ignore_pause
        return worker

    async def _on_delivery(self, delivery: Delivery) -> None:
        self.pending.append(Pending(next(_seq), delivery))

    async def _flush(self) -> None:
        """Ein Durchlauf der Schleife: vom Broker angestoßene Zustellungs-Tasks
        hängen ihre Zustellung an ``pending`` (ohne eigenes ``await``)."""
        await asyncio.sleep(0)

    def _record_run(self) -> None:
        assert self.current is not None
        self.runs.append((model_identity(self.current.headers, self.current.task_id), self._rc(self.current)))

    async def _perform(self) -> str:
        self._record_run()
        act = self.act
        if act.kind == "ok":
            return "ok"
        if act.kind == "noretry":
            raise NoRetry("permanent failure")
        if act.kind == "retry":
            raise Retry(delay=act.delay)
        if act.kind == "hang":
            await asyncio.Event().wait()  # Soft- oder Hard-Timeout bricht ab
        if act.kind == "crash":
            self.life.dead = True
            raise RuntimeError("worker killed")
        raise RuntimeError("retryable failure")

    @staticmethod
    def _rc(pending: Pending) -> int:
        return model_retry_count(pending.headers)

    @property
    def now_ms(self) -> int:
        return self.env.clock.now_ms()

    # -- Modell ------------------------------------------------------------------

    def _rate_preview(self) -> tuple[bool, int, int]:
        """``sliding_window_counter.lua``: (erlaubt, Fenster, retry_after_ms)."""
        now = self.now_ms
        window_ts = now - now % RATE_WINDOW_MS
        current = self.rate_counts.get(window_ts, 0)
        previous = self.rate_counts.get(window_ts - RATE_WINDOW_MS, 0)
        elapsed = now - window_ts
        estimated = previous * (1.0 - elapsed / RATE_WINDOW_MS) + current
        if estimated >= RATE_MAX:
            return False, window_ts, max(1, math.ceil(RATE_WINDOW_MS - elapsed))
        return True, window_ts, 0

    def predict(self, pending: Pending, act: Act, *, copy_failure: bool) -> Expect:
        """Verb, Kopie und Folgezustand einer Zustellung, in der Reihenfolge
        der Pipeline: Zustelllimit, Ausführungsschutz, Rate-Limit, Ausführung."""
        headers, task_id = pending.headers, pending.task_id
        delivered = lenient_count(headers.get("x-delivery-count"))
        acquired = lenient_count(headers.get("x-acquired-count"))
        threshold = DELIVERY_LIMIT - 1
        identity, rc = model_identity(headers, task_id), self._rc(pending)
        state = self.guard.get(identity, GuardState())
        if (delivered is not None and delivered >= threshold) or (acquired is not None and acquired >= threshold):
            # Vor dem Ausführungsschutz: kein Claim, also auch kein Done-Marker.
            expect = self._dead(Expect("dead_letter", "delivery_limit"), None, copy_failure=copy_failure)
            if state.done or (state.sup is not None and rc < state.sup):
                # R4Z-04: eine Neuzustellung einer abgeschlossenen bzw. überholten
                # Ausführung ist laut E-009/E-043 ein Duplikat; das Gate vor dem
                # Ausführungsschutz schickt sie in die DLQ.
                expect.suspect = "R4Z-04"
            return expect
        if state.done:
            return Expect("ack", "done")
        if state.sup is not None and rc < state.sup:
            return Expect("ack", "superseded")
        if state.running_until_ms is not None and state.running_until_ms > self.now_ms:
            prior = 0 if broker_counted(headers) else max(lenient_count(headers.get(HEADER_GUARD_DEFER_COUNT)) or 0, 0)
            expect = Expect(
                "retry_later",
                "guard_defer",
                copy_retry_count=rc,
                delay_ms=guard_defer_range_ms(prior + 1),
                guard_defer_attempt=prior + 1,
            )
            return self._copy_failed(expect, None) if copy_failure else expect
        claimed = replace(state, running_until_ms=None, dead_lease=False)
        if state.dead_lease:
            claimed.orphans += 1  # abgelaufen ohne Freigabe: Waise
        if claimed.orphans >= MAX_ORPHANS:
            return self._dead(
                Expect("dead_letter", "orphan_limit", guard_after=replace(claimed, done=True)),
                claimed,
                copy_failure=copy_failure,
            )
        rate_window: int | None = None
        if self.cfg.rate_limited:
            allowed, window_ts, retry_after = self._rate_preview()
            if not allowed:
                rl_count = max(lenient_count(headers.get("x-swarmq-rate-limit-count")) or 0, 0) + 1
                expect = Expect(
                    "retry_later",
                    "rate_limit",
                    copy_retry_count=rc,
                    delay_ms=(retry_after, retry_after),
                    guard_after=claimed,  # Lease vor dem Publish freigegeben
                    rate_limit_count=rl_count,
                )
                return self._copy_failed(expect, claimed) if copy_failure else expect
            rate_window = window_ts
        expect = self._execute(act, rc, claimed, copy_failure=copy_failure)
        expect.rate_consumed = rate_window
        return expect

    def _execute(self, act: Act, rc: int, claimed: GuardState, *, copy_failure: bool) -> Expect:
        if act.kind == "crash":
            dead = replace(claimed, dead_lease=True, running_until_ms=self.now_ms + GUARD_TTL_MS)
            return Expect(None, "crash", runs=True, guard_after=dead)
        if act.kind == "ok":
            return Expect("ack", "success", runs=True, guard_after=replace(claimed, done=True))
        if act.kind == "noretry" or rc >= self.cfg.max_retries:
            reason = "noretry" if act.kind == "noretry" else "exhausted"
            expect = Expect("dead_letter", reason, runs=True, guard_after=replace(claimed, done=True))
            return self._dead(expect, claimed, copy_failure=copy_failure)
        if act.kind == "retry":
            assert act.delay is not None
            exact = int(max(act.delay, 0.0) * 1000)
            delay = (exact, exact)
        else:
            delay = backoff_range_ms(self.cfg, rc)
        expect = Expect(
            "retry_later",
            "retry",
            runs=True,
            copy_retry_count=rc + 1,
            delay_ms=delay,
            guard_after=replace(claimed, sup=max(claimed.sup or 0, rc + 1)),
        )
        return self._copy_failed(expect, claimed) if copy_failure else expect

    @staticmethod
    def _copy_failed(expect: Expect, claimed: GuardState | None) -> Expect:
        """E-041: nichts gesendet -> das Original geht per ``redeliver`` zurück;
        ``GuardedBroker.redeliver`` gibt die Lease frei, kein Done-/Superseded-Marker."""
        return Expect(
            "redeliver",
            f"{expect.reason}+copy_failed",
            runs=expect.runs,
            stores_error=expect.stores_error,
            guard_after=claimed,
        )

    def _dead(self, expect: Expect, claimed: GuardState | None, *, copy_failure: bool) -> Expect:
        """Jeder Dead-Letter-Weg speichert zuerst das Fehlerergebnis (E-031)."""
        expect.stores_error = True
        return self._copy_failed(expect, claimed) if copy_failure else expect

    # -- Zustellung --------------------------------------------------------------

    def deliver(self, pending: Pending, act: Act, *, copy_failure: bool = False, lose_settle: bool = False) -> Expect:
        """Eine Zustellung durch die echte Pipeline samt Prüfung von K1 bis K10."""
        assert not (copy_failure and lose_settle)
        expect = self.predict(pending, act, copy_failure=copy_failure)
        assert expect.suspect is None, f"{pending} {act}: {expect.suspect} (K10) -- siehe r4z_04_*"
        expect.settle_lost = lose_settle and expect.verb in ("ack", "retry_later", "dead_letter")
        worker = {"child_death": self.cpu, "crash": self.doomed}.get(act.kind, self.io)
        runs_before = len(self.runs)
        self.pending.remove(pending)
        self.current = pending
        self.broker.copy_failure = copy_failure
        self.broker.lose_settle = expect.settle_lost
        self.act = act
        try:
            self.run(worker._handle_message(pending.delivery))
        finally:
            self.broker.copy_failure = False
            self.broker.lose_settle = False
            self.current = None
        calls = self.broker.calls.get(pending.delivery.handle, [])
        ran = len(self.runs) - runs_before
        self._check_verbs(pending, act, expect, calls, ran)
        if act.kind == "crash" and expect.verb is None:
            self.life.dead = False
            self.run(self.broker.crash_requeue(pending.delivery))
        if pending.delivery.handle in self.broker.lost:
            self.run(self.broker.requeue_lost(pending.delivery))
        self.run(self._flush())
        self._apply(pending, expect)
        return expect

    def _check_verbs(self, pending: Pending, act: Act, expect: Expect, calls: list[VerbCall], ran: int) -> None:
        where = f"{pending} {act}: erwartet {expect.verb} ({expect.reason}), Broker sah {calls}"
        succeeded = [call for call in calls if call.ok]
        if expect.verb is None:
            assert calls == [], f"{where} (K1: Verb nach dem Tod)"
        elif expect.settle_lost:
            # Kanal weg: das vorhergesagte Verb (Kopie liegt bzw. Ack ging
            # verloren), danach höchstens das gescheiterte ``redeliver`` des Rückfalls.
            assert calls, where
            assert (calls[0].verb, calls[0].settle_lost) == (expect.verb, True), f"{where} (K1/K2)"
            fallback = 1 if expect.verb == "retry_later" else 0
            assert [(c.verb, c.ok) for c in calls[1:]] == [("redeliver", False)] * fallback, f"{where} (K1)"
            succeeded = calls[:1]
        else:
            assert len(succeeded) == 1, f"{where} (K1: genau ein Verb)"
            assert succeeded[0].verb == expect.verb, f"{where} (K2)"
            failed = [call for call in calls if not call.ok]
            assert all(call.verb in ("retry_later", "dead_letter") for call in failed), f"{where} (K1)"
        assert ran == (1 if expect.runs else 0), f"{where}: {ran} Läufe, erwartet runs={expect.runs} (K2/K9)"
        if act.kind in ("hang", "child_death") and expect.runs and "copy_failed" not in expect.reason and succeeded:
            assert succeeded[0].verb in ("retry_later", "dead_letter"), f"{where} (K3, E-037)"
        if expect.verb == "retry_later":
            call = succeeded[0]
            assert call.headers is not None
            assert call.delay_ms is not None
            copied = call.headers.get(HEADER_RETRY_COUNT)
            if expect.reason == "retry":
                assert type(copied) is int, f"{where}: Retry-Kopie trägt {copied!r} (K5)"
                assert copied == expect.copy_retry_count, f"{where}: Retry-Kopie trägt {copied!r} (K4/K5)"
            else:  # Zurückstellung: kein Retry-Verbrauch, Zähler unverändert
                assert copied == pending.headers.get(HEADER_RETRY_COUNT), f"{where}: Zähler {copied!r} (K2)"
            assert expect.delay_ms is not None
            low, high = expect.delay_ms
            assert low <= call.delay_ms <= high, (
                f"{where}: Wartezeit {call.delay_ms} ms nicht in [{low}, {high}] (K4/K6)"
            )
            if expect.guard_defer_attempt is not None:
                assert call.headers.get(HEADER_GUARD_DEFER_COUNT) == expect.guard_defer_attempt, where
            if expect.rate_limit_count is not None:
                assert call.headers.get("x-swarmq-rate-limit-count") == expect.rate_limit_count, where
        if expect.verb == "dead_letter":
            call = succeeded[0]
            assert call.headers is not None
            assert call.headers.get(HEADER_RETRY_COUNT, None) == pending.headers.get(HEADER_RETRY_COUNT, None), (
                f"{where}: DLQ-Kopie mit anderem Retry-Zähler (K7)"
            )
            assert call.headers.get(HEADER_DLQ_GENERATION) == call.headers.get(HEADER_DEAD_LETTERED_AT), where
        if expect.stores_error:
            raw = self.run(self.env.backend.get_result(pending.task_id))
            assert raw is not None, f"{where}: kein Fehlerergebnis (K7, E-031)"
            assert decode_result(raw)["is_error"] is True, f"{where}: Ergebnis ohne Fehler (K7, E-031)"
        if expect.reason == "success":
            raw = self.run(self.env.backend.get_result(pending.task_id))
            assert raw is not None, where
            assert decode_result(raw)["is_error"] is False, where

    def _apply(self, pending: Pending, expect: Expect) -> None:
        identity = model_identity(pending.headers, pending.task_id)
        if expect.guard_after is not None:
            self.guard[identity] = expect.guard_after
        if expect.rate_consumed is not None:
            self.rate_counts[expect.rate_consumed] = self.rate_counts.get(expect.rate_consumed, 0) + 1
        if expect.runs and expect.verb in ("ack", "retry_later", "dead_letter"):
            key = (identity, self._rc(pending))
            self.settled_runs[key] = self.settled_runs.get(key, 0) + 1
        if expect.verb == "dead_letter":
            self.dlq_by_identity.setdefault(identity, []).append(expect.reason)

    # -- Regeln ------------------------------------------------------------------

    def publish(self, task_id: str | None = None) -> str:
        task_id = task_id or f"t{len(self.task_ids) + 1}"
        self.task_ids.append(task_id)
        body = json.dumps({"task": {"id": task_id, "queue": QUEUE, "name": TASK_NAME}, "args": [], "kwargs": {}})
        self.run(self.broker.publish(QUEUE, body.encode(), {}))
        self.run(self._flush())
        return task_id

    @initialize()
    def first_task(self) -> None:
        self.publish()

    @precondition(lambda self: len(self.task_ids) < MAX_CHAINS)
    @rule()
    def publish_task(self) -> None:
        self.publish()

    def deliverable(self) -> list[Pending]:
        """Wartende Zustellungen ohne bekannten Verdacht.

        F4-offen (R4Z-04): die Neuzustellung einer abgeschlossenen oder
        überholten Ausführung am Zustelllimit bleibt liegen, statt die
        Maschine rot zu färben; Nachweis ``r4z_04_*`` in
        ``retry_chain_scenarios.py``. Ausgeklammert an der auslösenden Regel
        über Auswahl und Vorbedingung, nie mit ``assume()`` (KTD3)."""
        return [p for p in self.pending if self.predict(p, Act("ok"), copy_failure=False).suspect is None]

    @precondition(lambda self: bool(self.deliverable()))
    @rule(
        data=st.data(),
        kind=st.sampled_from(("ok", "error", "error", "noretry", "retry", "hang", "child_death", "crash")),
        delay=st.sampled_from(RETRY_DELAYS),
        fault=st.sampled_from((None, None, None, None, "copy_failure", "lose_settle")),
    )
    def deliver_one(self, data: st.DataObject, kind: Kind, delay: float, fault: str | None) -> None:
        pending = data.draw(st.sampled_from(self.deliverable()), label="delivery")
        act = Act(kind, delay if kind == "retry" else None)
        faulty = kind != "crash"
        self.deliver(
            pending,
            act,
            copy_failure=faulty and fault == "copy_failure",
            lose_settle=faulty and fault == "lose_settle",
        )

    def duplicate(self, pending: Pending) -> None:
        """Doppelte Kopie (``PublishIndeterminate``, E-041): dieselben Header."""
        self.run(self.broker.publish(QUEUE, pending.delivery.body, dict(pending.headers)))
        self.run(self._flush())

    @precondition(lambda self: bool(self.pending))
    @rule(data=st.data())
    def duplicate_one(self, data: st.DataObject) -> None:
        self.duplicate(data.draw(st.sampled_from(self.pending), label="delivery"))

    def set_retry_count(self, pending: Pending, value: object) -> Pending:
        """UI-Republish mit geändertem ``x-swarmq-retry-count``."""
        headers = dict(pending.headers)
        if value is None:
            headers.pop(HEADER_RETRY_COUNT, None)
        else:
            headers[HEADER_RETRY_COUNT] = value
        self.pending.remove(pending)
        known = len(self.pending)
        self.run(self.broker.operator_move(pending.delivery, headers))
        self.run(self._flush())
        assert len(self.pending) == known + 1
        return self.pending[-1]

    @precondition(lambda self: bool(self.pending))
    @rule(data=st.data(), value=st.sampled_from(RETRY_COUNT_VALUES))
    def manipulate_header(self, data: st.DataObject, value: object) -> None:
        self.set_retry_count(data.draw(st.sampled_from(self.pending), label="delivery"), value)

    def advance(self, seconds: int) -> None:
        self.run(self.env.advance(seconds))
        self.run(self._flush())

    @rule(seconds=st.sampled_from((1, 2, 5, 30, 61, 90)))
    def advance_time(self, seconds: int) -> None:
        self.advance(seconds)

    def reconfigure(self, cfg: ChainCfg) -> None:
        """Deploy mit geänderter TaskInfo: Zustellungen danach sehen sie."""
        self.cfg = cfg
        self.task_class.task_info = cfg.task_info()

    @rule(
        max_retries=st.integers(min_value=0, max_value=3),
        backoff=st.sampled_from(("fixed", "linear", "exponential")),
        base_delay=st.sampled_from((1.0, 90.0)),
        timeouts=st.sampled_from(("soft", "hard")),
        rate_limited=st.booleans(),
    )
    def reconfigure_task(
        self, max_retries: int, backoff: Backoff, base_delay: float, timeouts: Timeouts, rate_limited: bool
    ) -> None:
        self.reconfigure(ChainCfg(max_retries, backoff, base_delay, timeouts, rate_limited))

    def replay(self, task_id: str, mode: ReplayMode) -> None:
        """Replay aus der DLQ: ``swarmq dlq retry`` (Fake bildet ``RabbitMQAdmin``
        nach) oder ein UI-Move der XQ-Nachricht."""
        known = len(self.pending)
        if mode in ("dlq_retry", "dlq_retry_twice"):
            published = len(self.pending)
            moved, _scanned = self.run(self.broker.retry_dead_letter(QUEUE, task_id))
            assert moved, f"dlq retry fand {task_id} nicht"
            self.run(self._flush())
            if mode == "dlq_retry_twice":
                first = self.pending[published]
                self.duplicate(first)
        else:
            found, _scanned = self.run(self.broker.inspect_dead_letter(QUEUE, task_id))
            assert found is not None
            headers = dict(found["headers"])
            if mode == "ui_move_str":
                headers = {k: str(v) if type(v) is int else v for k, v in headers.items()}
            if mode == "ui_move_without_replay_id":
                headers.pop(HEADER_REPLAY_ID, None)
                headers.pop(HEADER_DLQ_GENERATION, None)
            self.run(self.broker.publish(QUEUE, found["body"], headers))
            self.run(self._flush())
        assert len(self.pending) > known

    @precondition(lambda self: bool(self.broker.dead))
    @rule(
        data=st.data(),
        mode=st.sampled_from(("dlq_retry", "dlq_retry_twice", "ui_move", "ui_move_str", "ui_move_without_replay_id")),
    )
    def replay_dead_letter(self, data: st.DataObject, mode: ReplayMode) -> None:
        task_id = data.draw(st.sampled_from(sorted({task_id for task_id, _ in self.broker.dead})), label="task_id")
        if mode in ("dlq_retry", "dlq_retry_twice"):
            letters, _ = self.run(self.broker.list_dead_letters(QUEUE))
            if not any(letter["task_id"] == task_id for letter in letters):
                return  # schon per dlq retry entnommen
        else:
            found, _ = self.run(self.broker.inspect_dead_letter(QUEUE, task_id))
            if found is None:
                return
        self.replay(task_id, mode)

    # -- Invarianten -------------------------------------------------------------

    @invariant()
    def swarmq_copies_without_broker_counters(self) -> None:
        """K8 (E-054)."""
        for copy in self.broker.copies:
            leaked = sorted(set(copy.headers) & BROKER_DELIVERY_COUNTERS)
            assert not leaked, f"{copy.via}-Kopie trägt Broker-Zähler {leaked} (K8)"

    @invariant()
    def one_settled_run_per_attempt(self) -> None:
        """K9: je (Identität, Retry-Zähler) höchstens ein abgeschlossener Lauf."""
        for key, count in self.settled_runs.items():
            assert count <= 1, f"{key}: {count} abgeschlossene Läufe (K9)"

    @invariant()
    def one_guarded_dead_letter_per_identity(self) -> None:
        """K7: hinter dem Ausführungsschutz höchstens eine DLQ-Kopie je Identität."""
        for identity, reasons in self.dlq_by_identity.items():
            guarded = [reason for reason in reasons if reason != "delivery_limit"]
            assert len(guarded) <= 1, f"{identity}: DLQ-Kopien {reasons} (K7)"

    # -- Abbau -------------------------------------------------------------------

    @override
    async def close_subjects(self) -> None:
        broker = getattr(self, "broker", None)
        if broker is not None:
            await broker.close()


__all__ = [
    "Act",
    "ChainBroker",
    "ChainCfg",
    "ChainEnv",
    "Pending",
    "RetryChainMachine",
    "model_identity",
    "model_retry_count",
]
