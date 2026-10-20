"""Leader- und Fire-Index-Maschine (Fehlersuche Runde 4, Plan 2026-10-17-0941,
U7; R6-R8, KTD1-KTD3).

Subjekte: zwei echte ``SchedulerLoop`` (Kandidaten ``A`` und ``B``) über dem
gemeinsamen ``ScheduleStore`` und ``LockStore`` der Umgebung
(``InMemoryScheduleStore``/``InMemoryLockStore`` oder
``ValkeyScheduleStore``/``ValkeyLockStore`` über ``backend.schedules`` und
``backend.locks``), Oneshots über den echten ``Client.schedule(delay=)``. Der
Broker ist in beiden Stufen ein ``InMemoryBroker``; je Kandidat sitzt davor
ein Publisher mit schaltbarem Fehler (``BrokerConnectionError``,
``PublishIndeterminate`` mit und ohne Zustellung, ``UnroutableMessage``),
vor dem Lock-Store ein Wrapper mit schaltbarem Valkey-Ausfall (``acquire``
und ``renew`` werfen ``BackendConnectionError``). Ein dritter Worker ohne
Kandidatur meldet den statischen Cron ``static-1``.

Die Loops laufen nicht als Tasks: Die Regeln takten Wahl, Poll und Übernahme
einzeln über dieselben Aufrufe wie ``_election_loop``, ``_poll_loop`` und
``_adopt_loop`` (privat, wie in ``tests/unit/scheduler/test_loop.py``). Zeit:
``time.time`` in ``scheduler/loop.py``, ``scheduler/sync.py`` und
``client/schedule.py`` sowie ``stepdown._monotonic`` laufen auf der
``FakeClock``; die Uhr der Event-Loop (Zeitgrenze aus E-048) läuft echt plus
dem Vorlauf der ``FakeClock``. Alle Fristen sind ganze Sekunden.

Modell: Definitionen je Name (Art ``cli``/``static``/``oneshot``, Generation,
``task_id``, ``unroutable_since``), Fire-Index Name -> Score, festgeschriebene
Fire-Identitäten ``(Name, Generation, Score)``, je Kandidat Amtszeit,
letzter Erfolg bei Erwerb/Renew, Serie der Publish-Fehler (BZ-10) und Beginn
der Abkühlzeit. Festschreibungen, Übernahmen und Publishes werden beobachtet
und gegen die erlaubten Ausgänge geprüft; erst dann folgt das Modell.

Invarianten (Quelle):

- L0 Store gleich Modell: Definitionen und Fire-Index stimmen nach jeder
  Regel überein; kein Fire-Termin verschwindet oder entsteht ohne erlaubten
  Ausgang. Quelle: ``ScheduleStore``-Vertrag, E-046.
- L1 Owner-Check: ``commit_fire`` gelingt genau für den Halter des
  Leader-Locks; ein abgelehnter Commit schickt den Loop in den Standby; je
  Fire-Identität höchstens ein wirksamer Commit. Quelle: E-047,
  ``sched_commit.lua``.
- L2 Kein Fire ohne Fire-Index-Eintrag: jeder Publish trägt ``Name:Score``
  eines fälligen Index-Eintrags vom Beginn des Ticks; ein Oneshot feuert unter
  der ``task_id`` aus ``schedule()``. Quelle: F3-01/E-058, BZ-02.
- L3 Oneshot-Speichern ist idempotent: auch nach verlorener Antwort
  (byte-gleiches zweites Insert) steht der Eintrag im Fire-Index. Quelle:
  E-058.
- L4 Ein wirksamer Commit ohne Zustellung ist erklärt: unroutbarer Cron
  (E-003: nächster Termin, Signal ``cron_lost``), unroutbarer Oneshot (E-004:
  +60 s, ``unroutable_since`` bleibt; ab ``scheduler_unroutable_oneshot_max_s``
  entfernt mit Fehlerergebnis), Duplikat beim Unique-Claim (E-011/E-032),
  zurückgehaltener verwaister Cron (E-014: ``now + seen_interval``, nur ohne
  lebende Meldung in der letzten Übernahme der Amtszeit). Sonst gilt
  at-least-once (E-046).
- L5 Renew ``False`` heißt Standby; nach jedem Wahl-Takt ist ein Leader
  höchstens ``Fenster = TTL - min(op_timeout, (TTL-Heartbeat)/2)`` seit dem
  letzten erfolgreichen Erwerb oder Renew im Amt. Quelle: E-048 samt
  Nachtrag FS-31.
- L6 Rücktritt nach Publish-Fehlern genau dann, wenn die Serie aus E-013
  greift (mindestens 2 gezählte ``BrokerConnectionError`` über mindestens
  ``scheduler_publish_stepdown_s``, neue Serie nach Lücke über der Frist, mit
  jeder Amtszeit und nach einem gelungenen Publish); danach 2 x Lock-TTL kein
  Erwerbsversuch. Quelle: E-013, ``scheduler/stepdown.py``.
- L7 Übernahme nie vor dem Sync: In einer neuen Amtszeit publiziert der Poll
  erst nach der ersten Übernahme der Meldungen, höchstens 5 s fail-open.
  Quelle: E-014, ``docs/solutions/logic-errors/scheduler-cron-sync-clobber-test-flake-2026-06-01.md``.
- L8 Die Übernahme ändert nur ``static-1``: legt ihn an, wenn er gemeldet
  wird (bei laufender Meldung zwingend), plant ihn höchstens auf den nächsten
  Termin und entfernt ihn nie. Quelle: E-014, E-049 (Prune erst nach
  ``scheduler_prune_after_s``, hier nicht erreicht).
- L10 Zurückhalten ist Pflicht (nur Fake-Stufe): Fand die Übernahme der
  Amtszeit ``static-1`` bekannt (in ``report:known``, seit der letzten
  Meldung nicht per ``remove_schedule`` vergessen), aber ohne lebende
  Meldung (letzte Meldung älter als ``live_window_ms``), ist bis zur
  nächsten Übernahme jeder Commit von ``static-1`` durch diesen Kandidaten
  ein Rückhalt auf
  ``now + seen_interval`` ohne Publish; der erste Rückhalt seit Amtsantritt
  oder seit dem letzten Fire von ``static-1`` sendet
  ``SCHEDULER_CRON_UNOWNED`` mit ``held=True``. Quelle: E-014,
  ``scheduler/unowned.py`` (``_is_unowned``, ``_hold_unowned``).
- L9 Lebendigkeit (Regel ``settle``): Ohne Fehler hat das System nach höchstens
  12 Heartbeats einen Leader, und nach Übernahme und Poll ist kein Eintrag
  mehr fällig. Quelle: E-046.

Erlaubte Abweichungen (KTD3):

- E-047: Zwei Leader und doppelt publizierte Fires (veralteter Leader nach
  Lock-Verlust, Übernahme mitten im Tick). Der Commit des alten lehnt der
  Owner-Check ab.
- E-048: Rücktritt bei kurzer Störung (Renew-Fehler, Zeitgrenze), Wiederwahl
  per Selbsttreffer (FS-06b).
- E-011: Nach ``PublishIndeterminate`` hält der eigene Unique-Claim eines
  Crons mit ``unique_for`` den nächsten Termin als Duplikat zurück; das Fire
  kann dann ganz fehlen.
- E-014: Ein kurz nicht gemeldeter Cron fällt für diese Zeit aus.
- E-003: Der unroutbare Cron-Lauf fehlt endgültig (gezählt).

Nicht nachgebildet: ``check_orphaned_queues`` (BZ-14), Unique-Oneshots
(``cli_unique``, Lock-Maschine), Ausfälle von Poll und Commit, ein Wechsel der
Cron-Ausdrücke und Zeitzonen, Prune nach 24 h. Die Valkey-Stufe verschiebt
die Serverzeit der Cron-Meldungen nicht; dort bleibt eine Meldung lebendig,
und der Rückhalt (E-014) tritt nur im Fake auf. Die Pflicht L10 gilt deshalb
nur gegen ``InMemoryBackend``.

Annahmen der Umgebung: Alle Fristen und Zeitsprünge sind ganze Sekunden; die
echte Laufzeit der Befehle eines Beispiels bleibt unter 1 s (Valkey-Stufe,
``ttl_tolerance_ms`` der Umgebung), sonst laufen Lock und Unique-Claim in
Valkey vor dem Modell ab.

Grenze Fake/Valkey: Ist die Zeitgrenze beim Renew schon erreicht, ist das
Budget in ``_renew_lock`` negativ (``loop.py``). Der Fake-Renew suspendiert
nicht und gelingt vor dem Abbruch, der Valkey-Renew endet als
``TimeoutError``. Die Maschine erreicht den Fall nur über einen Leader, der
länger als das Fenster nicht getaktet hat (blockierter Loop, E-047).

E-013 und Erfolg: Ein gelungener Publish beginnt die Fehlerserie neu
(``PublishStepdown.success``). Gewollt und getestet in
``tests/unit/scheduler/test_stepdown.py`` (``test_a_successful_publish_resets_the_series``,
``test_a_successful_fire_in_between_keeps_the_leader``), ohne
``entscheidung``-Marker; das Register erwähnt es nicht.

Nicht prüfbar ohne Worker und Fire-Claim: ob ein doppelt publiziertes Fire
(E-047) doppelt ausgeführt wird. Die Maschine verbietet nur zwei Versuche
derselben Identität im selben Poll-Takt eines Kandidaten.

Bekannte Befunde: keine. Ein Befund würde per ``@precondition`` an der
auslösenden Regel mit Kommentar ``F4-nn`` ausgeklammert (KTD3, R8).
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, override
from unittest.mock import patch

import msgspec
from hypothesis import strategies as st
from hypothesis.stateful import invariant, rule

import swarmq.client.schedule as client_schedule_module
import swarmq.scheduler.loop as loop_module
import swarmq.scheduler.stepdown as stepdown_module
import swarmq.scheduler.sync as sync_module
from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.contracts import LockToken, PublishReceipt
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError, PublishIndeterminate, UnroutableMessage
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleDefinition, SchedulerLoop, ScheduleStore
from swarmq.scheduler.cron_time import next_fire_ms
from swarmq.scheduler.definition import parse_schedfire_key
from swarmq.scheduler.reports import cron_fingerprint, live_window_ms
from swarmq.signals import Signal, SignalDispatcher
from tests.support.inmemory import FakeClock, InMemoryBackend, InMemoryBroker
from tests.support.stateful import AsyncStateMachine

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.contracts.coordination import LockStore

CANDIDATES = ("A", "B")
CLI_CRON = "cli-1"
STATIC_CRON = "static-1"
CRON = "* * * * *"
TASK = "stateful_leader_ping"
QUEUE = "default"
ONESHOT_DELAYS_S = (1, 30, 75)
UNROUTABLE_ONESHOT_MAX_S = 600
STATIC_UNIQUE_FOR = (None, 120)
ADVANCE_S = (1, 4, 5, 10, 20, 25, 31, 60, 61)

PublishFault = Literal["connection", "indeterminate_lost", "indeterminate_delivered", "unroutable"]
PUBLISH_FAULTS: tuple[PublishFault | None, ...] = (
    None,
    "connection",
    "indeterminate_lost",
    "indeterminate_delivered",
    "unroutable",
)
MidTick = Literal["expire", "takeover"]
Kind = Literal["cli", "static", "oneshot"]

RECORDED_SIGNALS = (
    Signal.SCHEDULER_FIRE_UNROUTABLE,
    Signal.TASK_DUPLICATE_DETECTED,
    Signal.SCHEDULER_CRON_UNOWNED,
    Signal.SCHEDULER_STANDBY,
    Signal.SCHEDULER_LEADER,
    Signal.SCHEDULER_LEADER_FAILOVER,
)


def leader_config() -> SwarmQConfig:
    """Defaults bis auf die Frist für unroutbare Oneshots (sonst in 30
    Schritten nicht erreichbar)."""
    return SwarmQConfig(
        broker_url="amqp://unused",
        backend_url="valkey://unused",
        scheduler_unroutable_oneshot_max_s=float(UNROUTABLE_ONESHOT_MAX_S),
    )


class LeaderEnv(Protocol):
    """Store-Factory der Leader-Maschine (KTD2)."""

    @property
    def clock(self) -> FakeClock: ...

    @property
    def backend(self) -> InMemoryBackend | ValkeyBackend: ...

    async def advance(self, seconds: float) -> None: ...

    async def close(self) -> None: ...


# -- Beobachtungen ------------------------------------------------------------


@dataclass(frozen=True)
class PublishAttempt:
    cand: str
    name: str
    score: int
    task_id: str
    outcome: PublishFault | Literal["ok"]
    as_leader: bool
    unique: bool = False


@dataclass(frozen=True)
class CommitCall:
    cand: str
    owner: str
    name: str
    fired: int | None
    next_ms: int
    removes: bool
    holder: str | None
    before: int | None
    after: int | None
    result: bool

    @property
    def effective(self) -> bool:
        return self.result and self.holder == self.owner and self.fired is not None and self.before == self.fired


@dataclass(frozen=True)
class LockCall:
    cand: str
    op: Literal["acquire", "renew", "release"]
    result: bool | None  # None: geworfen


@dataclass(frozen=True)
class SignalSeen:
    cand: str
    signal: Signal
    kwargs: Mapping[str, Any]


Event = PublishAttempt | CommitCall | LockCall | SignalSeen


# -- Wrapper der Subjekte -----------------------------------------------------


class FakeTime:
    """Ersatz für das Modul ``time`` in den Scheduler-Modulen: ``time()`` liest
    die ``FakeClock``, alles andere geht ans echte Modul."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock

    def time(self) -> float:
        return self._clock.now_ms() / 1000

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


class CandidatePublisher:
    """Publisher eines Kandidaten mit schaltbarem Fehler vor dem gemeinsamen
    ``InMemoryBroker``; hält jeden Versuch fest."""

    def __init__(self, machine: LeaderMachine, cand: str, broker: InMemoryBroker) -> None:
        self._machine = machine
        self._cand = cand
        self._broker = broker
        self.fault: PublishFault | None = None

    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        await self._machine.run_mid_tick()
        headers = dict(headers or {})
        name, score = parse_schedfire_key(headers["x-swarmq-schedfire"])
        task_id = msgspec.json.decode(body)["task"]["id"]
        fault = self.fault
        state = self._machine.cands[self._cand].loop.state
        unique = "x-swarmq-unique-hash" in headers
        self._machine.events.append(
            PublishAttempt(self._cand, name, score, task_id, fault or "ok", as_leader=state == "leader", unique=unique)
        )
        if fault == "connection":
            raise BrokerConnectionError("publish failed (injected)")
        if fault == "unroutable":
            raise UnroutableMessage(f"no queue {queue!r} (injected)")
        if fault == "indeterminate_lost":
            # Gesendet, aber nicht angekommen: nichts im Broker.
            raise PublishIndeterminate("confirm lost, message lost (injected)")
        receipt = await self._broker.publish(queue, body, headers)
        if fault == "indeterminate_delivered":
            raise PublishIndeterminate("confirm lost, message delivered (injected)")
        return receipt


class CandidateLocks:
    """``LockStore`` eines Kandidaten: schaltbarer Valkey-Ausfall für
    ``acquire``/``renew``, Aufzeichnung von Erwerb, Renew und Freigabe."""

    def __init__(self, machine: LeaderMachine, cand: str, inner: Callable[[], LockStore]) -> None:
        self._machine = machine
        self._cand = cand
        self._inner = inner
        self.down = False

    def _record(self, op: Literal["acquire", "renew", "release"], result: bool | None) -> None:
        self._machine.events.append(LockCall(self._cand, op, result))

    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken | None:
        if self.down:
            self._record("acquire", None)
            raise BackendConnectionError("valkey down (injected)")
        token = await self._inner().acquire(name, owner=owner, ttl_ms=ttl_ms, max_concurrency=max_concurrency)
        self._record("acquire", token is not None)
        return token

    async def renew(self, token: LockToken, ttl_ms: int) -> bool:
        if self.down:
            self._record("renew", None)
            raise BackendConnectionError("valkey down (injected)")
        renewed = await self._inner().renew(token, ttl_ms)
        self._record("renew", renewed)
        return renewed

    async def release(self, token: LockToken) -> bool:
        if self.down:
            # stepdown.py: die Freigabe scheitert, das Lock läuft per TTL ab.
            self._record("release", None)
            raise BackendConnectionError("valkey down (injected)")
        released = await self._inner().release(token)
        self._record("release", released)
        return released

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner(), name)


class RecordingScheduleStore(ScheduleStore):
    """``ScheduleStore`` eines Loops, der jeden ``commit_fire`` samt Halter
    und Score davor und danach festhält."""

    def __init__(self, backend: Any, machine: LeaderMachine, cand: str) -> None:
        super().__init__(backend)
        self._machine = machine
        self._cand = cand
        self._schedules = backend.schedules
        self._locks = backend.locks

    @override
    async def commit_fire(
        self,
        schedule_name: str,
        next_fire_ts_ms: int,
        updated_def: ScheduleDefinition | None,
        owner_token: str,
        *,
        fired_ts_ms: int | None = None,
    ) -> bool:
        before = await self._schedules.fire_ts(schedule_name)
        holder = await self._locks.current_owner(SCHEDULER_LOCK_NAME)
        result = await super().commit_fire(
            schedule_name, next_fire_ts_ms, updated_def, owner_token, fired_ts_ms=fired_ts_ms
        )
        after = await self._schedules.fire_ts(schedule_name)
        self._machine.events.append(
            CommitCall(
                self._cand,
                owner_token,
                schedule_name,
                fired_ts_ms,
                next_fire_ts_ms,
                updated_def is None,
                holder,
                before,
                after,
                result,
            )
        )
        return result


class DoubleAddSchedules:
    """``backend.schedules`` mit verlorener Antwort beim Speichern: das
    Insert läuft, valkey-py wiederholt es, die Wiederholung meldet ``False``
    (F3-01, E-058)."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def add(self, name: str, payload: bytes, fire_ts_ms: int | None) -> bool:
        await self._inner.add(name, payload, fire_ts_ms)
        return cast("bool", await self._inner.add(name, payload, fire_ts_ms))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class ReplyLostBackend:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.schedules = DoubleAddSchedules(inner.schedules)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# -- Modell -------------------------------------------------------------------


@dataclass
class Entry:
    kind: Kind
    gen: int
    task_id: str | None = None
    unroutable_since: int | None = None


@dataclass
class Series:
    """Modell der Fehlerserie aus E-013 (``PublishStepdown``)."""

    count: int = 0
    first_ms: int = 0
    last_ms: int = 0

    def reset(self) -> None:
        self.count = 0

    def failure(self, now_ms: int, window_ms: int) -> bool:
        if self.count == 0 or now_ms - self.last_ms > window_ms:
            self.count = 0
            self.first_ms = now_ms
        self.last_ms = now_ms
        self.count += 1
        if self.count >= 2 and now_ms - self.first_ms >= window_ms:
            self.count = 0
            return True
        return False


@dataclass(eq=False)
class Cand:
    name: str
    owner: str
    loop: SchedulerLoop
    store: RecordingScheduleStore
    publisher: CandidatePublisher
    locks: CandidateLocks
    was_leader: bool = False
    term_start_ms: int | None = None
    adopted: bool = False
    reporting_at_adopt: bool = False
    # L10: static-1 bei der letzten Übernahme der Amtszeit verwaist (E-014),
    # und ob der Loop ihn gerade zurückhält (``_held_crons``).
    unowned_at_adopt: bool = False
    holding: bool = False
    last_ok_ms: int | None = None
    series: Series = field(default_factory=Series)
    stepdown_at_ms: int | None = None

    @override
    def __repr__(self) -> str:
        return f"Cand({self.name}, {self.loop.state})"


class LeaderMachine(AsyncStateMachine[LeaderEnv]):
    def __init__(self, open_env: Callable[[], Coroutine[Any, Any, LeaderEnv]]) -> None:
        self._patches = contextlib.ExitStack()
        super().__init__(open_env)
        try:
            self._setup()
        except BaseException:
            self.teardown()
            raise

    def _setup(self) -> None:
        clock = self.env.clock
        fake_time = FakeTime(clock)
        for module in (loop_module, sync_module, client_schedule_module):
            self._patches.enter_context(patch.object(module, "time", fake_time))
        self._patches.enter_context(patch.object(stepdown_module, "_monotonic", fake_time.time))
        self.start_ms = clock.now_ms()
        self.run(self._install_loop_clock())

        self.config = leader_config()
        ttl_s = self.config.scheduler_lock_ttl
        self.ttl_ms = round(ttl_s * 1000)
        margin = max(0.0, min(self.config.backend_op_timeout, (ttl_s - self.config.scheduler_heartbeat_interval) / 2))
        self.window_ms = round((ttl_s - margin) * 1000)
        assert self.config.scheduler_publish_stepdown_s is not None
        self.stepdown_window_ms = round(self.config.scheduler_publish_stepdown_s * 1000)
        self.seen_ms = round(self.config.scheduler_seen_interval_s * 1000)
        self.heartbeat_s = round(self.config.scheduler_heartbeat_interval)

        self.events: list[Event] = []
        self.mid_tick: Callable[[], Awaitable[None]] | None = None
        self.entries: dict[str, Entry] = {}
        self.index: dict[str, int] = {}
        self.committed: set[tuple[str, int, int]] = set()
        self.published_oneshots: set[str] = set()
        self.dropped_oneshots: set[str] = set()
        # Ausgänge der wirksamen Commits je Grund (für Suchläufe und Bericht).
        self.outcomes: collections.Counter[str] = collections.Counter()
        self._gen = 0
        self.reporting = False
        self.ever_reported = False
        self.last_report_ms: int | None = None
        # ``sq:{sched}:report:known`` enthält static-1: gemeldet und seither
        # nicht per ``remove_schedule`` vergessen (``definition.py``).
        self.static_known = False
        # L10 nur im Fake: Valkey führt die Meldungen mit Serverzeit.
        self.reports_age = isinstance(self.env.backend, InMemoryBackend)
        self.static_unique_for: int | None = None
        # Unique-Claim der Cron-Fires von static-1 (ein Hash für alle Fires):
        # gehalten bis ``static_claim_until_ms`` (E-011, BZ-09/B7).
        self.static_claim_until_ms = 0

        self.broker = InMemoryBroker(clock)
        self.run(self.broker.connect())
        backend = cast("Any", self.env.backend)
        self.admin = ScheduleStore(backend)
        self.client = Client(broker=cast("Any", self.broker), backend=backend)
        self.lossy_client = Client(broker=cast("Any", self.broker), backend=cast("Any", ReplyLostBackend(backend)))
        self.cands: dict[str, Cand] = {}
        for name in CANDIDATES:
            self.cands[name] = self._candidate(name, backend)

    def _candidate(self, name: str, backend: Any) -> Cand:
        owner = f"cand-{name}"
        signals = SignalDispatcher()
        for signal in RECORDED_SIGNALS:
            signals.connect(signal, self._signal_recorder(name, signal))
        store = RecordingScheduleStore(backend, self, name)
        publisher = CandidatePublisher(self, name, self.broker)
        locks = CandidateLocks(self, name, lambda: self.env.backend.locks)
        loop = SchedulerLoop(
            backend=backend,
            broker=cast("Any", publisher),
            config=self.config,
            lock_manager=cast("LockStore", locks),
            store=store,
            signal_dispatcher=signals,
            owner_token=owner,
            worker_id=f"worker-{name}",
            version="0.12.3",
        )
        return Cand(name, owner, loop, store, publisher, locks)

    def _signal_recorder(self, cand: str, signal: Signal) -> Callable[..., Awaitable[None]]:
        async def record(**kwargs: Any) -> None:
            self.events.append(SignalSeen(cand, signal, kwargs))

        return record

    async def _install_loop_clock(self) -> None:
        """Uhr der Event-Loop = echte Zeit plus Vorlauf der ``FakeClock``
        (Zeitgrenze und Renew-Budget aus E-048)."""
        loop = asyncio.get_running_loop()
        real = loop.time
        clock = self.env.clock
        start = self.start_ms

        def shifted() -> float:
            return real() + (clock.now_ms() - start) / 1000

        loop.time = shifted  # type: ignore[method-assign]  # Zeitsprung der FakeClock auch für asyncio.timeout

    # -- Hilfen ----------------------------------------------------------------

    @property
    def now_ms(self) -> int:
        return self.env.clock.now_ms()

    def cron_next(self) -> int:
        return next_fire_ms(CRON, None, self.now_ms)

    async def run_mid_tick(self) -> None:
        hook, self.mid_tick = self.mid_tick, None
        if hook is not None:
            await hook()

    def _new_gen(self) -> int:
        self._gen += 1
        return self._gen

    def _store_view(self) -> tuple[set[str], dict[str, int]]:
        async def read() -> tuple[set[str], dict[str, int]]:
            schedules = self.env.backend.schedules
            names = set(await schedules.list_all())
            scores: dict[str, int] = {}
            for name in names | set(self.index) | {CLI_CRON, STATIC_CRON}:
                score = await schedules.fire_ts(name)
                if score is not None:
                    scores[name] = score
            return names, scores

        return self.run(read())

    def _after_tick(self) -> None:
        """Übergang in eine neue Amtszeit (``_begin_term``,
        ``_reset_publish_stepdown``) im Modell nachziehen."""
        for cand in self.cands.values():
            leader = cand.loop.state == "leader"
            if leader and not cand.was_leader:
                cand.term_start_ms = self.now_ms
                cand.adopted = False
                cand.unowned_at_adopt = False
                cand.holding = False
                cand.series.reset()
            cand.was_leader = leader

    def _events_since(self, start: int) -> list[Event]:
        return self.events[start:]

    # -- Wahl ------------------------------------------------------------------

    def election_tick(self, cand_name: str) -> None:
        cand = self.cands[cand_name]
        start = len(self.events)
        self.run(cand.loop._election_tick())
        self._check_election(cand, self._events_since(start))
        self._after_tick()

    def _check_election(self, cand: Cand, events: Sequence[Event]) -> None:
        now = self.now_ms
        calls = [e for e in events if isinstance(e, LockCall) and e.cand == cand.name]
        for call in calls:
            if call.op == "acquire" and cand.stepdown_at_ms is not None:
                cooldown_end = cand.stepdown_at_ms + 2 * self.ttl_ms
                assert now >= cooldown_end, (
                    f"{cand}: Erwerbsversuch {now - cand.stepdown_at_ms} ms nach Rücktritt (E-013)"
                )
            if call.result is True and call.op in ("acquire", "renew"):
                cand.last_ok_ms = now
        if any(c.op == "renew" and c.result is False for c in calls):
            assert cand.loop.state != "leader", f"{cand}: Renew False, trotzdem Leader (E-048)"
        if cand.loop.state == "leader":
            assert cand.last_ok_ms is not None
            age = now - cand.last_ok_ms
            assert age < self.window_ms, f"{cand}: Leader {age} ms nach dem letzten Erfolg (E-048, Zeitgrenze)"

    @rule(cand=st.sampled_from(CANDIDATES))
    def tick_election(self, cand: str) -> None:
        self.election_tick(cand)

    # -- Übernahme der Meldungen -----------------------------------------------

    def adopt_tick(self, cand_name: str) -> None:
        """Wie ein Durchlauf von ``_adopt_loop`` (ohne BZ-14)."""
        cand = self.cands[cand_name]
        loop = cand.loop
        if loop.state != "leader":
            return
        names_before, scores_before = self._store_view()

        async def adopt() -> None:
            try:
                async with loop._fire_lock:
                    await loop.adopt_reported_crons()
            finally:
                loop._note_adopt_ran()

        self.run(adopt())
        cand.adopted = True
        cand.reporting_at_adopt = self.reporting
        cand.unowned_at_adopt = self.reports_age and self.static_known and not self._static_reported_live()
        names_after, scores_after = self._store_view()
        self._check_adopt(names_before, scores_before, names_after, scores_after)
        self._after_tick()

    def _static_reported_live(self) -> bool:
        """Lebt eine Meldung von static-1 (``choose_reports``)?"""
        window = live_window_ms(self.config.scheduler_seen_interval_s)
        return self.last_report_ms is not None and self.last_report_ms >= self.now_ms - window

    def _check_adopt(
        self,
        names_before: set[str],
        scores_before: dict[str, int],
        names_after: set[str],
        scores_after: dict[str, int],
    ) -> None:
        for name in names_before | names_after | set(scores_before) | set(scores_after):
            if name == STATIC_CRON:
                continue
            assert (name in names_before) == (name in names_after), f"Übernahme änderte {name} (L8)"
            assert scores_before.get(name) == scores_after.get(name), f"Übernahme verschob {name} (L8)"
        had, has = STATIC_CRON in names_before, STATIC_CRON in names_after
        assert not (had and not has), "Übernahme entfernte static-1 (E-014, E-049)"
        if not had and has:
            assert self.ever_reported, "Übernahme legte static-1 ohne Meldung an (L8)"
            assert scores_after.get(STATIC_CRON) == self.cron_next(), "static-1 nicht auf den nächsten Termin (L8)"
            self.entries[STATIC_CRON] = Entry("static", self._new_gen())
        if self.reporting:
            assert has, "gemeldeter static-1 nach der Übernahme nicht angelegt (L8)"
        if has:
            before, after = scores_before.get(STATIC_CRON), scores_after.get(STATIC_CRON)
            assert after in {before, self.cron_next()}, f"static-1 von {before} auf {after} geplant (L8)"
            if after is None:
                self.index.pop(STATIC_CRON, None)
            else:
                self.index[STATIC_CRON] = after

    @rule(cand=st.sampled_from(CANDIDATES))
    def tick_adopt(self, cand: str) -> None:
        self.adopt_tick(cand)

    # -- Poll ------------------------------------------------------------------

    def poll_tick(self, cand_name: str, mid: MidTick | None = None) -> None:
        """Wie ein Durchlauf von ``_poll_loop``; ``mid`` lässt das Leader-Lock
        beim ersten Publish ablaufen (``expire``) oder vom anderen Kandidaten
        übernehmen (``takeover``)."""
        cand = self.cands[cand_name]
        loop = cand.loop
        if loop.state != "leader":
            return
        other = next(c for c in self.cands.values() if c is not cand)
        start = len(self.events)
        index_before = dict(self.index)
        static_unique_for = self._stored_unique_for()
        entries_before = {name: Entry(e.kind, e.gen, e.task_id, e.unroutable_since) for name, e in self.entries.items()}
        if mid is not None:
            self.mid_tick = self._mid_hook(mid, other)

        async def poll() -> None:
            async with loop._fire_lock:
                await loop._poll_tick()

        self.run(poll())
        self.mid_tick = None
        events = self._events_since(start)
        self._check_static_claim(events, static_unique_for)
        self._check_poll(cand, events, index_before, entries_before)
        other_calls = [e for e in events if isinstance(e, LockCall) and e.cand == other.name]
        if other_calls:  # Übernahme mitten im Tick
            self._check_election(other, other_calls)
        self._after_tick()

    def _stored_unique_for(self) -> int | None:
        """``unique_for`` der gespeicherten Definition von static-1 (so feuert der Poll)."""
        raw = self.run(self.env.backend.schedules.get(STATIC_CRON))
        if raw is None:
            return None
        return msgspec.json.decode(raw, type=ScheduleDefinition).unique_for

    def _check_static_claim(self, events: list[Event], unique_for: int | None) -> None:
        """Modell des Unique-Claims von static-1: ein Fire mit ``unique_for``
        claimt nur bei freiem Flag und publiziert dann; bei Verbindungsfehler
        oder unroutbar gibt es den eigenen Claim frei, bei Erfolg und
        ``PublishIndeterminate`` bleibt er (E-011, BZ-09/B7)."""
        now = self.now_ms
        for attempt in (e for e in events if isinstance(e, PublishAttempt) and e.name == STATIC_CRON):
            assert attempt.unique == (unique_for is not None), f"static-1: Unique-Header={attempt.unique} (E-011)"
            if unique_for is None:
                continue
            assert now >= self.static_claim_until_ms, (
                f"static-1 publiziert trotz gehaltenem Claim bis {self.static_claim_until_ms} (E-011)"
            )
            if attempt.outcome in ("connection", "unroutable"):
                self.static_claim_until_ms = 0
            else:
                self.static_claim_until_ms = now + unique_for * 1000

    def _mid_hook(self, mid: MidTick, other: Cand) -> Callable[[], Awaitable[None]]:
        async def hook() -> None:
            await self.env.backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            if mid == "takeover":
                await other.loop._election_tick()

        return hook

    def _check_poll(
        self,
        cand: Cand,
        events: list[Event],
        index_before: dict[str, int],
        entries_before: dict[str, Entry],
    ) -> None:
        now = self.now_ms
        publishes = [e for e in events if isinstance(e, PublishAttempt)]
        seen: set[tuple[str, str, int]] = set()
        for attempt in publishes:
            key = (attempt.cand, attempt.name, attempt.score)
            assert key not in seen, f"{attempt.cand}: {attempt.name}:{attempt.score} zweimal in einem Poll-Takt (E-047)"
            seen.add(key)
            assert index_before.get(attempt.name) == attempt.score <= now, (
                f"Publish {attempt.name}:{attempt.score} ohne fälligen Fire-Index-Eintrag (L2, F3-01)"
            )
            entry = entries_before[attempt.name]
            if entry.kind == "oneshot":
                assert attempt.task_id == entry.task_id, (
                    f"Oneshot unter {attempt.task_id} statt {entry.task_id} (BZ-02)"
                )
                if attempt.outcome in ("ok", "indeterminate_delivered"):
                    self.published_oneshots.add(attempt.name)
            term_age = None if cand.term_start_ms is None else now - cand.term_start_ms
            if not cand.adopted and term_age is not None:
                assert term_age >= 5_000, f"{cand}: Publish {term_age} ms nach Amtsantritt vor der Übernahme (L7)"
        self._check_stepdown(cand, publishes, events)
        self._check_unowned_held(cand, events)
        for commit in (e for e in events if isinstance(e, CommitCall)):
            self._check_commit(commit, events, entries_before)
        if any(isinstance(e, CommitCall) and e.cand == cand.name and not e.result for e in events):
            assert cand.loop.state != "leader", f"{cand}: Commit abgelehnt, trotzdem Leader (L1, E-047)"

    def _check_unowned_held(self, cand: Cand, events: list[Event]) -> None:
        """L10: nach einer Übernahme ohne lebende Meldung hält der Kandidat
        static-1 zurück, der erste Rückhalt mit Signal (E-014)."""
        attempts = [
            e for e in events if isinstance(e, PublishAttempt) and e.cand == cand.name and e.name == STATIC_CRON
        ]
        commits = [e for e in events if isinstance(e, CommitCall) and e.cand == cand.name and e.name == STATIC_CRON]
        if not (cand.adopted and cand.unowned_at_adopt):
            if attempts or commits:
                cand.holding = False  # gefeuert: ``_release_unowned``
            return
        assert not attempts, f"{cand}: verwaister static-1 publiziert {attempts} (E-014)"
        signals = [
            e.kwargs
            for e in events
            if isinstance(e, SignalSeen)
            and e.cand == cand.name
            and e.signal is Signal.SCHEDULER_CRON_UNOWNED
            and e.kwargs["schedule_name"] == STATIC_CRON
        ]
        for commit in commits:
            assert commit.next_ms == self.now_ms + self.seen_ms, (
                f"{cand}: verwaister static-1 auf {commit.next_ms - self.now_ms} ms statt zurückgehalten (E-014)"
            )
        if commits and not cand.holding:
            assert [kwargs["held"] for kwargs in signals] == [True], (
                f"{cand}: erster Rückhalt von static-1 mit Signalen {signals} (E-014)"
            )
            cand.holding = True
        elif commits:
            assert not signals, f"{cand}: Signal {signals} ohne Übergang (E-014)"

    def _check_stepdown(self, cand: Cand, publishes: list[PublishAttempt], events: list[Event]) -> None:
        expected = False
        for attempt in (p for p in publishes if p.cand == cand.name):
            if attempt.outcome == "ok":
                cand.series.reset()
            elif attempt.outcome == "connection" and attempt.as_leader:
                expected = cand.series.failure(self.now_ms, self.stepdown_window_ms) or expected
        released = any(isinstance(e, LockCall) and e.cand == cand.name and e.op == "release" for e in events)
        assert released is expected, f"{cand}: Rücktritt={released}, Modell={expected} (E-013)"
        if released:
            assert cand.loop.state == "standby", f"{cand}: nach Rücktritt {cand.loop.state} (E-013)"
            cand.stepdown_at_ms = self.now_ms

    def _check_commit(self, commit: CommitCall, events: list[Event], entries_before: dict[str, Entry]) -> None:
        if commit.holder != commit.owner:
            assert not commit.result, f"Commit von {commit.owner} trotz Halter {commit.holder} (L1, E-047)"
            return
        assert commit.result, f"Commit des Halters {commit.owner} abgelehnt (L1)"
        if not commit.effective:
            return
        assert commit.fired is not None
        name, now = commit.name, self.now_ms
        entry = self.entries.get(name) or entries_before[name]
        identity = (name, entry.gen, commit.fired)
        assert identity not in self.committed, f"zweiter wirksamer Commit für {identity} (L1, E-047)"
        self.committed.add(identity)
        attempts = [
            e
            for e in events
            if isinstance(e, PublishAttempt) and e.cand == commit.cand and e.name == name and e.score == commit.fired
        ]
        delivered = any(a.outcome == "ok" for a in attempts)
        unroutable = any(a.outcome == "unroutable" for a in attempts)
        signals = [e for e in events if isinstance(e, SignalSeen) and e.cand == commit.cand]
        if entry.kind == "oneshot":
            self._check_oneshot_commit(commit, entry, delivered=delivered, unroutable=unroutable)
            return
        cron_next = self.cron_next()
        if delivered:
            reason = "published"
            expected = cron_next
        elif unroutable:
            lost = [
                s for s in signals if s.signal is Signal.SCHEDULER_FIRE_UNROUTABLE and s.kwargs["schedule_name"] == name
            ]
            assert lost, f"{name}: unroutbar ohne Signal (E-003)"
            assert lost[-1].kwargs["kind"] == "cron_lost", f"{name}: unroutbar ohne Zählung (E-003)"
            reason, expected = "E-003", cron_next
        elif entry.kind == "static" and any(s.signal is Signal.TASK_DUPLICATE_DETECTED for s in signals):
            assert now < self.static_claim_until_ms, f"{name}: Duplikat ohne gehaltenen Claim, Fire verloren (E-011)"
            reason, expected = "E-011/E-032", cron_next
        elif entry.kind == "static" and commit.next_ms == now + self.seen_ms:
            cand = self.cands[commit.cand]
            assert cand.adopted, f"{name}: zurückgehalten ohne Übernahme in der Amtszeit (E-014)"
            assert not cand.reporting_at_adopt, f"{name}: zurückgehalten, obwohl lebend gemeldet (E-014)"
            reason, expected = "E-014", now + self.seen_ms
        else:
            raise AssertionError(f"{name}:{commit.fired} festgeschrieben ohne Zustellung (E-046), Versuche {attempts}")
        self.outcomes[f"cron {reason}"] += 1
        assert not commit.removes, f"{name}: Cron-Definition beim Commit entfernt ({reason})"
        assert commit.next_ms == expected, f"{name}: nächster Termin {commit.next_ms}, erwartet {expected} ({reason})"
        self.index[name] = expected

    def _check_oneshot_commit(self, commit: CommitCall, entry: Entry, *, delivered: bool, unroutable: bool) -> None:
        name, now = commit.name, self.now_ms
        if delivered:
            assert commit.removes, f"{name}: zugestellter Oneshot bleibt stehen"
            assert commit.next_ms < 0, f"{name}: zugestellter Oneshot mit Folgetermin"
            self.outcomes["oneshot published"] += 1
            self._forget(name)
            return
        assert unroutable, f"{name}:{commit.fired} Oneshot festgeschrieben ohne Zustellung (E-046)"
        since = entry.unroutable_since if entry.unroutable_since is not None else now
        if now - since >= UNROUTABLE_ONESHOT_MAX_S * 1000:
            assert commit.removes, f"{name}: unroutbar seit {now - since} ms, nicht verworfen (E-004)"
            assert entry.task_id is not None
            result = self.run(self.env.backend.get_result(entry.task_id))
            assert result is not None, f"{name}: verworfen ohne Fehlerergebnis (E-004)"
            self.dropped_oneshots.add(name)
            self.outcomes["oneshot E-004 drop"] += 1
            self._forget(name)
            return
        assert commit.next_ms == now + 60_000, f"{name}: zurückgestellt auf {commit.next_ms - now} ms (E-004)"
        raw = self.run(self.env.backend.schedules.get(name))
        assert raw is not None
        stored = msgspec.json.decode(raw, type=ScheduleDefinition)
        assert stored.unroutable_since == since, (
            f"{name}: unroutable_since {stored.unroutable_since} != {since} (E-004)"
        )
        self.entries[name].unroutable_since = since
        self.outcomes["oneshot E-004 defer"] += 1
        self.index[name] = commit.next_ms

    def _forget(self, name: str) -> None:
        self.entries.pop(name, None)
        self.index.pop(name, None)

    @rule(cand=st.sampled_from(CANDIDATES), mid=st.sampled_from((None, None, None, "expire", "takeover")))
    def tick_poll(self, cand: str, mid: MidTick | None) -> None:
        self.poll_tick(cand, mid)

    # -- Crons und Oneshots ----------------------------------------------------

    def add_cli_cron(self) -> None:
        score = self.cron_next()
        definition = ScheduleDefinition(
            schedule_name=CLI_CRON,
            task_name=TASK,
            args=(),
            kwargs={},
            queue=QUEUE,
            cron=CRON,
            fire_ts_ms=score,
            source="cli",
        )
        added = self.run(self.admin.add_schedule(definition))
        assert added is (CLI_CRON not in self.entries), f"add_schedule={added} (CD11)"
        if added:
            self.entries[CLI_CRON] = Entry("cli", self._new_gen())
            self.index[CLI_CRON] = score

    @rule()
    def cli_cron_add(self) -> None:
        self.add_cli_cron()

    def remove_cron(self, name: str) -> None:
        removed = self.run(self.admin.remove_schedule(name))
        assert removed is (name in self.entries), f"remove_schedule({name})={removed}"
        if name == STATIC_CRON:
            self.static_known = False
        self._forget(name)

    @rule(name=st.sampled_from((CLI_CRON, STATIC_CRON)))
    def cron_remove(self, name: str) -> None:
        self.remove_cron(name)

    def report_static(self, unique_for: int | None) -> None:
        """Ein Worker ohne Kandidatur meldet ``static-1`` (wie ``_report_crons``)."""
        definition = ScheduleDefinition(
            schedule_name=STATIC_CRON,
            task_name=TASK,
            args=(),
            kwargs={},
            queue=QUEUE,
            cron=CRON,
            fire_ts_ms=None,
            source="static",
            unique_for=unique_for,
            unique_until="completion",
        )
        self.run(
            self.env.backend.schedules.report_cron(
                STATIC_CRON,
                cron_fingerprint(definition),
                msgspec.json.encode(definition),
                live_window_ms=live_window_ms(self.config.scheduler_seen_interval_s),
            )
        )
        self.reporting = True
        self.ever_reported = True
        self.last_report_ms = self.now_ms
        self.static_known = True
        self.static_unique_for = unique_for

    @rule(unique_for=st.sampled_from(STATIC_UNIQUE_FOR))
    def static_report(self, unique_for: int | None) -> None:
        self.report_static(unique_for)

    @rule()
    def static_withdraw(self) -> None:
        """Der meldende Worker läuft mit neuem Code ohne ``static-1``."""
        self.reporting = False

    @rule()
    def static_withdraw_and_outlive(self) -> None:
        """Der meldende Worker verstummt, die Loops laufen über das
        Lebendigkeitsfenster der Meldung hinaus; danach übernimmt jeder Leader
        die Meldungen und pollt bis zum nächsten Termin (L10). Ohne diese Regel
        erreicht die Suche den Rückhalt (E-014) kaum: Sie braucht dafür mehr
        als drei lange Zeitsprünge in Folge und eine Übernahme danach."""
        self.static_withdraw()
        window_s = live_window_ms(self.config.scheduler_seen_interval_s) // 1000
        self.run_for(window_s + self.heartbeat_s, blocked=None)
        for name in CANDIDATES:
            self.adopt_tick(name)
        self.run_for(self.seen_ms // 1000, blocked=None)

    def schedule_oneshot(self, delay_s: int, *, reply_lost: bool = False) -> str:
        client = self.lossy_client if reply_lost else self.client
        due = self.now_ms + delay_s * 1000
        task_id = self.run(client.schedule(TASK, queue=QUEUE, delay=delay_s))
        name = f"oneshot:{task_id}"
        self.entries[name] = Entry("oneshot", self._new_gen(), task_id=task_id)
        self.index[name] = due
        stored = self.run(self.env.backend.schedules.fire_ts(name))
        assert stored == due, f"{name}: Fire-Index {stored}, erwartet {due} (E-058, F3-01)"
        return task_id

    @rule(delay_s=st.sampled_from(ONESHOT_DELAYS_S), reply_lost=st.booleans())
    def oneshot_schedule(self, delay_s: int, reply_lost: bool) -> None:
        self.schedule_oneshot(delay_s, reply_lost=reply_lost)

    # -- Zeit und Störungen ----------------------------------------------------

    def advance(self, seconds: int) -> None:
        self.run(self.env.advance(seconds))
        if self.reporting:
            # Der meldende Worker meldet im Meldetakt; hier mindestens nach
            # jedem Zeitsprung, damit "meldet" stets "lebend gemeldet" heißt.
            self.report_static(self.static_unique_for)

    def run_for(self, seconds: int, blocked: str | None) -> None:
        """Zeit vorstellen, während die Loops laufen: je Heartbeat höchstens ein
        Schritt; jeder nicht blockierte Kandidat taktet danach Wahl, als Leader
        die erste Übernahme der Amtszeit und den Poll. ``blocked="both"``
        stellt nur die Uhr vor (alle Loops stehen)."""
        while seconds > 0:
            step = min(seconds, self.heartbeat_s)
            self.advance(step)
            seconds -= step
            for name in CANDIDATES:
                if blocked in (name, "both"):
                    continue
                self.election_tick(name)
                if not self.cands[name].adopted:
                    self.adopt_tick(name)
                self.poll_tick(name)

    @rule(seconds=st.sampled_from(ADVANCE_S), blocked=st.sampled_from((None, None, "A", "B", "both")))
    def clock_advance(self, seconds: int, blocked: str | None) -> None:
        self.run_for(seconds, blocked)

    @rule(cand=st.sampled_from(CANDIDATES), fault=st.sampled_from(PUBLISH_FAULTS))
    def publish_fault(self, cand: str, fault: PublishFault | None) -> None:
        self.cands[cand].publisher.fault = fault

    @rule(cand=st.sampled_from(CANDIDATES), down=st.booleans())
    def lock_fault(self, cand: str, down: bool) -> None:
        """Valkey für den Lock-Pfad des Kandidaten nicht erreichbar."""
        self.cands[cand].locks.down = down

    @rule()
    def leader_lock_lost(self) -> None:
        """Der Leader-Key verschwindet (Valkey-Ausfall, Failover ohne Daten)."""
        self.run(self.env.backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1))

    # -- Lebendigkeit ----------------------------------------------------------

    def settle(self) -> Cand:
        """Störungen aus; Wahl-Takte bis zu einem Leader, dann Übernahme und
        Poll. Danach ist nichts mehr fällig (L9, E-046)."""
        for cand in self.cands.values():
            cand.publisher.fault = None
            cand.locks.down = False
        leader: Cand | None = None
        for _ in range(12):
            for name in CANDIDATES:
                self.election_tick(name)
            holder = self.run(self.env.backend.locks.current_owner(SCHEDULER_LOCK_NAME))
            leader = next((c for c in self.cands.values() if c.loop.state == "leader" and c.owner == holder), None)
            if leader is not None:
                break
            self.advance(self.heartbeat_s)
        assert leader is not None, "nach 12 Heartbeats ohne Störung kein Leader (L9)"
        self.adopt_tick(leader.name)
        self.poll_tick(leader.name)
        due = {name: score for name, score in self._store_view()[1].items() if score <= self.now_ms}
        assert not due, f"nach Übernahme und Poll noch fällig: {due} (L9, E-046)"
        return leader

    @rule()
    def settle_all(self) -> None:
        self.settle()

    # -- Invarianten -----------------------------------------------------------

    @invariant()
    def store_matches_model(self) -> None:
        """L0."""
        names, scores = self._store_view()
        assert names == set(self.entries), f"Definitionen {sorted(names)}, Modell {sorted(self.entries)} (L0)"
        assert scores == self.index, f"Fire-Index {scores}, Modell {self.index} (L0)"

    def verify(self) -> None:
        self.store_matches_model()

    # -- Abbau -----------------------------------------------------------------

    @override
    async def close_subjects(self) -> None:
        for cand in getattr(self, "cands", {}).values():
            await cand.loop.abort()
        broker = getattr(self, "broker", None)
        if broker is not None:
            await broker.close()

    @override
    def teardown(self) -> None:
        try:
            super().teardown()
        finally:
            self._patches.close()


__all__ = [
    "CANDIDATES",
    "CLI_CRON",
    "STATIC_CRON",
    "CommitCall",
    "LeaderEnv",
    "LeaderMachine",
    "LockCall",
    "PublishAttempt",
]
