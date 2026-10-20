"""Lock-Maschine (Fehlersuche Runde 4, Plan 2026-10-17-0941, U6; R6-R8, KTD1-KTD3).

Subjekte: der ``LockStore`` der Umgebung (``InMemoryLockStore`` oder
``ValkeyLockStore`` über ``backend.locks``), je Worker ein echter
``LockKeeper`` mit Fake-Uhr und geparktem ``sleep`` (die Maschine taktet ihn),
Unique-Claims über den echten ``Client.schedule`` und die echte
``finalize.maybe_release_unique`` gegen ``claim_unique``/``release_unique``
des Backends. Der Broker ist in beiden Stufen ein ``InMemoryBroker``.

Modell: Lock-Name -> (Owner, Ablauf in ms); laufende Handler (Lock erworben,
Lease im Keeper); Locks ohne Keeper (wie das Leader-Lock, mit ``renew``);
Dedup-Hash -> (Owner, Ablauf); Nachrichten mit Modus, Start, Ende und Zahl der
Freigabeaufrufe. Die Zeit läuft nur über ``advance``; alle Fristen sind ganze
Sekunden.

Invarianten (Quelle):

- I1 Store gleich Modell: Halter und Rest-TTL jedes Locks stimmen mit dem
  Modell überein, bis auf die Toleranz der Umgebung (Valkey: Laufzeit der
  Befehle). Quelle: ``LockStore``-Vertrag (``contracts/coordination.py``,
  ``tests/contracts/coordination.py::LockStoreContract``).
- I2 Nur der Owner gibt frei: ``release``, ``renew`` und ``extend`` mit
  fremdem Token ändern nichts und liefern ``False``; ``release_unique`` mit
  fremdem Owner ebenso. Quelle:
  ``docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md`` Abschnitt 3
  („Pattern 3“ im Plan), ``lock_release.lua``, ``unique_release.lua``.
- I3 Nie zwei Owner: Je Lock höchstens ein laufender Handler ohne erlaubte
  Abweichung, und der ist Halter im Store; ein freier Lock wird erworben, ein
  gehaltener nicht. Quelle: E-022, ``lock_acquire.lua``.
- I4 Eine Lease läuft nur ab, wenn kein Takt kam: Taktet der Keeper eines
  Handlers rechtzeitig, behält der Handler Lock und Lease; nach jedem Takt hat
  jede gehaltene Lease mindestens ein Fenster Rest-TTL, die Frist wird nie
  verkürzt und höchstens auf ``max(bisherige Frist, jetzt + Fenster)`` gesetzt
  (Absturzfrist). Quelle: E-022, ``worker/lock_keeper.py`` (Modulbeschreibung).
- I5 Ein wiederholter Erwerb mit dem eigenen Token gewinnt, ohne die Frist neu
  zu setzen. Quelle: E-022 (Nachtrag FS-06b).
- I6 Der Rückbau nach einem Ausfall ist owner-geprüft: Ein Lock-Erwerb oder
  Unique-Claim mit verlorener Antwort, danach unter der eigenen frischen ID
  freigegeben, trifft nie einen fremden Lock oder ein fremdes Flag. Quelle:
  E-052.
- I7 ``unique_until="start"``: je Nachricht genau eine Freigabe; nach dem
  Start keine zweite; endet die Nachricht vor dem Start terminal, gibt das
  Ende frei (wie ``completion``). Quelle: E-034 samt Nachtrag FS-13.
- I8 ``unique_for=None`` claimt nie und überstimmt den TaskInfo-Default; ohne
  Hash gibt es keine Freigabe. Quelle: E-033.
- I9 Verlust wird gemeldet: Jede Lease, die ein Takt aus dem Keeper nimmt,
  meldet er mit genau einem Ereignis aus ``task_lock_lost`` (Stage
  ``running`` oder ``expired``) und ``lock_extend_unsupported``; bei
  ``running`` hält der Store den Lock beobachtbar nicht mehr (``observe``).
  Eine Lease, die der Takt behält, auch nach ``lock_extend_failed``, erzeugt
  kein Verlustereignis. Quelle: E-022, ``worker/lock_keeper.py``
  (``tick_once``, ``_extend``).

Erlaubte Abweichungen (KTD3):

- E-022: Bei blockiertem Loop (Regel ``advance`` mit ``blocked``, ohne Takt
  mindestens ein Fenster lang) läuft der Lock ab; der Handler läuft weiter,
  ein zweiter Halter darf erwerben, der Keeper meldet ``task_lock_lost``.
  Der betroffene Handler trägt danach ``excused``.
- E-022, Valkey-Ausfall beim Verlängern (Regel ``tick_with_extend_fault``):
  ``lock_extend_failed`` lässt die Lease im Keeper und die Frist im Store
  unverändert; ein Store ohne ``extend`` (``lock_extend_unsupported``, Q15)
  nimmt sie heraus, der Lock läuft nach seiner TTL ab. Der betroffene
  Handler trägt danach ``excused``.
- E-007: Ein Erwerb im Konflikt liefert ``None``; das Lock-Gate stellt
  zurück und wartet zeitbasiert. Kein Fehler.
- E-052 (Restlücke F6): Läuft die Freigabe vor dem Erwerb, bleibt ein
  verwaister Lock bis zur TTL stehen (Regel ``reply_lost_acquire`` mit
  ``release_first``).
- ``force_release`` ist der Operator-Pfad ohne Owner-Check
  (``LockStore.force_release``); er nimmt einem laufenden Handler den Lock.
  Keine E-ID, Vertrag.
- E-034 (Bewusste Folge): zwischen Freigabe beim Start und Ende kein
  Dedup-Schutz; ein ``schedule`` mit denselben Argumenten läuft dann durch.

Nicht nachgebildet: Semaphoren (``max_concurrency > 1``). Ihre Member laufen
in Valkey nach der TTL des nächsten Erwerbers ab (E-022, Bewusste Folgen),
im Fake je Slot; ein gemeinsames Modell braucht eine eigene Entscheidung.

Der Startpunkt der Worker-Pipeline (``WorkerPipeline._release_unique_on_start``
und ``ctx.start_release_reached = True`` in ``worker/pipeline.py``) ist privat;
die Regel ``start_message`` bildet diese zwei Zeilen nach. Das Ende läuft über
die öffentliche ``maybe_release_unique``.

Bekannte Befunde: keine. Ein Befund würde per ``@precondition`` an der
auslösenden Regel mit Kommentar ``F4-nn`` ausgeklammert (KTD3, R8).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, override

from hypothesis import strategies as st
from hypothesis.stateful import invariant, precondition, rule

from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery, LockToken, PublishReceipt
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.registry import TaskRegistry
from swarmq.serialization import compute_dedup_hash
from swarmq.task import Task, TaskInfo
from swarmq.worker.finalize import maybe_release_unique
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import release_acquired_locks
from swarmq.worker.lock_keeper import LockKeeper, keeper_tick_s, keeper_window_s
from tests.support.inmemory import FakeClock, InMemoryBroker
from tests.support.stateful import AsyncStateMachine
from tests.unit._log_capture import capture_swarmq_logs

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.contracts.coordination import LockStore
    from swarmq.worker.gates import WorkerBackend
    from tests.support.inmemory import InMemoryBackend

LOCK_NAMES = ("res-a", "res-b")
WORKERS = ("w1", "w2")
# Handler-Locks tragen real ``hard_timeout + 30 s`` (mindestens 31 s): Takt
# 11 s / 20 s / 20 s, Fenster 33 s / 60 s / 60 s (lock_keeper.py). Locks ohne
# Keeper (Leader-Pfad) dürfen kürzer sein.
HANDLER_TTLS_MS = (33_000, 60_000, 90_000)
PLAIN_TTLS_MS = (9_000, 30_000, 90_000)
UNIQUE_TASK = "stateful_unique"
UNIQUE_DEFAULT_FOR_S = 60
UNIQUE_ARGS = (1, 2)
UniqueMode = Literal["start", "completion"]
ExtendFault = Literal["failed", "unsupported"]
# Ereignisse, mit denen der Keeper eine Lease aus der Hand gibt (I9).
LOSS_EVENTS = frozenset({"task_lock_lost", "lock_extend_unsupported"})
KEEPER_EVENTS = LOSS_EVENTS | {"lock_extend_failed"}


class _Missing:
    """Argument weggelassen (der Client nimmt dann den TaskInfo-Default)."""

    @override
    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()


class StatefulUnique(Task):
    task_info = TaskInfo(
        name=UNIQUE_TASK,
        queues=["default"],
        unique_for=UNIQUE_DEFAULT_FOR_S,
        unique_until="start",
    )

    @override
    async def run(self) -> None:
        """Läuft nie: die Maschine fährt nur Claim und Freigaben."""


class LockEnv(Protocol):
    """Store-Factory der Lock-Maschine (KTD2)."""

    @property
    def clock(self) -> FakeClock: ...

    @property
    def backend(self) -> InMemoryBackend | ValkeyBackend: ...

    @property
    def ttl_tolerance_ms(self) -> int:
        """So viel ms darf die Rest-TTL im Store unter dem Modell liegen."""
        ...

    async def advance(self, seconds: float) -> None: ...

    async def close(self) -> None: ...


@dataclass
class Hold:
    owner: str
    expires_ms: int


@dataclass(eq=False)
class Handler:
    """Ein laufender Handler: Lock erworben, Lease im Keeper seines Workers."""

    seq: int
    worker: str
    token: LockToken
    ttl_ms: int
    tended_ms: int
    excused: str | None = None

    @override
    def __repr__(self) -> str:
        return f"H{self.seq}({self.worker}, {self.token.name}, ttl={self.ttl_ms // 1000}s)"


@dataclass(eq=False)
class Plain:
    """Ein Lock ohne Keeper (wie das Leader-Lock: ``renew``)."""

    seq: int
    token: LockToken

    @override
    def __repr__(self) -> str:
        return f"P{self.seq}({self.token.name})"


@dataclass(eq=False)
class Message:
    seq: int
    task_id: str
    dedup_hash: str | None
    mode: UniqueMode
    headers: dict[str, Any]
    ctx: GateContext
    started: bool = False
    terminal: bool = False

    @override
    def __repr__(self) -> str:
        return f"M{self.seq}({self.mode}, unique={self.dedup_hash is not None})"


class RecordingBroker(InMemoryBroker):
    """``InMemoryBroker``, der jeden Publish mit Headern festhält."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(clock)
        self.published: list[dict[str, Any]] = []

    @override
    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        receipt = await super().publish(queue, body, headers)
        self.published.append(dict(headers or {}))
        return receipt


class CountingReleases:
    """Reicht ``release_unique`` durch und hält Aufrufe und Ergebnisse je Owner fest."""

    def __init__(self, inner: InMemoryBackend | ValkeyBackend) -> None:
        self._inner = inner
        self.calls: dict[str, int] = {}
        self.results: dict[str, list[bool]] = {}

    async def release_unique(self, dedup_hash: str, owner: str, *, op_timeout: float | None = None) -> bool:
        self.calls[owner] = self.calls.get(owner, 0) + 1
        released = await self._inner.release_unique(dedup_hash, owner, op_timeout=op_timeout)
        self.results.setdefault(owner, []).append(released)
        return released


class RecordingLog:
    """``log`` für ``release_acquired_locks``: hält Warnungen fest."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.events.append((event, fields))


class ReplyLostBackend:
    """Der Claim wird geschrieben, die Antwort geht verloren (Timeout)."""

    def __init__(self, inner: InMemoryBackend | ValkeyBackend) -> None:
        self._inner = inner

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, *, op_timeout: float | None = None
    ) -> str | None:
        await self._inner.claim_unique(dedup_hash, owner, ttl_ms, op_timeout=op_timeout)
        raise BackendConnectionError("reply lost (timeout after the write)")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class FaultyExtend:
    """Store eines Keepers: ``extend`` scheitert auf Wunsch (Regel
    ``tick_with_extend_fault``), sonst geht er an den Store der Umgebung.
    ``failed`` wirft wie ein Valkey-Ausfall (``lock_extend_failed``),
    ``unsupported`` wie ein Store ohne ``extend`` (``lock_extend_unsupported``)."""

    def __init__(self, inner: Callable[[], Any]) -> None:
        self._inner = inner
        self.fault: ExtendFault | None = None
        self.hit: list[LockToken] = []

    async def extend(self, token: LockToken, ttl_ms: int) -> bool:
        if self.fault is not None:
            self.hit.append(token)
            if self.fault == "failed":
                raise BackendConnectionError("valkey down (injected)")
            raise NotImplementedError("store without extend (injected)")
        return cast("bool", await self._inner().extend(token, ttl_ms=ttl_ms))


async def _parked(_seconds: float) -> None:
    """``sleep`` des Keepers: die Hintergrundschleife ruht, die Maschine taktet."""
    await asyncio.Event().wait()


def tick_ms(ttl_ms: int) -> int:
    return round(keeper_tick_s(ttl_ms) * 1000)


def window_ms(ttl_ms: int) -> int:
    return round(keeper_window_s(ttl_ms) * 1000)


class LockMachine(AsyncStateMachine[LockEnv]):
    def __init__(self, open_env: Callable[[], Coroutine[Any, Any, LockEnv]]) -> None:
        super().__init__(open_env)
        try:
            self.holds: dict[str, Hold] = {}
            self.flags: dict[str, Hold] = {}
            self.handlers: list[Handler] = []
            self.plains: list[Plain] = []
            self.messages: list[Message] = []
            self._seq = 0
            self.keeper_stores = {worker: FaultyExtend(lambda: self.env.backend.locks) for worker in WORKERS}
            self.keepers = {worker: self._new_keeper(worker) for worker in WORKERS}
            self.broker = RecordingBroker(self.env.clock)
            self.run(self.broker.connect())
            self.registry = TaskRegistry()
            self.registry.register(StatefulUnique)
            backend = cast("Any", self.env.backend)
            self.client = Client(broker=self.broker, backend=backend, registry=self.registry)
            self.lossy_client = Client(
                broker=self.broker, backend=cast("Any", ReplyLostBackend(self.env.backend)), registry=self.registry
            )
            self.releases = CountingReleases(self.env.backend)
            self.config = SwarmQConfig(broker_url="amqp://unused", backend_url="valkey://unused")
        except BaseException:
            self.teardown()
            raise

    # -- Hilfen ----------------------------------------------------------------

    def _new_keeper(self, worker: str) -> LockKeeper:
        store = cast("LockStore", self.keeper_stores[worker])
        return LockKeeper(lambda: store, sleep=_parked, now=lambda: self.env.clock.now_ms() / 1000)

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    @property
    def now_ms(self) -> int:
        return self.env.clock.now_ms()

    def holder(self, name: str) -> Hold | None:
        """Halter laut Modell; abgelaufene Einträge fallen heraus."""
        hold = self.holds.get(name)
        if hold is not None and hold.expires_ms <= self.now_ms:
            del self.holds[name]
            return None
        return hold

    def flag(self, dedup_hash: str) -> Hold | None:
        hold = self.flags.get(dedup_hash)
        if hold is not None and hold.expires_ms <= self.now_ms:
            del self.flags[dedup_hash]
            return None
        return hold

    def model_release(self, token: LockToken) -> bool:
        """Modell von ``release``: nur der Owner gibt frei (I2)."""
        hold = self.holder(token.name)
        if hold is None or hold.owner != token.owner:
            return False
        del self.holds[token.name]
        return True

    def model_acquire(self, name: str, owner: str, ttl_ms: int) -> bool:
        """Modell von ``acquire``: frei -> erworben; eigener Token -> erworben
        ohne neue Frist (I5); fremd -> ``None`` (E-007: der Aufrufer wartet)."""
        hold = self.holder(name)
        if hold is None:
            self.holds[name] = Hold(owner, self.now_ms + ttl_ms)
            return True
        return hold.owner == owner

    def _excuse_untended(self, worker: str) -> None:
        """E-022: ohne Takt mindestens ein Fenster lang darf der Lock ablaufen."""
        for handler in self.handlers:
            if handler.worker == worker and self.now_ms - handler.tended_ms >= window_ms(handler.ttl_ms):
                handler.excused = handler.excused or "E-022 (blockierter Loop)"

    async def _locks_acquire(self, name: str, owner: str, ttl_ms: int) -> LockToken | None:
        return await self.env.backend.locks.acquire(name, owner=owner, ttl_ms=ttl_ms, max_concurrency=1)

    def _tick(self, worker: str, fault: ExtendFault | None = None) -> None:
        """Ein Takt des Keepers von ``worker`` samt Prüfung von I4 und I9;
        ``fault`` lässt jedes ``extend`` dieses Takts scheitern."""
        keeper = self.keepers[worker]
        before = {h: (self.holder(h.token.name), self.now_ms) for h in self.handlers if h.worker == worker}
        kept_before = [h for h in before if keeper.keeps(h.token)]
        store = self.keeper_stores[worker]
        store.fault, store.hit = fault, []
        try:
            with capture_swarmq_logs() as logs:
                self.run(keeper.tick_once())
        finally:
            store.fault = None
        for handler in before:
            if handler.token in store.hit:
                handler.excused = handler.excused or f"E-022 (lock_extend_{fault})"
        self._check_loss_reported(worker, kept_before, logs)
        for handler, (hold, now) in before.items():
            handler.tended_ms = now
            if not keeper.keeps(handler.token):
                assert handler.excused is not None, f"{handler}: Lease ohne erlaubte Abweichung verloren (I4)"
                continue
            if hold is None or hold.owner != handler.token.owner:
                continue
            observed = self.run(self.env.backend.locks.observe(handler.token.name, max_concurrency=1))
            assert handler.token.owner in observed.owners, f"{handler}: Keeper hält eine Lease ohne Lock (I4)"
            old = hold.expires_ms - now
            # Fällig ist die Lease, wenn ihre Frist (im Keeper gleich der im
            # Store) weniger als ein Fenster entfernt ist; dann setzt ``extend``
            # mindestens ein Fenster, sonst bleibt die Frist. Das deckt I4: nie
            # kürzer, mindestens ein Fenster, höchstens max(Frist, Fenster).
            predicted = max(old, window_ms(handler.ttl_ms))
            # Nach einer erlaubten Abweichung (etwa erneuter Erwerb mit eigenem
            # Token) kennt der Keeper eine andere Frist; dann auch „unverändert“.
            candidates = [predicted] if handler.excused is None else [predicted, old]
            tolerance = self.env.ttl_tolerance_ms
            matching = [c for c in candidates if c - tolerance <= observed.pttl_ms <= c]
            assert matching, (
                f"{handler}: Rest-TTL {observed.pttl_ms} ms nach dem Takt, Modell {candidates} ms "
                f"(Toleranz {tolerance} ms) (I4)"
            )
            hold.expires_ms = now + matching[0]

    def _check_loss_reported(self, worker: str, kept_before: list[Handler], logs: list[dict[str, Any]]) -> None:
        """I9: genau eine Verlustmeldung je herausgefallener Lease, keine für
        eine behaltene. Geprüft gegen ``keeps`` und ``observe``, nicht gegen
        das Modell (Toleranz der Valkey-Stufe)."""
        keeper = self.keepers[worker]
        entries: dict[str, list[dict[str, Any]]] = {}
        for entry in logs:
            if entry["event"] in KEEPER_EVENTS:
                entries.setdefault(entry["task_id"], []).append(entry)
        owners = {handler.token.owner for handler in kept_before}
        assert set(entries) <= owners, f"{worker}: Meldung ohne gehaltene Lease {entries} (I9)"
        for handler in kept_before:
            own = entries.get(handler.token.owner, [])
            assert all(entry["lock_name"] == handler.token.name for entry in own), f"{handler}: {own} (I9)"
            losses = [entry for entry in own if entry["event"] in LOSS_EVENTS]
            if keeper.keeps(handler.token):
                assert not losses, f"{handler}: Lease behalten, trotzdem {losses} (I9, E-022)"
                continue
            events = [entry["event"] for entry in own]
            assert len(losses) == 1, f"{handler}: Lease verloren mit {events} statt einer Verlustmeldung (I9, E-022)"
            loss = losses[0]
            if loss["event"] != "task_lock_lost":
                continue
            assert loss["stage"] in {"running", "expired"}, f"{handler}: task_lock_lost mit {loss['stage']} (I9)"
            if loss["stage"] == "running":
                observed = self.run(self.env.backend.locks.observe(handler.token.name, max_concurrency=1))
                assert handler.token.owner not in observed.owners, (
                    f"{handler}: task_lock_lost (running), der Store hält den Lock noch (I9)"
                )

    # -- Locks mit Keeper ------------------------------------------------------

    def start(self, worker: str, name: str, ttl_ms: int) -> Handler | None:
        seq = self._next()
        owner = f"{worker}-{seq}"
        free = self.holder(name) is None

        async def acquire_and_keep() -> LockToken | None:
            token = await self._locks_acquire(name, owner, ttl_ms)
            if token is not None:
                self.keepers[worker].keep(token, ttl_ms, task_id=owner, task_name="stateful_locked")
            return token

        token = self.run(acquire_and_keep())
        assert (token is not None) is free, f"{name}: frei={free}, erworben={token is not None} (I3)"
        if token is None:
            return None
        assert token == LockToken(name=name, owner=owner, max_concurrency=1)
        self.model_acquire(name, owner, ttl_ms)
        handler = Handler(seq, worker, token, ttl_ms, tended_ms=self.now_ms)
        self.handlers.append(handler)
        return handler

    @rule(worker=st.sampled_from(WORKERS), name=st.sampled_from(LOCK_NAMES), ttl_ms=st.sampled_from(HANDLER_TTLS_MS))
    def start_handler(self, worker: str, name: str, ttl_ms: int) -> None:
        self.start(worker, name, ttl_ms)

    def finish(self, handler: Handler) -> None:
        """Ende des Handlers über die echte ``release_acquired_locks`` (Lease
        aus dem Keeper, dann owner-geprüfte Freigabe). Sie liefert nichts; ein
        gescheitertes Release meldet sie als ``task_lock_lost`` (stage
        ``release``)."""
        handler_id = uuid.uuid4()
        log = RecordingLog()
        expected = self.model_release(handler.token)
        self.run(
            release_acquired_locks(
                self.env.backend.locks,
                {handler_id: [handler.token]},
                handler_id,
                [handler.token],
                log=log,
                drop=self.keepers[handler.worker].drop,
            )
        )
        released = not log.events
        assert not self.keepers[handler.worker].keeps(handler.token), f"{handler}: Lease nach dem Ende im Keeper"
        assert released is expected, f"{handler}: release={released} ({log.events}), Modell={expected} (I2)"
        if handler.excused is None:
            assert released, f"{handler}: Handler ohne erlaubte Abweichung hat seinen Lock verloren (I3)"
        self.handlers.remove(handler)

    @precondition(lambda self: bool(self.handlers))
    @rule(data=st.data())
    def finish_handler(self, data: st.DataObject) -> None:
        self.finish(data.draw(st.sampled_from(self.handlers), label="handler"))

    def acquire_again(self, handler: Handler) -> None:
        """FS-06b: die Transport-Wiederholung desselben Erwerbs (I5)."""
        expected = self.model_acquire(handler.token.name, handler.token.owner, handler.ttl_ms)
        token = self.run(self._locks_acquire(handler.token.name, handler.token.owner, handler.ttl_ms))
        assert (token == handler.token) is expected, f"{handler}: Wiederholung={token}, Modell={expected} (I5)"

    @precondition(lambda self: bool(self.handlers))
    @rule(data=st.data())
    def acquire_handler_again(self, data: st.DataObject) -> None:
        self.acquire_again(data.draw(st.sampled_from(self.handlers), label="handler"))

    @rule(worker=st.sampled_from(WORKERS))
    def crash_worker(self, worker: str) -> None:
        """SIGKILL: kein Release, die Locks laufen über ihre TTL ab (E-022)."""
        self.keepers[worker].close()
        self.keepers[worker] = self._new_keeper(worker)
        self.handlers = [h for h in self.handlers if h.worker != worker]

    @rule(worker=st.sampled_from(WORKERS))
    def tick(self, worker: str) -> None:
        self._tick(worker)

    @rule(worker=st.sampled_from(WORKERS), fault=st.sampled_from(("failed", "unsupported")))
    def tick_with_extend_fault(self, worker: str, fault: ExtendFault) -> None:
        """Eine Sekunde später ein Takt, in dem jedes ``extend`` von ``worker``
        scheitert (E-022, Q15). Die Sekunde macht die Leases fällig, die der
        letzte Takt verlängert hat; die anderen Keeper takten normal."""
        self.run(self.env.advance(1))
        for other in WORKERS:
            if other != worker and self.keepers[other].kept:
                self._tick(other)
        self._tick(worker, fault)

    @rule(seconds=st.integers(min_value=1, max_value=70), blocked=st.sampled_from((None, None, None, "w1", "w2")))
    def advance(self, seconds: int, blocked: str | None) -> None:
        """Zeit vorstellen; jeder nicht blockierte Keeper taktet wie seine
        Schleife (höchstens ein Takt Abstand). Der blockierte taktet erst am
        Ende, wenn sein Loop wieder läuft und der überfällige ``sleep``
        sofort zurückkehrt."""
        remaining = seconds * 1000
        while remaining > 0:
            ticking = [w for w in WORKERS if w != blocked and self.keepers[w].kept]
            ticks = [tick_ms(h.ttl_ms) for h in self.handlers if h.worker in ticking]
            step = min([remaining, *ticks])
            self.run(self.env.advance(step / 1000))
            remaining -= step
            for worker in ticking:
                self._tick(worker)
        if blocked is not None:
            self._excuse_untended(blocked)
            if self.keepers[blocked].kept:
                self._tick(blocked)

    # -- Locks ohne Keeper (Leader-Pfad) ---------------------------------------

    @rule(name=st.sampled_from(LOCK_NAMES), ttl_ms=st.sampled_from(PLAIN_TTLS_MS))
    def plain_acquire(self, name: str, ttl_ms: int) -> None:
        seq = self._next()
        owner = f"plain-{seq}"
        free = self.holder(name) is None
        token = self.run(self._locks_acquire(name, owner, ttl_ms))
        assert (token is not None) is free, f"{name}: frei={free}, erworben={token is not None} (I3)"
        if token is not None:
            self.model_acquire(name, owner, ttl_ms)
            self.plains.append(Plain(seq, token))

    @precondition(lambda self: bool(self.plains))
    @rule(data=st.data(), ttl_ms=st.sampled_from(PLAIN_TTLS_MS))
    def plain_renew(self, data: st.DataObject, ttl_ms: int) -> None:
        plain = data.draw(st.sampled_from(self.plains), label="plain")
        hold = self.holder(plain.token.name)
        expected = hold is not None and hold.owner == plain.token.owner
        renewed = self.run(self.env.backend.locks.renew(plain.token, ttl_ms))
        assert renewed is expected, f"{plain}: renew={renewed}, Modell={expected} (I2)"
        if hold is not None and expected:
            hold.expires_ms = self.now_ms + ttl_ms  # PEXPIRE: darf verkürzen

    @precondition(lambda self: bool(self.plains))
    @rule(data=st.data())
    def plain_release(self, data: st.DataObject) -> None:
        plain = data.draw(st.sampled_from(self.plains), label="plain")
        expected = self.model_release(plain.token)
        released = self.run(self.env.backend.locks.release(plain.token))
        assert released is expected, f"{plain}: release={released}, Modell={expected} (I2)"
        self.plains.remove(plain)

    # -- Fremde Zugriffe, Ausfall, Operator ------------------------------------

    @rule(name=st.sampled_from(LOCK_NAMES), op=st.sampled_from(("release", "renew", "extend")))
    def foreign_touch(self, name: str, op: str) -> None:
        """Ein fremder Token ändert nichts (I2)."""
        forged = LockToken(name=name, owner="intruder", max_concurrency=1)
        locks = self.env.backend.locks
        if op == "release":
            expected = self.model_release(forged)
            result = self.run(locks.release(forged))
        elif op == "renew":
            expected = False
            result = self.run(locks.renew(forged, 90_000))
        else:
            expected = False
            result = self.run(locks.extend(forged, 90_000))
        assert result is expected, f"{op} mit fremdem Token auf {name}: {result} (I2)"

    @rule(worker=st.sampled_from(WORKERS), name=st.sampled_from(LOCK_NAMES), release_first=st.booleans())
    def reply_lost_acquire(self, worker: str, name: str, release_first: bool) -> None:
        """E-052: Erwerb mit verlorener Antwort, Rückbau unter der frischen ID."""
        owner = f"{worker}-lost-{self._next()}"
        attempt = LockToken(name=name, owner=owner, max_concurrency=1)
        locks = self.env.backend.locks
        if release_first:
            # F6: die Freigabe überholt den Erwerb, der Lock verwaist bis zur TTL.
            released = self.run(locks.release(attempt))
            assert released is self.model_release(attempt), f"Rückbau vor Erwerb gab {name} frei (I6)"
            acquired = self.run(self._locks_acquire(name, owner, 9_000))
            assert (acquired is not None) is self.model_acquire(name, owner, 9_000)
        else:
            acquired = self.run(self._locks_acquire(name, owner, 9_000))
            assert (acquired is not None) is self.model_acquire(name, owner, 9_000)
            released = self.run(locks.release(attempt))
            assert released is self.model_release(attempt), f"Rückbau traf {name} falsch (I6)"

    @rule(name=st.sampled_from(LOCK_NAMES), really=st.sampled_from((False, False, True)))
    def force_release_rarely(self, name: str, really: bool) -> None:
        """Operator-Eingriff, gedrosselt: er entschuldigt Handler und soll die
        Suche nicht dominieren."""
        if really:
            self.force_release(name)

    def force_release(self, name: str) -> None:
        expected = self.holder(name) is not None
        released = self.run(self.env.backend.locks.force_release(name, max_concurrency=1))
        assert released is expected, f"force_release {name}: {released}, Modell={expected}"
        self.holds.pop(name, None)
        for handler in self.handlers:
            if handler.token.name == name:
                handler.excused = handler.excused or "force_release (Operator-Pfad)"

    @precondition(lambda self: bool(self.handlers))
    @rule(data=st.data())
    def take_over_handler_lock(self, data: st.DataObject) -> None:
        """Der Lock eines laufenden Handlers geht an einen anderen Halter
        (Operator-Pfad, dann ein neuer Erwerb): Der nächste fällige Takt
        bekommt ``False`` von ``extend`` und meldet ``task_lock_lost`` (stage
        ``running``, I9). Ohne diese Regel erreicht die Suche den Fall selten."""
        handler = data.draw(st.sampled_from(self.handlers), label="handler")
        self.force_release(handler.token.name)
        self.plain_acquire(name=handler.token.name, ttl_ms=PLAIN_TTLS_MS[-1])

    # -- Unique ----------------------------------------------------------------

    def schedule(
        self,
        arg: int,
        unique_for: float | _Missing | None = MISSING,
        unique_until: UniqueMode | _Missing = MISSING,
    ) -> str:
        kwargs: dict[str, Any] = {}
        if not isinstance(unique_for, _Missing):
            kwargs["unique_for"] = unique_for
        if not isinstance(unique_until, _Missing):
            kwargs["unique_until"] = unique_until
        effective_for = UNIQUE_DEFAULT_FOR_S if isinstance(unique_for, _Missing) else unique_for
        default_mode: UniqueMode = "start" if isinstance(unique_for, _Missing) else "completion"
        mode: UniqueMode = default_mode if isinstance(unique_until, _Missing) else unique_until
        published = len(self.broker.published)

        task_id = self.run(self.client.schedule(UNIQUE_TASK, arg, **kwargs))

        dedup_hash = compute_dedup_hash(UNIQUE_TASK, (arg,), {})
        if effective_for is None:
            assert len(self.broker.published) == published + 1, "unique_for=None wurde nicht veröffentlicht (I8)"
            headers = self.broker.published[-1]
            assert HEADER_UNIQUE_HASH not in headers, "unique_for=None trägt einen Unique-Hash (I8)"
            self.messages.append(self._message(task_id, None, mode, headers))
            return task_id
        hold = self.flag(dedup_hash)
        if hold is not None:
            assert task_id == hold.owner, f"Duplikat liefert {task_id}, Modell-Halter {hold.owner}"
            assert len(self.broker.published) == published, "Duplikat wurde veröffentlicht"
            return task_id
        assert len(self.broker.published) == published + 1, "freies Flag, trotzdem nicht veröffentlicht"
        headers = self.broker.published[-1]
        assert headers.get(HEADER_UNIQUE_HASH) == dedup_hash
        assert headers.get(HEADER_UNIQUE_UNTIL) == mode
        self.flags[dedup_hash] = Hold(task_id, self.now_ms + round(effective_for * 1000))
        self.messages.append(self._message(task_id, dedup_hash, mode, headers))
        return task_id

    def _message(self, task_id: str, dedup_hash: str | None, mode: UniqueMode, headers: dict[str, Any]) -> Message:
        ctx = GateContext(task_id=task_id, task_name=UNIQUE_TASK, queue="default")
        return Message(self._next(), task_id, dedup_hash, mode, headers, ctx)

    @rule(
        arg=st.sampled_from(UNIQUE_ARGS),
        unique_for=st.sampled_from((MISSING, None, 30, 120)),
        unique_until=st.sampled_from((MISSING, "start", "completion")),
    )
    def schedule_task(self, arg: int, unique_for: float | _Missing | None, unique_until: UniqueMode | _Missing) -> None:
        self.schedule(arg, unique_for, unique_until)

    def _model_release_flag(self, message: Message) -> bool:
        """Modell von ``release_unique``: nur der Owner gibt frei (I2)."""
        if message.dedup_hash is None:
            return False
        hold = self.flag(message.dedup_hash)
        if hold is not None and hold.owner == message.task_id:
            del self.flags[message.dedup_hash]
            return True
        return False

    def _check_release_result(self, message: Message, expected: bool) -> None:
        results = self.releases.results.get(message.task_id, [])
        assert results[-1:] == [expected], f"{message}: release_unique={results}, Modell={expected} (I2)"

    def start_message(self, message: Message) -> None:
        """Nachbildung des Startpunkts (``worker/pipeline.py``, privat)."""
        if message.dedup_hash is not None and message.mode == "start":
            self.run(
                self.releases.release_unique(
                    message.dedup_hash, message.task_id, op_timeout=self.config.backend_op_timeout
                )
            )
            self._check_release_result(message, self._model_release_flag(message))
        message.ctx.start_release_reached = True
        message.started = True

    @precondition(lambda self: any(not m.started and not m.terminal for m in self.messages))
    @rule(data=st.data())
    def start_pending(self, data: st.DataObject) -> None:
        pending = [m for m in self.messages if not m.started and not m.terminal]
        self.start_message(data.draw(st.sampled_from(pending), label="message"))

    def end_message(self, message: Message) -> None:
        """Terminales Ende über die echte ``maybe_release_unique``."""
        delivery = Delivery(body=b"{}", headers=message.headers, queue="default")
        self.run(
            maybe_release_unique(
                delivery,
                message.ctx,
                StatefulUnique.task_info,
                unique_hash=message.dedup_hash,
                backend=cast("WorkerBackend", self.releases),
                config=self.config,
            )
        )
        if message.dedup_hash is not None and not (message.mode == "start" and message.started):
            self._check_release_result(message, self._model_release_flag(message))
        message.terminal = True

    @precondition(lambda self: any(not m.terminal for m in self.messages))
    @rule(data=st.data())
    def end_open(self, data: st.DataObject) -> None:
        self.end_message(data.draw(st.sampled_from([m for m in self.messages if not m.terminal]), label="message"))

    @rule(arg=st.sampled_from(UNIQUE_ARGS))
    def reply_lost_schedule(self, arg: int) -> None:
        """E-052: Claim geschrieben, Antwort verloren; der Client gibt unter
        der frischen ``task_id`` frei und wirft. Ein fremdes Flag bleibt."""
        published = len(self.broker.published)
        try:
            self.run(self.lossy_client.schedule(UNIQUE_TASK, arg, unique_for=30))
        except BackendConnectionError:
            pass
        else:
            raise AssertionError("verlorene Claim-Antwort ohne BackendConnectionError")
        assert len(self.broker.published) == published, "trotz verlorener Claim-Antwort veröffentlicht"

    # -- Invarianten -----------------------------------------------------------

    @invariant()
    def store_matches_model(self) -> None:
        """I1."""
        locks = self.env.backend.locks
        tolerance = self.env.ttl_tolerance_ms
        for name in LOCK_NAMES:
            hold = self.holder(name)
            owner = self.run(locks.current_owner(name))
            observed = self.run(locks.observe(name, max_concurrency=1))
            if hold is None:
                assert owner is None, f"{name}: Store-Halter {owner}, Modell frei (I1)"
                assert observed.pttl_ms == -2, f"{name}: Modell frei, Store PTTL {observed.pttl_ms} (I1)"
                continue
            assert owner == hold.owner, f"{name}: Store-Halter {owner}, Modell {hold.owner} (I1)"
            nominal = hold.expires_ms - self.now_ms
            assert nominal - tolerance <= observed.pttl_ms <= nominal, (
                f"{name}: Rest-TTL {observed.pttl_ms} ms, Modell {nominal} ms (I1)"
            )

    @invariant()
    def never_two_owners(self) -> None:
        """I3 und I4: jeder Handler ohne erlaubte Abweichung hält Lock und Lease."""
        for name in LOCK_NAMES:
            active = [h for h in self.handlers if h.token.name == name and h.excused is None]
            assert len(active) <= 1, f"{name}: zwei Halter ohne erlaubte Abweichung {active} (I3)"
            hold = self.holder(name)
            for handler in active:
                assert hold is not None, f"{handler}: Lock abgelaufen, obwohl rechtzeitig getaktet (I4)"
                assert hold.owner == handler.token.owner, f"{handler}: Lock gehört {hold.owner} (I3)"
                assert self.keepers[handler.worker].keeps(handler.token), f"{handler}: Lease fehlt im Keeper (I4)"

    @invariant()
    def unique_released_exactly_once(self) -> None:
        """I7 und I8: Freigabeaufrufe je Nachricht."""
        for message in self.messages:
            calls = self.releases.calls.get(message.task_id, 0)
            if message.dedup_hash is None:
                assert calls == 0, f"{message}: Freigabe ohne Unique-Hash (I8)"
                continue
            done = message.terminal or (message.mode == "start" and message.started)
            assert calls == (1 if done else 0), f"{message}: {calls} Freigaben (I7)"

    def verify(self) -> None:
        """Alle Invarianten auf einmal (für deterministische Abläufe)."""
        self.store_matches_model()
        self.never_two_owners()
        self.unique_released_exactly_once()

    # -- Abbau -----------------------------------------------------------------

    @override
    async def close_subjects(self) -> None:
        for keeper in getattr(self, "keepers", {}).values():
            keeper.close()
        broker = getattr(self, "broker", None)
        if broker is not None:
            await broker.close()


__all__ = ["MISSING", "LockEnv", "LockMachine"]
