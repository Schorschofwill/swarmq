"""Scheduler-Loop (Plan U11) — verbatim aus ``scheduler.py``: Election
(Leader-Lock über den ``LockStore``-Vertrag), Poll/Fire mit Schedfire-Dedup
und Handover-Marker. Seit U12 (Plan 2026-10-08-1541) meldet jeder Loop die
statischen Crons seines Workers; der Leader übernimmt die Meldungen aller
Worker im Meldetakt. Der Cron-Sync lebt als Mixin in ``sync.py``."""

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol, override

import msgspec

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal, SignalDispatcher

if TYPE_CHECKING:
    from swarmq.contracts import Publisher
    from swarmq.contracts.coordination import LockStore

from swarmq.contracts.coordination import LockToken
from swarmq.contracts.coordination import ScheduleStore as ContractScheduleStore
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.epoch import SchedulerEpoch
from swarmq.scheduler.fire import SchedulerFire
from swarmq.scheduler.fire_plan import SchedulerFirePlan
from swarmq.scheduler.leader_version import LeaderVersionWatch, SchedulerLeaderVersion, swarmq_version
from swarmq.scheduler.orphaned_queues import SchedulerOrphanedQueues
from swarmq.scheduler.prune import SchedulerCronPrune
from swarmq.scheduler.reports import cron_fingerprint, live_window_ms
from swarmq.scheduler.stepdown import SchedulerStepdown
from swarmq.scheduler.sync import SchedulerCronSync
from swarmq.scheduler.unowned import SchedulerUnowned
from swarmq.scheduler.unroutable import SchedulerUnroutable, _SchedulerState
from swarmq.scheduler.valkey_watch import SchedulerValkeyWatch

logger = get_logger(__name__)


class SchedulerBackend(Protocol):
    """Struktureller Backend-Ausschnitt des Loops (U11/U15): Schedfire-Dedup
    über ``claim_unique`` (ResultBackend) plus der ``schedules``-Store der
    Fassade (Handover-Marker, rohe Definitionen für den Cron-Sync)."""

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, *, op_timeout: float | None = None
    ) -> str | None: ...

    @property
    def schedules(self) -> ContractScheduleStore: ...


_HANDOVER_MARKER_TTL_MS_FLOOR = 5_000
"""Minimum TTL for the handover marker (5s). The actual TTL used is
``max(floor, heartbeat_interval*2*1000)`` so a slow standby still
observes the marker on its next election tick — otherwise every
clean handover would time out before the successor polls and emit
FAILOVER instead of LEADER."""


def _make_owner_token() -> str:
    """Human-readable owner identity for the leader lock.

    Format: ``{hostname}:{pid}:{uuid4_short}``. Exposed via
    ``swarmq schedule status`` so operators can tell which worker is
    currently leader, rather than seeing an opaque 32-char hex blob.
    """
    host = socket.gethostname()
    pid = os.getpid()
    suffix = uuid.uuid4().hex[:8]
    return f"{host}:{pid}:{suffix}"


class SchedulerLoop(
    SchedulerCronSync,
    SchedulerCronPrune,
    SchedulerLeaderVersion,
    SchedulerFirePlan,
    SchedulerUnroutable,
    SchedulerUnowned,
    SchedulerFire,
    SchedulerStepdown,
    SchedulerEpoch,
    SchedulerValkeyWatch,
    SchedulerOrphanedQueues,
):
    """Embedded scheduler — one instance per worker.

    Runs four cooperating coroutines:

    - ``_election_loop``: at each ``heartbeat_interval`` tick either
      attempts ``acquire`` (standby) or ``renew`` (leader). On acquire
      success, consults the transient handover marker to decide
      whether to emit :class:`~swarmq.signals.Signal.SCHEDULER_LEADER`
      (clean handover or first-ever start) or
      :class:`~swarmq.signals.Signal.SCHEDULER_LEADER_FAILOVER` (lock
      TTL expired with no marker). On renew success, emits
      :class:`~swarmq.signals.Signal.SCHEDULER_HEARTBEAT`.
    - ``_poll_loop``: at each ``poll_interval`` tick, standbys no-op;
      the leader reads due schedules, publishes to the main queue
      with an ``x-swarmq-schedfire`` header, and commits via the
      owner-checked Lua script.
    - ``_report_loop``: every ``scheduler_seen_interval_s`` (and once in
      :meth:`start`) reports this worker's static crons, in any role.
    - ``_adopt_loop``: the leader adopts the youngest live report per
      cron right after acquiring the lock and then every
      ``scheduler_seen_interval_s`` (Plan 2026-10-08-1541, KTD10), and
      removes static crons no worker reports any more (KTD8, ``prune.py``).

    :meth:`stop` is the graceful path: emits HANDOVER, writes a
    short-TTL marker under the outgoing token, releases the lock, and
    cancels the sub-coroutines. A non-graceful exit (crash/SIGKILL)
    leaves no marker, and the next standby to acquire observes the
    missing marker and emits FAILOVER.
    """

    def __init__(
        self,
        *,
        backend: SchedulerBackend,
        broker: Publisher,
        config: SwarmQConfig,
        lock_manager: LockStore,
        store: ScheduleStore,
        signal_dispatcher: SignalDispatcher,
        owner_token: str | None = None,
        worker_id: str | None = None,
        static_crons: Sequence[ScheduleDefinition] = (),
        version: str | None = None,
        consumed_queues: Sequence[str] = (),
        candidate: bool = True,
    ) -> None:
        self._backend = backend
        self._broker = broker
        self._config = config
        self._lock_manager = lock_manager
        self._store = store
        self._signals = signal_dispatcher
        self._owner_token = owner_token or _make_owner_token()
        # LockStore-Vertrag (U11): Token deterministisch aus Name+Owner —
        # acquire liefert denselben Token, renew/release nehmen ihn.
        self._lock_token = LockToken(name=SCHEDULER_LOCK_NAME, owner=self._owner_token, max_concurrency=1)
        self._worker_id = worker_id or self._owner_token
        # Statische Crons dieses Workers (eigene Registry, Zone aufgelöst);
        # gemeldet beim Start und danach im Meldetakt (KTD10).
        self._static_crons = tuple(static_crons)
        # Konsumierte Queues dieses Workers, gemeldet im Meldetakt (BZ-14).
        self._consumed_queues = tuple(consumed_queues)
        # P1 (Plan 2026-10-12-1105): ``False`` bewirbt sich nie um die
        # Leader-Rolle; nur der Melde-Takt laeuft (Crons, Version, Queues).
        self._candidate = candidate
        # swarmq-Version dieses Workers (KTD11), Default aus den Metadaten.
        self._version = version or swarmq_version()
        self._leader_watch = LeaderVersionWatch()
        # Zuletzt gesehener Leader (Standby), für dessen Handover-Marker.
        self._last_seen_leader: str | None = None
        # Drossel der Unroutable-Logs je Schedule (BZ-03, ``unroutable.py``).
        self._unroutable_logged_at: dict[str, float] = {}

        self._state: _SchedulerState = "standby"
        # Ruecktritt nach Publish-Fehlern (BZ-10, ``stepdown.py``).
        self._init_stepdown()
        self._prev_leader_seen = False
        """Set to True when election sees the lock held by *someone*
        (acquire returned False). Used together with the
        handover-marker check to distinguish first-ever start from
        TTL-expiry failover."""

        self._election_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._report_task: asyncio.Task[None] | None = None
        self._adopt_task: asyncio.Task[None] | None = None
        self._adopt_now = asyncio.Event()
        # Poll-Tick und Übernahme schließen sich aus: eine Übernahme zwischen
        # poll_due und commit_fire plante sonst nach der alten Cron.
        self._fire_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

        # Consecutive renew-ERROR counter. A renew that
        # returns False (lock observably gone) already steps down, but a
        # renew that RAISES (Valkey unreachable) previously kept
        # state=="leader" indefinitely — the poll loop fired through the
        # entire outage while a standby took over after TTL expiry
        # (split-brain for the outage duration). After `limit`
        # consecutive failures the leader proactively steps down, one
        # heartbeat BEFORE the lock TTL can have expired.
        self._renew_failures = 0
        self._renew_failure_limit = max(
            1,
            int(config.scheduler_lock_ttl / config.scheduler_heartbeat_interval) - 1,
        )
        # Zweiter Ausloeser (FS-31, KTD11): Zeitgrenze ``Start des letzten
        # erfolgreichen Renews + ttl - Marge`` auf der Uhr der Event-Loop.
        # Marge ist ``backend_op_timeout``, hoechstens die halbe Spanne
        # zwischen Heartbeat und TTL: sonst laege die Grenze bei grossem
        # op_timeout vor dem naechsten regulaeren Renew, und ein gesunder
        # Leader traete bei jedem Takt zurueck.
        # ``None``: noch keine Amtszeit mit bekanntem Start.
        self._renew_deadline: float | None = None
        self._renew_window = config.scheduler_lock_ttl - max(
            0.0,
            min(
                config.backend_op_timeout,
                (config.scheduler_lock_ttl - config.scheduler_heartbeat_interval) / 2,
            ),
        )

    # ---- lifecycle ------------------------------------------------------

    @property
    def state(self) -> _SchedulerState:
        return self._state

    @property
    def owner_token(self) -> str:
        return self._owner_token

    async def start(self) -> None:
        """Launch election + poll coroutines (without ``candidate`` only the
        report loop). Idempotent: the report loop runs in both roles."""
        if self._report_task is not None:
            return
        self._stop_event.clear()
        await self._report_crons()
        self._report_task = asyncio.create_task(self._report_loop())
        if not self._candidate:
            return
        self._election_task = asyncio.create_task(self._election_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._adopt_task = asyncio.create_task(self._adopt_loop())

    async def stop(self) -> None:
        """Graceful shutdown. Releases the leader lock if held, sets
        the handover marker so the next acquirer emits LEADER instead
        of FAILOVER, then cancels both sub-coroutines.

        Ordering: state transition and
        stop_event are set BEFORE lock release. Otherwise a window
        exists where the lock is already released (another worker can
        acquire it) while our own poll_loop still believes it is the
        leader — one spurious publish batch can fire inside that
        window. Setting state first prevents our poll_loop from
        starting a new tick after the release.
        """
        if self._state == "leader":
            await self._emit(Signal.SCHEDULER_LEADER_HANDOVER)
            try:
                # 2x heartbeat converted to ms — explicit factor-of-2
                # safety margin over the seconds-to-ms conversion.
                marker_ttl_ms = max(
                    _HANDOVER_MARKER_TTL_MS_FLOOR,
                    int(self._config.scheduler_heartbeat_interval * 2 * 1000),
                )
                await self._backend.schedules.set_handover_marker(self._owner_token, marker_ttl_ms)
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_handover_marker_failed",
                    **_error_fields(exc),
                )
            # Transition state BEFORE release — closes the window
            # where poll_loop runs as leader while the lock is gone.
            self._state = "standby"
            self._stop_event.set()
            try:
                await self._lock_manager.release(self._lock_token)
            except BackendConnectionError as exc:
                logger.warning("scheduler_lock_release_failed", **_error_fields(exc))

        # Idempotent — the leader path above already set this; this
        # call covers the standby path where the
        # early-transition block was skipped entirely. Do not remove
        # either call; they belong to different control-flow branches.
        self._stop_event.set()
        await self._cancel_sub_tasks()
        self._state = "stopped"

    async def abort(self) -> None:
        """Crash-style teardown: cancel the election + poll sub-coroutines
        WITHOUT the graceful handover/lock-release that :meth:`stop` performs.

        For the cancel path — process crash, ``start_worker()`` task
        cancellation, ProcessManager reload — where a graceful release is
        neither possible nor desirable: a real crash leaves no chance to emit
        HANDOVER, and the leader lock simply expires by TTL so a successor
        takes over via failover. Idempotent; safe to call after ``stop`` or a
        prior ``abort`` (tasks already None/done). This is the sub-task cleanup
        tail of ``stop`` minus the leader-release block.
        """
        self._stop_event.set()
        self._state = "stopped"
        await self._cancel_sub_tasks()

    async def _cancel_sub_tasks(self) -> None:
        tasks = (
            self._election_task,
            self._poll_task,
            self._report_task,
            self._adopt_task,
        )
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        for task in tasks:
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError, Exception:  # noqa: BLE001, S110 - Teardown best effort
                    pass
        self._election_task = None
        self._poll_task = None
        self._report_task = None
        self._adopt_task = None

    # ---- cron reports (KTD10) -------------------------------------------

    async def _report_loop(self) -> None:
        interval = self._config.scheduler_seen_interval_s
        try:
            while not self._stop_event.is_set():
                await self._sleep_until_stop(interval)
                if not self._stop_event.is_set():
                    await self._report_crons()
        except asyncio.CancelledError:
            return

    async def _report_crons(self) -> None:
        """Eigene statische Crons und swarmq-Version melden, in jeder Rolle."""
        live_window = live_window_ms(self._config.scheduler_seen_interval_s)
        try:
            for definition in self._static_crons:
                await self._backend.schedules.report_cron(
                    definition.schedule_name,
                    cron_fingerprint(definition),
                    msgspec.json.encode(definition),
                    live_window_ms=live_window,
                )
            await self._backend.schedules.report_worker_version(
                self._worker_id,
                self._version,
                ttl_ms=live_window,
            )
        except Exception as exc:  # noqa: BLE001 - Worker arbeitet weiter, nächster Takt meldet erneut
            # Der Worker arbeitet weiter; der nächste Takt meldet erneut.
            logger.warning("scheduler_cron_report_failed", **_error_fields(exc))
        await self._report_queues()
        # BZ-04: jeder Loop kennt den Epoch, auch ein späterer Leader.
        await self._observe_epoch()

    async def _adopt_loop(self) -> None:
        """Leader-Takt der Übernahme (KTD10): sofort nach dem Erwerb der
        Leader-Rolle, danach alle ``scheduler_seen_interval_s``."""
        interval = self._config.scheduler_seen_interval_s
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._adopt_now.wait(), timeout=interval)
                except TimeoutError:
                    pass
                # Uebernahme-Takt direkt nach dem Erwerb: keine Queue-Pruefung
                # (BZ-14), der eigene Worker startet meist noch (Consumer).
                takeover = self._adopt_now.is_set()
                self._adopt_now.clear()
                if self._state == "leader":
                    try:
                        async with self._fire_lock:
                            await self.adopt_reported_crons()
                    except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Schleife läuft weiter
                        logger.warning("scheduler_cron_adopt_failed", **_error_fields(exc))
                    finally:
                        # BZ-11: der Poll wartet nur auf den ersten Versuch
                        # der Amtszeit, nicht auf dessen Gelingen (fail-open).
                        self._note_adopt_ran()
                    # BZ-14: ausserhalb des Fire-Locks, mit eigenem Zeitlimit.
                    if not takeover:
                        await self.check_orphaned_queues()
        except asyncio.CancelledError:
            return

    # ---- election loop --------------------------------------------------

    async def _election_loop(self) -> None:
        interval = self._config.scheduler_heartbeat_interval
        try:
            while not self._stop_event.is_set():
                try:
                    await self._election_tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Schleife läuft weiter
                    logger.warning("scheduler_election_tick_error", **_error_fields(exc))
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _election_tick(self) -> None:
        if self._state == "leader":
            await self._leader_tick()
            return

        # Verbleibender Zustand: standby
        if self._in_stepdown_cooldown():
            return
        acquire_started = asyncio.get_running_loop().time()
        try:
            acquired = (
                await self._lock_manager.acquire(
                    SCHEDULER_LOCK_NAME,
                    owner=self._owner_token,
                    ttl_ms=int(self._config.scheduler_lock_ttl * 1000),
                    max_concurrency=1,  # mutex
                )
                is not None
            )
        except BackendConnectionError as exc:
            logger.warning("scheduler_acquire_error", **_error_fields(exc))
            return

        if not acquired:
            self._prev_leader_seen = True
            await self._check_leader_version()
            return

        # Acquired the lock — decide which signal to emit.
        try:
            had_handover = await self._take_handover_marker()
        except BackendConnectionError as exc:
            logger.warning("scheduler_handover_marker_read_failed", **_error_fields(exc))
            had_handover = False

        # FS-31: ein frischer Erwerb setzt die TTL frühestens beim Aufruf. Der
        # Selbsttreffer (FS-06b) setzt sie gar nicht: Dann lebt der Key nur bis
        # zur Frist der alten Amtszeit. Die Verlängerung bekommt trotzdem das
        # volle Budget ab dem Erwerb; scheitert sie, gilt die frühere Grenze.
        previous_deadline = self._renew_deadline
        self._renew_deadline = acquire_started + self._renew_window
        # BZ-07: die Übernahme (Handover-Marker) kann dauern; direkt danach
        # verlängern, damit das Lock nicht vor der ersten Verlängerung abläuft.
        renewed = await self._renew_after_takeover()
        if renewed is False:
            logger.warning("scheduler_lock_lost")
            self._prev_leader_seen = True
            return
        if renewed is None and previous_deadline is not None:
            self._renew_deadline = min(self._renew_deadline, previous_deadline)
        if renewed is None and self._renew_deadline_passed():
            # Review U14: Erwerb und Marker-Lesen haben das Fenster verbraucht;
            # ohne Verlaengerung kann der Key schon abgelaufen sein. Amt nicht
            # antreten, Key nicht loeschen; der naechste Takt holt die Rolle
            # per Selbsttreffer (FS-06b) zurueck, falls der Key noch eigen ist.
            logger.warning("scheduler_renew_deadline_stepdown", consecutive_failures=0)
            self._prev_leader_seen = True
            return

        # BZ-11: neue Amtszeit -- die Meldungen der alten gelten nicht mehr.
        self._begin_term(int(self._wall_time() * 1000))
        self._reset_publish_stepdown()
        self._state = "leader"
        # Neue Amtszeit, neues Renew-Budget. Scheiterte die Verlängerung, kann
        # der Key nur noch kurz leben (Wiederwahl mit eigenem Token lässt die
        # Rest-TTL stehen, FS-06b): der nächste Fehler tritt sofort zurück (E-048).
        self._renew_failures = 0 if renewed else self._renew_failure_limit - 1
        await self._write_leader_version()
        await self._observe_leader(self._owner_token, self._version)
        if had_handover or not self._prev_leader_seen:
            await self._emit(Signal.SCHEDULER_LEADER)
        else:
            await self._emit(Signal.SCHEDULER_LEADER_FAILOVER)

        # KTD10: die Cron-Meldungen aller Worker sofort übernehmen, nicht erst
        # nach einem Meldetakt.
        self._adopt_now.set()

    async def _leader_tick(self) -> None:
        """Wahl-Takt als Leader: verlängern oder zurücktreten (E-048)."""
        try:
            still_leader = await self._renew_lock()
        except (BackendConnectionError, TimeoutError) as exc:
            # A renew ERROR (or timeout, FS-31) must not keep the
            # leader in office indefinitely. After `limit`
            # consecutive failures (≈ one heartbeat before the lock
            # TTL is provably expired) step down proactively so the
            # old leader stops firing BEFORE a standby can take
            # over — closing the unbounded split-brain window.
            # Zweiter Ausloeser (KTD11): ist die Zeitgrenze erreicht,
            # sofort, unabhaengig vom Zaehler. Der Renew ist auf die
            # Restzeit begrenzt und endet spaetestens dort.
            self._renew_failures += 1
            logger.warning(
                "scheduler_renew_error",
                **_error_fields(exc),
                consecutive_failures=self._renew_failures,
                step_down_at=self._renew_failure_limit,
            )
            if self._renew_failures >= self._renew_failure_limit:
                logger.warning(
                    "scheduler_renew_failures_stepdown",
                    consecutive_failures=self._renew_failures,
                )
                await self._step_down_after_renew_failures()
            elif self._renew_deadline_passed():
                logger.warning("scheduler_renew_deadline_stepdown", consecutive_failures=self._renew_failures)
                await self._step_down_after_renew_failures()
            return
        self._renew_failures = 0
        if still_leader:
            await self._write_leader_version()
            await self._emit(Signal.SCHEDULER_HEARTBEAT)
        else:
            logger.warning("scheduler_lock_lost")
            self._state = "standby"
            self._prev_leader_seen = True
            # Revive SCHEDULER_STANDBY (declared but never emitted):
            # a genuine leader->standby transition, the natural point
            # for it. Emitted once per transition, not per tick, so it
            # carries signal without noise.
            await self._emit(Signal.SCHEDULER_STANDBY)

    async def _take_handover_marker(self) -> bool:
        """Ohne gesehenen Vorgänger ohne Argument (älterer Vertrag)."""
        schedules: ContractScheduleStore = self._backend.schedules
        if self._last_seen_leader is None:
            return await schedules.take_handover_marker()
        return await schedules.take_handover_marker(previous_owner=self._last_seen_leader)

    async def _step_down_after_renew_failures(self) -> None:
        """Ruecktritt ohne Loeschen des Keys und ohne Abkuehlzeit (E-048)."""
        self._renew_failures = 0
        self._state = "standby"
        self._prev_leader_seen = True
        await self._emit(Signal.SCHEDULER_STANDBY)

    def _renew_deadline_passed(self) -> bool:
        return self._renew_deadline is not None and asyncio.get_running_loop().time() >= self._renew_deadline

    async def _renew_lock(self) -> bool:
        """Renew, begrenzt auf ``min(backend_op_timeout, Restzeit bis zur
        Zeitgrenze)`` (FS-31): ein haengendes Valkey haelt den Leader nicht
        ueber die Lock-TTL hinaus im Amt. Ein Erfolg schiebt die Grenze; als
        Start zaehlt der Zeitpunkt vor dem Aufruf, frueher kann Valkey nicht
        verlaengert haben. Wirft ``BackendConnectionError`` oder ``TimeoutError``."""
        started = asyncio.get_running_loop().time()
        budget = self._config.backend_op_timeout
        if self._renew_deadline is not None:
            budget = min(budget, self._renew_deadline - started)
        async with asyncio.timeout(budget):
            renewed = await self._lock_manager.renew(self._lock_token, int(self._config.scheduler_lock_ttl * 1000))
        if renewed:
            self._renew_deadline = started + self._renew_window
        return renewed

    async def _renew_after_takeover(self) -> bool | None:
        """``False`` nur, wenn das Lock beobachtbar weg ist; ``None`` bei einem
        Fehler beim Verlängern. Der Fehler kostet die frisch erworbene Rolle
        nicht, verbraucht aber das Renew-Budget bis auf einen Fehlschlag."""
        try:
            return await self._renew_lock()
        except (BackendConnectionError, TimeoutError) as exc:
            logger.warning("scheduler_renew_error", **_error_fields(exc))
            return None

    # ---- poll loop ------------------------------------------------------

    async def _poll_loop(self) -> None:
        interval = self._config.scheduler_poll_interval
        try:
            while not self._stop_event.is_set():
                if self._state == "leader":
                    try:
                        async with self._fire_lock:
                            await self._poll_tick()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Schleife läuft weiter
                        logger.warning("scheduler_poll_tick_error", **_error_fields(exc))
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _poll_tick(self) -> None:
        now_ms = int(time.time() * 1000)
        if not self._poll_may_fire(now_ms):
            # BZ-11: direkt nach dem Erwerb erst die Meldungen abwarten.
            return
        try:
            due = await self._store.poll_due(now_ms, limit=100, owner_token=self._owner_token)
        except BackendConnectionError as exc:
            logger.warning("scheduler_poll_due_failed", **_error_fields(exc))
            return

        for schedule_name, score_ms, definition in due:
            if self._state != "leader":
                # Lost leadership mid-batch — abort remaining items so
                # the new leader handles them (Unit 10 'lock lost
                # mid-batch' scenario).
                return
            if definition.disabled:
                continue
            if self._is_unowned(schedule_name, definition):
                await self._hold_unowned(schedule_name, score_ms, definition, now_ms)
                continue
            await self._release_unowned(schedule_name, task_name=definition.task_name)
            await self._fire_one(schedule_name, score_ms, definition)

    # ---- helpers --------------------------------------------------------

    @override
    def _wall_time(self) -> float:
        """Wanduhr fuer ``fire.py``; liest ``time`` dieses Moduls (Q17)."""
        return time.time()

    @property
    @override
    def _log(self) -> Any:
        """Logger fuer ``fire.py``: Name und Patch-Pfad bleiben ``swarmq.scheduler.loop`` (Q17)."""
        return logger

    async def _sleep_until_stop(self, seconds: float) -> None:
        """Sleep for *seconds*, but short-circuit on stop."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            return

    @override
    async def _emit(self, signal: Signal, **extra: Any) -> None:
        try:
            await self._signals.dispatch(signal, worker_id=self._worker_id, **extra)
        except Exception as exc:  # noqa: BLE001 - Signal-Handler darf Scheduler nicht stoppen
            logger.warning(
                "scheduler_signal_dispatch_error",
                signal=signal.value,
                **_error_fields(exc),
            )
