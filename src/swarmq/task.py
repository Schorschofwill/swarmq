"""SwarmQ Task base class and TaskInfo."""

from __future__ import annotations

import annotationlib
import asyncio
import enum
import inspect
import json
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, override

from croniter import CroniterBadCronError, CroniterBadDateError, croniter

from swarmq.config import normalize_timezone
from swarmq.limits import parse_rate_limit
from swarmq.locking import compute_lock_ttl_ms
from swarmq.logging import _error_fields, get_logger
from swarmq.seconds import validate_unique_for, whole_seconds

if TYPE_CHECKING:
    # U11 (KTD7): Vertragstyp statt Implementierung — ``task`` liegt in der
    # Import-Hülle des reinen ``scheduler``-Pakets.
    from swarmq.contracts import ResultBackend
    from swarmq.workflow import Signature

_progress_logger = get_logger(__name__)

_MAX_TASK_NAME_LEN = 255

# Cron syntax we intentionally do NOT support in v1: L (last), W (nearest
# weekday), # (nth weekday of month). Rejected at construction so the failure
# surfaces at task-module import time rather than inside the scheduler loop.
_FORBIDDEN_CRON_CHARS = re.compile(r"[LW#]")

# Task.run parameter names reserved by Client.schedule. A task that accepts
# these as its own arguments would silently lose its payload because
# schedule(delay=) / schedule(eta=) / schedule(unique_for=) intercepts them
# before kwargs forwarding.
_RESERVED_RUN_PARAM_NAMES = frozenset({"delay", "eta", "unique_for", "unique_until"})

UniqueUntil = Literal["start", "completion"]
# R4.3: three-level priority mapped onto queue name suffixes. Plan
# rules out RabbitMQ native priority queues (not Quorum-compatible) in
# favour of separate high/normal/low queues with weighted prefetch
# ratios on the consumer side.
PriorityLevel = Literal["high", "normal", "low"]
_VALID_UNIQUE_UNTIL: frozenset[str] = frozenset({"start", "completion"})


class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"
    FIRE_AND_FORGET = "fire_and_forget"


_VALID_BACKOFF_STRATEGIES = {"exponential", "linear", "fixed"}


@dataclass(frozen=True)
class TaskInfo:
    """Declarative task metadata. Frozen — set once at class definition.

    Field notes (only the less-obvious ones):
      * ``rate_limit``: optional Celery-style rate limit string,
        e.g. ``"100/m"``, ``"10/s"``, ``"1000/h"``. Enforced globally
        across all workers via Valkey. Limited messages are requeued
        with a per-Lua-script delay rather than DLQ'd. Validated at
        construction (a bad string raises ``ValueError`` immediately).
        Supported units: ``s``, ``m``/``min``, ``h``/``hour``.
      * ``lock``: optional resource name (or format string like
        ``"user-{user_id}"``) that gates concurrent execution of this
        task across all workers. With ``max_concurrency=1`` (default)
        it acts as a Mutex; with ``max_concurrency>1`` as a Semaphore.
        **Requires ``hard_timeout`` to be set** — the lock TTL is
        derived from it via ``hard_timeout + 30s`` (the TTL must outlive
        the task's permitted runtime so the lock cannot expire while the
        holder still runs; see ``compute_lock_ttl_ms``). Setting ``lock``
        without ``hard_timeout`` raises ``ValueError`` at TaskInfo
        construction (fail-fast at module import).
      * ``max_concurrency``: only meaningful when ``lock`` is set.
        Maximum number of concurrent task instances allowed across all
        workers for this lock name. Defaults to 1 (Mutex).
    """

    name: str
    queues: tuple[str, ...] = field(default=())
    soft_timeout: int | None = None
    hard_timeout: int | None = None
    delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE
    result_ttl: int | None = None
    max_retries: int = 0
    retry_backoff: str = "exponential"
    retry_exceptions: tuple[type[BaseException], ...] | None = None
    retry_base_delay: float = 1.0
    rate_limit: str | None = None
    lock: str | None = None
    max_concurrency: int = 1
    cron: str | None = None
    catchup: bool = False
    unique_for: int | None = None
    unique_until: UniqueUntil = "completion"
    # R4.4 Task Expiry. When set, the message carries an
    # x-swarmq-enqueued-at header at publish time; the worker checks
    # (now - enqueued_at) > max_age at consume time and routes the
    # message to the DLQ with a TASK_EXPIRED signal instead of
    # running it. Seconds. None disables expiry.
    max_age: int | None = None
    # R4.3 Task Priorities. 'high' and 'low' get a queue-name suffix
    # ('.high' / '.low') at publish time. Worker-side auto-declaration
    # is gated on SwarmQConfig.priorities=True (opt-in to keep existing
    # single-queue deployments unchanged).
    priority: PriorityLevel = "normal"
    # Cron-Zeitzone (Plan 2026-10-08-1541, KTD1): IANA-Name, in dem der
    # Cron-Ausdruck gilt. None heißt UTC.
    timezone: str | None = None
    # Abschaltbares Dead-Lettering (Plan 2026-10-08-1541, KTD7): terminale
    # Fehler dieses Tasks in die DLQ legen. None heisst: es gilt
    # ``SwarmQConfig.dead_letter_terminal_failures``; ein gesetzter Wert gewinnt.
    dead_letter: bool | None = None

    def __init__(
        self,
        name: str,
        queues: list[str],
        soft_timeout: int | None = None,
        hard_timeout: int | None = None,
        delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        result_ttl: float | None = None,
        max_retries: int = 0,
        retry_backoff: str = "exponential",
        retry_exceptions: list[type[BaseException]] | None = None,
        retry_base_delay: float = 1.0,
        rate_limit: str | None = None,
        lock: str | None = None,
        max_concurrency: int = 1,
        cron: str | None = None,
        catchup: bool = False,
        unique_for: float | None = None,
        unique_until: UniqueUntil = "completion",
        max_age: int | None = None,
        priority: PriorityLevel = "normal",
        timezone: str | None = None,
        dead_letter: bool | None = None,
    ) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "queues", tuple(queues))
        object.__setattr__(self, "soft_timeout", soft_timeout)
        object.__setattr__(self, "hard_timeout", hard_timeout)
        object.__setattr__(self, "delivery", delivery)
        object.__setattr__(self, "result_ttl", result_ttl)
        object.__setattr__(self, "max_retries", max_retries)
        object.__setattr__(self, "retry_backoff", retry_backoff)
        object.__setattr__(
            self,
            "retry_exceptions",
            tuple(retry_exceptions) if retry_exceptions is not None else None,
        )
        object.__setattr__(self, "retry_base_delay", retry_base_delay)
        object.__setattr__(self, "rate_limit", rate_limit)
        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "max_concurrency", max_concurrency)
        object.__setattr__(self, "cron", cron)
        object.__setattr__(self, "catchup", catchup)
        object.__setattr__(self, "unique_for", unique_for)
        object.__setattr__(self, "unique_until", unique_until)
        object.__setattr__(self, "max_age", max_age)
        object.__setattr__(self, "priority", priority)
        object.__setattr__(self, "timezone", timezone)
        object.__setattr__(self, "dead_letter", dead_letter)
        self.__post_init__()

    def __post_init__(self) -> None:
        # E-055: float-Sekunden (``timedelta(...).total_seconds()``) als int.
        object.__setattr__(self, "result_ttl", whole_seconds(self.result_ttl, "result_ttl"))
        self._validate_identity_and_retry()
        self._validate_limits_and_locking()
        self._validate_scheduling()
        self._validate_unique()

    def _validate_identity_and_retry(self) -> None:
        if not self.name:
            raise ValueError("name must not be empty")
        if len(self.name) > _MAX_TASK_NAME_LEN:
            raise ValueError("name must not exceed 255 characters")
        if not self.queues:
            raise ValueError("queues must have at least 1 element")
        if self.soft_timeout is not None and self.hard_timeout is not None and self.hard_timeout <= self.soft_timeout:
            raise ValueError("hard_timeout must be greater than soft_timeout")
        if self.retry_backoff not in _VALID_BACKOFF_STRATEGIES:
            raise ValueError(
                f"retry_backoff must be one of {sorted(_VALID_BACKOFF_STRATEGIES)}, got {self.retry_backoff!r}"
            )
        if self.retry_base_delay < 0:
            raise ValueError("retry_base_delay must be >= 0")

    def _validate_limits_and_locking(self) -> None:
        if self.rate_limit is not None:
            # Validate eagerly at construction so a bad string fails at
            # task-module import time, not inside the worker loop.
            parse_rate_limit(self.rate_limit)
        if self.max_age is not None and self.max_age <= 0:
            raise ValueError("max_age must be > 0 when set")
        if self.priority not in ("high", "normal", "low"):
            raise ValueError(f"priority must be 'high' | 'normal' | 'low', got {self.priority!r}")
        if self.max_concurrency < 1:
            raise ValueError(f"max_concurrency must be >= 1 (got {self.max_concurrency})")
        if self.max_concurrency != 1 and self.lock is None:
            raise ValueError("max_concurrency requires lock to be set")
        if self.lock is not None:
            # Empty / whitespace-only lock would render to the bare Valkey
            # namespace key "sq:lock:" and silently collide with every
            # other empty-locked task in the system. Reject at construction.
            if not self.lock.strip():
                raise ValueError("lock must not be empty or whitespace-only")
            # Locks need a finite ceiling for safe TTL derivation. Surface
            # the requirement at TaskInfo construction (module import) so
            # the bug fires at the obvious site, not inside the worker loop
            # at first schedule. compute_lock_ttl_ms encapsulates the
            # contract — its ValueError is the right message verbatim.
            compute_lock_ttl_ms(self.hard_timeout)

    def _validate_scheduling(self) -> None:
        # catchup=True is reserved for a future release; fail fast at
        # construction rather than letting the user
        # discover it on first fire (potentially days later for monthly
        # crons). Feld bleibt für Forward-Compat erhalten.
        # ValueError (not NotImplementedError) — this is a reserved-but-
        # unsupported API value, not an abstract method stub. Code that
        # catches NotImplementedError for abstract-method handling must
        # not accidentally swallow it.
        if self.catchup:
            raise ValueError("catchup=True is reserved for a future release; set catchup=False")

        if self.cron is not None:
            # Reject extended cron-syntax (L/W/#) at construction.
            if _FORBIDDEN_CRON_CHARS.search(self.cron):
                raise ValueError(
                    f"cron uses unsupported extended syntax (L/W/#): {self.cron!r}; use a standard 5-field expression"
                )
            # F3-08 (E-065): wie der Leader den nächsten Termin berechnen;
            # ein Ausdruck ohne Termin (``0 0 30 2 *``) feuerte sonst still nie.
            # CroniterBadDateError erbt von ValueError, muss also vorher stehen.
            try:
                croniter(self.cron, datetime.now(UTC)).get_next(datetime)
            except CroniterBadDateError as exc:
                raise ValueError(f"cron never fires: {self.cron!r} has no next fire time") from exc
            except (CroniterBadCronError, ValueError) as exc:
                raise ValueError(f"cron is not a valid expression: {self.cron!r} ({exc})") from exc

        if self.timezone is not None:
            if self.cron is None:
                raise ValueError(f"timezone requires cron (got timezone={self.timezone!r})")
            # Plan U3: eine ausdrückliche UTC-Angabe bleibt "UTC" (None
            # heißt „nicht gesetzt“ und fiele auf den globalen Standard
            # zurück); erst die gespeicherte Definition kennt UTC als None.
            object.__setattr__(self, "timezone", normalize_timezone(self.timezone) or "UTC")

    def _validate_unique(self) -> None:
        # R3.5 Unique Tasks validation (origin CD6):
        # - unique_for == 0 → ValueError (fail-fast; silent normalize to
        #   None would mask user typos).
        # - unique_for < 0 → ValueError.
        # - unique_until not in literal domain → ValueError (defensive;
        #   type annotation already guards this at msgspec/IDE layer).
        # - unique_until set without unique_for → silently ignored (no
        #   ValueError to avoid sentinel discrimination on the frozen
        #   dataclass "is the value default or explicit").
        # - float wird nach den Rohwert-Prüfungen ganzzahlig (E-055).
        if self.unique_for is not None:
            object.__setattr__(self, "unique_for", validate_unique_for(self.unique_for))
        if self.unique_until not in _VALID_UNIQUE_UNTIL:
            raise ValueError(f"unique_until must be one of {sorted(_VALID_UNIQUE_UNTIL)}, got {self.unique_until!r}")


_PROGRESS_MIN_INTERVAL_S = 0.5
"""R3.7: throttle progress-update writes to at most 1 per this
interval, UNLESS current == total (final-flush always honoured)."""


class Task:
    task_info: TaskInfo
    _registered_tasks: ClassVar[list[type[Task]]] = []

    # R3.7 progress-tracking state. The worker sets these as instance
    # attrs after constructing the task and before pre_execution.
    # Class-level defaults mean subclasses that override __init__
    # without calling super() still get the attrs — any Subclass().x
    # lookup falls back to the class dict. All defaults are immutable
    # scalars so there is no shared-state hazard.
    _task_id: str | None = None
    _backend: ResultBackend | None = None
    _task_start_time: float | None = None  # time.monotonic(): elapsed/ETA
    _task_started_wall: float | None = None  # time.time(): started_at (F3-15)
    _result_ttl_ms: int = 86_400_000  # 24h fallback
    _last_progress_update: float = 0.0  # instance write shadows on first update
    _progress_final_flushed: bool = False  # single final flush per instance
    # Per-instance asyncio.Lock for throttle read-compare-write.
    # Lazy-initialised on first update_progress call (asyncio.Lock cannot
    # be created at module-load time when no event loop is running, and a
    # class-level instance would be shared across all Task instances).
    _progress_lock: asyncio.Lock | None = None

    @override
    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "task_info"):
            _check_run_signature_reserved_params(cls)
            # Auto-wrap lifecycle methods for optional Dishka injection. No-op
            # when dishka is absent or no FromDishka params are declared. Runs
            # AFTER the reserved-param check so a reserved name still fails on
            # the raw signature. swarmq.dishka imports without dishka.
            from swarmq.dishka import maybe_wrap_task

            maybe_wrap_task(cls)
            Task._registered_tasks.append(cls)

    @classmethod
    def _clear_registered_tasks(cls) -> None:
        cls._registered_tasks.clear()

    @classmethod
    def s(cls, *args: object, **kwargs: object) -> Signature:
        """Build a mutable workflow Signature for this task (R2). The
        predecessor's result is injected as the first arg in a chain
        unless the signature is immutable (see :meth:`si`)."""
        from swarmq.workflow import Signature

        return Signature(
            cls.task_info.name,
            args,
            kwargs,
            immutable=False,
            _is_fire_and_forget=(cls.task_info.delivery is DeliveryMode.FIRE_AND_FORGET),
        )

    @classmethod
    def si(cls, *args: object, **kwargs: object) -> Signature:
        """Build an immutable workflow Signature (R3): the predecessor's
        result is NOT injected — the bound args are kept verbatim."""
        from swarmq.workflow import Signature

        return Signature(
            cls.task_info.name,
            args,
            kwargs,
            immutable=True,
            _is_fire_and_forget=(cls.task_info.delivery is DeliveryMode.FIRE_AND_FORGET),
        )

    async def pre_execution(self) -> None:
        pass

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def post_execution(self) -> None:
        pass

    async def update_progress(
        self,
        current: int,
        total: int,
        message: str = "",
    ) -> None:
        """Record progress for this task (R3.7).

        Throttled at 500ms per Task instance; the *final* update
        (``current == total``) always flushes regardless. No-op when
        the task wasn't wired by a worker (unit-test usage), when
        ``total <= 0``, or when ``current < 0``.

        Progress is best-effort: transient Valkey failures are logged
        and swallowed — they must not crash the task body. The stored
        HASH TTL matches the resolved result_ttl, so progress cleanup
        is automatic once the result itself TTL-expires.

        ETA is computed as ``elapsed / current * (total - current)``
        and is ``None`` at the zeroth update (current == 0). Simple
        linear extrapolation — adequate for UX progress bars, not a
        precise completion estimate for non-linear workloads.
        """
        if self._backend is None or self._task_id is None:
            # Unit-test usage or worker wiring bug. Silently no-op so
            # the task body stays portable across direct-call and
            # worker-dispatched invocations.
            return
        if total <= 0 or current < 0:
            return

        # Lazy-init the lock — asyncio.Lock cannot live as a class
        # attribute (would be shared across instances) and cannot be
        # created at module-load time without a running loop.
        if self._progress_lock is None:
            self._progress_lock = asyncio.Lock()

        # Hold the lock across the throttle-check + reservation step
        # only. The HSET / publish run outside the lock so heavy progress
        # bursts don't serialise on Valkey latency. Reserving the
        # timestamp BEFORE awaiting prevents the read-compare-write race
        # surfaced by U3.8 stress test (200 concurrent calls all read
        # stale _last_progress_update, all pass the throttle, all write).
        async with self._progress_lock:
            loop_time = time.monotonic()
            is_final = current >= total
            if is_final and self._progress_final_flushed:
                return
            if not is_final and loop_time - self._last_progress_update < _PROGRESS_MIN_INTERVAL_S:
                return
            # Reserve the slot now so concurrent callers see the new
            # timestamp on their throttle check.
            self._last_progress_update = loop_time
            if is_final:
                self._progress_final_flushed = True

        percent = (current / total * 100.0) if total > 0 else 0.0
        started_at_iso = (
            datetime.fromtimestamp(self._task_started_wall, tz=UTC).isoformat()
            if self._task_started_wall is not None
            else ""
        )
        elapsed = (loop_time - self._task_start_time) if self._task_start_time is not None else 0.0
        if current > 0 and elapsed > 0 and not is_final:
            eta_seconds: int | None = int(elapsed / current * (total - current))
        else:
            eta_seconds = None

        fields: dict[str, str] = {
            "current": str(current),
            "total": str(total),
            "percent": f"{percent:.2f}",
            "message": message,
            "updated_at": datetime.now(UTC).isoformat(),
            "started_at": started_at_iso,
            "eta_seconds": str(eta_seconds) if eta_seconds is not None else "",
        }

        # Write HASH + publish Pub/Sub — both best-effort. A Valkey
        # blip must not abort task execution; the authoritative state
        # is the HASH (Pub/Sub is the optional real-time channel).
        try:
            await self._backend.set_progress(
                self._task_id,
                fields,
                ttl_ms=self._result_ttl_ms,
            )
        except Exception as exc:  # noqa: BLE001 - Fortschritt best effort, Fehler wird geloggt
            _progress_logger.warning(
                "update_progress_hset_failed",
                task_id=self._task_id,
                **_error_fields(exc),
            )
            return  # Skip publish if HSET failed — no point notifying.

        try:
            await self._backend.publish_progress(
                self._task_id,
                json.dumps(fields).encode("utf-8"),
            )
        except Exception as exc:  # noqa: BLE001 - Fortschritt best effort, Fehler wird geloggt
            _progress_logger.warning(
                "update_progress_publish_failed",
                task_id=self._task_id,
                **_error_fields(exc),
            )


def _check_run_signature_reserved_params(cls: type) -> None:
    """Reject Task subclasses whose run() accepts reserved parameter names.

    ``delay`` and ``eta`` are reserved by Client.schedule() as scheduling
    instructions. A task that accepts them as its own payload arguments
    would silently lose those values once scheduled with ``delay=`` or
    ``eta=``. Surface the conflict at class-registration time with a
    migration hint, rather than at first schedule.
    """
    run = cls.__dict__.get("run")
    if run is None:
        # Subclass inherits Task.run — no new signature to inspect.
        return
    try:
        params = inspect.signature(run, annotation_format=annotationlib.Format.FORWARDREF).parameters
    except TypeError, ValueError:
        # Builtin / C-implemented run — skip.
        return
    conflicts = _RESERVED_RUN_PARAM_NAMES & set(params)
    if conflicts:
        conflict_list = ", ".join(sorted(conflicts))
        raise ValueError(
            f"task {cls.__name__!r} accepts reserved parameter name(s) "
            f"[{conflict_list}], which conflicts with Client.schedule's "
            f"scheduling keywords. Rename the task parameter(s) "
            f"(e.g. 'delay' -> 'retry_delay', 'eta' -> 'run_at', "
            f"'unique_for' -> 'dedup_ttl', 'unique_until' -> 'dedup_mode'). "
            f"There is no escape-hatch keyword in Client.schedule — the "
            f"rename is required."
        )


def resolve_priority_queue(base_queue: str, priority: PriorityLevel) -> str:
    """Return the routing-key queue name for a given base queue + priority.

    'normal' maps to the base queue unchanged; 'high' and 'low' append
    the priority as a suffix. Consumers must declare the suffixed
    variants separately — see :meth:`RabbitMQBroker.declare_queue`.
    """
    if priority == "normal":
        return base_queue
    return f"{base_queue}.{priority}"
