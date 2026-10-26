"""Pruefungen der ``SwarmQConfig`` (Plan 0.12.0, MR 0, Q8).

``ConfigValidation`` ist ein Mixin ohne eigenen Zustand und ohne Rueckimport
von ``swarmq.config``; die Felder, die es liest, stehen als Stubs unter
``TYPE_CHECKING``. ``_ENV_OVERRIDES`` und die Felder bleiben in ``config.py``.
"""

from __future__ import annotations

import math
import sys
import threading
from typing import TYPE_CHECKING, ClassVar

from swarmq.exceptions import ConfigurationError
from swarmq.keyspace import validate_namespace
from swarmq.logging_settings import VALID_LOG_LEVELS, valid_drain_timeout, validate_logger_levels

# Re-Export: die Grenze lebt im Blattmodul ``seconds`` (F4-19, kein Importzyklus).
from swarmq.seconds import MAX_DEADLINE_MS as MAX_DEADLINE_MS
from swarmq.seconds import MAX_DEADLINE_S as MAX_DEADLINE_S
from swarmq.seconds import whole_seconds
from swarmq.valkey_policy import EVICTION_CHECK_MODES

#: ``concurrency`` wird ``prefetch_count`` in ``basic.qos``, ein AMQP-short (F3-38).
MAX_CONCURRENCY = 65535
#: Zählfelder, die als Queue-Argument an RabbitMQ gehen, sind AMQP-``long long``
#: (E-063): RabbitMQ nimmt ``x-delivery-limit`` bis 2**63-1 an, 2**63 lässt
#: schon pamqp beim Kodieren mit ``TypeError`` scheitern (Exit 1 statt 78).
MAX_BROKER_COUNT = 2**63 - 1
#: Zählfelder mit einer engeren Grenze als ``sys.float_info.max``.
_COUNT_BOUNDS: dict[str, int] = {"delivery_limit": MAX_BROKER_COUNT}


def require_number(name: str, value: float) -> None:
    """Sekundenwert muss vergleichbar sein; eine ``timedelta`` oder ein
    String scheiterte sonst mit einem Vergleichsfehler ohne Feldnamen
    (die Annotation schuetzt zur Laufzeit nicht)."""
    try:
        _ = value > 0
    except TypeError as exc:
        msg = f"{name} must be a number of seconds, got {type(value).__module__}.{type(value).__qualname__}"
        raise TypeError(msg) from exc


def check_deadline_bound(name: str, value: float | None, *, positive: bool = False) -> None:
    """Frist oder TTL außerhalb von ``SwarmQConfig`` endlich und höchstens
    ``MAX_DEADLINE_S`` (E-063, Nachtrag 2026-10-19): ``TaskInfo`` und der
    Aufruf-Parameter ``unique_for`` enden als ``PX``/``EX`` in Valkey, als
    Per-Message-Expiration bzw. als Laufzeitdeckel (``hard_timeout``); ein
    größerer Wert scheiterte erst im Worker bzw. beim Claim. ``nan`` und
    ``-inf`` bestehen den Vergleich mit der Grenze und fallen deshalb eigens
    durch. ``ValueError`` wie die übrigen ``TaskInfo``-Prüfungen (E-065).

    ``positive=True`` (Nachtrag 2026-10-19, 0.13.1) verlangt zusätzlich
    einen Wert größer 0, etwa für ``hard_timeout``."""
    if value is None:
        return
    require_number(name, value)
    if value > MAX_DEADLINE_S:
        msg = f"{name} must be at most {MAX_DEADLINE_S} seconds, got {value}"
        raise ValueError(msg)
    if not math.isfinite(value):
        msg = f"{name} must be a finite number of seconds, got {value}"
        raise ValueError(msg)
    if positive:
        check_positive_seconds(name, value)


def check_positive_seconds(name: str, value: float | None) -> None:
    """Laufzeitfrist groesser 0 (E-063, Nachtrag 2026-10-19, 0.13.1):
    ``not value > 0`` laesst auch ``nan`` und ``-inf`` durchfallen; ``inf``
    und grosse Werte bleiben erlaubt (``soft_timeout`` hat keine Obergrenze)."""
    if value is None:
        return
    require_number(name, value)
    if not value > 0:
        msg = f"{name} must be > 0 seconds, got {value}"
        raise ValueError(msg)


def valid_backend_connect_timeout(seconds: float) -> bool:
    """Gesamtfrist des Verbindungsaufbaus zum Backend (E-075): groesser 0 und
    hoechstens ``threading.TIMEOUT_MAX``; ``nan`` und ``inf`` fallen durch."""
    return 0 < seconds <= threading.TIMEOUT_MAX


class ConfigValidation:
    if TYPE_CHECKING:
        broker_url: str
        backend_url: str
        result_ttl: int
        shutdown_timeout: int
        concurrency: int
        log_level: str
        log_lifecycle_level: str
        log_config: str | None
        configure_logging: bool
        logger_levels: tuple[tuple[str, str], ...]
        log_drain_timeout: float
        backend_connect_timeout_s: float
        retry_max_delay: float
        queue_type: str
        queue_dead_letter_strategy: str
        broker_publish_connection: str
        backend_defer_base_delay: float
        backend_defer_max_delay: float
        scheduler_heartbeat_interval: float
        scheduler_lock_ttl: float
        scheduler_dedup_ttl: float | None
        scheduler_seen_interval_s: float
        scheduler_prune: str
        scheduler: str
        scheduler_prune_after_s: float
        pool: str
        heartbeat_interval_s: float
        heartbeat_timeout_s: float
        execution_guard_ttl_s: float
        execution_guard_done_ttl_s: float | None
        unknown_task_grace_s: float
        lock_wait_max_s: float
        valkey_eviction_check: str
        namespace: str | None
        worker_id: str | None
        scheduler_publish_stepdown_s: float | None
        _POSITIVE_FIELDS: ClassVar[tuple[str, ...]]
        _FINITE_FIELDS: ClassVar[tuple[str, ...]]
        _COUNT_FIELDS: ClassVar[frozenset[str]]
        _VALID_POOLS: ClassVar[frozenset[str]]
        _VALID_PRUNE_MODES: ClassVar[tuple[str, ...]]
        _VALID_SCHEDULER_MODES: ClassVar[tuple[str, ...]]

        def _apply_pool_defaults(self) -> None: ...

    def _validate_pool(self) -> None:
        if self.pool not in self._VALID_POOLS:
            msg = f"pool must be one of {sorted(self._VALID_POOLS)}, got {self.pool!r}"
            raise ConfigurationError(msg)
        for field_name in ("recycle_rss_mb", "recycle_max_tasks"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                msg = f"{field_name} must be > 0 or unset, got {value}"
                raise ConfigurationError(msg)
        if self.heartbeat_timeout_s <= self.heartbeat_interval_s:
            msg = (
                "heartbeat_timeout_s must be greater than heartbeat_interval_s, "
                f"got {self.heartbeat_timeout_s} <= {self.heartbeat_interval_s}"
            )
            raise ConfigurationError(msg)

    # ``shutdown_timeout`` steht bewusst NICHT in der ``> 0``-Pruefung: 0
    # bedeutet "sofort abbrechen" und wird so auch genutzt (E-018). Dass ein
    # knapper Wert dem Herunterfahren kein Publish-Budget mehr laesst, faengt
    # die anteilige Reserve in ``WorkerLifecycle._shutdown_publish_budget`` ab.
    # Negativ ist dagegen ein Konfigurationsfehler (F3-42).

    def _validate_shutdown_timeout(self) -> None:
        if self.shutdown_timeout < 0:
            msg = f"shutdown_timeout must be >= 0, got {self.shutdown_timeout}"
            raise ConfigurationError(msg)

    def _validate_result_ttl(self) -> None:
        # E-055: float-Sekunden werden int (aufgerundet); inf/nan sind ein
        # ConfigurationError (Exit 78) statt eines Fehlers zur Laufzeit. Der
        # Bereich wird auf dem Rohwert geprüft, sonst wird -0.5 still zu 0
        # (nan passiert diese Prüfung und scheitert in whole_seconds).
        if self.result_ttl < 0:
            msg = f"result_ttl must be >= 0, got {self.result_ttl}"
            raise ConfigurationError(msg)
        try:
            self.result_ttl = whole_seconds(self.result_ttl, "result_ttl")
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc
        if self.result_ttl > MAX_DEADLINE_S:
            # E-063, Nachtrag 2026-10-17: Grenze des Ziels (Valkey ``EX``).
            msg = f"result_ttl must be at most {MAX_DEADLINE_S} seconds, got {self.result_ttl}"
            raise ConfigurationError(msg)

    def _validate_ranges(self) -> None:
        """Bereich, Endlichkeit und Grenze jedes Feldes; läuft in
        ``__post_init__`` vor den Ableitungen und vor jeder Relationsprüfung,
        damit die Meldung das ursächliche Feld nennt und keine Ableitung an
        einem zu großen Wert scheitert (E-063)."""
        self._validate_positive()
        self._validate_shutdown_timeout()
        if not valid_backend_connect_timeout(self.backend_connect_timeout_s):
            # E-075: ``asyncio.timeout`` braucht eine darstellbare Frist; die
            # eigene Meldung geht der allgemeinen Grenze vor.
            msg = (
                "backend_connect_timeout_s must be > 0 and at most threading.TIMEOUT_MAX seconds, "
                f"got {self.backend_connect_timeout_s}"
            )
            raise ConfigurationError(msg)
        self._validate_bounds()

    def _validate_core(self) -> None:
        self._apply_pool_defaults()
        self._validate_pool()
        if not self.broker_url:
            msg = "broker_url must not be empty"
            raise ConfigurationError(msg)

        if not self.backend_url:
            msg = "backend_url must not be empty"
            raise ConfigurationError(msg)

        self._validate_result_ttl()

        if self.concurrency < 1:
            msg = f"concurrency must be >= 1, got {self.concurrency}"
            raise ConfigurationError(msg)
        if self.concurrency > MAX_CONCURRENCY:
            msg = f"concurrency must be at most {MAX_CONCURRENCY}, got {self.concurrency}"
            raise ConfigurationError(msg)

        if self.unknown_task_grace_s < 0:
            msg = f"unknown_task_grace_s must be >= 0, got {self.unknown_task_grace_s}"
            raise ConfigurationError(msg)

        self._validate_logging()

        valid_queue_types = {"quorum", "classic"}
        if self.queue_type not in valid_queue_types:
            msg = f"queue_type must be one of {sorted(valid_queue_types)}, got {self.queue_type!r}"
            raise ConfigurationError(msg)
        strategies = ("auto", "at-least-once", "legacy")
        if self.queue_dead_letter_strategy not in strategies:
            msg = (
                f"queue_dead_letter_strategy must be one of {list(strategies)}, got {self.queue_dead_letter_strategy!r}"
            )
            raise ConfigurationError(msg)
        self._validate_eviction_check()
        self._validate_namespace()
        self._validate_worker_id()
        self._validate_publish_connection()
        if self.queue_dead_letter_strategy == "at-least-once" and self.queue_type != "quorum":
            # Review MR 8, H3: nur Quorum-Queues kennen at-least-once-Dead-Lettering.
            msg = "queue_dead_letter_strategy='at-least-once' requires queue_type='quorum'; use 'auto' or 'legacy'"
            raise ConfigurationError(msg)

        if self.backend_defer_max_delay < self.backend_defer_base_delay:
            msg = (
                "backend_defer_max_delay must be >= backend_defer_base_delay, "
                f"got {self.backend_defer_max_delay} < {self.backend_defer_base_delay}"
            )
            raise ConfigurationError(msg)

    def _validate_namespace(self) -> None:
        # BZ-10: leer gilt wie ungesetzt (auch 0.11.1 behandelt "" so).
        try:
            self.namespace = validate_namespace(self.namespace)
        except ValueError as exc:
            raise ConfigurationError(f"SWARMQ_NAMESPACE/namespace: {exc}") from exc

    def _validate_worker_id(self) -> None:
        # F4-06 (KTD12, E-029): ``|`` trennt im Versionsschlüssel
        # ``<worker_identity>|<owner_token>``; ``schedule status`` leitet die
        # Worker-ID über das letzte ``|`` ab, das muss eindeutig bleiben.
        if self.worker_id is not None and "|" in self.worker_id:
            msg = f"SWARMQ_WORKER_ID/worker_id must not contain '|', got {self.worker_id!r}"
            raise ConfigurationError(msg)

    def _validate_publish_connection(self) -> None:
        if self.broker_publish_connection not in ("separate", "shared"):
            msg = (
                "broker_publish_connection must be one of ['separate', 'shared'], "
                f"got {self.broker_publish_connection!r}"
            )
            raise ConfigurationError(msg)

    def _validate_eviction_check(self) -> None:
        if self.valkey_eviction_check not in EVICTION_CHECK_MODES:
            msg = (
                f"valkey_eviction_check must be one of {list(EVICTION_CHECK_MODES)}, got {self.valkey_eviction_check!r}"
            )
            raise ConfigurationError(msg)

    def _validate_positive(self) -> None:
        for field_name in self._POSITIVE_FIELDS:
            value = getattr(self, field_name)
            # None nur bei optionalen Fristen (``dead_letter_ttl_s``: unbegrenzt).
            if value is not None and value <= 0:
                msg = f"{field_name} must be > 0, got {value}"
                raise ConfigurationError(msg)

    def _validate_bounds(self) -> None:
        """E-063: ``inf``/``nan`` bestehen ``<= 0``, werden aber in ganze
        Millisekunden umgerechnet oder an Broker und Valkey gegeben (Review
        MR 13, P2-3; F3-04). ``None`` heisst bei optionalen Fristen unbegrenzt.

        Nachtrag 2026-10-17 (PF-10, PF-11): auch ein endlicher Wert muss
        innerhalb der Grenze seines Ziels liegen. ``math.isfinite`` nur auf
        ``float``; ein ``int`` über ``float_info.max`` ließe es scheitern."""
        for field_name in self._FINITE_FIELDS:
            value = getattr(self, field_name)
            if value is None:
                continue
            if isinstance(value, float) and not math.isfinite(value):
                msg = f"{field_name} must be a finite number, got {value}"
                raise ConfigurationError(msg)
            if field_name in self._COUNT_FIELDS:
                bound = _COUNT_BOUNDS.get(field_name, sys.float_info.max)
                if value > bound:
                    msg = f"{field_name} must be at most {bound}, got {value}"
                    raise ConfigurationError(msg)
            elif value > MAX_DEADLINE_S:
                msg = f"{field_name} must be at most {MAX_DEADLINE_S} seconds, got {value}"
                raise ConfigurationError(msg)

    def _validate_logging(self) -> None:
        """Logging-Felder pruefen (R13/R14); CRITICAL gilt ueberall (KTD8)."""
        for field_name in ("log_level", "log_lifecycle_level"):
            value = getattr(self, field_name)
            if isinstance(value, str):
                # E-086: wie ``SWARMQ_LOG_LEVEL`` im Worker ohne Rücksicht auf
                # Groß- und Kleinschreibung.
                value = value.strip().upper()
                setattr(self, field_name, value)
            if value not in VALID_LOG_LEVELS:
                msg = f"{field_name} must be one of {sorted(VALID_LOG_LEVELS)}, got {value!r}"
                raise ConfigurationError(msg)
        self.logger_levels = validate_logger_levels(self.logger_levels, "logger_levels")
        if not valid_drain_timeout(self.log_drain_timeout):
            # Nicht unter ``_POSITIVE_FIELDS``: ``inf``/``nan`` bestehen ``<= 0``.
            msg = (
                f"log_drain_timeout must be > 0 and at most threading.TIMEOUT_MAX seconds, got {self.log_drain_timeout}"
            )
            raise ConfigurationError(msg)
        if self.log_config is not None and not self.configure_logging:
            msg = (
                "log_config cannot be combined with configure_logging=False: "
                "a logging config file needs logging to be configured"
            )
            raise ConfigurationError(msg)

    def _validate_publish_stepdown(self) -> None:
        stepdown = self.scheduler_publish_stepdown_s
        assert stepdown is not None  # Default (scheduler_lock_ttl) in __post_init__
        # Vergleich statt ``math.isfinite``: ``nan`` und ``inf`` fallen durch, und
        # ein int über float_info.max wird nicht in float umgewandelt (E-063).
        if not 0 <= stepdown <= MAX_DEADLINE_S:
            msg = (
                f"scheduler_publish_stepdown_s must be >= 0, finite and at most {MAX_DEADLINE_S} seconds "
                f"(0 disables), got {stepdown!r}"
            )
            raise ConfigurationError(msg)

    def _validate_scheduler_relations(self) -> None:
        self._validate_publish_stepdown()
        if self.scheduler_lock_ttl < self.scheduler_heartbeat_interval * 2:
            msg = (
                "scheduler_lock_ttl must be at least 2x "
                "scheduler_heartbeat_interval to survive a missed heartbeat; "
                f"got lock_ttl={self.scheduler_lock_ttl}, "
                f"heartbeat_interval={self.scheduler_heartbeat_interval}"
            )
            raise ConfigurationError(msg)

        # scheduler_dedup_ttl is guaranteed non-None here (default filled
        # in __post_init__ before validation).
        assert self.scheduler_dedup_ttl is not None
        if self.scheduler_dedup_ttl <= 0:
            msg = f"scheduler_dedup_ttl must be > 0, got {self.scheduler_dedup_ttl}"
            raise ConfigurationError(msg)

        if self.scheduler_dedup_ttl < self.retry_max_delay:
            msg = (
                "scheduler_dedup_ttl must be >= retry_max_delay so that a "
                "retried schedule-fire cannot bypass consumer-side dedup; "
                f"got dedup_ttl={self.scheduler_dedup_ttl}, "
                f"retry_max_delay={self.retry_max_delay}"
            )
            raise ConfigurationError(msg)

    def _validate_scheduler_mode(self) -> None:
        """Scheduler je Worker abschaltbar (Plan 2026-10-12-1105, P1)."""
        if self.scheduler not in self._VALID_SCHEDULER_MODES:
            msg = f"scheduler must be one of {list(self._VALID_SCHEDULER_MODES)}, got {self.scheduler!r}"
            raise ConfigurationError(msg)

    def _validate_scheduler_prune(self) -> None:
        """Cron-Aufraeumen (KTD8)."""
        if self.scheduler_prune not in self._VALID_PRUNE_MODES:
            msg = f"scheduler_prune must be one of {list(self._VALID_PRUNE_MODES)}, got {self.scheduler_prune!r}"
            raise ConfigurationError(msg)
        # Eine Meldung lebt drei Meldetakte; eine kuerzere Frist raeumte
        # Crons lebender Worker ab. Gerundet, damit 3 x 0.1 == 0.3 gilt.
        if self.scheduler_prune_after_s < round(3 * self.scheduler_seen_interval_s, 9):
            msg = (
                "scheduler_prune_after_s must be >= 3x scheduler_seen_interval_s, "
                f"got prune_after_s={self.scheduler_prune_after_s}, "
                f"seen_interval_s={self.scheduler_seen_interval_s}"
            )
            raise ConfigurationError(msg)

    def _validate_execution_guard(self) -> None:
        """Done-/Superseded-Marker muessen den Running-Marker ueberdauern
        (KTD5; Vorbild ``scheduler_dedup_ttl``)."""
        assert self.execution_guard_done_ttl_s is not None  # Default in __post_init__
        if self.execution_guard_done_ttl_s < self.execution_guard_ttl_s:
            msg = (
                "execution_guard_done_ttl_s must be >= execution_guard_ttl_s, "
                f"got {self.execution_guard_done_ttl_s} < {self.execution_guard_ttl_s}"
            )
            raise ConfigurationError(msg)
