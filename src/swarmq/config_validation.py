"""Pruefungen der ``SwarmQConfig`` (Plan 0.12.0, MR 0, Q8).

``ConfigValidation`` ist ein Mixin ohne eigenen Zustand und ohne Rueckimport
von ``swarmq.config``; die Felder, die es liest, stehen als Stubs unter
``TYPE_CHECKING``. ``_ENV_OVERRIDES`` und die Felder bleiben in ``config.py``.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, ClassVar

from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import VALID_LOG_LEVELS, valid_drain_timeout, validate_logger_levels


class ConfigValidation:
    if TYPE_CHECKING:
        broker_url: str
        backend_url: str
        result_ttl: int
        concurrency: int
        log_level: str
        log_lifecycle_level: str
        log_config: str | None
        configure_logging: bool
        logger_levels: tuple[tuple[str, str], ...]
        log_drain_timeout: float
        retry_max_delay: float
        queue_type: str
        queue_dead_letter_strategy: str
        backend_defer_base_delay: float
        backend_defer_max_delay: float
        scheduler_heartbeat_interval: float
        scheduler_lock_ttl: float
        scheduler_dedup_ttl: float | None
        scheduler_seen_interval_s: float
        scheduler_prune: str
        scheduler_prune_after_s: float
        pool: str
        heartbeat_interval_s: float
        heartbeat_timeout_s: float
        execution_guard_ttl_s: float
        execution_guard_done_ttl_s: float | None
        unknown_task_grace_s: float
        lock_wait_max_s: float
        _POSITIVE_FIELDS: ClassVar[tuple[str, ...]]
        _VALID_POOLS: ClassVar[frozenset[str]]
        _VALID_PRUNE_MODES: ClassVar[tuple[str, ...]]

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

    # ``shutdown_timeout`` steht bewusst NICHT hier: 0 bedeutet "sofort
    # abbrechen" und wird so auch genutzt. Dass ein knapper Wert dem
    # Herunterfahren kein Publish-Budget mehr laesst, faengt stattdessen die
    # anteilige Reserve in ``WorkerLifecycle._shutdown_publish_budget`` ab.

    def _validate_core(self) -> None:
        self._apply_pool_defaults()
        self._validate_pool()
        if not self.broker_url:
            msg = "broker_url must not be empty"
            raise ConfigurationError(msg)

        if not self.backend_url:
            msg = "backend_url must not be empty"
            raise ConfigurationError(msg)

        if self.result_ttl < 0:
            msg = f"result_ttl must be >= 0, got {self.result_ttl}"
            raise ConfigurationError(msg)

        if self.concurrency < 1:
            msg = f"concurrency must be >= 1, got {self.concurrency}"
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
        if self.queue_dead_letter_strategy == "at-least-once" and self.queue_type != "quorum":
            # Review MR 8, H3: nur Quorum-Queues kennen at-least-once-Dead-Lettering.
            msg = "queue_dead_letter_strategy='at-least-once' requires queue_type='quorum'; use 'auto' or 'legacy'"
            raise ConfigurationError(msg)

        for field_name in self._POSITIVE_FIELDS:
            value = getattr(self, field_name)
            # None nur bei optionalen Fristen (``dead_letter_ttl_s``: unbegrenzt).
            if value is not None and value <= 0:
                msg = f"{field_name} must be > 0, got {value}"
                raise ConfigurationError(msg)

        self._validate_finite()

        if self.backend_defer_max_delay < self.backend_defer_base_delay:
            msg = (
                "backend_defer_max_delay must be >= backend_defer_base_delay, "
                f"got {self.backend_defer_max_delay} < {self.backend_defer_base_delay}"
            )
            raise ConfigurationError(msg)

    def _validate_finite(self) -> None:
        """Review MR 13, P2-3: ``inf``/``nan`` bestehen ``<= 0``; das Lock-Gate
        rechnet ``lock_wait_max_s`` in ganze Millisekunden um."""
        if not math.isfinite(self.lock_wait_max_s):
            msg = f"lock_wait_max_s must be a finite number of seconds, got {self.lock_wait_max_s}"
            raise ConfigurationError(msg)

    def _validate_logging(self) -> None:
        """Logging-Felder pruefen (R13/R14); CRITICAL gilt ueberall (KTD8)."""
        for field_name in ("log_level", "log_lifecycle_level"):
            value = getattr(self, field_name)
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

    def _validate_scheduler_relations(self) -> None:
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
