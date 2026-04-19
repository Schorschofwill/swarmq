"""SwarmQ configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from swarmq.exceptions import ConfigurationError


@dataclass
class SwarmQConfig:
    broker_url: str = ""
    backend_url: str = ""
    result_ttl: int = 86400
    queues: list[str] = field(default_factory=lambda: ["default"])
    concurrency: int = 10
    log_level: str = "INFO"
    log_json: bool = True
    retry_max_delay: float = 3600.0
    shutdown_timeout: int = 30
    queue_type: str = "quorum"
    cancel_poll_interval: float = 1.0
    # Upper bound for individual Valkey operations used by the cancel
    # pipeline (is_cancelled / store_result / clear_cancel_flag). Slow
    # Valkey would otherwise wedge the cancel monitor at TCP-default
    # latency. 5s is conservative for a local Valkey deployment
    # (REV-P1-3).
    backend_op_timeout: float = 5.0
    scheduler_poll_interval: float = 1.0
    scheduler_heartbeat_interval: float = 10.0
    scheduler_lock_ttl: float = 30.0
    # Sentinel — None means "compute max(60, retry_max_delay + 300) in
    # __post_init__". Explicit value overrides the computation.
    scheduler_dedup_ttl: float | None = None
    # In-flight marker TTL (seconds). Pre-run SETNX with this TTL
    # blocks duplicate schedfire deliveries WHILE the task runs; the
    # long-TTL completed marker takes over once the task finishes
    # (Unit 1 / H-2). 300s default covers typical task runtimes plus
    # a large safety margin before the marker expires and a
    # redelivery of a still-in-flight message could double-run.
    scheduler_inflight_ttl: float = 300.0
    # R4.5 Burst-Mode: when True, the worker exits cleanly once all
    # configured queues have been idle for `burst_idle_timeout` seconds
    # without delivering a single message. Used for batch/cron-style
    # deployments where the worker is meant to drain a queue and exit
    # rather than long-poll forever.
    burst_mode: bool = False
    burst_idle_timeout: float = 5.0

    def __post_init__(self) -> None:
        env_broker = os.environ.get("SWARMQ_BROKER_URL", "")
        if env_broker:
            self.broker_url = env_broker

        env_backend = os.environ.get("SWARMQ_BACKEND_URL", "")
        if env_backend:
            self.backend_url = env_backend

        env_ttl = os.environ.get("SWARMQ_RESULT_TTL", "")
        if env_ttl:
            self.result_ttl = int(env_ttl)

        env_queues = os.environ.get("SWARMQ_QUEUES", "")
        if env_queues:
            self.queues = [q.strip() for q in env_queues.split(",") if q.strip()]

        env_concurrency = os.environ.get("SWARMQ_CONCURRENCY", "")
        if env_concurrency:
            self.concurrency = int(env_concurrency)

        env_log_level = os.environ.get("SWARMQ_LOG_LEVEL", "")
        if env_log_level:
            self.log_level = env_log_level

        env_log_json = os.environ.get("SWARMQ_LOG_JSON", "")
        if env_log_json:
            self.log_json = env_log_json.lower() in ("true", "1", "yes")

        env_retry_max_delay = os.environ.get("SWARMQ_RETRY_MAX_DELAY", "")
        if env_retry_max_delay:
            self.retry_max_delay = float(env_retry_max_delay)

        env_shutdown_timeout = os.environ.get("SWARMQ_SHUTDOWN_TIMEOUT", "")
        if env_shutdown_timeout:
            self.shutdown_timeout = int(env_shutdown_timeout)

        env_queue_type = os.environ.get("SWARMQ_QUEUE_TYPE", "")
        if env_queue_type:
            self.queue_type = env_queue_type

        env_cancel_poll = os.environ.get("SWARMQ_CANCEL_POLL_INTERVAL", "")
        if env_cancel_poll:
            self.cancel_poll_interval = float(env_cancel_poll)

        env_backend_op_timeout = os.environ.get("SWARMQ_BACKEND_OP_TIMEOUT", "")
        if env_backend_op_timeout:
            self.backend_op_timeout = float(env_backend_op_timeout)

        env_sched_poll = os.environ.get("SWARMQ_SCHEDULER_POLL_INTERVAL", "")
        if env_sched_poll:
            self.scheduler_poll_interval = float(env_sched_poll)

        env_sched_heartbeat = os.environ.get("SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL", "")
        if env_sched_heartbeat:
            self.scheduler_heartbeat_interval = float(env_sched_heartbeat)

        env_sched_lock_ttl = os.environ.get("SWARMQ_SCHEDULER_LOCK_TTL", "")
        if env_sched_lock_ttl:
            self.scheduler_lock_ttl = float(env_sched_lock_ttl)

        env_sched_dedup_ttl = os.environ.get("SWARMQ_SCHEDULER_DEDUP_TTL", "")
        if env_sched_dedup_ttl:
            self.scheduler_dedup_ttl = float(env_sched_dedup_ttl)

        env_sched_inflight_ttl = os.environ.get("SWARMQ_SCHEDULER_INFLIGHT_TTL", "")
        if env_sched_inflight_ttl:
            self.scheduler_inflight_ttl = float(env_sched_inflight_ttl)

        # dedup_ttl default: max(60, retry_max_delay + 300) to cover the
        # retry horizon and prevent retried schedule-fires from bypassing
        # consumer-side SETNX dedup (review-finding P1-1).
        if self.scheduler_dedup_ttl is None:
            self.scheduler_dedup_ttl = max(60.0, self.retry_max_delay + 300.0)

        # Validation
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

        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level not in valid_levels:
            msg = f"log_level must be one of {sorted(valid_levels)}, got {self.log_level!r}"
            raise ConfigurationError(msg)

        valid_queue_types = {"quorum", "classic"}
        if self.queue_type not in valid_queue_types:
            msg = f"queue_type must be one of {sorted(valid_queue_types)}, got {self.queue_type!r}"
            raise ConfigurationError(msg)

        if self.cancel_poll_interval <= 0:
            msg = f"cancel_poll_interval must be > 0, got {self.cancel_poll_interval}"
            raise ConfigurationError(msg)

        if self.backend_op_timeout <= 0:
            msg = (
                f"backend_op_timeout must be > 0, got {self.backend_op_timeout}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_poll_interval <= 0:
            msg = (
                "scheduler_poll_interval must be > 0, got "
                f"{self.scheduler_poll_interval}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_heartbeat_interval <= 0:
            msg = (
                "scheduler_heartbeat_interval must be > 0, got "
                f"{self.scheduler_heartbeat_interval}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_lock_ttl <= 0:
            msg = (
                f"scheduler_lock_ttl must be > 0, got {self.scheduler_lock_ttl}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_lock_ttl < self.scheduler_heartbeat_interval * 2:
            msg = (
                "scheduler_lock_ttl must be at least 2x "
                "scheduler_heartbeat_interval to survive a missed heartbeat; "
                f"got lock_ttl={self.scheduler_lock_ttl}, "
                f"heartbeat_interval={self.scheduler_heartbeat_interval}"
            )
            raise ConfigurationError(msg)

        # scheduler_dedup_ttl is guaranteed non-None here (default filled above).
        assert self.scheduler_dedup_ttl is not None
        if self.scheduler_dedup_ttl <= 0:
            msg = (
                "scheduler_dedup_ttl must be > 0, got "
                f"{self.scheduler_dedup_ttl}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_dedup_ttl < self.retry_max_delay:
            msg = (
                "scheduler_dedup_ttl must be >= retry_max_delay so that a "
                "retried schedule-fire cannot bypass consumer-side dedup; "
                f"got dedup_ttl={self.scheduler_dedup_ttl}, "
                f"retry_max_delay={self.retry_max_delay}"
            )
            raise ConfigurationError(msg)

        if self.scheduler_inflight_ttl <= 0:
            msg = (
                "scheduler_inflight_ttl must be > 0, got "
                f"{self.scheduler_inflight_ttl}"
            )
            raise ConfigurationError(msg)
