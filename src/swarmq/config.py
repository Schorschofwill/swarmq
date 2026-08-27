"""SwarmQ configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import ClassVar

from swarmq.exceptions import ConfigurationError

# Credential-free placeholder used as ``broker_url`` / ``backend_url`` when a
# connection object is injected (RabbitMQBroker(connection=...) /
# ValkeyBackend(client=...)). A fixed literal — never None, never derived
# from the borrowed object whose repr can embed credentials — keeps every
# _url reader (this validator, sanitize_url log sites) working without a
# None-guard. Single source of truth for the broker, the backend, and the
# config field it lands in. See docs/brainstorms/inject-existing-pools.
INJECTED_URL = "<injected>"


def _parse_port(raw: str, env_var: str) -> int:
    """Parse an env-var port value with range validation.

    Mirrors the CLI-side ``_positive_int`` guard. A bad port surfaces
    here as ``ConfigurationError`` rather than a confusing ``OSError``
    at bind time.
    """
    try:
        port = int(raw)
    except ValueError as exc:
        raise ConfigurationError(
            f"{env_var} must be an integer, got {raw!r}"
        ) from exc
    if not 1 <= port <= 65535:
        raise ConfigurationError(
            f"{env_var} must be in 1..65535, got {port}"
        )
    return port


def _parse_int(raw: str, env_var: str) -> int:
    """Parse an integer env var as ``ConfigurationError`` on garbage.

    ``SWARMQ_CONCURRENCY=abc`` previously surfaced as a raw
    ``ValueError`` traceback — inconsistent with the ``_parse_port``
    guard and opaque for operators.
    """
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(
            f"{env_var} must be an integer, got {raw!r}"
        ) from exc


def _parse_float(raw: str, env_var: str) -> float:
    """Parse a float env var as ``ConfigurationError`` on garbage."""
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigurationError(
            f"{env_var} must be a number, got {raw!r}"
        ) from exc


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
    # latency. 5s is conservative for a local Valkey deployment.
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
    # (Unit 1). 300s default covers typical task runtimes plus
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
    # R4.3 Task Priorities. When True, every queue in ``queues`` gets
    # auto-expanded to its high/.normal/.low triplet on declare + consume,
    # with weighted prefetch ratios (3:2:1) so high-priority work gets
    # proportionally more in-flight slots under saturation. Opt-in so
    # existing single-queue deployments stay on their one-queue topology.
    priorities: bool = False
    # R5.1/R5.2 auto-wire. When set, Worker.start()
    # constructs a :class:`Metrics`, registers signal collectors, and
    # serves /metrics on the given port. Same for health on its port.
    # None on both disables the HTTP surfaces — the operator can still
    # build a Metrics instance manually and attach their own server.
    metrics_port: int | None = None
    metrics_host: str = "127.0.0.1"
    health_port: int | None = None
    health_host: str = "127.0.0.1"
    # Cadence of the worker metrics sampler that
    # populates queue_depth / dlq_messages / worker_uptime_seconds and
    # emits WORKER_HEARTBEAT. Passive AMQP declares are cheap; 10s is a
    # sensible default for backlog/DLQ alerting without polling pressure.
    metrics_sample_interval: float = 10.0
    # Upper bound for an incoming task-message body. The
    # worker rejects (DLQ-route, no decode) anything larger — msgspec
    # guards recursion depth but not size, so a rogue producer could
    # otherwise force arbitrarily large in-memory decodes. 16 MiB is far
    # above any sane task payload; raise via env if legitimate.
    max_message_body_bytes: int = 16 * 1024 * 1024

    # (Env-Var, Feldname, Parser) — eine Zeile pro Override statt der
    # früheren 27 if-Blöcke; ``__post_init__`` wendet sie der Reihe nach an.
    _ENV_OVERRIDES: ClassVar[tuple[tuple[str, str, str], ...]] = (
        ("SWARMQ_BROKER_URL", "broker_url", "str"),
        ("SWARMQ_BACKEND_URL", "backend_url", "str"),
        ("SWARMQ_RESULT_TTL", "result_ttl", "int"),
        ("SWARMQ_QUEUES", "queues", "csv"),
        ("SWARMQ_CONCURRENCY", "concurrency", "int"),
        ("SWARMQ_LOG_LEVEL", "log_level", "str"),
        ("SWARMQ_LOG_JSON", "log_json", "bool"),
        ("SWARMQ_RETRY_MAX_DELAY", "retry_max_delay", "float"),
        ("SWARMQ_SHUTDOWN_TIMEOUT", "shutdown_timeout", "int"),
        ("SWARMQ_QUEUE_TYPE", "queue_type", "str"),
        ("SWARMQ_CANCEL_POLL_INTERVAL", "cancel_poll_interval", "float"),
        ("SWARMQ_BACKEND_OP_TIMEOUT", "backend_op_timeout", "float"),
        ("SWARMQ_SCHEDULER_POLL_INTERVAL", "scheduler_poll_interval", "float"),
        ("SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL", "scheduler_heartbeat_interval", "float"),
        ("SWARMQ_METRICS_SAMPLE_INTERVAL", "metrics_sample_interval", "float"),
        ("SWARMQ_SCHEDULER_LOCK_TTL", "scheduler_lock_ttl", "float"),
        ("SWARMQ_SCHEDULER_DEDUP_TTL", "scheduler_dedup_ttl", "float"),
        ("SWARMQ_SCHEDULER_INFLIGHT_TTL", "scheduler_inflight_ttl", "float"),
        ("SWARMQ_MAX_MESSAGE_BODY_BYTES", "max_message_body_bytes", "int"),
        # R4.5 Burst-Mode / R4.3 Priorities / R5.1-R5.2 Metrics+Health.
        # Port-Ranges werden beim Parsen validiert, damit eine kaputte
        # Env-Var eine ConfigurationError statt eines OSError beim Bind gibt.
        ("SWARMQ_BURST_MODE", "burst_mode", "bool"),
        ("SWARMQ_BURST_IDLE_TIMEOUT", "burst_idle_timeout", "float"),
        ("SWARMQ_PRIORITIES", "priorities", "bool"),
        ("SWARMQ_METRICS_PORT", "metrics_port", "port"),
        ("SWARMQ_METRICS_HOST", "metrics_host", "str"),
        ("SWARMQ_HEALTH_PORT", "health_port", "port"),
        ("SWARMQ_HEALTH_HOST", "health_host", "str"),
    )

    # Felder mit der gemeinsamen Zusage "> 0" (Meldungsformat identisch).
    _POSITIVE_FIELDS: ClassVar[tuple[str, ...]] = (
        "cancel_poll_interval",
        "backend_op_timeout",
        "max_message_body_bytes",
        "scheduler_poll_interval",
        "scheduler_heartbeat_interval",
        "metrics_sample_interval",
        "scheduler_lock_ttl",
        "scheduler_inflight_ttl",
        "burst_idle_timeout",
    )

    def _apply_env_overrides(self) -> None:
        for env_name, field_name, kind in self._ENV_OVERRIDES:
            raw = os.environ.get(env_name, "")
            if not raw:
                continue
            value: object
            if kind == "int":
                value = _parse_int(raw, env_name)
            elif kind == "float":
                value = _parse_float(raw, env_name)
            elif kind == "bool":
                value = raw.lower() in ("true", "1", "yes")
            elif kind == "csv":
                value = [q.strip() for q in raw.split(",") if q.strip()]
            elif kind == "port":
                value = _parse_port(raw, env_name)
            else:
                value = raw
            setattr(self, field_name, value)

    def _validate_core(self) -> None:
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

        for field_name in self._POSITIVE_FIELDS:
            value = getattr(self, field_name)
            if value <= 0:
                msg = f"{field_name} must be > 0, got {value}"
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

    def __post_init__(self) -> None:
        self._apply_env_overrides()

        # dedup_ttl default: max(60, retry_max_delay + 300) to cover the
        # retry horizon and prevent retried schedule-fires from bypassing
        # consumer-side SETNX dedup.
        if self.scheduler_dedup_ttl is None:
            self.scheduler_dedup_ttl = max(60.0, self.retry_max_delay + 300.0)

        self._validate_core()
        self._validate_scheduler_relations()
