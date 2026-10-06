"""SwarmQ configuration."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from typing import ClassVar

from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import (
    VALID_LOG_FORMATS,
    VALID_LOG_LEVELS,
    LoggingSettings,
    parse_logger_levels,
    split_csv,
    validate_logger_levels,
)

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


def _parse_log_format(raw: str, env_var: str) -> bool:
    """``human``/``json`` als Wert fuer ``log_json`` (KTD8)."""
    log_format = raw.strip().lower()
    if log_format not in VALID_LOG_FORMATS:
        raise ConfigurationError(
            f"{env_var} must be one of {list(VALID_LOG_FORMATS)}, got {raw!r}"
        )
    return log_format == "json"


def observability_port_ranges(
    base_metrics: int | None, base_health: int | None, upper: int
) -> tuple[list[int] | None, list[int] | None]:
    """Die Portbereiche, die ``upper`` Worker-Slots belegen (R5-R7/KTD3).

    Je Bereich ``[erster, letzter]`` oder ``None``, wenn kein Basisport
    gesetzt ist. Eine Quelle fuer die Pruefung, den ``--check``-Payload und
    das Startlog — drei getrennte Rechnungen waeren drei Gelegenheiten, sich
    um eins zu vertun.
    """
    span = max(upper, 1) - 1
    metrics = None if base_metrics is None else [base_metrics, base_metrics + span]
    health = None if base_health is None else [base_health, base_health + span]
    return metrics, health


@dataclass
class SwarmQConfig:
    broker_url: str = ""
    backend_url: str = ""
    result_ttl: int = 86400
    queues: list[str] = field(default_factory=lambda: ["default"])
    concurrency: int = 10
    # Eingebettet steuern ``log_level``/``log_json`` den Opt-in ``log_queue``
    # (``swarmq``-Level, Format der Drop-Meldung); ``SwarmQ`` richtet sonst
    # nichts ein (Plan 2026-10-06-0831, KTD10).
    log_level: str = "INFO"
    log_json: bool = True
    # Logging-Einstellungen (Plan 2026-10-06, R13/KTD7). Eingebettet sind
    # sie die Quelle fuer die Prozesse, die die Bibliothek selbst startet.
    log_config: str | None = None
    configure_logging: bool = True
    logger_levels: tuple[tuple[str, str], ...] = ()
    log_queue_size: int = 10_000
    log_lifecycle_level: str = "DEBUG"
    # Opt-in-Queue fuer eingebettete Apps (KTD10), standardmaessig aus.
    log_queue: bool = False
    # Die im Supervisor aufgeloesten Einstellungen (U7, KTD7). Gesetzt, sind
    # sie die Quelle der Logging-Felder oben, und die Env-Overrides gelten
    # fuer diese Felder nicht: die CLI-Prioritaet ist dort schon angewandt.
    logging_settings: LoggingSettings | None = None
    retry_max_delay: float = 3600.0
    shutdown_timeout: int = 30
    # Obergrenze fuer einen kompletten Publish-Vorgang inklusive aller
    # internen Wiederholversuche (Plan 2026-08-28-001, R1/R3). Ohne
    # Deckel wartet ein Publish auf ein Publisher-Confirm, das bei einem
    # still toten Transport nie eintrifft — im Referenzlauf ein
    # 62-minuetiger Stillstand aus einer fuenfsekuendigen Stoerung.
    # 120s liegt weit ueber jedem legitimen Publish und trifft damit
    # praktisch nur pathologische Faelle. Waehrend des Herunterfahrens
    # deckelt zusaetzlich die kuerzere Shutdown-Frist (R12).
    publish_timeout_s: float = 120.0
    queue_type: str = "quorum"
    # Zustellbudget einer Quorum-Queue (``x-delivery-limit``, B5). Ohne
    # Deckel stellt der Broker einen Task, der den Prozess deterministisch
    # abschiesst (SIGSEGV/OOM), ewig neu zu — er erreicht ``max_retries``
    # nie, weil der Worker vor dem Zaehlen stirbt. Nach dem Budget wirft
    # der Broker die Nachricht selbst in die Dead-Letter-Queue.
    #
    # 20 deckt drei bis vier fachliche Wiederholungen je Zustellung plus
    # Puffer fuer Broker-Schluckauf. Dieser Puffer ist seit dem
    # Publish-Deckel (``publish_timeout_s``) schmaler: Bricht ein Publish
    # im Worker an seiner Frist ab, wird die bearbeitete Nachricht neu
    # zugestellt statt bestaetigt, und jede Runde kostet eine Zustellung.
    # Wer laengere Broker-Stoerungen fahren muss, hebt den Wert; wer ein
    # engeres Zustellfenster erzwingen will, senkt ihn.
    #
    # Gilt nur fuer ``queue_type = "quorum"`` — klassische Queues kennen
    # das Argument nicht.
    delivery_limit: int = 20
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

    # CPU-Modus (Plan 2026-09-03-001, R1/R3/R15/R16/R28) und IO-Worker-Schutz
    # (R21/R22). ``recycle_*`` ist Opt-in; im CPU-Modus bekommen DI-Kinder
    # die Voreinstellungen aus der H-Serie (250 MB / 1000 Tasks), siehe
    # ``_apply_pool_defaults``.
    pool: str = "io"
    recycle_rss_mb: int | None = None
    recycle_max_tasks: int | None = None
    heartbeat_interval_s: float = 5.0
    heartbeat_timeout_s: float = 60.0
    consumer_timeout_s: float = 1800.0
    malloc_trim_threshold: int = 131072
    malloc_arena_max: int = 2
    compute_thread_limit: int = 1

    # Worker-Identitaet (Plan 2026-09-08-001, R3/KTD2). ``worker_slot`` reicht
    # der Supervisor durch (kein Env — er allein vergibt Slots);
    # ``worker_id`` ist die Basis, die ein Betreiber setzen kann.
    worker_slot: int | None = None
    worker_id: str | None = None
    # Budget, in dem ein Prozess seinen Beobachtungsport erneut zu binden
    # versucht (R8/KTD4). Kein Env. Der Default gilt fuer den eingebetteten
    # Betrieb, wo kein Vorgaenger den Port haelt; unter dem Supervisor setzt
    # ``worker_subprocess_entry`` ihn auf beide Freigabefristen hoch.
    bind_retry_budget_s: float = 5.0

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
        # Nach SWARMQ_LOG_JSON, damit SWARMQ_LOG_FORMAT gewinnt (KTD8).
        ("SWARMQ_LOG_FORMAT", "log_json", "log_format"),
        ("SWARMQ_LOG_CONFIG", "log_config", "str"),
        ("SWARMQ_LOG_QUEUE_SIZE", "log_queue_size", "int"),
        ("SWARMQ_LOG_LIFECYCLE_LEVEL", "log_lifecycle_level", "str"),
        ("SWARMQ_LOG_QUEUE", "log_queue", "bool"),
        ("SWARMQ_CONFIGURE_LOGGING", "configure_logging", "bool"),
        ("SWARMQ_LOGGER_LEVELS", "logger_levels", "logger_levels"),
        ("SWARMQ_RETRY_MAX_DELAY", "retry_max_delay", "float"),
        ("SWARMQ_SHUTDOWN_TIMEOUT", "shutdown_timeout", "int"),
        ("SWARMQ_PUBLISH_TIMEOUT_S", "publish_timeout_s", "float"),
        ("SWARMQ_DELIVERY_LIMIT", "delivery_limit", "int"),
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
        ("SWARMQ_POOL", "pool", "str"),
        ("SWARMQ_RECYCLE_RSS_MB", "recycle_rss_mb", "int"),
        ("SWARMQ_RECYCLE_MAX_TASKS", "recycle_max_tasks", "int"),
        ("SWARMQ_HEARTBEAT_INTERVAL_S", "heartbeat_interval_s", "float"),
        ("SWARMQ_HEARTBEAT_TIMEOUT_S", "heartbeat_timeout_s", "float"),
        ("SWARMQ_CONSUMER_TIMEOUT_S", "consumer_timeout_s", "float"),
        ("SWARMQ_MALLOC_TRIM_THRESHOLD", "malloc_trim_threshold", "int"),
        ("SWARMQ_MALLOC_ARENA_MAX", "malloc_arena_max", "int"),
        ("SWARMQ_COMPUTE_THREAD_LIMIT", "compute_thread_limit", "int"),
        ("SWARMQ_WORKER_ID", "worker_id", "str"),
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
        "publish_timeout_s",
        "delivery_limit",
        "heartbeat_interval_s",
        "heartbeat_timeout_s",
        "consumer_timeout_s",
        "malloc_trim_threshold",
        "malloc_arena_max",
        "compute_thread_limit",
        "log_queue_size",
    )
    _VALID_POOLS: ClassVar[frozenset[str]] = frozenset({"io", "cpu"})
    # Voreinstellungen fuer wiederverwendete DI-Kinder im CPU-Modus (R28, H-Serie).
    CPU_DEFAULT_RECYCLE_RSS_MB: ClassVar[int] = 250
    CPU_DEFAULT_RECYCLE_MAX_TASKS: ClassVar[int] = 1000

    def _apply_pool_defaults(self) -> None:
        if self.pool != "cpu":
            return
        if self.recycle_rss_mb is None:
            self.recycle_rss_mb = self.CPU_DEFAULT_RECYCLE_RSS_MB
        if self.recycle_max_tasks is None:
            self.recycle_max_tasks = self.CPU_DEFAULT_RECYCLE_MAX_TASKS

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

    @property
    def worker_identity(self) -> str:
        """Stabile Kennung dieses Workers fuer Signale, Metrik-Labels und den
        Logkontext (R3).

        ``<basis>:<slot>`` unter dem Supervisor, ``<basis>:p<pid>`` im
        eingebetteten Betrieb. Der Slot ueberlebt den Respawn — genau darum
        geht es: eine Zeitreihe je Worker-Platz statt einer je Prozess. Das
        ``p`` haelt die beiden Formen auseinander, denn ohne Praefix waere
        ``host:7`` mal ein Slot und mal ein Prozess.
        """
        base = self.worker_id or socket.gethostname()
        if self.worker_slot is None:
            return f"{base}:p{os.getpid()}"
        return f"{base}:{self.worker_slot}"

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
            elif kind == "log_format":
                value = _parse_log_format(raw, env_name)
            elif kind == "logger_levels":
                value = parse_logger_levels(split_csv(raw), env_name)
            else:
                value = raw
            setattr(self, field_name, value)

    def _apply_slot_port_offset(self) -> None:
        """Beobachtungsports sind Basisports; Slot s bindet ``basis + s``
        (R5/KTD3).

        Die Ableitung laeuft NACH ``_apply_env_overrides``, damit auch der
        Umgebungswert als Basis gilt — sonst haette ein
        ``SWARMQ_METRICS_PORT`` im Kind den Versatz wieder ueberschrieben und
        alle Kinder haetten erneut denselben Port gebunden.
        """
        if not self.worker_slot:
            return
        if self.metrics_port is not None:
            self.metrics_port += self.worker_slot
        if self.health_port is not None:
            self.health_port += self.worker_slot

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

        self._validate_logging()

        valid_queue_types = {"quorum", "classic"}
        if self.queue_type not in valid_queue_types:
            msg = f"queue_type must be one of {sorted(valid_queue_types)}, got {self.queue_type!r}"
            raise ConfigurationError(msg)

        for field_name in self._POSITIVE_FIELDS:
            value = getattr(self, field_name)
            if value <= 0:
                msg = f"{field_name} must be > 0, got {value}"
                raise ConfigurationError(msg)

    def _validate_logging(self) -> None:
        """Logging-Felder pruefen (R13/R14); CRITICAL gilt ueberall (KTD8)."""
        for field_name in ("log_level", "log_lifecycle_level"):
            value = getattr(self, field_name)
            if value not in VALID_LOG_LEVELS:
                msg = (
                    f"{field_name} must be one of {sorted(VALID_LOG_LEVELS)}, "
                    f"got {value!r}"
                )
                raise ConfigurationError(msg)
        self.logger_levels = validate_logger_levels(self.logger_levels, "logger_levels")
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

    def _apply_logging_settings(self) -> None:
        settings = self.logging_settings
        if settings is None:
            return
        self.log_level = settings.level
        self.log_json = settings.format == "json"
        self.log_config = settings.log_config
        self.configure_logging = settings.configure
        self.logger_levels = settings.logger_levels
        self.log_queue_size = settings.queue_size
        self.log_lifecycle_level = settings.lifecycle_level

    def __post_init__(self) -> None:
        self._apply_env_overrides()
        self._apply_logging_settings()
        self._apply_slot_port_offset()

        # dedup_ttl default: max(60, retry_max_delay + 300) to cover the
        # retry horizon and prevent retried schedule-fires from bypassing
        # consumer-side SETNX dedup.
        if self.scheduler_dedup_ttl is None:
            self.scheduler_dedup_ttl = max(60.0, self.retry_max_delay + 300.0)

        self._validate_core()
        self._validate_scheduler_relations()
