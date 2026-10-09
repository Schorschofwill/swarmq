"""SwarmQ configuration."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from typing import ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from swarmq.config_validation import ConfigValidation
from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import (
    VALID_LOG_FORMATS,
    LoggingSettings,
    parse_logger_levels,
    split_csv,
)

# Credential-free placeholder used as ``broker_url`` / ``backend_url`` when a
# connection object is injected (RabbitMQBroker(connection=...) /
# ValkeyBackend(client=...)). A fixed literal — never None, never derived
# from the borrowed object whose repr can embed credentials — keeps every
# _url reader (this validator, sanitize_url log sites) working without a
# None-guard. Single source of truth for the broker, the backend, and the
# config field it lands in. See docs/brainstorms/inject-existing-pools.
INJECTED_URL = "<injected>"


# KTD1: Schreibweisen fuer UTC, die zu ``None`` normalisiert werden, damit
# sich der Fingerabdruck gespeicherter Definitionen beim Upgrade nicht aendert.
_UTC_ALIASES: frozenset[str] = frozenset({"UTC", "Etc/UTC"})


def normalize_timezone(value: str | None, field_name: str = "timezone") -> str | None:
    """Cron-Zeitzone pruefen und normalisieren (Plan 2026-10-08-1541, KTD1).

    ``None``, ``"UTC"`` und ``"Etc/UTC"`` ergeben ``None``; jeder andere
    Name muss sich per ``zoneinfo`` aufloesen lassen, sonst ``ValueError``
    mit Feld- und Zonennamen. Gemeinsam genutzt von ``TaskInfo`` und
    ``SwarmQConfig``, damit beide dieselbe Schreibweise speichern.
    """
    if value is None:
        return None
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        msg = f"{field_name} is not a known IANA zone: {value!r}"
        raise ValueError(msg) from exc
    return None if value in _UTC_ALIASES else value


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


def parse_env_value(kind: str, raw: str, env_name: str) -> object:
    """Einen ``SWARMQ_*``-Wert nach seiner Art aus ``_ENV_OVERRIDES`` parsen.

    Eine Quelle fuer ``SwarmQConfig`` und die CLI-Aufloesung der Worker-
    Optionen (``swarmq.cli.worker_options``), damit beide dieselbe Umgebung
    gleich lesen.
    """
    if kind == "int":
        return _parse_int(raw, env_name)
    if kind == "float":
        return _parse_float(raw, env_name)
    if kind == "optional_float":
        # ``none`` schaltet eine Frist ab (Plan 2026-10-08-1541, KTD6).
        return None if raw.strip().lower() == "none" else _parse_float(raw, env_name)
    if kind == "bool":
        return raw.lower() in ("true", "1", "yes")
    if kind == "csv":
        return [q.strip() for q in raw.split(",") if q.strip()]
    if kind == "port":
        return _parse_port(raw, env_name)
    if kind == "log_format":
        return _parse_log_format(raw, env_name)
    if kind == "logger_levels":
        return parse_logger_levels(split_csv(raw), env_name)
    return raw


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
class SwarmQConfig(ConfigValidation):
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
    # Frist fuer jedes Leeren der Log-Queues (Plan 2026-10-06-1811, KTD11).
    log_drain_timeout: float = 5.0
    # Opt-in-Queue fuer eingebettete Apps (KTD10), standardmaessig aus.
    log_queue: bool = False
    # Die im Supervisor aufgeloesten Einstellungen (U7, KTD7). Gesetzt, sind
    # sie die Quelle der Logging-Felder oben, und die Env-Overrides gelten
    # fuer diese Felder nicht: die CLI-Prioritaet ist dort schon angewandt.
    logging_settings: LoggingSettings | None = None
    # Felder, die der Aufrufer schon mit CLI > Env > Default aufgeloest hat
    # (``swarmq worker``): die Env-Overrides gelten fuer sie nicht, sonst
    # schluege ``SWARMQ_CONCURRENCY`` im Worker die Option ``--concurrency``.
    # Eingebettet bleibt die Menge leer -- kwargs, dann Env wie bisher.
    resolved_fields: tuple[str, ...] = ()
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
    # Zurueckstellung, wenn ein Gate vor der Ausfuehrung am Backend scheitert:
    # die Nachricht geht als neue Nachricht ueber die Delay-Queue zurueck
    # (Wartezeit ``base * 2**(n-1)``, gedeckelt), statt per Requeue das
    # Zustellbudget zu verbrauchen. Kein Retry, kein Retry-Budget.
    backend_defer_base_delay: float = 1.0
    backend_defer_max_delay: float = 60.0
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
    # Standard-Zeitzone fuer Cron-Ausdruecke ohne eigene ``TaskInfo.timezone``
    # (Plan 2026-10-08-1541, KTD1). None heisst UTC. Muss im selben Deploy
    # umgestellt werden wie die Ausdruecke, die sie betrifft.
    scheduler_timezone: str | None = None
    # Meldetakt der Cron-Meldungen (Plan 2026-10-08-1541, KTD10): jeder Worker
    # meldet seine statischen Crons beim Start und danach in diesem Abstand;
    # der Leader waehlt im selben Takt. Eine Meldung lebt 3 Takte.
    scheduler_seen_interval_s: float = 60.0
    # Cron-Aufraeumen ueber Lebenszeichen (Plan 2026-10-08-1541, KTD8): der
    # Leader entfernt statische Crons, die mindestens einmal gemeldet wurden
    # und seit ``scheduler_prune_after_s`` von keinem Worker mehr gemeldet
    # werden. ``log`` loggt nur, ``off`` schaltet das Aufraeumen ab.
    scheduler_prune: str = "on"
    scheduler_prune_after_s: float = 86400.0
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
    # Consumer-Waechter (Plan 2026-10-08-1541, U6, KTD3): Takt, in dem der
    # Worker seine Consumer gegen den Broker prueft und verlorene neu anmeldet,
    # und die Frist, ab der ``/ready`` eine Queue ohne aktiven Consumer meldet.
    consumer_watch_interval_s: float = 5.0
    consumer_ready_grace_s: float = 30.0
    # Ausfuehrungsschutz (Plan 2026-10-08-1541, KTD5): eine erneut
    # zugestellte Nachricht laeuft nicht neben dem Original, nicht nach dessen
    # Abschluss und nicht neben einem Retry. Running-Marker mit TTL
    # ``execution_guard_ttl_s`` (laufend verlaengert); Done-/Superseded-Marker
    # mit ``execution_guard_done_ttl_s``; ab ``execution_guard_max_orphans``
    # abgelaufenen Markern ohne Freigabe (Absturzschleife) geht die Zustellung
    # in die DLQ.
    execution_guard: bool = True
    execution_guard_ttl_s: float = 60.0
    # None heisst ``max(retry_max_delay, consumer_timeout_s) + 300``,
    # mindestens ``execution_guard_ttl_s``:
    # zurueckgestellte Kopien koennen hinter langen Retry-Verzoegerungen
    # stecken, und ab RabbitMQ 4.3 bleiben Zustellungen bis zum
    # Consumer-Timeout ausgecheckt.
    execution_guard_done_ttl_s: float | None = None
    execution_guard_max_orphans: int = 3
    # Verfallsfrist einer von swarmq in die DLQ gelegten Nachricht, gemessen
    # ab dem Dead-Lettering (Plan 2026-10-08-1541, KTD6). Lesen per ``dlq
    # list``/``inspect`` verlaengert sie nicht. None heisst unbegrenzt.
    dead_letter_ttl_s: float | None = 604800.0
    # Terminale Task-Fehler (``NoRetry``, nicht wiederholbare Ausnahme,
    # Retries ausgeschoepft) in die DLQ legen (KTD7). Aus: Fehlerergebnis
    # speichern und bestaetigen wie vor MR !12. ``TaskInfo.dead_letter`` gewinnt.
    dead_letter_terminal_failures: bool = True
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
    #: Die Felder, die ``_apply_logging_settings`` aus ``logging_settings``
    #: setzt. ``log_queue`` gehoert nicht dazu: ``LoggingSettings`` kennt den
    #: eingebetteten Opt-in nicht, ``SWARMQ_LOG_QUEUE`` bleibt also die einzige
    #: Quelle -- und als ``bool`` kann der Wert nicht ungueltig sein.
    _SETTINGS_FIELDS: ClassVar[frozenset[str]] = frozenset({
        "log_level", "log_json", "log_config", "configure_logging",
        "logger_levels", "log_queue_size", "log_lifecycle_level", "log_drain_timeout",
    })

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
        ("SWARMQ_LOG_DRAIN_TIMEOUT", "log_drain_timeout", "float"),
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
        ("SWARMQ_BACKEND_DEFER_BASE_DELAY", "backend_defer_base_delay", "float"),
        ("SWARMQ_BACKEND_DEFER_MAX_DELAY", "backend_defer_max_delay", "float"),
        ("SWARMQ_SCHEDULER_POLL_INTERVAL", "scheduler_poll_interval", "float"),
        ("SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL", "scheduler_heartbeat_interval", "float"),
        ("SWARMQ_METRICS_SAMPLE_INTERVAL", "metrics_sample_interval", "float"),
        ("SWARMQ_SCHEDULER_LOCK_TTL", "scheduler_lock_ttl", "float"),
        ("SWARMQ_SCHEDULER_DEDUP_TTL", "scheduler_dedup_ttl", "float"),
        ("SWARMQ_SCHEDULER_INFLIGHT_TTL", "scheduler_inflight_ttl", "float"),
        ("SWARMQ_SCHEDULER_TIMEZONE", "scheduler_timezone", "str"),
        ("SWARMQ_SCHEDULER_SEEN_INTERVAL_S", "scheduler_seen_interval_s", "float"),
        ("SWARMQ_SCHEDULER_PRUNE", "scheduler_prune", "str"),
        ("SWARMQ_SCHEDULER_PRUNE_AFTER_S", "scheduler_prune_after_s", "float"),
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
        ("SWARMQ_CONSUMER_WATCH_INTERVAL_S", "consumer_watch_interval_s", "float"),
        ("SWARMQ_CONSUMER_READY_GRACE_S", "consumer_ready_grace_s", "float"),
        ("SWARMQ_EXECUTION_GUARD", "execution_guard", "bool"),
        ("SWARMQ_EXECUTION_GUARD_TTL_S", "execution_guard_ttl_s", "float"),
        ("SWARMQ_EXECUTION_GUARD_DONE_TTL_S", "execution_guard_done_ttl_s", "float"),
        ("SWARMQ_EXECUTION_GUARD_MAX_ORPHANS", "execution_guard_max_orphans", "int"),
        ("SWARMQ_DEAD_LETTER_TTL_S", "dead_letter_ttl_s", "optional_float"),
        ("SWARMQ_DEAD_LETTER_TERMINAL_FAILURES", "dead_letter_terminal_failures", "bool"),
        ("SWARMQ_MALLOC_TRIM_THRESHOLD", "malloc_trim_threshold", "int"),
        ("SWARMQ_MALLOC_ARENA_MAX", "malloc_arena_max", "int"),
        ("SWARMQ_COMPUTE_THREAD_LIMIT", "compute_thread_limit", "int"),
        ("SWARMQ_WORKER_ID", "worker_id", "str"),
    )

    # Felder mit der gemeinsamen Zusage "> 0" (Meldungsformat identisch).
    _POSITIVE_FIELDS: ClassVar[tuple[str, ...]] = (
        "cancel_poll_interval",
        "backend_op_timeout",
        "backend_defer_base_delay",
        "backend_defer_max_delay",
        "max_message_body_bytes",
        "scheduler_poll_interval",
        "scheduler_heartbeat_interval",
        "metrics_sample_interval",
        "scheduler_lock_ttl",
        "scheduler_inflight_ttl",
        "scheduler_seen_interval_s",
        "scheduler_prune_after_s",
        "burst_idle_timeout",
        "publish_timeout_s",
        "delivery_limit",
        "heartbeat_interval_s",
        "heartbeat_timeout_s",
        "consumer_timeout_s",
        "consumer_watch_interval_s",
        "consumer_ready_grace_s",
        "execution_guard_ttl_s",
        "execution_guard_max_orphans",
        "dead_letter_ttl_s",
        "malloc_trim_threshold",
        "malloc_arena_max",
        "compute_thread_limit",
        "log_queue_size",
    )
    _VALID_POOLS: ClassVar[frozenset[str]] = frozenset({"io", "cpu"})
    _VALID_PRUNE_MODES: ClassVar[tuple[str, ...]] = ("off", "log", "on")
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

    @property
    def consumed_queues(self) -> list[str]:
        """Die Queues, die ein Worker konsumiert — Grundlage fuer Deklaration,
        Consumer-Waechter und ``/ready`` (Plan 2026-10-08-1541, U6).

        R4.3 Task Priorities: with priorities each base queue expands into
        the high/normal/low triplet. High-priority drains first under LIGHT
        load simply because RabbitMQ delivers from whichever of the three
        queues has a message. Under FULL saturation the channel-level
        prefetch is shared fairly — true weighted prefetch (3:2:1) would
        require separate channels per priority, which is deferred. Operators
        still get queue-level visibility of priority splits (DLQ list,
        queue-depth metrics per priority) and the routing contract
        (priority="high" → {queue}.high).
        """
        if not self.priorities:
            return list(self.queues)
        return [name for base in self.queues for name in (f"{base}.high", base, f"{base}.low")]

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
        # Mit aufgeloesten Settings (KTD7) und fuer ``resolved_fields`` gilt die
        # CLI-Prioritaet schon; ein ueberstimmter, ungueltiger Env-Wert darf
        # den Worker nicht mehr mit Exit 78 beenden, also wird er gar nicht
        # erst geparst.
        skipped = set(self.resolved_fields)
        if self.logging_settings is not None:
            skipped |= self._SETTINGS_FIELDS
        for env_name, field_name, kind in self._ENV_OVERRIDES:
            raw = os.environ.get(env_name, "")
            if not raw or field_name in skipped:
                continue
            setattr(self, field_name, parse_env_value(kind, raw, env_name))

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
        self.log_drain_timeout = settings.drain_timeout

    def __post_init__(self) -> None:
        self._apply_env_overrides()
        self._apply_logging_settings()
        self._apply_slot_port_offset()

        # dedup_ttl default: max(60, retry_max_delay + 300) to cover the
        # retry horizon and prevent retried schedule-fires from bypassing
        # consumer-side SETNX dedup.
        if self.scheduler_dedup_ttl is None:
            self.scheduler_dedup_ttl = max(60.0, self.retry_max_delay + 300.0)

        if self.execution_guard_done_ttl_s is None:
            self.execution_guard_done_ttl_s = max(
                max(self.retry_max_delay, self.consumer_timeout_s) + 300.0, self.execution_guard_ttl_s,
            )

        self._validate_core()
        self._validate_scheduler_relations()
        self._validate_scheduler_prune()
        self._validate_execution_guard()
        try:
            self.scheduler_timezone = normalize_timezone(
                self.scheduler_timezone, "scheduler_timezone"
            )
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc
