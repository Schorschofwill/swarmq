"""Tests for swarmq.config — SwarmQConfig dataclass."""

import os

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def test_config_default_values():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.broker_url == "redis://localhost"
    assert cfg.backend_url == "redis://localhost"
    assert cfg.result_ttl == 86400
    assert cfg.queues == ["default"]
    assert cfg.concurrency == 10
    assert cfg.log_level == "INFO"


def test_config_with_explicit_values():
    cfg = SwarmQConfig(
        broker_url="amqp://rabbit",
        backend_url="redis://cache",
        result_ttl=3600,
        queues=["high", "low"],
        concurrency=5,
        log_level="DEBUG",
    )
    assert cfg.broker_url == "amqp://rabbit"
    assert cfg.backend_url == "redis://cache"
    assert cfg.result_ttl == 3600
    assert cfg.queues == ["high", "low"]
    assert cfg.concurrency == 5
    assert cfg.log_level == "DEBUG"


def test_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("SWARMQ_RESULT_TTL", "3600")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.result_ttl == 3600


def test_programmatic_value_overrides_default():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        result_ttl=7200,
    )
    assert cfg.result_ttl == 7200


def test_env_var_overrides_programmatic_value(monkeypatch):
    monkeypatch.setenv("SWARMQ_RESULT_TTL", "999")
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        result_ttl=7200,
    )
    assert cfg.result_ttl == 999


def test_env_var_queues_comma_separated(monkeypatch):
    monkeypatch.setenv("SWARMQ_QUEUES", "a,b,c")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.queues == ["a", "b", "c"]


def test_negative_result_ttl_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            result_ttl=-1,
        )


def test_zero_concurrency_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            concurrency=0,
        )


def test_invalid_log_level_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            log_level="INVALID",
        )


def test_empty_env_var_is_ignored(monkeypatch):
    monkeypatch.setenv("SWARMQ_RESULT_TTL", "")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.result_ttl == 86400


def test_empty_broker_url_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(broker_url="", backend_url="redis://localhost")


def test_empty_backend_url_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(broker_url="redis://localhost", backend_url="")


def test_config_log_json_default_true():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.log_json is True


def test_config_log_json_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_LOG_JSON", "false")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.log_json is False


def test_config_log_json_env_true(monkeypatch):
    monkeypatch.setenv("SWARMQ_LOG_JSON", "true")
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        log_json=False,
    )
    assert cfg.log_json is True


# ---------------------------------------------------------------------------
# retry_max_delay (Unit 2.2)
# ---------------------------------------------------------------------------


def test_config_retry_max_delay_default():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.retry_max_delay == 3600.0


def test_config_retry_max_delay_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_RETRY_MAX_DELAY", "7200.0")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.retry_max_delay == 7200.0


def test_config_retry_max_delay_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        retry_max_delay=1800.0,
    )
    assert cfg.retry_max_delay == 1800.0


# ---------------------------------------------------------------------------
# queue_type (Unit 5)
# ---------------------------------------------------------------------------


def test_config_queue_type_default_quorum():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.queue_type == "quorum"


def test_config_queue_type_classic_valid():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        queue_type="classic",
    )
    assert cfg.queue_type == "classic"


def test_config_queue_type_invalid_raises():
    with pytest.raises(ConfigurationError, match="queue_type"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            queue_type="invalid",
        )


def test_config_queue_type_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("SWARMQ_QUEUE_TYPE", "classic")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.queue_type == "classic"


# --- cancel_poll_interval -------------------------------------------------

def test_config_cancel_poll_interval_default():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.cancel_poll_interval == 1.0


def test_config_cancel_poll_interval_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        cancel_poll_interval=0.5,
    )
    assert cfg.cancel_poll_interval == 0.5


def test_config_cancel_poll_interval_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_CANCEL_POLL_INTERVAL", "2.5")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.cancel_poll_interval == 2.5


def test_config_cancel_poll_interval_zero_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            cancel_poll_interval=0,
        )


def test_config_cancel_poll_interval_negative_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            cancel_poll_interval=-1.0,
        )


# ---------------------------------------------------------------------------
# Scheduler config fields (Unit 2)
# ---------------------------------------------------------------------------


def test_config_scheduler_poll_interval_default():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_poll_interval == 1.0


def test_config_scheduler_heartbeat_interval_default():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_heartbeat_interval == 10.0


def test_config_scheduler_lock_ttl_default():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_lock_ttl == 30.0


def test_config_scheduler_dedup_ttl_default_covers_retry_max_delay():
    """Default must satisfy max(60, retry_max_delay + 300) — safety window."""
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    # Default retry_max_delay=3600 → dedup_ttl=3900
    assert cfg.scheduler_dedup_ttl == 3900.0


def test_config_scheduler_dedup_ttl_default_scales_with_retry_max_delay():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        retry_max_delay=7200.0,
    )
    assert cfg.scheduler_dedup_ttl == 7500.0


def test_config_scheduler_dedup_ttl_default_floor_is_60():
    """When retry_max_delay + 300 < 60, default floor is 60s."""
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        retry_max_delay=0.0,
    )
    assert cfg.scheduler_dedup_ttl == 300.0  # 0 + 300 = 300 > 60


def test_config_scheduler_poll_interval_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_poll_interval=0.5,
    )
    assert cfg.scheduler_poll_interval == 0.5


def test_config_scheduler_heartbeat_interval_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_heartbeat_interval=5.0,
    )
    assert cfg.scheduler_heartbeat_interval == 5.0


def test_config_scheduler_lock_ttl_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_lock_ttl=60.0,
    )
    assert cfg.scheduler_lock_ttl == 60.0


def test_config_scheduler_dedup_ttl_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_dedup_ttl=7200.0,
    )
    assert cfg.scheduler_dedup_ttl == 7200.0


def test_config_scheduler_poll_interval_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_SCHEDULER_POLL_INTERVAL", "2.5")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_poll_interval == 2.5


def test_config_scheduler_heartbeat_interval_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL", "7.0")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_heartbeat_interval == 7.0


def test_config_scheduler_lock_ttl_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_SCHEDULER_LOCK_TTL", "45.0")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_lock_ttl == 45.0


def test_config_scheduler_dedup_ttl_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_SCHEDULER_DEDUP_TTL", "5000.0")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_dedup_ttl == 5000.0


def test_config_scheduler_poll_interval_zero_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_poll_interval=0.0,
        )


def test_config_scheduler_heartbeat_interval_negative_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_heartbeat_interval=-1.0,
        )


def test_config_scheduler_lock_ttl_zero_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_lock_ttl=0.0,
        )


def test_config_scheduler_dedup_ttl_zero_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_dedup_ttl=0.0,
        )


def test_config_scheduler_lock_ttl_must_exceed_heartbeat_doubled():
    """lock_ttl >= heartbeat*2 is a safety margin against heartbeat misses."""
    with pytest.raises(ConfigurationError, match="scheduler_lock_ttl"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_heartbeat_interval=10.0,
            scheduler_lock_ttl=15.0,  # <20 = heartbeat*2
        )


def test_config_scheduler_lock_ttl_exactly_heartbeat_doubled_ok():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_heartbeat_interval=10.0,
        scheduler_lock_ttl=20.0,
    )
    assert cfg.scheduler_lock_ttl == 20.0


def test_config_scheduler_dedup_ttl_must_cover_retry_max_delay():
    """dedup_ttl < retry_max_delay would allow duplicate retried fires."""
    with pytest.raises(ConfigurationError, match="scheduler_dedup_ttl"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            retry_max_delay=3600.0,
            scheduler_dedup_ttl=60.0,  # << 3600
        )


def test_config_scheduler_dedup_ttl_equal_retry_max_delay_ok():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        retry_max_delay=3600.0,
        scheduler_dedup_ttl=3600.0,
    )
    assert cfg.scheduler_dedup_ttl == 3600.0


def test_config_scheduler_inflight_ttl_default():
    """Unit 1 / H-2: pre-run marker TTL. 300s is wide enough for most
    task runtimes without leaving a crashed worker's slot locked for
    the full completed-marker TTL."""
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_inflight_ttl == 300.0


def test_config_scheduler_inflight_ttl_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_SCHEDULER_INFLIGHT_TTL", "120.0")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.scheduler_inflight_ttl == 120.0


def test_config_scheduler_inflight_ttl_explicit():
    cfg = SwarmQConfig(
        broker_url="redis://localhost",
        backend_url="redis://localhost",
        scheduler_inflight_ttl=60.0,
    )
    assert cfg.scheduler_inflight_ttl == 60.0


def test_config_scheduler_inflight_ttl_zero_raises():
    with pytest.raises(ConfigurationError, match="scheduler_inflight_ttl"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_inflight_ttl=0,
        )


def test_config_scheduler_inflight_ttl_negative_raises():
    with pytest.raises(ConfigurationError):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            scheduler_inflight_ttl=-5.0,
        )


# ---------------------------------------------------------------------------
# Phase 4/5 auto-wire env vars (ce-review PS-02 / T-06)
# ---------------------------------------------------------------------------


def test_swarmq_burst_mode_env_enables(monkeypatch):
    monkeypatch.setenv("SWARMQ_BURST_MODE", "1")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.burst_mode is True


def test_swarmq_burst_idle_timeout_env_override(monkeypatch):
    monkeypatch.setenv("SWARMQ_BURST_IDLE_TIMEOUT", "2.5")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.burst_idle_timeout == 2.5


def test_swarmq_burst_idle_timeout_zero_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_BURST_IDLE_TIMEOUT", "0")
    with pytest.raises(ConfigurationError, match="burst_idle_timeout"):
        SwarmQConfig(broker_url="x", backend_url="x")


def test_swarmq_priorities_env_enables(monkeypatch):
    monkeypatch.setenv("SWARMQ_PRIORITIES", "yes")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.priorities is True


def test_swarmq_metrics_port_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_METRICS_PORT", "9999")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.metrics_port == 9999


def test_swarmq_metrics_port_non_integer_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_METRICS_PORT", "abc")
    with pytest.raises(ConfigurationError, match="must be an integer"):
        SwarmQConfig(broker_url="x", backend_url="x")


def test_swarmq_metrics_port_out_of_range_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_METRICS_PORT", "99999")
    with pytest.raises(ConfigurationError, match="1..65535"):
        SwarmQConfig(broker_url="x", backend_url="x")


def test_swarmq_health_port_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_HEALTH_PORT", "8081")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.health_port == 8081


def test_swarmq_metrics_host_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_METRICS_HOST", "0.0.0.0")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.metrics_host == "0.0.0.0"


def test_swarmq_health_host_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_HEALTH_HOST", "0.0.0.0")
    cfg = SwarmQConfig(broker_url="x", backend_url="x")
    assert cfg.health_host == "0.0.0.0"


# ---------------------------------------------------------------------------
# Härtung R2 P2: numerische Env-Parses → ConfigurationError statt ValueError
# ---------------------------------------------------------------------------

_NUMERIC_ENV_VARS = [
    "SWARMQ_RESULT_TTL",
    "SWARMQ_CONCURRENCY",
    "SWARMQ_RETRY_MAX_DELAY",
    "SWARMQ_SHUTDOWN_TIMEOUT",
    "SWARMQ_CANCEL_POLL_INTERVAL",
    "SWARMQ_BACKEND_OP_TIMEOUT",
    "SWARMQ_BACKEND_DEFER_BASE_DELAY",
    "SWARMQ_BACKEND_DEFER_MAX_DELAY",
    "SWARMQ_SCHEDULER_POLL_INTERVAL",
    "SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL",
    "SWARMQ_METRICS_SAMPLE_INTERVAL",
    "SWARMQ_SCHEDULER_LOCK_TTL",
    "SWARMQ_SCHEDULER_DEDUP_TTL",
    "SWARMQ_SCHEDULER_INFLIGHT_TTL",
    "SWARMQ_BURST_IDLE_TIMEOUT",
    "SWARMQ_MAX_MESSAGE_BODY_BYTES",
]


@pytest.mark.parametrize("env_var", _NUMERIC_ENV_VARS)
def test_numeric_env_garbage_raises_configuration_error(monkeypatch, env_var):
    """Härtung R2 P2: ``SWARMQ_CONCURRENCY=abc`` warf einen rohen
    ``ValueError`` mit Traceback — inkonsistent zur RR-02-Härtung der
    Port-Parses und für Operatoren undurchsichtig. Alle numerischen
    Env-Parses müssen als ConfigurationError mit Variablennamen
    surfacen."""
    monkeypatch.setenv(env_var, "abc")
    with pytest.raises(ConfigurationError, match=env_var):
        SwarmQConfig(broker_url="x", backend_url="x")


# --- Publish-Deadline (Plan 2026-08-28-001, U1 / R3) ---


def test_config_publish_timeout_default_120():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.publish_timeout_s == 120.0


def test_config_publish_timeout_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_PUBLISH_TIMEOUT_S", "45.5")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.publish_timeout_s == 45.5


def test_config_publish_timeout_non_numeric_env_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_PUBLISH_TIMEOUT_S", "soon")
    with pytest.raises(ConfigurationError, match="must be a number"):
        SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")


def test_config_publish_timeout_zero_raises():
    with pytest.raises(ConfigurationError, match="publish_timeout_s must be > 0"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            publish_timeout_s=0.0,
        )


def test_config_publish_timeout_negative_raises():
    with pytest.raises(ConfigurationError, match="publish_timeout_s must be > 0"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            publish_timeout_s=-1.0,
        )


# --- Zustellbudget der Quorum-Queues (B5) ---


def test_config_delivery_limit_default_20():
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.delivery_limit == 20


def test_config_delivery_limit_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_DELIVERY_LIMIT", "50")
    cfg = SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")
    assert cfg.delivery_limit == 50


def test_config_delivery_limit_non_numeric_env_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_DELIVERY_LIMIT", "viele")
    with pytest.raises(ConfigurationError, match="must be a number|must be an integer"):
        SwarmQConfig(broker_url="redis://localhost", backend_url="redis://localhost")


def test_config_delivery_limit_zero_raises():
    # 0 bedeutet nicht "kein Limit" — es waere ein Budget, das keine einzige
    # Zustellung erlaubt. Wer das Sicherheitsnetz abschalten will, faehrt
    # klassische Queues; dort kennt der Broker das Argument nicht.
    with pytest.raises(ConfigurationError, match="delivery_limit must be > 0"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            delivery_limit=0,
        )


def test_config_delivery_limit_negative_raises():
    with pytest.raises(ConfigurationError, match="delivery_limit must be > 0"):
        SwarmQConfig(
            broker_url="redis://localhost",
            backend_url="redis://localhost",
            delivery_limit=-1,
        )


# --- Plan 2026-09-03-001: Pool-Modus, Recycling-Schwellen, Lebenszeichen ---


def _cfg(**kwargs):
    return SwarmQConfig(broker_url="amqp://rabbit", backend_url="redis://cache", **kwargs)


def test_pool_defaults_to_io_with_recycling_off():
    cfg = _cfg()
    assert cfg.pool == "io"
    assert cfg.recycle_rss_mb is None
    assert cfg.recycle_max_tasks is None
    assert cfg.heartbeat_interval_s == 5.0
    assert cfg.heartbeat_timeout_s == 60.0
    assert cfg.consumer_timeout_s == 1800.0


def test_cpu_pool_applies_h_series_recycling_defaults():
    cfg = _cfg(pool="cpu")
    assert cfg.recycle_rss_mb == 250
    assert cfg.recycle_max_tasks == 1000


def test_cpu_pool_keeps_explicit_recycling_thresholds():
    cfg = _cfg(pool="cpu", recycle_rss_mb=300, recycle_max_tasks=50)
    assert (cfg.recycle_rss_mb, cfg.recycle_max_tasks) == (300, 50)


def test_invalid_pool_raises():
    with pytest.raises(ConfigurationError, match="pool must be one of"):
        _cfg(pool="gpu")


@pytest.mark.parametrize("field", ["recycle_rss_mb", "recycle_max_tasks"])
def test_non_positive_recycling_threshold_raises(field):
    with pytest.raises(ConfigurationError, match=f"{field} must be > 0"):
        _cfg(**{field: 0})


def test_heartbeat_timeout_must_exceed_interval():
    with pytest.raises(ConfigurationError, match="heartbeat_timeout_s must be greater"):
        _cfg(heartbeat_interval_s=10.0, heartbeat_timeout_s=10.0)


def test_env_overrides_for_pool_fields(monkeypatch):
    monkeypatch.setenv("SWARMQ_POOL", "cpu")
    monkeypatch.setenv("SWARMQ_RECYCLE_RSS_MB", "300")
    monkeypatch.setenv("SWARMQ_CONSUMER_TIMEOUT_S", "900")
    cfg = _cfg()
    assert cfg.pool == "cpu"
    assert cfg.recycle_rss_mb == 300
    assert cfg.recycle_max_tasks == 1000
    assert cfg.consumer_timeout_s == 900.0


def test_invalid_env_recycling_threshold_raises(monkeypatch):
    monkeypatch.setenv("SWARMQ_RECYCLE_MAX_TASKS", "viele")
    with pytest.raises(ConfigurationError):
        _cfg()


class TestWorkerIdentity:
    """U2 (Plan 2026-09-08-001, R3/KTD2): stabile Worker-Kennung.

    Der Slot ueberlebt den Respawn, der PID nicht — ein Dashboard, das nach
    ``worker_id`` gruppiert, verlor bisher bei jedem Ersatzprozess seine
    Zeitreihe (``str(id(self))`` wechselte sogar ohne Respawn).
    """

    def test_slot_identity_uses_hostname_and_slot(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_WORKER_ID", raising=False)
        monkeypatch.setattr("swarmq.config.socket.gethostname", lambda: "h")
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", worker_slot=2)
        assert cfg.worker_identity == "h:2"

    def test_env_worker_id_replaces_the_hostname_base(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_WORKER_ID", "phx")
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", worker_slot=2)
        assert cfg.worker_identity == "phx:2"

    def test_embedded_worker_without_slot_uses_the_pid(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_WORKER_ID", raising=False)
        monkeypatch.setattr("swarmq.config.socket.gethostname", lambda: "h")
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        # Das ``p`` haelt die beiden Formen auseinander: ohne Praefix waere
        # ``h:7`` mal ein Slot und mal ein Prozess.
        assert cfg.worker_identity == f"h:p{os.getpid()}"


class TestPortsPerSlot:
    """U3 (R5): ``--metrics-port``/``--health-port`` sind Basisports; der
    Worker in Slot s bindet ``basis + s``.

    Vorher band jedes Kind denselben Port: das erste gewann, die uebrigen
    scheiterten, und der Lauf lief unterbesetzt weiter."""

    def test_slot_is_added_to_the_base_port(self):
        cfg = SwarmQConfig(
            broker_url="amqp://x", backend_url="valkey://x",
            metrics_port=9000, health_port=9100, worker_slot=3,
        )
        assert cfg.metrics_port == 9003
        assert cfg.health_port == 9103

    def test_env_port_is_the_base_not_the_final_port(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", worker_slot=1)
        assert cfg.metrics_port == 9001

    def test_without_a_slot_the_base_port_is_bound(self):
        cfg = SwarmQConfig(
            broker_url="amqp://x", backend_url="valkey://x", metrics_port=9000,
        )
        assert cfg.metrics_port == 9000

    def test_slot_zero_keeps_the_base_port(self):
        cfg = SwarmQConfig(
            broker_url="amqp://x", backend_url="valkey://x", metrics_port=9000, worker_slot=0,
        )
        assert cfg.metrics_port == 9000


class TestLoggingFields:
    """Plan 2026-10-06, U5 (R13/KTD7/KTD8): jede Logging-Einstellung gibt es
    auch als ``SwarmQConfig``-Feld mit eigener Umgebungsvariable.

    Test-Liste:
    - [x] Defaults der neuen Felder
    - [x] Env-Overrides je Feld (inkl. SWARMQ_LOGGER_LEVELS als CSV)
    - [x] SWARMQ_LOG_FORMAT setzt log_json und gewinnt gegen SWARMQ_LOG_JSON
    - [x] Ungueltige Werte: ConfigurationError
    - [x] log_config zusammen mit configure_logging=False: ConfigurationError
    - [x] CRITICAL fuer log_level und log_lifecycle_level
    - [x] (U7, KTD7) ``logging_settings``: die Logging-Felder kommen aus dem
          aufgeloesten Objekt und sind von den Env-Overrides ausgenommen
    - [x] ``logging_settings``: ein ungueltiger Logging-Env-Wert wird nicht
          mehr geparst (die CLI hat ihn im Supervisor schon ueberstimmt)
    - [x] ohne ``logging_settings`` wird derselbe Env-Wert weiter geprueft
    - [x] (Plan 2026-10-06-1811, U4) ``log_drain_timeout``: Default 5.0, Env
          ``SWARMQ_LOG_DRAIN_TIMEOUT``, <= 0 ist ein Fehler, aus
          ``logging_settings`` uebernommen
    - [x] (Review A1) ``log_drain_timeout`` ``inf``, ``nan`` oder groesser
          ``threading.TIMEOUT_MAX`` -- als Argument wie per Env: Fehler.
          Sonst wuerfe ``Thread.join`` im Compute-Kind ``OverflowError``
          (``nan``: jedes Leeren gaebe sofort auf)
    """

    @staticmethod
    def _cfg(**kwargs):
        return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)

    def test_defaults(self):
        cfg = self._cfg()
        assert cfg.log_config is None
        assert cfg.configure_logging is True
        assert cfg.logger_levels == ()
        assert cfg.log_queue_size == 10_000
        assert cfg.log_lifecycle_level == "DEBUG"
        assert cfg.log_queue is False
        assert cfg.log_drain_timeout == 5.0

    def test_env_overrides_apply(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_LOG_CONFIG", "/etc/swarmq/logging.json")
        monkeypatch.setenv("SWARMQ_LOG_QUEUE_SIZE", "500")
        monkeypatch.setenv("SWARMQ_LOG_LIFECYCLE_LEVEL", "INFO")
        monkeypatch.setenv("SWARMQ_LOG_QUEUE", "true")
        monkeypatch.setenv("SWARMQ_LOG_DRAIN_TIMEOUT", "0.5")
        cfg = self._cfg()
        assert cfg.log_drain_timeout == 0.5
        assert cfg.log_config == "/etc/swarmq/logging.json"
        assert cfg.log_queue_size == 500
        assert cfg.log_lifecycle_level == "INFO"
        assert cfg.log_queue is True

    def test_env_can_switch_off_configuring(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_CONFIGURE_LOGGING", "false")
        assert self._cfg().configure_logging is False

    def test_env_logger_levels_are_parsed_into_pairs(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_LOGGER_LEVELS", "aio_pika=WARNING,valkey=error")
        assert self._cfg().logger_levels == (("aio_pika", "WARNING"), ("valkey", "ERROR"))

    def test_env_log_format_sets_log_json_and_wins_over_log_json(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "human")
        monkeypatch.setenv("SWARMQ_LOG_JSON", "true")
        assert self._cfg().log_json is False
        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "json")
        assert self._cfg(log_json=False).log_json is True

    def test_unknown_env_log_format_is_a_configuration_error(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "xml")
        with pytest.raises(ConfigurationError, match=r"SWARMQ_LOG_FORMAT.*'xml'"):
            self._cfg()

    def test_unknown_lifecycle_level_is_a_configuration_error(self):
        with pytest.raises(ConfigurationError, match=r"log_lifecycle_level.*'LOUD'"):
            self._cfg(log_lifecycle_level="LOUD")

    @pytest.mark.parametrize("size", [0, -1])
    def test_non_positive_queue_size_is_a_configuration_error(self, size):
        with pytest.raises(ConfigurationError, match="log_queue_size must be > 0"):
            self._cfg(log_queue_size=size)

    @pytest.mark.parametrize("seconds", [0, -1.5])
    def test_non_positive_drain_timeout_is_a_configuration_error(self, seconds):
        with pytest.raises(ConfigurationError, match="log_drain_timeout must be > 0"):
            self._cfg(log_drain_timeout=seconds)

    @pytest.mark.parametrize("raw", ["inf", "nan", "1e10"])
    @pytest.mark.parametrize("via", ["argument", "env"])
    def test_unbounded_drain_timeout_is_a_configuration_error(self, monkeypatch, raw, via):
        kwargs = {}
        if via == "env":
            monkeypatch.setenv("SWARMQ_LOG_DRAIN_TIMEOUT", raw)
        else:
            kwargs["log_drain_timeout"] = float(raw)
        with pytest.raises(ConfigurationError, match="log_drain_timeout must be > 0"):
            self._cfg(**kwargs)

    def test_logger_level_pairs_are_validated_and_normalised(self):
        assert self._cfg(logger_levels=(("aiormq", "error"),)).logger_levels == (
            ("aiormq", "ERROR"),
        )
        with pytest.raises(ConfigurationError, match=r"logger_levels.*'LOUD'"):
            self._cfg(logger_levels=(("aio_pika", "LOUD"),))

    def test_log_config_without_configuring_is_a_configuration_error(self):
        with pytest.raises(ConfigurationError, match=r"log_config.*configure_logging"):
            self._cfg(log_config="logging.json", configure_logging=False)

    def test_critical_is_accepted(self):
        cfg = self._cfg(log_level="CRITICAL", log_lifecycle_level="CRITICAL")
        assert (cfg.log_level, cfg.log_lifecycle_level) == ("CRITICAL", "CRITICAL")

    def test_resolved_settings_win_over_the_env(self, monkeypatch):
        """KTD7: ``--log-level INFO`` gilt im Worker auch bei
        ``SWARMQ_LOG_LEVEL=DEBUG`` -- ``app.config.log_level`` zeigt den
        tatsaechlich wirksamen Wert."""
        from swarmq.logging_settings import LoggingSettings

        monkeypatch.setenv("SWARMQ_LOG_LEVEL", "DEBUG")

        cfg = self._cfg(logging_settings=LoggingSettings(level="INFO", level_explicit=True))

        assert cfg.log_level == "INFO"

    def test_every_logging_field_comes_from_the_settings(self, monkeypatch):
        from swarmq.logging_settings import LoggingSettings

        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "json")
        monkeypatch.setenv("SWARMQ_LOG_QUEUE_SIZE", "5")
        monkeypatch.setenv("SWARMQ_LOG_DRAIN_TIMEOUT", "9")
        settings = LoggingSettings(
            format="human", configure=False, queue_size=7, lifecycle_level="INFO",
            logger_levels=(("aio_pika", "ERROR"),), drain_timeout=0.25,
        )

        cfg = self._cfg(logging_settings=settings)

        assert cfg.log_drain_timeout == 0.25
        assert (cfg.log_json, cfg.configure_logging, cfg.log_queue_size) == (False, False, 7)
        assert (cfg.log_lifecycle_level, cfg.logger_levels) == ("INFO", (("aio_pika", "ERROR"),))

    @pytest.mark.parametrize(
        ("env_name", "raw"),
        [
            ("SWARMQ_LOG_FORMAT", "jsonl"),
            ("SWARMQ_LOG_QUEUE_SIZE", "many"),
            ("SWARMQ_LOGGER_LEVELS", "aio_pika"),
        ],
    )
    def test_invalid_logging_env_is_ignored_with_resolved_settings(
        self, monkeypatch, env_name, raw,
    ):
        """Review #5: ``--log-format json`` ueberstimmt im Supervisor ein
        ``SWARMQ_LOG_FORMAT=jsonl``. Parste der Worker das Env trotzdem,
        endete jeder Worker mit Exit 78, obwohl die CLI gewonnen hat."""
        from swarmq.logging_settings import LoggingSettings

        monkeypatch.setenv(env_name, raw)

        cfg = self._cfg(logging_settings=LoggingSettings(format="json", queue_size=7))

        assert (cfg.log_json, cfg.log_queue_size, cfg.logger_levels) == (True, 7, ())

    def test_invalid_logging_env_is_still_rejected_without_settings(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "jsonl")
        with pytest.raises(ConfigurationError, match=r"SWARMQ_LOG_FORMAT.*'jsonl'"):
            self._cfg()


# === CLI-Vorrang fuer Worker-Optionen (CLI > Env > kwargs > Default) ===
# Test-Liste:
# - [x] resolved_fields: Env ueberschreibt ein aufgeloestes Feld nicht
# - [x] eingebettet ohne resolved_fields: kwargs, dann Env wie bisher
#       (test_env_var_overrides_programmatic_value und die Env-Tests oben)
# Die Aufloesung selbst testet tests/unit/test_cli_worker_options.py.


def test_resolved_field_is_not_overridden_by_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_CONCURRENCY", "5")
    cfg = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x",
        concurrency=20, resolved_fields=("concurrency",),
    )
    assert cfg.concurrency == 20


# === Zurueckstellung bei Backend-Ausfall (Gate-Backoff) ===
# Test-Liste:
# - [x] Defaults 1 s / 60 s
# - [x] Env-Overrides SWARMQ_BACKEND_DEFER_BASE_DELAY / _MAX_DELAY
# - [x] Werte <= 0 -> ConfigurationError
# - [x] Obergrenze kleiner als Basis -> ConfigurationError


def test_backend_defer_delays_default_to_one_and_sixty_seconds():
    cfg = _cfg()
    assert cfg.backend_defer_base_delay == 1.0
    assert cfg.backend_defer_max_delay == 60.0


def test_backend_defer_delays_from_env(monkeypatch):
    monkeypatch.setenv("SWARMQ_BACKEND_DEFER_BASE_DELAY", "0.5")
    monkeypatch.setenv("SWARMQ_BACKEND_DEFER_MAX_DELAY", "30")
    cfg = _cfg()
    assert cfg.backend_defer_base_delay == 0.5
    assert cfg.backend_defer_max_delay == 30.0


@pytest.mark.parametrize("field", ["backend_defer_base_delay", "backend_defer_max_delay"])
@pytest.mark.parametrize("value", [0.0, -1.0])
def test_backend_defer_delays_must_be_positive(field, value):
    with pytest.raises(ConfigurationError, match=f"{field} must be > 0"):
        _cfg(**{field: value})


def test_backend_defer_max_delay_below_base_raises():
    with pytest.raises(ConfigurationError, match="backend_defer_max_delay must be >= backend_defer_base_delay"):
        _cfg(backend_defer_base_delay=10.0, backend_defer_max_delay=5.0)
