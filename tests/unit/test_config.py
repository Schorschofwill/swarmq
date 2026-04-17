"""Tests for swarmq.config — SwarmQConfig dataclass."""

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
