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
