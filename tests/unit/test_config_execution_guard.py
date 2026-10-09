"""Konfiguration des Ausführungsschutzes (Plan 2026-10-08-1541, U8, KTD5).

Test-Liste:
1. Standard: ``execution_guard`` an, ``execution_guard_ttl_s`` 60,
   ``execution_guard_max_orphans`` 3.
2. ``execution_guard_done_ttl_s`` ohne Wert: ``max(retry_max_delay,
   consumer_timeout_s) + 300``.
3. ... mindestens ``execution_guard_ttl_s``.
4. Explizit kleiner als ``execution_guard_ttl_s`` → ``ConfigurationError``.
5. ``execution_guard_ttl_s`` > 0 und ``execution_guard_max_orphans`` >= 1.
6. Env-Mapping aller vier Felder.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_defaults() -> None:
    cfg = _cfg()
    assert cfg.execution_guard is True
    assert cfg.execution_guard_ttl_s == 60.0
    assert cfg.execution_guard_max_orphans == 3


def test_done_ttl_default_covers_retry_delay_and_consumer_timeout() -> None:
    assert _cfg(retry_max_delay=3600.0, consumer_timeout_s=1800.0).execution_guard_done_ttl_s == 3900.0
    assert _cfg(retry_max_delay=60.0, consumer_timeout_s=1800.0).execution_guard_done_ttl_s == 2100.0


def test_done_ttl_default_is_at_least_the_running_ttl() -> None:
    cfg = _cfg(retry_max_delay=10.0, consumer_timeout_s=10.0, execution_guard_ttl_s=900.0)
    assert cfg.execution_guard_done_ttl_s == 900.0


def test_explicit_done_ttl_below_running_ttl_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="execution_guard_done_ttl_s"):
        _cfg(execution_guard_ttl_s=120.0, execution_guard_done_ttl_s=60.0)


@pytest.mark.parametrize(
    ("field", "value"),
    [("execution_guard_ttl_s", 0.0), ("execution_guard_max_orphans", 0)],
)
def test_ttl_and_orphan_limit_must_be_positive(field: str, value: float) -> None:
    with pytest.raises(ConfigurationError, match=field):
        _cfg(**{field: value})


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_EXECUTION_GUARD", "false")
    monkeypatch.setenv("SWARMQ_EXECUTION_GUARD_TTL_S", "30")
    monkeypatch.setenv("SWARMQ_EXECUTION_GUARD_DONE_TTL_S", "900")
    monkeypatch.setenv("SWARMQ_EXECUTION_GUARD_MAX_ORPHANS", "5")
    cfg = _cfg()
    assert cfg.execution_guard is False
    assert cfg.execution_guard_ttl_s == 30.0
    assert cfg.execution_guard_done_ttl_s == 900.0
    assert cfg.execution_guard_max_orphans == 5
