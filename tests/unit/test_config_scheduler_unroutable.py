"""Frist fuer unroutbare Oneshots (Plan 0.12.0, MR 6, BZ-03).

Test-Liste:
1. [x] Standard 86400 s.
2. [x] Env ``SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S``.
3. [x] Wert muss > 0 sein.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_default_is_one_day() -> None:
    assert _cfg().scheduler_unroutable_oneshot_max_s == 86400.0


def test_env_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S", "3600")
    assert _cfg().scheduler_unroutable_oneshot_max_s == 3600.0


@pytest.mark.parametrize("value", [0, -1.0])
def test_must_be_positive(value: float) -> None:
    with pytest.raises(ConfigurationError, match="scheduler_unroutable_oneshot_max_s must be > 0"):
        _cfg(scheduler_unroutable_oneshot_max_s=value)
