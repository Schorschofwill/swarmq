"""Gnadenfrist fuer unbekannte Tasks (Plan 0.12.0, MR 11, BZ-06).

Test-Liste:
1. [x] Standard 900 s.
2. [x] Env ``SWARMQ_UNKNOWN_TASK_GRACE_S``.
3. [x] 0 ist erlaubt (schaltet die Frist ab), negativ nicht.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


@pytest.mark.entscheidung("E-008")
def test_default_is_fifteen_minutes() -> None:
    assert _cfg().unknown_task_grace_s == 900.0


def test_env_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_UNKNOWN_TASK_GRACE_S", "60")
    assert _cfg().unknown_task_grace_s == 60.0


def test_zero_switches_the_grace_off() -> None:
    assert _cfg(unknown_task_grace_s=0).unknown_task_grace_s == 0.0


def test_must_not_be_negative() -> None:
    with pytest.raises(ConfigurationError, match="unknown_task_grace_s must be >= 0"):
        _cfg(unknown_task_grace_s=-1.0)
