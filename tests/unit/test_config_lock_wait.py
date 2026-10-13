"""Obergrenze der Wartezeit bei Lock-Konflikten (Plan 0.12.0, MR 13, BZ-05).

Test-Liste:
1. [x] Standard 21600 s (6 h).
2. [x] Env ``SWARMQ_LOCK_WAIT_MAX_S``.
3. [x] Muss > 0 sein.
4. [x] Muss endlich sein: inf/nan bestehen ``<= 0`` und liessen jeden
       Konflikt mit OverflowError/ValueError scheitern (Review MR 13, P2-3).
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


@pytest.mark.entscheidung("E-007")
def test_default_is_six_hours() -> None:
    assert _cfg().lock_wait_max_s == 21600.0


def test_env_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_LOCK_WAIT_MAX_S", "600")
    assert _cfg().lock_wait_max_s == 600.0


def test_must_be_positive() -> None:
    with pytest.raises(ConfigurationError, match="lock_wait_max_s must be > 0"):
        _cfg(lock_wait_max_s=0)


@pytest.mark.parametrize("value", [float("inf"), float("nan"), "inf", "nan"])
def test_must_be_finite(monkeypatch: pytest.MonkeyPatch, value: object) -> None:
    """Review MR 13, P2-3: ``inf``/``nan`` bestehen die Pruefung ``<= 0``;
    ``int(inf * 1000)`` im Lock-Gate warf dann bei jedem Konflikt."""
    kwargs: dict[str, object] = {}
    if isinstance(value, str):
        monkeypatch.setenv("SWARMQ_LOCK_WAIT_MAX_S", value)
    else:
        kwargs["lock_wait_max_s"] = value
    with pytest.raises(ConfigurationError, match="lock_wait_max_s must be a finite number"):
        _cfg(**kwargs)
