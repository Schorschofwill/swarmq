"""Konfiguration des Cron-Aufräumens (Plan 2026-10-08-1541, U10, KTD8).

Test-Liste:
1. Standard: ``scheduler_prune`` ``"on"``, ``scheduler_prune_after_s`` 86400.
2. ``scheduler_prune`` nur ``off``, ``log``, ``on``; sonst ``ConfigurationError``.
3. ``scheduler_prune_after_s`` > 0.
4. Kreuzvalidierung: ``scheduler_prune_after_s`` >= 3 x
   ``scheduler_seen_interval_s`` (sonst gälte ein lebender Cron als tot).
5. Env-Mapping beider Felder.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_defaults() -> None:
    cfg = _cfg()
    assert cfg.scheduler_prune == "on"
    assert cfg.scheduler_prune_after_s == 86400.0


@pytest.mark.parametrize("mode", ["off", "log", "on"])
def test_prune_modes_are_accepted(mode: str) -> None:
    assert _cfg(scheduler_prune=mode).scheduler_prune == mode


@pytest.mark.parametrize("mode", ["", "ON", "dry-run", "true"])
def test_unknown_prune_mode_is_rejected(mode: str) -> None:
    with pytest.raises(ConfigurationError, match="scheduler_prune must be one of"):
        _cfg(scheduler_prune=mode)


@pytest.mark.parametrize("value", [0, -1.0])
def test_prune_after_must_be_positive(value: float) -> None:
    with pytest.raises(ConfigurationError, match="scheduler_prune_after_s must be > 0"):
        _cfg(scheduler_prune_after_s=value)


def test_prune_after_must_cover_three_seen_intervals() -> None:
    with pytest.raises(ConfigurationError, match="scheduler_prune_after_s must be >= 3x scheduler_seen_interval_s"):
        _cfg(scheduler_seen_interval_s=60.0, scheduler_prune_after_s=179.0)


def test_prune_after_of_exactly_three_seen_intervals_is_accepted() -> None:
    assert _cfg(scheduler_seen_interval_s=60.0, scheduler_prune_after_s=180.0).scheduler_prune_after_s == 180.0


def test_env_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER_PRUNE", "log")
    monkeypatch.setenv("SWARMQ_SCHEDULER_PRUNE_AFTER_S", "3600")
    cfg = _cfg()
    assert cfg.scheduler_prune == "log"
    assert cfg.scheduler_prune_after_s == 3600.0


@pytest.mark.parametrize(("seen", "prune_after"), [(0.05, 0.15), (0.1, 0.3)])
def test_exact_multiple_is_accepted_despite_float_rounding(seen: float, prune_after: float) -> None:
    cfg = _cfg(scheduler_seen_interval_s=seen, scheduler_prune_after_s=prune_after)
    assert cfg.scheduler_prune_after_s == prune_after
