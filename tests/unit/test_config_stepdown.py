"""``scheduler_publish_stepdown_s`` (Plan 0.12.0, MR 15, BZ-10).

Test-Liste:
- [x] Default: ``scheduler_lock_ttl``
- [x] ``SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S`` setzt den Wert; 0 ist erlaubt (aus)
- [x] negativ oder nicht endlich -> ``ConfigurationError``
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


@pytest.mark.entscheidung("E-013")
def test_default_is_the_scheduler_lock_ttl() -> None:
    assert _cfg(scheduler_lock_ttl=40.0).scheduler_publish_stepdown_s == 40.0


@pytest.mark.parametrize(("raw", "expected"), [("5", 5.0), ("0", 0.0)])
def test_environment_sets_the_value(monkeypatch: pytest.MonkeyPatch, raw: str, expected: float) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S", raw)

    assert _cfg().scheduler_publish_stepdown_s == expected


@pytest.mark.parametrize("value", [-1.0, float("inf"), float("nan")])
def test_negative_or_infinite_is_rejected(value: float) -> None:
    from swarmq.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="scheduler_publish_stepdown_s"):
        _cfg(scheduler_publish_stepdown_s=value)
