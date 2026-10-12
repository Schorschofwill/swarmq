"""Scheduler je Worker abschaltbar (Plan 2026-10-12-1105, MR 5, P1, T1).

Test-Liste:
1. Standard: ``scheduler == "on"``.
2. ``SWARMQ_SCHEDULER=off`` -> ``"off"``; die Env überschreibt den
   Programmwert (Konvention der Env-Overrides).
3. Ungültiger Wert (``"no"``) -> ``ConfigurationError``, die Meldung nennt
   ``on`` und ``off``.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]  # Test-Helfer reicht beliebige Felder durch


def test_default_is_on() -> None:
    assert _cfg().scheduler == "on"


def test_environment_off_overrides_the_program_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_SCHEDULER", "off")
    assert _cfg().scheduler == "off"
    assert _cfg(scheduler="on").scheduler == "off"


@pytest.mark.parametrize("value", ["no", "", "OFF", "false"])
def test_invalid_value_is_rejected_naming_on_and_off(value: str) -> None:
    with pytest.raises(ConfigurationError, match="scheduler must be one of") as exc_info:
        _cfg(scheduler=value)
    assert "'on'" in str(exc_info.value)
    assert "'off'" in str(exc_info.value)
