"""Schalter der Eviction-Pruefung (Plan 0.12.0, MR 14, BZ-04).

Test-Liste:
1. [x] Standard ``strict``.
2. [ ] Env ``SWARMQ_VALKEY_EVICTION_CHECK`` (``warn``, ``off``; Gross/Klein egal).
3. [ ] Ungueltiger Wert -> ``ConfigurationError``.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_default_is_strict() -> None:
    assert _cfg().valkey_eviction_check == "strict"


@pytest.mark.parametrize(("raw", "expected"), [("warn", "warn"), ("off", "off"), (" Strict ", "strict")])
def test_env_mapping(monkeypatch: pytest.MonkeyPatch, raw: str, expected: str) -> None:
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", raw)
    assert _cfg().valkey_eviction_check == expected


def test_invalid_value_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "loud")
    with pytest.raises(ConfigurationError, match="valkey_eviction_check must be one of"):
        _cfg()
