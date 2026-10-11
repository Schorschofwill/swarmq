"""``SwarmQConfig.namespace`` / ``SWARMQ_NAMESPACE`` (Plan 0.12.0, MR 15, BZ-10).

Test-Liste:
- [x] ``SWARMQ_NAMESPACE=app1`` -> ``config.namespace == "app1"``; ungesetzt/leer -> ``None``
- [x] ungueltiger Namensraum -> ``ConfigurationError`` (Exit 78)
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_namespace_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    assert _cfg().namespace == "app1"


def test_empty_namespace_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "")

    assert _cfg().namespace is None
    assert _cfg(namespace="").namespace is None


@pytest.mark.parametrize("source", ["kwarg", "environment"])
def test_invalid_namespace_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch, source: str) -> None:
    from swarmq.exceptions import ConfigurationError

    if source == "environment":
        monkeypatch.setenv("SWARMQ_NAMESPACE", "app*")
    with pytest.raises(ConfigurationError, match="namespace"):
        _cfg(**({"namespace": "app*"} if source == "kwarg" else {}))
