"""Namensraum nur kleingeschrieben (Review MR 15; technische Entscheidung
von Claude am 2026-10-11, neues Feature ohne Bestand).

Valkey-Keys unterscheiden Gross- und Kleinschreibung: ``App1`` und ``app1``
waeren zwei getrennte Apps, die sich nur in der Schreibweise unterscheiden.

Test-Liste:
- [x] Grossbuchstaben -> ``ValueError`` mit Hinweis auf Kleinschreibung
- [x] ``SWARMQ_NAMESPACE=App1`` -> ``ConfigurationError`` (Exit 78)
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.timeout(5)


@pytest.mark.parametrize("value", ["App1", "APP", "billing-Prod"])
def test_uppercase_namespace_is_rejected_with_a_hint(value: str) -> None:
    from swarmq.keyspace import validate_namespace

    with pytest.raises(ValueError, match="lowercase"):
        validate_namespace(value)


def test_uppercase_environment_namespace_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.config import SwarmQConfig
    from swarmq.exceptions import ConfigurationError

    monkeypatch.setenv("SWARMQ_NAMESPACE", "App1")

    with pytest.raises(ConfigurationError, match="lowercase: 'app1'"):
        SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
