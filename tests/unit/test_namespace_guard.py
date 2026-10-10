"""0.11.1 verweigert einen gesetzten Namensraum (Patch 0.11.1, Rollback-Ziel vor 0.12).

Ab 0.12 setzt ``SWARMQ_NAMESPACE`` ein Präfix vor alle Valkey-Keys und
Kanäle. Ein Rollback auf 0.11.x fiele sonst still in den Zustand ohne
Präfix: Termine, Locks und Ergebnisse lägen unter anderen Keys. 0.11.1
verweigert deshalb den Start, sobald die Variable gesetzt ist.

Test-Liste:
1. ``SWARMQ_NAMESPACE`` gesetzt → ``SwarmQConfig`` wirft ``ConfigurationError``
   mit Hinweis auf 0.12.
2. Leer gesetzt → kein Fehler (wie nicht gesetzt).
3. Producer: ``SwarmQ(...)`` wirft ``ConfigurationError``.
4. ``swarmq worker`` endet mit Exit 78.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg() -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")


def test_set_namespace_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    with pytest.raises(ConfigurationError, match=r"SWARMQ_NAMESPACE.*swarmq >= 0\.12"):
        _cfg()


def test_empty_namespace_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "")

    assert _cfg().broker_url == "amqp://x"


def test_producer_app_rejects_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    with pytest.raises(ConfigurationError, match=r"swarmq >= 0\.12"):
        SwarmQ(broker=RabbitMQBroker("amqp://x"), backend=ValkeyBackend("valkey://x"))


def test_worker_cli_exits_78(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.cli import main
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://x")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://x")
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    assert main(["worker", "--module=json"]) == CONFIG_ERROR_EXIT_CODE == 78
