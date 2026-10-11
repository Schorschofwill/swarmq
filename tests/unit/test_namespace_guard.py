"""Namensraum an den Einstiegen (Plan 0.12.0, MR 15, BZ-10).

Bis 0.11.1 lehnte swarmq einen gesetzten ``SWARMQ_NAMESPACE`` ab (Sperre
``namespace_guard``, Rollback-Ziel vor 0.12). Ab 0.12 ist der Namensraum ein
Feld von ``SwarmQConfig``; die Einstiege lehnen nur noch einen **ungueltigen**
Wert ab. 0.11.1 verweigert einen gesetzten Namensraum weiterhin, darum ist nach
dem Setzen ein Rollback nur auf >= 0.12 moeglich (Plan Abschnitt 7).

Test-Liste:
1. ``SWARMQ_NAMESPACE`` gesetzt -> ``SwarmQConfig`` uebernimmt ihn.
2. Leer gesetzt -> kein Namensraum (wie nicht gesetzt).
3. Producer: ``SwarmQ(...)`` mit gesetztem Namensraum startet, Config und
   Backend tragen ihn.
4. ``swarmq worker`` mit ungueltigem Namensraum endet mit Exit 78.
5. ``swarmq schedule``, ``dlq`` und ``inspect`` bauen Backend bzw. Broker
   direkt (ohne ``SwarmQConfig``) -> ungueltiger Namensraum: Exit 78, bevor
   eine Verbindung entsteht.
6. ``swarmq worker --check`` meldet einen ungueltigen Namensraum als Fehler
   (Exit 1 nach dem ``--check``-Vertrag).
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig


def _cfg() -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")


def test_set_namespace_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    assert _cfg().namespace == "app1"


def test_empty_namespace_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_NAMESPACE", "")

    assert _cfg().namespace is None


def test_producer_app_accepts_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    monkeypatch.setenv("SWARMQ_NAMESPACE", "app1")

    app = SwarmQ(broker=RabbitMQBroker("amqp://x"), backend=ValkeyBackend("valkey://x"))

    assert (app.config.namespace, app.backend.namespace) == ("app1", "app1")


def test_worker_cli_exits_78_on_an_invalid_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    from swarmq.cli import main
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://x")
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://x")
    monkeypatch.setenv("SWARMQ_NAMESPACE", "a:b")

    assert main(["worker", "--module=json"]) == CONFIG_ERROR_EXIT_CODE == 78


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["schedule", "list"], id="schedule-list"),
        pytest.param(["schedule", "remove", "nightly"], id="schedule-remove"),
        pytest.param(["dlq", "list", "--queue", "default"], id="dlq-list"),
        pytest.param(["inspect", "--module", "json"], id="inspect"),
    ],
)
def test_cli_commands_exit_78_on_an_invalid_namespace_before_connecting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], argv: list[str]
) -> None:
    from swarmq.cli import _shared, main

    async def must_not_connect() -> None:
        raise AssertionError("no backend/broker may be built with an invalid namespace")

    monkeypatch.setattr(_shared, "_backend_from_env", must_not_connect)
    monkeypatch.setattr(_shared, "_broker_from_env", must_not_connect)
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://x")
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://x")
    monkeypatch.setenv("SWARMQ_NAMESPACE", "a:b")

    assert main(argv) == 78
    assert "invalid namespace" in capsys.readouterr().err


def test_worker_check_reports_an_invalid_namespace(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    from swarmq.cli import main

    monkeypatch.setenv("SWARMQ_NAMESPACE", "a:b")

    assert main(["worker", "--module=json", "--check"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert "invalid namespace" in json.dumps(payload)
