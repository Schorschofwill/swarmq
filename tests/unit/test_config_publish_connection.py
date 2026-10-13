"""``broker_publish_connection`` (Plan 0.12.0, MR 16, BZ-16).

Test-Liste:
- [x] Default ``separate``
- [x] ``SWARMQ_BROKER_PUBLISH_CONNECTION`` setzt den Wert
- [x] anderer Wert -> ``ConfigurationError``
- [x] ``SwarmQ`` reicht den Wert an den Broker durch
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


@pytest.mark.entscheidung("E-025")
def test_default_is_separate() -> None:
    assert _cfg().broker_publish_connection == "separate"


def test_environment_sets_the_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_BROKER_PUBLISH_CONNECTION", "shared")

    assert _cfg().broker_publish_connection == "shared"


def test_other_values_are_rejected() -> None:
    from swarmq.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="broker_publish_connection"):
        _cfg(broker_publish_connection="both")


def test_swarmq_hands_the_mode_to_the_broker() -> None:
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    app = SwarmQ(RabbitMQBroker("amqp://x"), ValkeyBackend("valkey://x"), broker_publish_connection="shared")

    assert app.broker.publish_connection == "shared"
