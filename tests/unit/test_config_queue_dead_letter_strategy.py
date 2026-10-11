"""``SWARMQ_QUEUE_DEAD_LETTER_STRATEGY`` (Plan 0.12.0, MR 8, BZ-19; Abschnitt 1, Nachtrag 2026-10-10).

Test-Liste:
1. [x] Standard ``auto``.
2. [x] Env ``SWARMQ_QUEUE_DEAD_LETTER_STRATEGY``.
3. [x] Nur ``auto``, ``at-least-once`` oder ``legacy``.
4. [x] ``SwarmQ`` reicht den Wert an den Broker.
5. [x] ``at-least-once`` mit ``SWARMQ_QUEUE_TYPE=classic`` -> ``ConfigurationError`` (Review MR 8, H3);
       ``auto`` und ``legacy`` bleiben mit classic erlaubt.
"""

from __future__ import annotations

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: object) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)  # type: ignore[arg-type]


def test_default_is_auto() -> None:
    assert _cfg().queue_dead_letter_strategy == "auto"


def test_env_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_QUEUE_DEAD_LETTER_STRATEGY", "legacy")
    assert _cfg().queue_dead_letter_strategy == "legacy"


@pytest.mark.parametrize("value", ["auto", "at-least-once", "legacy"])
def test_valid_values(value: str) -> None:
    assert _cfg(queue_dead_letter_strategy=value).queue_dead_letter_strategy == value


@pytest.mark.parametrize("value", ["", "at-most-once", "AUTO"])
def test_other_values_are_rejected(value: str) -> None:
    with pytest.raises(ConfigurationError, match="queue_dead_letter_strategy must be one of"):
        _cfg(queue_dead_letter_strategy=value)


def test_swarmq_hands_the_strategy_to_the_broker() -> None:
    from swarmq.app import SwarmQ
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

    app = SwarmQ(RabbitMQBroker("amqp://x"), ValkeyBackend("valkey://x"), queue_dead_letter_strategy="at-least-once")

    assert app.broker.dead_letter_strategy == "at-least-once"


def test_strict_mode_with_classic_queues_is_rejected() -> None:
    """Review MR 8, H3: classic-Queues kennen kein at-least-once-Dead-Lettering;
    der strenge Modus waere dort still wirkungslos."""
    with pytest.raises(ConfigurationError, match="queue_dead_letter_strategy='at-least-once'.*quorum"):
        _cfg(queue_type="classic", queue_dead_letter_strategy="at-least-once")
