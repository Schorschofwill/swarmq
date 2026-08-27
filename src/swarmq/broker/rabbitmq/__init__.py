"""RabbitMQ-Broker (Paket seit Plan U5).

``RabbitMQBroker`` erfüllt ``swarmq.contracts.Broker`` und ``BrokerAdmin``;
der Importpfad ``swarmq.broker.rabbitmq.RabbitMQBroker`` bleibt gültig. Die
Fassade re-exportiert bewusst weder ``aio_pika`` noch ``logger`` — Tests
patchen die Submodule (``swarmq.broker.rabbitmq.connection.aio_pika…``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.broker.rabbitmq.admin import RabbitMQAdmin


class RabbitMQBroker(RabbitMQAdmin):
    """Verbindung + Zustellung/Verben + Admin in einer Klasse (MRO:
    ``RabbitMQAdmin → RabbitMQDelivery → RabbitMQConnection``)."""


__all__ = ["RabbitMQBroker"]

if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import Broker, BrokerAdmin

    _conforms_broker: type[Broker] = RabbitMQBroker
    _conforms_admin: type[BrokerAdmin] = RabbitMQBroker
