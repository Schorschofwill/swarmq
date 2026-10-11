"""Broker- und Backend-Verträge der 0.2.0-Restrukturierung (R1, KTD1–KTD4).

Siehe ``docs/design/2026-09-0-2-0-vertraege-und-modulschnitt.md``.
"""

from swarmq.contracts.backend import BatchResultBackend, ResultBackend
from swarmq.contracts.broker import (
    BatchPublisher,
    Broker,
    BrokerAdmin,
    Consumer,
    DeliveryCallback,
    Publisher,
)
from swarmq.contracts.control import Setpoint, SetpointRead, SetpointUnreadable
from swarmq.contracts.coordination import (
    ChordStore,
    CronReport,
    CronReports,
    ExecutionGuardStore,
    GuardClaim,
    LeaderInfo,
    LockObservation,
    LockStore,
    LockToken,
    QueueReports,
    RateLimitDecision,
    RateLimitStore,
    ScheduleStore,
)
from swarmq.contracts.message import Delivery, PublishReceipt

__all__ = [
    "BatchPublisher",
    "BatchResultBackend",
    "Broker",
    "BrokerAdmin",
    "ChordStore",
    "Consumer",
    "CronReport",
    "CronReports",
    "Delivery",
    "DeliveryCallback",
    "ExecutionGuardStore",
    "GuardClaim",
    "LeaderInfo",
    "LockObservation",
    "LockStore",
    "LockToken",
    "PublishReceipt",
    "Publisher",
    "QueueReports",
    "RateLimitDecision",
    "RateLimitStore",
    "ResultBackend",
    "ScheduleStore",
    "Setpoint",
    "SetpointRead",
    "SetpointUnreadable",
]
