"""Pre-Execute-Gates der Worker-Pipeline (Plan U7, KTD5/KTD6).

Jede Stufe vor der Ausführung ist ein ``Gate`` mit ``apply(delivery, ctx) ->
DeliveryOutcome``: ``Continue`` lässt die Zustellung weiter, ``Redelivered``
und ``Terminal`` beenden sie. Jedes Gate kapselt seine Fehlerbehandlung
(Backend-Fehler → ``Redelivered`` über den Broker) und seinen heutigen
Fallback (KTD3) — der Runner kennt keine Gate-Spezialfälle.

Die Gates sprechen die Vertragsverben (``redeliver``/``retry_later``/
``dead_letter``); Rückstellungen zielen auf die Zustellungs-Queue —
der Band-Erhalt beim Retry ist die benannte Verhaltenskorrektur (KTD2, U8).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from swarmq.contracts import Broker, BrokerAdmin, ResultBackend
from swarmq.contracts.coordination import (
    ChordStore,
    LockStore,
    LockToken,
    RateLimitStore,
    ScheduleStore,
)
from swarmq.worker.outcome import DeliveryOutcome

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher
    from swarmq.task import TaskInfo


class WorkerBroker(Broker, BrokerAdmin, Protocol):
    """Broker-Bedarf des Workers (U15): die vier Verben + Consume-Lifecycle
    (``Broker``) und die Tiefen/DLX-Seite (``BrokerAdmin``)."""


class WorkerBackend(ResultBackend, Protocol):
    """Backend-Bedarf des Workers (U15): der ``ResultBackend``-Vertrag plus
    die Koordinations-Stores der Fassade (existieren nach ``connect()``)."""

    @property
    def schedules(self) -> ScheduleStore: ...

    @property
    def locks(self) -> LockStore: ...

    @property
    def rate_limit(self) -> RateLimitStore: ...

    @property
    def chords(self) -> ChordStore: ...


# Worker-weite Registry-Form eines gehaltenen Locks (Token-API seit U15).
LockEntry = LockToken


@dataclass
class GateContext:
    """Pro-Zustellung-Kontext, den der Runner nach dem Decode aufbaut."""

    task_id: str
    task_name: str
    queue: str  # Queue aus dem Body (bis U8; danach ``delivery.queue``)
    task_info: TaskInfo | None = None
    task_class: type | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    handler_id: uuid.UUID | None = None
    acquired_locks: list[LockEntry] = field(default_factory=list)


@dataclass
class GateDeps:
    """Kollaborateure, die der Worker an die Gates reicht.

    ``rate_limiter``/``lock_manager`` sind Getter, weil ``Worker.start()`` sie
    erst nach ``backend.connect()`` baut (und Tests sie direkt setzen).
    """

    broker: WorkerBroker
    backend: WorkerBackend
    config: SwarmQConfig
    signals: SignalDispatcher
    registry: TaskRegistry
    rate_limiter: Callable[[], RateLimitStore | None]
    lock_manager: Callable[[], LockStore | None]
    finalize_schedfire: Callable[[Any, bool], Awaitable[None]]
    maybe_release_unique: Callable[[Any, str, TaskInfo | None], Awaitable[None]]
    extract_valid_workflow_id: Callable[[Any], str | None]
    claim_schedfire: Callable[[Any, str], None]
    register_lock: Callable[[uuid.UUID, LockEntry], None]


class Gate(Protocol):
    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome: ...


async def route_via_dlx(broker: Any, delivery: Any) -> None:
    """Terminaler DLX-Fallback (KTD3): ``BrokerAdmin.route_via_dlx`` — bei
    RabbitMQ ``nack(requeue=False)`` über die Broker-Konfiguration, ohne
    Diagnose-Header. Ein Broker ohne diese Fähigkeit veröffentlicht
    stattdessen an sein Dead-Letter-Ziel mit den Original-Headern; der Kern
    referenziert ``BrokerAdmin`` dadurch nicht als Typ."""
    admin = getattr(broker, "route_via_dlx", None)
    if admin is not None:
        await admin(delivery)
        return
    await broker.dead_letter(delivery, headers=dict(delivery.headers or {}))


__all__ = [
    "Gate",
    "GateContext",
    "GateDeps",
    "LockEntry",
    "WorkerBackend",
    "WorkerBroker",
    "route_via_dlx",
]
