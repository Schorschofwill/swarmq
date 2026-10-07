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
from swarmq.headers import HEADER_BACKEND_DEFER_COUNT, HEADER_SCHEDFIRE_CLAIM
from swarmq.signals import Signal
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


# Hoechstzahl der Tokens in ``x-swarmq-schedfire-claim``. Jeder Claim mit
# unklarem Ausgang fuegt eins hinzu; 16 decken bei Standard-Backoff
# (1 s ... 60 s) gut zehn Minuten Folgefehler ab -- laenger als die
# Standard-TTL des Claims (300 s), danach ist ein aelteres Token ohnehin
# abgelaufen.
MAX_SCHEDFIRE_CLAIM_TOKENS = 16


@dataclass(frozen=True)
class SchedfireClaim:
    """Schedfire-Claim einer Zustellung: Fire-Schluessel und Owner-Token.

    Liegt am ``GateContext`` der Zustellung (nicht in einem Register des
    Workers) -- der Runner finalisiert ihn von dort, eine andere Zustellung
    kann ihn nicht treffen."""

    key: str
    owner: str
    # Tokens, die der Claim nach einem Aufruf mit unklarem Ausgang noch
    # tragen kann (die angebotenen Uebernahme-Tokens); leer, sobald der
    # Claim sicher ``owner`` gehoert.
    candidates: tuple[str, ...] = ()

    def header_value(self) -> str:
        """Wert von ``x-swarmq-schedfire-claim`` einer Kopie: das eigene
        Token zuerst, dann die Kandidaten (kommagetrennt, gedeckelt auf
        ``MAX_SCHEDFIRE_CLAIM_TOKENS``; die aeltesten fallen weg)."""
        return ",".join((self.owner, *self.candidates)[:MAX_SCHEDFIRE_CLAIM_TOKENS])


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
    workflow_id: str | None = None  # gepruefter Workflow-Header (U10: Logkontext im Kind)
    # Schedfire-Claim, den diese Zustellung gewonnen hat; jede Kopie traegt
    # sein Owner-Token als ``x-swarmq-schedfire-claim`` weiter.
    schedfire_claim: SchedfireClaim | None = None


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
    finalize_schedfire: Callable[[GateContext, bool], Awaitable[None]]  # (ctx, terminal)
    maybe_release_unique: Callable[[Any, str, TaskInfo | None], Awaitable[None]]
    extract_valid_workflow_id: Callable[[Any], str | None]
    register_lock: Callable[[uuid.UUID, LockEntry], None]


# Zaehler, die der Broker je Zustellung fuehrt (Quorum-Queues). Eine Kopie
# ueber ``retry_later`` ist eine neue Nachricht mit eigenem Zustellbudget:
# mitkopiert hielte das Delivery-Count-Gate sie fuer haengend, obwohl der
# Broker sie noch nie zugestellt hat (RabbitMQ 4 behaelt einen vom Publisher
# gesetzten ``x-delivery-count`` bei, 3.13 zaehlt zudem Requeues).
BROKER_DELIVERY_COUNTERS = frozenset({"x-delivery-count", "x-acquired-count"})

# Nicht in Kopien uebernommen: die Broker-Zaehler (s. o.) und der Zaehler der
# Zurueckstellungen bei Backend-Ausfall. Wer eine Kopie macht, ohne am Backend
# gescheitert zu sein (Retry, Rate-Limit-/Lock-Requeue), hat das Backend
# erreicht -- die Stoerung ist ueberstanden, eine naechste beginnt wieder mit
# der Basiswartezeit. Die Zurueckstellung selbst liest den bisherigen Stand
# vom Original und zaehlt weiter.
_NOT_COPIED = BROKER_DELIVERY_COUNTERS | {HEADER_BACKEND_DEFER_COUNT}


def republish_headers(delivery: Any, ctx: GateContext) -> dict[str, Any]:
    """Header fuer eine neue Kopie von *delivery* (Retry, Zurueckstellung,
    Requeue): alle Header des Originals ohne die Zustellzaehler des Brokers
    und ohne ``x-swarmq-backend-defer-count`` (die Zurueckstellung setzt ihn
    selbst). Hielt die Zustellung einen Schedfire-Claim, traegt die Kopie
    dessen Owner-Token -- sie uebernimmt den Claim, falls dessen Freigabe
    scheitert."""
    headers = {
        key: value for key, value in (delivery.headers or {}).items()
        if key not in _NOT_COPIED
    }
    if ctx.schedfire_claim is not None:
        headers[HEADER_SCHEDFIRE_CLAIM] = ctx.schedfire_claim.header_value()
    return headers


async def redeliver_in_place(deps: GateDeps, delivery: Any, ctx: GateContext) -> None:
    """``nack(requeue=True)`` des Originals; ein gehaltener Schedfire-Claim
    wird VORHER freigegeben (owner-geprueft, begrenzt).

    Das Original kehrt ohne ``x-swarmq-schedfire-claim`` zurueck: stuende der
    Claim noch, wenn die Redelivery ihn prueft, bestaetigte sie sich als
    Duplikat. Scheitert die Freigabe (Backend weg), wartet der Fire auf die
    Claim-TTL. Kein Ersatz durch eine Kopie (``retry_later``): die Requeue-
    Pfade sind Fallbacks fuer einen gestoerten Broker oder zaehlen bewusst
    aufs Zustelllimit (Logikfehler -> DLQ)."""
    await deps.finalize_schedfire(ctx, False)
    await deps.broker.redeliver(delivery)


class Gate(Protocol):
    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome: ...


async def route_via_dlx(
    deps: GateDeps,
    delivery: Any,
    *,
    reason: str,
    task_id: str | None = None,
    task_name: str | None = None,
    error: str | None = None,
) -> None:
    """Terminaler DLX-Fallback (KTD3): ``BrokerAdmin.route_via_dlx`` — bei
    RabbitMQ ``nack(requeue=False)`` über die Broker-Konfiguration, ohne
    Diagnose-Header. Ein Broker ohne diese Fähigkeit veröffentlicht
    stattdessen an sein Dead-Letter-Ziel mit den Original-Headern; der Kern
    referenziert ``BrokerAdmin`` dadurch nicht als Typ.

    Danach feuert ``TASK_REJECTED`` mit *reason* (U6, R12): dieser Weg war
    stumm, eine Nachricht landete im ``.XQ``, ohne dass ein Signal feuerte
    oder ein Zähler stieg. Das Signal folgt dem ERFOLGREICHEN Verb — sonst
    meldete es eine Ablehnung, die nie stattfand. Es ist selbst kein Verb,
    die Ack-Invariante der Pipeline bleibt also unberührt.
    """
    admin = getattr(deps.broker, "route_via_dlx", None)
    if admin is not None:
        await admin(delivery)
    else:
        await deps.broker.dead_letter(delivery, headers=dict(delivery.headers or {}))

    await deps.signals.dispatch(
        Signal.TASK_REJECTED,
        queue=getattr(delivery, "queue", None),
        reason=reason,
        task_id=task_id,
        task_name=task_name,
        error=error,
    )


__all__ = [
    "Gate",
    "GateContext",
    "GateDeps",
    "LockEntry",
    "SchedfireClaim",
    "WorkerBackend",
    "WorkerBroker",
    "redeliver_in_place",
    "route_via_dlx",
]
