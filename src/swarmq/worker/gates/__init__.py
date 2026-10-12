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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from swarmq.backoff import backend_defer_delay
from swarmq.contracts import Broker, BrokerAdmin, ResultBackend
from swarmq.contracts.coordination import (
    ChordStore,
    LockStore,
    LockToken,
    RateLimitStore,
    ScheduleStore,
)
from swarmq.exceptions import SettleFailedAfterPublish
from swarmq.headers import (
    HEADER_BACKEND_DEFER_COUNT,
    HEADER_GUARD_DEFER_COUNT,
    HEADER_SCHEDFIRE_CLAIM,
    LOCK_WAIT_STATE_HEADERS,
)
from swarmq.identity import copy_headers
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal
from swarmq.worker.outcome import DeliveryOutcome
from swarmq.worker.redelivery import redeliver_pause_s

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher
    from swarmq.task import TaskInfo


logger = get_logger(__name__)


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
    # TTL des Completed-Markers in ms (Plan 0.12.0, MR 12, BZ-12): folgt der
    # Wartezeit des Fires in der Queue; None = ``scheduler_dedup_ttl``.
    completed_ttl_ms: int | None = None

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
    # (ctx, terminal) -> ``True``, wenn kein Claim gehalten wurde oder die
    # Finalisierung gelang; ``False``, wenn der Claim stehen blieb.
    finalize_schedfire: Callable[[GateContext, bool], Awaitable[bool]]
    maybe_release_unique: Callable[[Any, str, TaskInfo | None], Awaitable[None]]
    extract_valid_workflow_id: Callable[[Any], str | None]
    register_lock: Callable[[uuid.UUID, LockEntry], None]
    # BZ-29: Pause vor jedem ``redeliver`` (Sekunden -> Warten). Der Worker
    # setzt sie (bricht beim Shutdown ab); ``None`` = keine Pause.
    redeliver_pause: Callable[[float], Awaitable[None]] | None = None
    # BZ-13: Workflow eines terminal endenden Schritts fehlmarkieren
    # (delivery, ctx, error_message, may_redeliver) -> ``False``, wenn die
    # Zustellung deshalb neu zugestellt wurde. Der Worker setzt es
    # (``RetryHandler.fail_workflow``); ``None`` = keine Markierung.
    fail_workflow: Callable[[Any, GateContext, str, bool], Awaitable[bool]] | None = None
    # BZ-25 (MR 13): erworbenen Lock dem Keeper uebergeben (token, ttl_ms, ctx);
    # der Worker setzt es (``LockKeeper.keep``); ``None`` = keine Verlaengerung.
    keep_lock: Callable[[LockToken, int, GateContext], None] | None = None


# Zaehler, die der Broker je Zustellung fuehrt (Quorum-Queues). Eine Kopie
# ueber ``retry_later`` ist eine neue Nachricht mit eigenem Zustellbudget:
# mitkopiert hielte das Delivery-Count-Gate sie fuer haengend, obwohl der
# Broker sie noch nie zugestellt hat (RabbitMQ 4 behaelt einen vom Publisher
# gesetzten ``x-delivery-count`` bei, 3.13 zaehlt zudem Requeues).
BROKER_DELIVERY_COUNTERS = frozenset({"x-delivery-count", "x-acquired-count"})

# Nicht in Kopien uebernommen: die Broker-Zaehler (s. o.) und die Zaehler der
# Zurueckstellungen bei Backend-Ausfall und durch den Ausfuehrungsschutz. Wer
# eine Kopie macht, ohne am Backend gescheitert zu sein (Retry, Rate-Limit-/
# Lock-Requeue), hat das Backend erreicht -- die Stoerung ist ueberstanden,
# eine naechste beginnt wieder mit der Basiswartezeit. Die Zurueckstellung
# selbst liest den bisherigen Stand vom Original und zaehlt weiter.
_NOT_COPIED = BROKER_DELIVERY_COUNTERS | {HEADER_BACKEND_DEFER_COUNT, HEADER_GUARD_DEFER_COUNT}


def republish_headers(delivery: Any, ctx: GateContext) -> dict[str, Any]:
    """Header fuer eine neue Kopie von *delivery* (Retry, Zurueckstellung,
    Requeue): alle Header des Originals ohne die Zustellzaehler des Brokers
    und ohne ``x-swarmq-backend-defer-count`` (die Zurueckstellung setzt ihn
    selbst); ``x-death`` geht als ``x-swarmq-xdeath-base`` mit (MR 12). Hielt
    die Zustellung einen Schedfire-Claim, traegt die Kopie dessen Owner-Token
    -- sie uebernimmt den Claim, falls dessen Freigabe scheitert. Hat der
    Broker die Zustellung neu zugestellt, entfaellt die Lock-Wartezeit."""
    # MR 12 (BZ-08): ``x-death`` wird zur Base -- der Broker baut es beim
    # naechsten Dead-Lettern neu auf (Spike S5), die Identitaet bleibt so stabil.
    headers = {key: value for key, value in copy_headers(delivery.headers).items() if key not in _NOT_COPIED}
    if broker_redelivered(delivery.headers or {}):
        # Review MR 13, P2-4: eine Neuzustellung durch den Broker (Absturz
        # nach dem Acquire, Shutdown-Requeue) gehoert nicht mehr zur
        # Lock-Wartezeit ihrer Kopie; ohne Broker-Zaehler saehe die Kopie
        # sonst wieder aktuell aus.
        for header in LOCK_WAIT_STATE_HEADERS:
            headers.pop(header, None)
    if ctx.schedfire_claim is not None:
        headers[HEADER_SCHEDFIRE_CLAIM] = ctx.schedfire_claim.header_value()
    return headers


def _as_count(raw: Any) -> int:
    """Nicht-negativer Zaehler; ein kaputter Header zaehlt wie keiner."""
    try:
        count = int(raw)
    except TypeError, ValueError:
        return 0
    return max(count, 0)


def prior_deferrals(headers: Mapping[str, Any], counter_header: str = HEADER_BACKEND_DEFER_COUNT) -> int:
    """Bisherige Zurueckstellungen in Folge.

    Eine Zurueckstellungs-Kopie traegt keine Broker-Zaehler (neue Nachricht).
    Traegt die Zustellung welche, hat der Broker sie nach einer Zustellung
    zurueckgegeben, die nicht zurueckgestellt hat (Requeue nach bestandenen
    Gates, Absturz, Fallback): die Folge ist unterbrochen, die Zaehlung
    beginnt neu. Klassische Queues fuehren keine solchen Zaehler.
    """
    if broker_redelivered(headers):
        return 0
    return _as_count(headers.get(counter_header))


def broker_redelivered(headers: Mapping[str, Any]) -> bool:
    """Hat der Broker diese Zustellung nach einer frueheren zurueckgegeben
    (Requeue, Absturz, Shutdown)? Kopien tragen keine Broker-Zaehler; klassische
    Queues fuehren keine -- dort ist eine Neuzustellung nicht erkennbar."""
    return any(_as_count(headers.get(counter)) > 0 for counter in BROKER_DELIVERY_COUNTERS)


def deferral_copy(deps: GateDeps, delivery: Any, ctx: GateContext) -> tuple[dict[str, Any], int, int]:
    """Header, Zaehlerstand und Wartezeit (ms) der Kopie einer Zurueckstellung
    bei Backend-Ausfall: ``republish_headers`` plus hochgezaehltem
    ``x-swarmq-backend-defer-count``, Wartezeit nach ``backend_defer_delay``.
    Vor der Freigabe des Claims aufrufen -- sonst fehlt der Kopie dessen
    Token."""
    headers = republish_headers(delivery, ctx)
    attempt = prior_deferrals(delivery.headers or {}) + 1
    headers[HEADER_BACKEND_DEFER_COUNT] = attempt
    delay_s = backend_defer_delay(
        attempt,
        deps.config.backend_defer_base_delay,
        deps.config.backend_defer_max_delay,
    )
    return headers, attempt, int(delay_s * 1000)


async def redeliver_in_place(deps: GateDeps, delivery: Any, ctx: GateContext) -> None:
    """``nack(requeue=True)`` des Originals; ein gehaltener Schedfire-Claim
    wird VORHER freigegeben (owner-geprueft, begrenzt).

    Das Original kehrt ohne ``x-swarmq-schedfire-claim`` zurueck: stuende der
    Claim noch, wenn die Redelivery ihn prueft, bestaetigte sie sich als
    Duplikat -- der Fire (bzw. der Workflow-Advance) waere verloren, nicht
    nur bis zur Claim-TTL verzoegert. Scheitert die Freigabe deshalb (Backend
    weg, fremde Ausnahme eines Stores), geht statt des ``nack`` eine Kopie
    mit dem Owner-Token ueber die Delay-Queue (``retry_later``, Backoff und
    Zaehler wie eine Zurueckstellung bei Backend-Ausfall); sie uebernimmt den
    Claim. Erst wenn auch dieser Publish scheitert, bleibt der ``nack``.

    Zustelllimit: die Kopie erbt keinen Broker-Zaehler. Sie entsteht nur bei
    gescheiterter Freigabe, also bei einem Ausfall; erreicht die Kopie das
    Backend wieder, gelingt die Freigabe beim naechsten Requeue, und der
    ``nack`` zaehlt wieder aufs Zustelllimit (Logikfehler -> DLQ)."""
    claim = ctx.schedfire_claim
    if claim is None:
        await _pause_then_redeliver(deps, delivery)
        return
    # Vor der Freigabe gebaut: sie nimmt den Claim vom Kontext.
    headers, attempt, delay_ms = deferral_copy(deps, delivery, ctx)
    if await deps.finalize_schedfire(ctx, False):  # noqa: FBT003 - Callable-Feld ohne Parameternamen
        await _pause_then_redeliver(deps, delivery)
        return
    fields = {"task_id": ctx.task_id, "task_name": ctx.task_name, "schedfire_key": claim.key}
    try:
        # Publish-then-ack: ``retry_later`` bestaetigt das Original erst nach
        # dem Publisher-Confirm der Kopie.
        await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
    except Exception as exc:  # noqa: BLE001 - Broker gestoert, letzter Ausweg nack
        # Broker ebenfalls gestoert: letzter Ausweg ``nack``.
        logger.warning("task_copy_failed_redelivered", **fields, **_error_fields(exc))
        await _pause_then_redeliver(deps, delivery)
        return
    logger.warning("task_requeued_as_copy", **fields, defer_count=attempt, delay_ms=delay_ms)


async def _pause_then_redeliver(deps: GateDeps, delivery: Any) -> None:
    """``redeliver`` nach der Pause aus ``redeliver_pause_s`` (BZ-29). Eine
    scheiternde Pause verhindert das ``nack`` nicht (die Zustellung bliebe
    sonst unbestaetigt bis zum Kanalende); ein Abbruch geht durch."""
    if deps.redeliver_pause is not None:
        try:
            await deps.redeliver_pause(redeliver_pause_s(delivery.headers or {}))
        except Exception as exc:  # noqa: BLE001 - scheiternde Pause darf nack nicht verhindern
            logger.warning("redeliver_pause_failed", **_error_fields(exc))
    await deps.broker.redeliver(delivery)


class Gate(Protocol):
    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome: ...


async def dead_letter_copy(deps: GateDeps, delivery: Any, headers: Mapping[str, Any]) -> bool:
    """Kopie in die XQ, Original bestaetigen. ``True``: die Kopie liegt dort,
    nur das Ack scheiterte (``SettleFailedAfterPublish``) -- das ist
    terminal wie ein Erfolg; die Neuzustellung des Originals ueberspringt der
    Ausfuehrungsschutz als ``done``. Jeder andere Fehler geht durch."""
    try:
        await deps.broker.dead_letter(delivery, headers=headers)
    except SettleFailedAfterPublish as exc:
        logger.warning("dlq_ack_failed_after_publish", **_error_fields(exc))
        return True
    return False


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
    await fail_workflow_best_effort(deps, delivery, task_id, task_name, error or reason)


async def fail_workflow_best_effort(
    deps: GateDeps, delivery: Any, task_id: str | None, task_name: str | None, error_message: str
) -> None:
    """BZ-13: ein per DLX beendeter Workflow-Schritt markiert seinen Workflow
    als fehlgeschlagen -- best effort, ohne Neuzustellung (die Nachricht
    liegt schon im ``.XQ``). Ein Fehler hier bricht den Pfad nicht ab."""
    if deps.fail_workflow is None:
        return
    ctx = GateContext(task_id=task_id or "", task_name=task_name or "", queue=getattr(delivery, "queue", None) or "")
    try:
        await deps.fail_workflow(delivery, ctx, error_message, False)  # noqa: FBT003 - Callable-Feld ohne Parameternamen
    except Exception as exc:  # noqa: BLE001 - best effort, bricht den Pfad nicht ab
        logger.warning("workflow_fail_mark_failed", task_id=task_id, **_error_fields(exc), redelivered=False)


__all__ = [
    "Gate",
    "GateContext",
    "GateDeps",
    "LockEntry",
    "SchedfireClaim",
    "WorkerBackend",
    "WorkerBroker",
    "fail_workflow_best_effort",
    "redeliver_in_place",
    "route_via_dlx",
]
