"""Broker-Verträge (Entwurf §2.2, KTD2/KTD3).

Kern: ``Publisher`` (Client-Seite), ``Consumer`` (Worker-Seite, vier Verben
auf ``Delivery``), ``Broker`` als Vereinigung. Erweiterungen: ``BrokerAdmin``
(Dead-Letter-Operationen, Tiefen, ``route_via_dlx``) und ``BatchPublisher``
(R4.2-Andockstelle). Der Kern referenziert die Erweiterungen nie.

``@runtime_checkable`` prüft nur Member-Präsenz, nicht Signaturen — der
Vertragsbeweis ist mypy strict über die Konformitäts-Anker (KTD1).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from swarmq.contracts.message import Delivery, PublishReceipt

DeliveryCallback = Callable[[Delivery], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class QueueStatus:
    """Zustand einer Queue aus dem passiven Declare (Plan 0.12.0, MR 18,
    BZ-14): bereitliegende Nachrichten und angemeldete Consumer. Optionale
    Broker-Faehigkeit ``queue_statuses`` (Q15), nicht Teil von ``BrokerAdmin``."""

    message_count: int
    consumer_count: int


# Nicht-leere ``__slots__`` lehnt CPython für tuple-Unterklassen ab; ``hit_limit``
# braucht deshalb das Instanz-``__dict__``.
class DeadLetterScan[T](tuple[T, int]):  # noqa: SLOT001 - hit_limit ist Instanzattribut, tuple erlaubt keine Slots
    """Ergebnis der DLQ-Lesebefehle von ``BrokerAdmin`` (F4-05, E-028).

    Entpackt wie ``(erstes, scanned)`` und erfüllt damit die Signaturen
    ``tuple[..., int]``. ``hit_limit`` sagt, ob das Lesen am Limit endete;
    ``scanned`` zählt abgelaufene Einträge nicht, die aber Lesungen
    verbrauchen. Implementierungen ohne das Flag liefern ein schlichtes
    Tupel; Aufrufer fallen dann auf ``scanned >= limit`` zurück."""

    hit_limit: bool

    def __new__(cls, first: T, scanned: int, *, hit_limit: bool) -> DeadLetterScan[T]:
        scan = super().__new__(cls, (first, scanned))
        scan.hit_limit = hit_limit
        return scan

    def __getnewargs_ex__(self) -> tuple[tuple[T, int], dict[str, bool]]:
        # copy/deepcopy/pickle rufen ``__new__`` mit diesen Argumenten auf;
        # das geerbte ``__getnewargs__`` des Tupels passt nicht zur Signatur.
        return (self[0], self[1]), {"hit_limit": self.hit_limit}


@runtime_checkable
class Publisher(Protocol):
    """Veröffentlichen — alles, was der Client braucht."""

    async def connect(self) -> None:
        """Idempotent; Treiberfehler → ``BrokerConnectionError``."""
        ...

    async def close(self) -> None:
        """Schließt nur Eigenes; eine geliehene Verbindung bleibt offen."""
        ...

    async def ping(self) -> None:
        """Wirft ``BrokerConnectionError``, wenn nicht verbunden oder nicht bereit."""
        ...

    async def declare_queue(self, queue: str, /) -> None:
        """Macht ``queue`` samt Verzögerungs- und Dead-Letter-Ziel zustellbereit; idempotent."""
        ...

    async def publish(
        self,
        queue: str,
        body: bytes,
        headers: Mapping[str, Any] | None = None,
    ) -> PublishReceipt:
        """Persistente, bestätigte Veröffentlichung; Fehlschlag → ``BrokerConnectionError``.

        Ein Abbruch der eigenen Task geht als ``asyncio.CancelledError``
        weiter, nie verschluckt. Optional markiert der Publisher ihn als
        ``PublishCancelledUnsent``, wenn nachweislich nichts gesendet wurde;
        ohne diese Markierung gilt der Abbruch als unbestimmt (E-041), und
        der Client hält den Unique-Claim.
        """
        ...

    def set_publish_deadline(self, at: float | None) -> None:
        """Deckelt jeden Publish zusätzlich auf einen absoluten Zeitpunkt.

        ``at`` liegt auf der ``loop.time()``-Skala; ``None`` hebt die
        Begrenzung auf. Wirksam wird stets die kürzere aus dieser Frist und
        der konfigurierten Publish-Deadline, und bereits laufende Publishes
        werden nachgezogen (Plan 2026-08-28-001, R12).

        Der Worker setzt sie zu Beginn des Herunterfahrens, damit ein
        hängender Publish innerhalb der Gnadenfrist mit Fehler zurückkehrt
        statt vom Supervisor abgeschossen zu werden. Ein Producer ohne
        Worker ruft sie nie und läuft immer gegen die Konfiguration.
        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """Zustellen und die vier Verben (KTD2)."""

    async def consume(self, queue: str, on_delivery: DeliveryCallback, *, concurrency: int) -> None:
        """Liefert jede Nachricht genau einmal als ``Delivery``; höchstens
        ``concurrency`` unbestätigte Zustellungen gleichzeitig."""
        ...

    async def cancel_consume(self) -> None:
        """Keine weiteren Zustellungen; bereits zugestellte bleiben bestätigbar."""
        ...

    async def ack(self, delivery: Delivery) -> None:
        """Erledigt. Danach ist kein weiteres Verb auf dieser Zustellung zulässig.

        Kanal- oder Verbindungsverlust → ``BrokerConnectionError``; die
        Zustellung bleibt unbestätigt und kommt erneut. Gilt ebenso für
        ``redeliver`` und ``route_via_dlx``."""
        ...

    async def redeliver(self, delivery: Delivery) -> None:
        """Transient: dieselbe Nachricht wird ohne Verzögerung erneut zugestellt.
        Kanal- oder Verbindungsverlust → ``BrokerConnectionError``."""
        ...

    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        """Veröffentlicht Body + ``headers`` an das Verzögerungsziel von
        ``delivery.queue`` (Rückkehr in genau diese Queue nach ``delay_ms``),
        dann Bestätigung des Originals. Publish-Fehler → ``BrokerConnectionError``,
        Original bleibt unbestätigt."""
        ...

    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        """Veröffentlicht Body + ``headers`` an das Dead-Letter-Ziel von
        ``delivery.queue``, dann Bestätigung des Originals. Publish-Fehler →
        ``BrokerConnectionError``, Original bleibt unbestätigt."""
        ...


@runtime_checkable
class Broker(Publisher, Consumer, Protocol):
    """Vereinigung von ``Publisher`` und ``Consumer`` — der Worker-Vertrag."""


@runtime_checkable
class BrokerAdmin(Protocol):
    """Optionale Erweiterung (KTD3): Dead-Letter-Operationen, Tiefen und der
    DLX-Fallback. Konsumenten: ``swarmq dlq``, Metrics-Sampler, Health.

    Die vier Lesebefehle dürfen statt des Tupels ein ``DeadLetterScan``
    liefern; dessen ``hit_limit`` meldet, ob das Lesen am Limit endete
    (F4-05, E-028)."""

    async def list_dead_letters(self, queue: str, limit: int = 100) -> tuple[list[dict[str, Any]], int]: ...

    async def inspect_dead_letter(
        self, queue: str, task_id: str, scan_limit: int = 1000
    ) -> tuple[dict[str, Any] | None, int]: ...

    async def retry_dead_letter(self, queue: str, task_id: str, scan_limit: int = 1000) -> tuple[bool, int]: ...

    async def retry_all_dead_letters(self, queue: str, limit: int = 10_000) -> tuple[int, int]: ...

    async def purge_dead_letters(self, queue: str) -> int: ...

    async def queue_depth(self, queue: str) -> int: ...

    async def dead_letter_depth(self, queue: str) -> int: ...

    async def route_via_dlx(self, delivery: Delivery) -> None:
        """Leitet die Zustellung ohne Diagnose-Header über die Broker-
        Konfiguration an das Dead-Letter-Ziel (heute ``nack(requeue=False)``).
        Terminaler Pfad für nicht dekodierbare oder unbekannte Nachrichten und
        Fallback der Lock-Pfade, wenn ``dead_letter`` scheitert. Kanal- oder
        Verbindungsverlust → ``BrokerConnectionError``."""
        ...


@runtime_checkable
class BatchPublisher(Protocol):
    """R4.2-Andockstelle (Entwurf §3): mehrere Veröffentlichungen mit einer
    gemeinsamen Bestätigung; ein ``PublishReceipt`` pro Element. Kein Broker
    muss sie in 0.2.0 implementieren."""

    async def publish_many(
        self,
        queue: str,
        items: Sequence[tuple[bytes, Mapping[str, Any] | None]],
    ) -> list[PublishReceipt]: ...
