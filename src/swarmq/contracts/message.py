"""Transportneutrale Nachrichtentypen der Broker-Verträge (Entwurf §2.1).

``Delivery`` ist, was ein ``Consumer`` an den Worker gibt; ``PublishReceipt``
ist der Beweis eines erfolgten ``Publisher.publish`` (publish-then-mark, §4).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Delivery:
    """Eine zugestellte Nachricht.

    ``queue`` ist die Queue, aus der zugestellt wurde — inklusive
    Prioritäts-Suffix — und nicht die im Body genannte Queue. ``handle`` ist
    das broker-private Zustellungs-Handle; der Kern reicht es nur an die
    Verben des ``Consumer`` zurück und interpretiert es nie. Zwei Zustellungen
    derselben Nachricht (z. B. nach ``redeliver``) sind gleich, auch wenn
    ihre Handles verschieden sind.
    """

    body: bytes
    headers: Mapping[str, Any]
    queue: str
    handle: object = field(default=None, compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    """Beleg einer bestätigten Veröffentlichung. Nur ``Publisher.publish``
    erzeugt ihn; ``mark_enqueued`` verlangt ihn (AE5)."""

    queue: str
    message_id: str | None = None
