"""Epoch des Datenbestands (Plan 0.12.0, MR 14, BZ-04).

Ein einzelner Key ohne TTL (``sq:{sched}:epoch``) mit einem Zufallswert. Jeder
Loop merkt sich den Wert, den er gesehen hat. Fehlt der Key spaeter oder traegt
er einen anderen Wert, hat Valkey seine Daten verloren (Neustart ohne
Persistenz, ``FLUSHALL``, Failover mit verlorenen Writes): Abschaltungen per
``swarmq schedule remove``, CLI-Crons und Termine sind weg, statische Crons
kommen aktiv zurueck. Das meldet ``valkey_data_loss_detected``.

Grenzen: Ein Flush bei gleichzeitigem Neustart aller Prozesse bleibt
unerkannt (niemand kennt den alten Wert), ebenso ein Verlust seit einem
RDB-Snapshot, der den Epoch mit zurueckbringt.
"""

from __future__ import annotations

import uuid
from typing import Any

from swarmq.logging import get_logger

logger = get_logger(__name__)


class SchedulerEpoch:
    # Mixin ueber dem Loop-Zustand (``_backend`` setzt ``SchedulerLoop.__init__``).
    _backend: Any
    _known_epoch: str | None = None

    async def _observe_epoch(self) -> None:
        """Epoch lesen bzw. anlegen und mit dem bekannten vergleichen. Wirft
        nie; ein Store ohne ``ensure_epoch`` laesst die Pruefung aus (Q15)."""
        candidate = uuid.uuid4().hex
        try:
            previous = await self._backend.schedules.ensure_epoch(candidate)
        except Exception as exc:  # noqa: BLE001 - Store ohne ensure_epoch lässt Prüfung aus
            logger.debug("valkey_epoch_unavailable", error_type=type(exc).__name__, error=str(exc))
            return
        if previous is not None and not isinstance(previous, str):
            return
        current = candidate if previous is None else previous
        known = self._known_epoch
        self._known_epoch = current
        if known is None or known == current:
            return
        logger.warning(
            "valkey_data_loss_detected",
            previous_epoch=known,
            epoch=current,
            hint=(
                "Valkey lost its data (restart without persistence, FLUSHALL, failover with lost writes): "
                "crons disabled with 'swarmq schedule remove' are active again, CLI crons and delayed tasks "
                "are gone; check the crons with 'swarmq schedule list' and run Valkey with appendonly yes"
            ),
        )
