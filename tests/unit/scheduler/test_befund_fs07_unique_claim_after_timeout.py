"""Nachweis FS-07 (Scheduler): Ein Timeout nach erfolgreichem Unique-Claim des
Leaders gibt den Claim nicht frei.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-07; Quelle K-4).

``_fire_one`` (``scheduler/fire.py``) ruft fuer einen Cron mit ``unique_for``
``claim_unique`` mit ``op_timeout`` auf. Laeuft der Aufruf in den Timeout,
nachdem Valkey das Flag schon gesetzt hat, kehrt der Leader ohne
``_release_own_claim`` zurueck. Das Flag haelt eine nie publizierte
``task_id``; der naechste Poll desselben Termins sieht ``dedup_detected``,
committet und ueberspringt das Fire -- bis ``unique_for`` ablaeuft. Erwartet:
nach dem Timeout wird der eigene Claim (owner-geprueft) freigegeben, der
naechste Poll publiziert.

Der Lock-Teil steht in
``tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py``.

Testliste:
# [x] claim_unique setzt das Flag, antwortet aber mit Timeout -> der naechste
#     Poll desselben Termins publiziert
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import ScheduleDefinition
from swarmq.signals import SignalDispatcher
from tests.unit.scheduler._helpers import _make_loop  # pyright: ignore[reportUnknownVariableType] - ungetypter Bestandshelfer

pytestmark = [pytest.mark.timeout(5)]

_SCORE_MS = 1_700_000_000_000


class _FlagStore:
    """``SET NX GET`` und owner-gepruefte Freigabe; der erste Claim landet,
    seine Antwort geht aber im Timeout verloren."""

    def __init__(self) -> None:
        self.flags: dict[str, str] = {}
        self._first = True

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, op_timeout: float | None = None
    ) -> str | None:
        existing = self.flags.get(dedup_hash)
        if existing is None:
            self.flags[dedup_hash] = owner
        if self._first:
            self._first = False
            raise BackendConnectionError(f"setnx_unique timed out after {op_timeout}s")
        return existing

    async def release_unique(self, dedup_hash: str, owner: str, op_timeout: float | None = None) -> bool:
        if self.flags.get(dedup_hash) != owner:
            return False
        del self.flags[dedup_hash]
        return True


@pytest.mark.befund("FS-07")
async def test_unique_claim_written_before_timeout_does_not_block_the_next_poll(
    backend: MagicMock,
    broker: MagicMock,
    config: SwarmQConfig,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    flags = _FlagStore()
    backend.claim_unique = AsyncMock(side_effect=flags.claim_unique)
    backend.release_unique = AsyncMock(side_effect=flags.release_unique)
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    definition = ScheduleDefinition(
        schedule_name="fs07_nightly",
        task_name="fs07_nightly_task",
        args=(),
        kwargs={},
        queue="default",
        cron="*/5 * * * *",
        fire_ts_ms=_SCORE_MS,
        source="static",
        unique_for=3600,
    )

    await loop._fire_one("fs07_nightly", _SCORE_MS, definition)  # Timeout, Flag liegt
    await loop._fire_one("fs07_nightly", _SCORE_MS, definition)  # derselbe Termin erneut

    assert broker.publish.await_count == 1, (
        f"Fire nach Claim-Timeout als Duplikat verworfen; Flag haelt nie publizierte task_id: {flags.flags}"
    )
