"""Valkey-Chords (Plan U18, KTD4): ``chord_complete.lua``.

``ChordManager`` ist die heutige Klasse (verbatim aus
``swarmq.workflow_orchestration``, dort weiterhin importierbar bis U15) und
erfüllt den Vertrag ``ChordStore`` über ``record_member``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import valkey.asyncio

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.exceptions import BackendConnectionError
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

# Nur ohne Namensraum (Review MR 15, P2-3): ``chord_set_key`` und der
# Store-Default nennen die Keys ohne Praefix; das Backend reicht seinen
# KeySpace (``keys=``) durch.
_DEFAULT_KEYS = KeySpace()


def chord_set_key(wf_id: str, chord_id: str) -> str:
    """Key for the per-chord completed-member set (SADD of task ids)."""
    return _DEFAULT_KEYS.chord(wf_id, chord_id)


class ChordManager:
    """Async wrapper around ``chord_complete.lua``.

    One instance per worker, constructed in ``Worker.start()`` after the
    backend connects (the registered ``AsyncScript`` needs a live client).
    Follows the collaborator-owned-script pattern of ``RateLimiter``.
    """

    def __init__(self, client: valkey.asyncio.Valkey, *, keys: KeySpace | None = None) -> None:
        self._client = client
        self._keys = keys if keys is not None else _DEFAULT_KEYS
        self._script = client.register_script(load_script("chord_complete"))

    async def record_and_check_complete(
        self,
        wf_id: str,
        chord_id: str,
        member_id: str,
        expected_n: int,
        ttl_ms: int,
    ) -> bool:
        """Record one header-member completion; return True iff the chord is
        now complete (all ``expected_n`` distinct members done) (R12).

        Idempotent under redelivery: a member already in the completed set
        does not advance the count. This returns True on EVERY call once the
        set has reached N — including post-completion redeliveries — so the
        true one-shot is the downstream publish-then-mark ``step_tasks["/b"]``
        marker in :meth:`WorkflowAdvancer._enqueue_node`, not a flag consumed
        here. That ordering guarantees the body is never lost under a crash
        in the completion→publish window (at worst a rare idempotent
        double-enqueue, absorbed by step idempotency).

        Treiberfehler wandelt ``as_backend_error`` wie in den übrigen Stores
        (F4-11): ein Ausfall (Transport, Zeitablauf, ``OOM``) wird
        ``BackendConnectionError``, eine andere Fehlerantwort (``WRONGTYPE``,
        Skriptfehler) ``BackendResponseError``.
        """
        try:
            result = cast(
                "int",
                await self._script(keys=[self._keys.chord(wf_id, chord_id)], args=[member_id, expected_n, ttl_ms]),
            )
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"chord record failed for {wf_id!r} {chord_id!r}: {exc}", exc) from exc
        return bool(result == 1)

    async def record_member(self, wf_id: str, chord_id: str, member_id: str, *, expected: int, ttl_ms: int) -> bool:
        """``ChordStore``-Vertrag: dieselbe Zusage wie ``record_and_check_complete``."""
        return await self.record_and_check_complete(wf_id, chord_id, member_id, expected, ttl_ms)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ChordStore

    _conforms_chords: type[ChordStore] = ChordManager
