"""Worker-side workflow orchestration helpers (U3, U5, U6, U7).

This module holds the worker-side machinery that advances workflows —
distinct from the infrastructure-free topology layer in ``workflow.py``
(client/producer side). The :class:`ChordManager` wraps the atomic
``chord_complete.lua`` script; the advancement and depth-guard helpers
added in later units live here too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.lua import load_script

if TYPE_CHECKING:
    import valkey.asyncio


def chord_set_key(wf_id: str, chord_id: str) -> str:
    """Key for the per-chord completed-member set (SADD of task ids)."""
    return f"sq:wf:chord:{wf_id}:{chord_id}"


def chord_flag_key(wf_id: str, chord_id: str) -> str:
    """Key for the one-shot 'body enqueued' flag."""
    return f"sq:wf:chordflag:{wf_id}:{chord_id}"


class ChordManager:
    """Async wrapper around ``chord_complete.lua``.

    One instance per worker, constructed in ``Worker.start()`` after the
    backend connects (the registered ``AsyncScript`` needs a live client).
    Follows the collaborator-owned-script pattern of ``RateLimiter``.
    """

    def __init__(self, client: valkey.asyncio.Valkey) -> None:
        self._client = client
        self._script = client.register_script(load_script("chord_complete"))

    async def record_and_check_complete(
        self,
        wf_id: str,
        chord_id: str,
        member_id: str,
        expected_n: int,
        ttl_ms: int,
    ) -> bool:
        """Record one header-member completion; return True iff this call
        is the one that should enqueue the chord body (R12).

        Idempotent under redelivery: a member already in the completed set
        does not advance the count, and the body-enqueue decision is guarded
        by a one-shot flag set inside the same script. Exactly one call over
        the chord's lifetime returns True.
        """
        result = await self._script(
            keys=[chord_set_key(wf_id, chord_id), chord_flag_key(wf_id, chord_id)],
            args=[member_id, expected_n, ttl_ms],
        )
        return bool(result == 1)
