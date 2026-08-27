"""Client-Ergebnisseite (Plan U10) — verbatim: get_result(s), Fortschritt,
run, cancel. Das Warten läuft seit U17 über die Vertragsmethoden
``wait_for_result``/``watch_progress`` des Backends."""

from __future__ import annotations

import asyncio
import json as _json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    pass

from swarmq.exceptions import (
    ResultNotReadyError,
)
from swarmq.progress import ProgressInfo

logger = structlog.get_logger()


# Default TTL for the cancel flag. 24h is wide enough to outlive the
# typical retry-horizon + redelivery window while still self-healing
# orphaned flags within a day (REV-P3 cleanup). Callers who want a
# tighter window (e.g. deterministic task_id systems vulnerable to
# stale-flag-zombie, see Client.cancel docstring) can override.
_DEFAULT_CANCEL_FLAG_TTL_MS = 86_400_000  # 24h


class ClientResults:
    async def get_result(
        self,
        task_id: str,
        timeout: float = 10,
        polling: float = 0.2,
    ) -> Any:
        """Retrieve a task result, waiting up to *timeout* seconds.

        ``timeout == 0`` is a single GET. Otherwise the wait (Pub/Sub with
        polling fallback, every GET bound to the remaining deadline —
        Härtung R2 P1-2) lives in ``ResultBackend.wait_for_result`` since
        U17; ``polling`` is kept for API compatibility.
        """
        if timeout == 0:
            raw = await self._backend.get_result(task_id)
        else:
            raw = await self._backend.wait_for_result(task_id, timeout=timeout)
        if raw is None:
            raise ResultNotReadyError(task_id)
        return self._unpack(raw)

    async def get_progress(self, task_id: str) -> ProgressInfo | None:
        """Read the current progress HASH for *task_id*.

        Returns ``None`` when no progress has been recorded — either
        the task never called :meth:`Task.update_progress`, the TTL
        has expired, or the task_id is unknown. The caller should
        treat None as "nothing observed yet", not "task complete".

        One-shot read; use :meth:`watch_progress` for a live stream.

        Returns None when the HASH exists but is malformed (missing
        required fields or non-parseable numerics). A malformed HASH is
        treated as "nothing observable" rather than surfacing a decode
        error — callers should never crash on a corrupt payload written
        by a rogue publisher or a future schema version.
        """
        raw = await self._backend.get_progress(task_id)
        if raw is None:
            return None
        try:
            return ProgressInfo.from_hash(raw)
        except ValueError:
            return None

    async def watch_progress(
        self, task_id: str,
    ) -> AsyncIterator[ProgressInfo]:
        """Stream progress updates for *task_id*.

        The Pub/Sub subscription, the initial HASH snapshot, the
        resubscribe-after-disconnect discipline (2026-04-17 toxiproxy
        spike) and the setup-failure fallback live in
        ``ResultBackend.watch_progress`` since U17; it yields JSON payloads
        of the progress fields. This layer decodes them into
        :class:`ProgressInfo` and drops malformed payloads silently — the
        iterator never crashes on a bad publish. It never completes on its
        own; the caller ``break``s when satisfied.
        """
        async for payload in self._backend.watch_progress(task_id):
            try:
                fields = _json.loads(payload)
                info = ProgressInfo.from_hash(fields)
            except ValueError:
                # Malformed publish (rogue producer, future schema) — drop
                # and continue; never crash the iterator on a bad payload.
                continue
            if info is not None:
                yield info

    async def get_results(
        self,
        task_ids: list[str],
        timeout: float = 10,
        poll_interval: float = 0.005,
    ) -> list[Any]:
        """Retrieve results for multiple tasks using MGET-based batch polling.

        Polls every *poll_interval* seconds until all results are available
        or *timeout* is exceeded.
        """
        if not task_ids:
            return []

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        raw_list: list[bytes | None] = [None] * len(task_ids)

        while loop.time() < deadline:
            raw_list = await self._backend.get_results_batch(task_ids)
            if all(r is not None for r in raw_list):
                return [self._unpack(r) for r in raw_list]
            await asyncio.sleep(poll_interval)

        missing = [
            tid for tid, raw in zip(task_ids, raw_list)  # noqa: B905  # gleich lang per Konstruktion
            if raw is None
        ]
        raise ResultNotReadyError(
            f"Timeout waiting for task_ids: {missing}"
        )

    async def run(
        self,
        task_name: str,
        *args: Any,
        queue: str | None = None,
        timeout: float = 10,
        polling: float = 0.2,
        **kwargs: Any,
    ) -> Any:
        """Schedule a task and wait for its result."""
        task_id = await self.schedule(task_name, *args, queue=queue, **kwargs)
        return await self.get_result(task_id, timeout=timeout, polling=polling)

    async def cancel(
        self, task_id: str, ttl_ms: int = _DEFAULT_CANCEL_FLAG_TTL_MS,
    ) -> bool:
        """Request cancellation of *task_id*.

        Fire-and-forget: sets the Valkey cancel flag (with TTL) and
        returns immediately.  Does **not** wait for a worker to react.

        Args:
            task_id: the task to cancel.  Must be a non-empty string;
                format is not validated otherwise — cancellation for
                unknown IDs is harmless and self-heals via TTL.
            ttl_ms: how long the flag lives in Valkey, in milliseconds.
                Defaults to 24 hours.  The SwarmQ app wrapper passes
                ``config.result_ttl * 1000`` so the flag outlives a
                typical retry window.

        Returns:
            True when the flag was newly set, False when it already
            existed (i.e. a prior cancel() call is still in flight).

        .. warning::

           **Stale-flag zombie class** (REV-P2-5, accepted limitation).
           The cancel flag lives for ``ttl_ms`` (24h by default) in
           Valkey. If the same ``task_id`` is reused within that window
           (e.g. deterministic IDs, nack-requeue redelivery after a
           crash before clear_cancel_flag ran), the stale flag will
           cancel the next delivery too. Two mitigations if you hit
           this in production: (a) keep ``task_id`` unique per
           schedule (uuid.uuid7 defaults do this), (b) shorten
           ``ttl_ms`` to less than your expected redelivery window.
           A flag-nonce scheme is tracked for a future release if
           a real caller reports the issue.
        """
        if not task_id:
            raise ValueError("task_id must be a non-empty string")
        return await self._backend.set_cancel_flag(task_id, ttl_ms)
