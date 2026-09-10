"""Valkey-``ResultBackend`` (Plan U17, KTD4): Ergebnisse, Warten und
Benachrichtigung, Fortschritt, Cancel-Flags, Workflow-Zustand, Dedup-Flags.

Die Pub/Sub-Wartelogik („auf Ergebnis warten", „Fortschritt beobachten")
wandert aus ``client.py`` hierher, inklusive des aktiven Resubscribe nach
Verbindungsabbruch (valkey-py resubscribed Pub/Sub nicht selbst — toxiproxy-
Spike 2026-04-17). Die heutigen Methodennamen bleiben als Aliase (KTD10).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import structlog
import valkey.asyncio
from valkey.exceptions import ValkeyError

from swarmq.backend.valkey.connection import ValkeyConnection
from swarmq.exceptions import BackendConnectionError
from swarmq.lua import load_script

logger = structlog.get_logger()

# Polling-Rückfall, wenn Pub/Sub die Benachrichtigung verpasst (Client-Erbe).
_FALLBACK_POLL_S = 0.005
# Höchstens so lange wird pro Wartevorgang auf Pub/Sub gesetzt, bevor auf
# Polling umgeschaltet wird (Client-Erbe: ``min(1.0, remaining)``).
_PUBSUB_WINDOW_S = 1.0


class ValkeyResults(ValkeyConnection):
    def __init__(
        self,
        url: str | None = None,
        *,
        client: valkey.asyncio.Valkey | None = None,
        pool: valkey.asyncio.ConnectionPool | None = None,
    ) -> None:
        super().__init__(url, client=client, pool=pool)
        # R3.5: Lua script is lazy-registered on first use (registration
        # requires a connected client). See _ensure_unique_release_script.
        self._unique_release_script: Any = None
        # KTD11: Compare-and-Set des Workflow-Zustands, ebenfalls lazy.
        self._workflow_cas_script: Any = None

    # ------------------------------------------------------------------
    # Keys und Kanäle (Wire-Format, KTD12 — unverändert)
    # ------------------------------------------------------------------

    def _key(self, task_id: str) -> str:
        return f"sq:result:{task_id}"

    def _cancel_key(self, task_id: str) -> str:
        return f"sq:cancel:{task_id}"

    def _unique_key(self, dedup_hash: str) -> str:
        return f"sq:unique:{dedup_hash}"

    def _progress_key(self, task_id: str) -> str:
        return f"sq:progress:{task_id}"

    def _workflow_key(self, wf_id: str) -> str:
        return f"sq:wf:{wf_id}"

    def _workflow_cancel_key(self, wf_id: str) -> str:
        return f"sq:wf:cancel:{wf_id}"

    @staticmethod
    def notify_channel_name(task_id: str) -> str:
        return f"sq:notify:{task_id}"

    def progress_channel_name(self, task_id: str) -> str:
        """Return the Pub/Sub channel name for *task_id*'s progress stream.

        Public API: consumers (e.g. :class:`Client.watch_progress`)
        subscribe to this channel. Stable across releases.
        """
        return f"sq:progress:channel:{task_id}"

    def workflow_notify_channel(self, wf_id: str) -> str:
        """Pub/Sub channel a workflow's ``get()`` waiter subscribes to and
        the worker advancement/failure path publishes to (U4/U6). Distinct
        from the per-task ``sq:notify:{task_id}`` channel — workflow liveness
        is keyed by workflow id, which is stable from ``apply()`` time."""
        return f"sq:wf:notify:{wf_id}"

    # ------------------------------------------------------------------
    # Ergebnisse
    # ------------------------------------------------------------------

    async def store_result(self, task_id: str, data: bytes, ttl_ms: int, op_timeout: float | None = None) -> None:
        """Persist a task result under its task_id key with TTL.

        *op_timeout* (seconds) bounds the Valkey SET so a slow backend
        cannot wedge the worker finally-block.
        """
        await self._run("store_result", self._require_client().set(self._key(task_id), data, px=ttl_ms), op_timeout)

    async def get_result(self, task_id: str) -> bytes | None:
        raw: bytes | None = await self._run("get_result", self._require_client().get(self._key(task_id)))
        return raw

    async def delete_result(self, task_id: str) -> None:
        await self._run("delete_result", self._require_client().delete(self._key(task_id)))

    async def get_results(self, task_ids: Sequence[str]) -> list[bytes | None]:
        if not task_ids:
            return []
        keys = [self._key(task_id) for task_id in task_ids]
        raw: list[bytes | None] = await self._run("get_results", self._require_client().mget(*keys))
        return raw

    async def notify_result(self, task_id: str) -> None:
        try:
            await self._require_client().publish(self.notify_channel_name(task_id), "done")
        except (BackendConnectionError, ValkeyError, OSError):
            logger.warning("publish_notification failed", task_id=task_id, exc_info=True)

    async def _pubsub_window(
        self,
        pubsub: Any,
        get_within_deadline: Callable[[], Awaitable[bytes | None]],
        deadline: float,
    ) -> bytes | None:
        """Pub/Sub-Fenster: auf Benachrichtigung warten, dann erneut lesen.
        ``None`` = kein Ergebnis im Fenster (Aufrufer pollt weiter)."""
        loop = asyncio.get_running_loop()
        remaining = deadline - loop.time()
        pub_deadline = loop.time() + min(_PUBSUB_WINDOW_S, remaining)
        while loop.time() < pub_deadline:
            try:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
            except Exception:
                return None  # Pub/Sub error, fall through to polling
            if msg is not None:
                raw = await get_within_deadline()
                if raw is not None:
                    return raw
        return None

    @staticmethod
    async def _close_pubsub(pubsub: Any, channel: str) -> None:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass

    async def wait_for_result(self, task_id: str, *, timeout: float) -> bytes | None:
        """Liefert das Ergebnis, sobald vorhanden; ``None`` bei Ablauf.

        Ablauf (Client-Erbe): Pub/Sub zuerst abonnieren
        (schließt das Rennen zwischen Erst-GET und Abonnement), dann Erst-GET,
        dann ein Pub/Sub-Fenster, dann Polling. Jeder GET ist an die
        Rest-Deadline gebunden, damit ein hängendes Valkey den Aufrufer nicht
        über ``timeout`` hinaus blockiert.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        async def _get_within_deadline() -> bytes | None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                return await asyncio.wait_for(self.get_result(task_id), timeout=remaining)
            except TimeoutError:
                return None

        channel = self.notify_channel_name(task_id)
        pubsub: Any = self._pubsub()
        try:
            await pubsub.subscribe(channel)
        except Exception:
            pubsub = None  # Pub/Sub unavailable, fall through to polling

        try:
            raw = await _get_within_deadline()
            if raw is not None:
                return raw
            if pubsub is not None:
                raw = await self._pubsub_window(pubsub, _get_within_deadline, deadline)
                if raw is not None:
                    return raw
            while loop.time() < deadline:
                raw = await _get_within_deadline()
                if raw is not None:
                    return raw
                await asyncio.sleep(_FALLBACK_POLL_S)
            return None
        finally:
            if pubsub is not None:
                await self._close_pubsub(pubsub, channel)

    # ------------------------------------------------------------------
    # R3.7 Fortschritt — HASH-Zustand + Pub/Sub-Fan-out
    # ------------------------------------------------------------------

    async def set_progress(
        self, task_id: str, fields: Mapping[str, str], ttl_ms: int, op_timeout: float | None = None
    ) -> None:
        """Write the progress HASH for *task_id* with a refreshed TTL.

        Uses a pipeline so the HSET and PEXPIRE are a single round-trip
        — atomicity between them is not required, but the cost saving
        is meaningful for progress-heavy tasks. Raises
        :class:`BackendConnectionError` on transport failure; the
        caller (Task.update_progress) swallows-and-logs to keep
        progress best-effort.
        """
        key = self._progress_key(task_id)

        async def _write() -> None:
            async with self._require_client().pipeline(transaction=False) as pipe:
                pipe.hset(key, mapping=dict(fields))
                pipe.pexpire(key, ttl_ms)
                await pipe.execute()

        await self._run("set_progress", _write(), op_timeout)

    async def get_progress(self, task_id: str, op_timeout: float | None = None) -> dict[bytes, bytes] | None:
        """Return the raw Valkey HASH for *task_id* or None when absent.

        None covers both "never written" and "TTL-expired". Caller
        decodes via :meth:`ProgressInfo.from_hash`.
        """
        raw: dict[bytes, bytes] = await self._run(
            "get_progress", self._require_client().hgetall(self._progress_key(task_id)), op_timeout
        )
        return raw if raw else None

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        """PUBLISH the progress delta to the per-task channel.

        Failures are logged and swallowed — Pub/Sub is best-effort and
        the authoritative state is the HASH, not the channel. Mirror
        the :meth:`notify_result` discipline.
        """
        try:
            await self._require_client().publish(self.progress_channel_name(task_id), payload)
        except (BackendConnectionError, ValkeyError, OSError):
            logger.warning("publish_progress_failed", task_id=task_id, exc_info=True)

    async def watch_progress(self, task_id: str) -> AsyncIterator[bytes]:
        """Fortschritts-Payloads (JSON der HASH-Felder) bis zum Abbruch.

        Abonniert ZUERST, liefert dann den aktuellen HASH-Stand als Payload
        (überbrückt „vor dem Abonnement veröffentlicht"), danach jede
        Veröffentlichung. Bei Pub/Sub-Fehlern: HASH-Schnappschuss, aktives
        Resubscribe, 0,5 s Backoff. Setup-Fehler: ein Schnappschuss, dann
        sauberes Ende. Ungültige Payloads (kein ``bytes``) werden verworfen.
        """
        channel = self.progress_channel_name(task_id)
        pubsub: Any = self._pubsub()

        async def _subscribe_and_drain() -> None:
            await pubsub.subscribe(channel)
            # Drain the subscribe-confirmation so later get_message
            # calls don't return it as a false-positive update.
            await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)

        async def _snapshot() -> bytes | None:
            raw = await self.get_progress(task_id)
            if raw is None:
                return None
            return json.dumps({k.decode(): v.decode() for k, v in raw.items()}).encode()

        try:
            try:
                await _subscribe_and_drain()
            except Exception:
                fallback = await _snapshot()
                if fallback is not None:
                    yield fallback
                return

            initial = await _snapshot()
            if initial is not None:
                yield initial

            while True:
                try:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                except Exception:
                    # Pub/Sub-Fehler: Schnappschuss überbrückt, dann aktives
                    # Resubscribe mit 0,5 s Backoff.
                    fallback = await _snapshot()
                    if fallback is not None:
                        yield fallback
                    await self._try_resubscribe(_subscribe_and_drain)
                    continue
                if msg is None or msg.get("type") != "message":
                    continue
                data = msg.get("data")
                if isinstance(data, bytes):
                    yield data
        finally:
            await self._close_pubsub(pubsub, channel)

    @staticmethod
    async def _try_resubscribe(subscribe_and_drain: Any) -> None:
        try:
            await subscribe_and_drain()
        except Exception:
            pass
        await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    # Cancel-Flags
    # ------------------------------------------------------------------

    async def set_cancel_flag(self, task_id: str, ttl_ms: int, op_timeout: float | None = None) -> bool:
        """Set the cancel flag for *task_id* atomically with TTL (SET NX PX).
        Returns True when the flag was newly set."""
        result = await self._run(
            "set_cancel_flag",
            self._require_client().set(self._cancel_key(task_id), "1", nx=True, px=ttl_ms),
            op_timeout,
        )
        return bool(result)

    async def is_cancelled(self, task_id: str, op_timeout: float | None = None) -> bool:
        result = await self._run("is_cancelled", self._require_client().exists(self._cancel_key(task_id)), op_timeout)
        return bool(result == 1)

    async def clear_cancel_flag(self, task_id: str, op_timeout: float | None = None) -> None:
        """Delete the cancel flag. Failures are logged and swallowed — an
        orphan flag self-heals via TTL, and a DEL failure must not break
        the worker pipeline (plan Unit 3 / Unit 6 Flag-DEL-Failure rule)."""
        try:
            await self._run("clear_cancel_flag", self._require_client().delete(self._cancel_key(task_id)), op_timeout)
        except BackendConnectionError as exc:
            logger.warning("cancel_flag_delete_failed", task_id=task_id, error=str(exc))

    # ------------------------------------------------------------------
    # R4.1 Workflow — Zustand, Cancel-Flag, Benachrichtigung
    # ------------------------------------------------------------------

    async def store_workflow_state(self, wf_id: str, data: bytes, ttl_ms: int, op_timeout: float | None = None) -> None:
        """Persist the workflow topology+status blob under its wf-id key
        with TTL. Overwrites on each call."""
        await self._run(
            "store_workflow_state", self._require_client().set(self._workflow_key(wf_id), data, px=ttl_ms), op_timeout
        )

    def _ensure_workflow_cas_script(self) -> Any:
        """Lazy-register workflow_state_cas.lua on first use (needs a live client)."""
        client = self._client
        if client is None:
            raise BackendConnectionError("ValkeyBackend not connected; call connect() before workflow-state operations")
        if self._workflow_cas_script is None:
            self._workflow_cas_script = client.register_script(load_script("workflow_state_cas"))
        return self._workflow_cas_script

    async def compare_and_store_workflow_state(
        self, wf_id: str, data: bytes, ttl_ms: int, *, expected_version: int, op_timeout: float | None = None
    ) -> bool:
        """Compare-and-Set (KTD11): ``workflow_state_cas.lua`` vergleicht die
        ``version`` des Bestands mit *expected_version* und schreibt *data*
        byteweise mit ``PX``. ``True`` bei Schreibung, ``False`` bei Konflikt
        oder fehlendem Schluessel."""
        script = self._ensure_workflow_cas_script()
        result = await self._run(
            "compare_and_store_workflow_state",
            script(keys=[self._workflow_key(wf_id)], args=[expected_version, data, ttl_ms]),
            op_timeout,
        )
        return int(result) == 1

    async def get_workflow_state(self, wf_id: str, op_timeout: float | None = None) -> bytes | None:
        raw: bytes | None = await self._run(
            "get_workflow_state", self._require_client().get(self._workflow_key(wf_id)), op_timeout
        )
        return raw

    async def set_workflow_cancel_flag(self, wf_id: str, ttl_ms: int, op_timeout: float | None = None) -> bool:
        result = await self._run(
            "set_workflow_cancel_flag",
            self._require_client().set(self._workflow_cancel_key(wf_id), "1", nx=True, px=ttl_ms),
            op_timeout,
        )
        return bool(result)

    async def is_workflow_cancelled(self, wf_id: str, op_timeout: float | None = None) -> bool:
        result = await self._run(
            "is_workflow_cancelled", self._require_client().exists(self._workflow_cancel_key(wf_id)), op_timeout
        )
        return bool(result == 1)

    async def notify_workflow(self, wf_id: str) -> None:
        """Wake any ``handle.get()`` waiting on this workflow (R16 liveness).
        Best-effort: failures are logged and swallowed."""
        try:
            await self._require_client().publish(self.workflow_notify_channel(wf_id), "done")
        except (BackendConnectionError, ValkeyError, OSError):
            logger.warning("publish_workflow_notification failed", wf_id=wf_id, exc_info=True)

    async def wait_for_workflow(self, wf_id: str, *, timeout: float) -> bool:
        """``True`` bei Benachrichtigung vor Ablauf, sonst ``False``."""
        channel = self.workflow_notify_channel(wf_id)
        pubsub: Any = self._pubsub()
        try:
            await pubsub.subscribe(channel)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while loop.time() < deadline:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
                if msg is not None:
                    return True
            return False
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # R3.5 Unique Tasks — Dedup-Flags
    # ------------------------------------------------------------------

    def _ensure_unique_release_script(self) -> Any:
        """Lazy-register unique_release.lua on first use (needs a live client)."""
        client = self._client
        if client is None:
            raise BackendConnectionError("ValkeyBackend not connected; call connect() before unique-release operations")
        if self._unique_release_script is None:
            self._unique_release_script = client.register_script(load_script("unique_release"))
        return self._unique_release_script

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, op_timeout: float | None = None
    ) -> str | None:
        """Set dedup flag atomically (``SET NX GET PX``); return the existing
        owner on conflict, ``None`` when claimed."""
        result = await self._run(
            "setnx_unique",
            self._require_client().set(self._unique_key(dedup_hash), owner, nx=True, get=True, px=ttl_ms),
            op_timeout,
        )
        # valkey-py returns bytes on GET; None means NX was granted.
        if result is None:
            return None
        if isinstance(result, bytes):
            return result.decode("utf-8")
        return str(result)

    async def release_unique(self, dedup_hash: str, owner: str, op_timeout: float | None = None) -> bool:
        """Release the dedup flag only if *owner* matches (unique_release.lua).
        ``False`` for a foreign owner, an expired flag, or any failure — a
        release failure must not break the worker's terminal path."""
        try:
            script = self._ensure_unique_release_script()
            result = await self._run(
                "unique_release", script(keys=[self._unique_key(dedup_hash)], args=[owner]), op_timeout
            )
        except BackendConnectionError as exc:
            logger.warning("unique_release_backend_error", dedup_hash=dedup_hash, task_id=owner, error=str(exc))
            return False
        return int(result) == 1

    # ------------------------------------------------------------------
    # Aliase der heutigen Namen (KTD10; entfallen in U15)
    # ------------------------------------------------------------------







if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ResultBackend

    _conforms_results: type[ResultBackend] = ValkeyResults
