"""Valkey result backend for SwarmQ."""

import asyncio

import structlog
import valkey.asyncio
from valkey.asyncio.retry import Retry
from valkey.backoff import ExponentialBackoff
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError
from valkey.exceptions import ValkeyError

from swarmq.exceptions import BackendConnectionError, ConfigurationError
from swarmq.lua import load_script

logger = structlog.get_logger()

# Transient transport errors that should be retried transparently by the
# valkey-py client. Application-level errors (ResponseError, DataError,
# etc.) are NOT retried — they're programmer errors, not connectivity.
_TRANSIENT_ERRORS: list[type[Exception]] = [
    ValkeyConnectionError,
    ValkeyTimeoutError,
    OSError,
]

# Credential-free placeholder for ``_url`` when a client/pool is injected.
# Using a fixed literal (never ``None``, never derived from the borrowed
# object) keeps every downstream reader — SwarmQConfig validation, log
# formatting, sanitize_url — working without a None-guard and without ever
# leaking the caller's connection credentials.
_INJECTED_URL = "<injected>"


class ValkeyBackend:
    def __init__(
        self,
        url: str | None = None,
        *,
        client: valkey.asyncio.Valkey | None = None,
        pool: valkey.asyncio.ConnectionPool | None = None,
    ) -> None:
        """Construct a backend from a URL, an existing client, or a pool.

        Exactly one source must be given:

        * ``url`` — swarmq builds and owns the client (default path).
        * ``client`` — an already-constructed ``valkey.asyncio.Valkey``;
          swarmq borrows it and never closes it (see :meth:`close`).
        * ``pool`` — a ``valkey.asyncio.ConnectionPool``; swarmq wraps a
          client around it but leaves the pool's lifecycle to the caller.

        Passing more than one (e.g. ``url`` *and* ``client``) is a
        :class:`ConfigurationError` rather than a silent precedence rule.

        Injected client/pool objects are intended for the producer
        (task-sending) path only — see ``SwarmQ.start_worker``.

        Caveats for the injected path: the borrowed client/pool keeps the
        caller's own retry / health-check configuration (it does NOT
        inherit swarmq's), and it must target the same Valkey database the
        rest of the swarmq config expects — swarmq does not validate this,
        and a mismatch silently reads/writes results in the wrong namespace.
        """
        sources = [
            ("url", url is not None),
            ("client", client is not None),
            ("pool", pool is not None),
        ]
        given = [name for name, present in sources if present]
        if not given:
            raise ConfigurationError(
                "ValkeyBackend requires exactly one of: url, client, or pool"
            )
        if len(given) > 1:
            raise ConfigurationError(
                "ValkeyBackend accepts exactly one of url, client, or pool — "
                f"got {', '.join(given)}"
            )

        # _owns_client is the single source of truth for whether close()
        # may tear the client down. A borrowed client/pool belongs to the
        # caller (see docs/solutions worker-cancel-safe-teardown): closing
        # it would pull the connection out from under the caller's app.
        if client is not None:
            self._url: str = _INJECTED_URL
            self._client: valkey.asyncio.Valkey | None = client
            self._owns_client = False
        elif pool is not None:
            self._url = _INJECTED_URL
            # valkey-py sets auto_close_connection_pool=False automatically
            # when a pool is passed in, so the borrowed pool survives even
            # an accidental aclose() — _owns_client=False is the contract.
            self._client = valkey.asyncio.Valkey(connection_pool=pool)
            self._owns_client = False
        else:
            self._url = url
            self._client = None
            self._owns_client = True
        # R3.5: Lua script is lazy-registered on first use (registration
        # requires a connected client). See _ensure_unique_release_script.
        self._unique_release_script: valkey.asyncio.client.Script | None = None

    async def connect(self) -> None:
        # Idempotent, and a no-op when a client was injected: the borrowed
        # client/pool already exists, so swarmq builds nothing here.
        if self._client is not None:
            return
        try:
            # Reliability hardening (2026-04-17 toxiproxy spike): command-
            # level retry on transient transport errors with exponential
            # backoff capped at 2s. health_check_interval=30 detects
            # half-open connections before they surface to callers.
            self._client = valkey.asyncio.Valkey.from_url(
                self._url,
                retry=Retry(ExponentialBackoff(cap=2.0, base=0.05), retries=5),
                retry_on_error=_TRANSIENT_ERRORS,
                health_check_interval=30,
            )
            await self._client.ping()
        except (ValkeyConnectionError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def close(self) -> None:
        if self._client is not None:
            # Only tear down a client swarmq built itself. A borrowed
            # client/pool (injected at construction) is left open for the
            # caller — closing it here would break the caller's own app.
            if self._owns_client:
                await self._client.aclose()
            self._client = None

    def _key(self, task_id: str) -> str:
        return f"sq:result:{task_id}"

    def _cancel_key(self, task_id: str) -> str:
        return f"sq:cancel:{task_id}"

    # ---- Scheduler key helpers (Unit 5) ------------------------------------
    def _sched_zset_key(self) -> str:
        return "sq:sched:zset"

    def _sched_defs_key(self) -> str:
        return "sq:sched:defs"

    def _sched_fired_key(self, schedfire_key: str) -> str:
        return f"sq:sched:fired:{schedfire_key}"

    def _sched_handover_key(self, owner_token: str) -> str:
        return f"sq:sched:handover:{owner_token}"

    def _unique_key(self, dedup_hash: str) -> str:
        return f"sq:unique:{dedup_hash}"

    def _ensure_unique_release_script(self) -> valkey.asyncio.client.Script:
        """Lazy-register unique_release.lua on first use.

        Registration needs a live client; we register lazily so the backend
        doesn't require Lua-script files present at ValkeyBackend() construction
        time. Returns the registered AsyncScript (reused across calls).

        Raises BackendConnectionError when called before ``connect()`` — the
        caller path (release_unique) then logs-and-swallows consistent with
        its owner-check-can-fail contract.
        """
        if self._client is None:
            raise BackendConnectionError(
                "ValkeyBackend not connected; call connect() before "
                "unique-release operations"
            )
        if self._unique_release_script is None:
            self._unique_release_script = self._client.register_script(
                load_script("unique_release")
            )
        return self._unique_release_script

    async def store_result(
        self, task_id: str, result_data: bytes, ttl_ms: int,
        op_timeout: float | None = None,
    ) -> None:
        """Persist a task result under its task_id key with TTL.

        *op_timeout* (seconds) bounds the Valkey SET so a slow backend
        cannot wedge the worker finally-block (REV-P1-3).
        """
        try:
            coro = self._client.set(self._key(task_id), result_data, px=ttl_ms)
            if op_timeout is not None:
                await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"store_result timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def get_result(self, task_id: str) -> bytes | None:
        try:
            return await self._client.get(self._key(task_id))
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def delete_result(self, task_id: str) -> None:
        try:
            await self._client.delete(self._key(task_id))
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def publish_notification(self, task_id: str) -> None:
        try:
            await self._client.publish(f"sq:notify:{task_id}", "done")
        except (ValkeyError, OSError):
            logger.warning("publish_notification failed", task_id=task_id, exc_info=True)

    async def get_results_batch(self, task_ids: list[str]) -> list[bytes | None]:
        if not task_ids:
            return []
        keys = [self._key(task_id) for task_id in task_ids]
        try:
            return await self._client.mget(*keys)
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    def create_pubsub(self) -> valkey.asyncio.client.PubSub:
        return self._client.pubsub()

    # -----------------------------------------------------------------------
    # R3.7 Progress tracking — HASH state + Pub/Sub fan-out
    # -----------------------------------------------------------------------

    def _progress_key(self, task_id: str) -> str:
        return f"sq:progress:{task_id}"

    def progress_channel_name(self, task_id: str) -> str:
        """Return the Pub/Sub channel name for *task_id*'s progress stream.

        Public API: consumers (e.g. :class:`Client.watch_progress`)
        subscribe to this channel. Stable across releases.
        """
        return f"sq:progress:channel:{task_id}"

    async def set_progress(
        self,
        task_id: str,
        fields: dict[str, str],
        ttl_ms: int,
        op_timeout: float | None = None,
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
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.hset(key, mapping=fields)
                pipe.pexpire(key, ttl_ms)
                coro = pipe.execute()
                if op_timeout is not None:
                    await asyncio.wait_for(coro, timeout=op_timeout)
                else:
                    await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"set_progress timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def get_progress(
        self, task_id: str, op_timeout: float | None = None,
    ) -> dict[bytes, bytes] | None:
        """Return the raw Valkey HASH for *task_id* or None when absent.

        None covers both "never written" and "TTL-expired". Caller
        decodes via :meth:`ProgressInfo.from_hash`.
        """
        try:
            coro = self._client.hgetall(self._progress_key(task_id))
            if op_timeout is not None:
                raw = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                raw = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"get_progress timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return raw if raw else None

    async def publish_progress(
        self, task_id: str, payload: bytes,
    ) -> None:
        """PUBLISH the progress delta to the per-task channel.

        Failures are logged and swallowed — Pub/Sub is best-effort and
        the authoritative state is the HASH, not the channel. Mirror
        the :meth:`publish_notification` discipline.
        """
        try:
            await self._client.publish(
                self.progress_channel_name(task_id), payload,
            )
        except (ValkeyError, OSError):
            logger.warning(
                "publish_progress_failed",
                task_id=task_id, exc_info=True,
            )

    async def set_cancel_flag(
        self, task_id: str, ttl_ms: int, op_timeout: float | None = None,
    ) -> bool:
        """Set the cancel flag for *task_id* atomically with TTL.

        Returns True when the flag was newly set, False when it already
        existed.  Uses ``SET NX PX`` — a single round-trip that handles
        idempotency (NX) and expiry (PX) together.  No Lua script needed.

        *op_timeout* (seconds) wraps the underlying Valkey call in
        ``asyncio.wait_for`` so a slow Valkey cannot wedge the caller
        (REV-P1-3). Timeout surfaces as BackendConnectionError, same
        as any other backend fault. None = no timeout enforced at this
        layer (backwards-compatible default).
        """
        try:
            coro = self._client.set(
                self._cancel_key(task_id), "1", nx=True, px=ttl_ms
            )
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"set_cancel_flag timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return bool(result)

    async def is_cancelled(
        self, task_id: str, op_timeout: float | None = None,
    ) -> bool:
        """Return True when a cancel flag exists for *task_id*.

        *op_timeout* (seconds) wraps the Valkey EXISTS in
        ``asyncio.wait_for`` so the cancel monitor cannot wedge
        waiting on a slow backend (REV-P1-3).
        """
        try:
            coro = self._client.exists(self._cancel_key(task_id))
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"is_cancelled timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return result == 1

    # -----------------------------------------------------------------------
    # R4.1 Workflow primitives — state, cancel flag, notification (U2)
    # -----------------------------------------------------------------------

    def _workflow_key(self, wf_id: str) -> str:
        return f"sq:wf:{wf_id}"

    def _workflow_cancel_key(self, wf_id: str) -> str:
        return f"sq:wf:cancel:{wf_id}"

    def workflow_notify_channel(self, wf_id: str) -> str:
        """Pub/Sub channel a workflow's ``get()`` waiter subscribes to and
        the worker advancement/failure path publishes to (U4/U6). Distinct
        from the per-task ``sq:notify:{task_id}`` channel — workflow liveness
        is keyed by workflow id, which is stable from ``apply()`` time."""
        return f"sq:wf:notify:{wf_id}"

    async def store_workflow_state(
        self, wf_id: str, state_data: bytes, ttl_ms: int,
        op_timeout: float | None = None,
    ) -> None:
        """Persist the workflow topology+status blob under its wf-id key
        with TTL. Overwrites on each call (the worker rewrites status as the
        workflow advances)."""
        try:
            coro = self._client.set(self._workflow_key(wf_id), state_data, px=ttl_ms)
            if op_timeout is not None:
                await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"store_workflow_state timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def get_workflow_state(
        self, wf_id: str, op_timeout: float | None = None,
    ) -> bytes | None:
        """Return the workflow state blob, or None when absent/expired
        (the latter maps to ``WorkflowNotFound`` at the client layer, R14)."""
        try:
            coro = self._client.get(self._workflow_key(wf_id))
            if op_timeout is not None:
                return await asyncio.wait_for(coro, timeout=op_timeout)
            return await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"get_workflow_state timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def set_workflow_cancel_flag(
        self, wf_id: str, ttl_ms: int, op_timeout: float | None = None,
    ) -> bool:
        """Set the workflow-level cancel flag atomically with TTL (R17).

        Returns True when newly set, False when it already existed. The
        worker checks this flag in the same operation that decides the next
        enqueue, so 'not yet enqueued' is well-defined under the advancement
        race. Mirrors ``set_cancel_flag`` (SET NX PX)."""
        try:
            coro = self._client.set(
                self._workflow_cancel_key(wf_id), "1", nx=True, px=ttl_ms
            )
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"set_workflow_cancel_flag timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return bool(result)

    async def is_workflow_cancelled(
        self, wf_id: str, op_timeout: float | None = None,
    ) -> bool:
        """Return True when a workflow-level cancel flag exists (R17)."""
        try:
            coro = self._client.exists(self._workflow_cancel_key(wf_id))
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"is_workflow_cancelled timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return result == 1

    async def publish_workflow_notification(self, wf_id: str) -> None:
        """Wake any ``handle.get()`` waiting on this workflow (R16 liveness).

        Best-effort: a dropped notification falls back to the get() state
        poll, so transient publish failures are logged and swallowed —
        consistent with the per-task ``publish_notification``."""
        try:
            await self._client.publish(self.workflow_notify_channel(wf_id), "done")
        except (ValkeyError, OSError):
            logger.warning(
                "publish_workflow_notification failed", wf_id=wf_id, exc_info=True
            )

    # -----------------------------------------------------------------------
    # R3.5 Unique Tasks — dedup flag operations
    # -----------------------------------------------------------------------

    async def setnx_unique(
        self, dedup_hash: str, task_id: str, ttl_ms: int,
        op_timeout: float | None = None,
    ) -> str | None:
        """Set dedup flag atomically; return existing owner on conflict.

        Uses ``SET NX GET PX`` — a single atomic round-trip that either
        claims the flag (returns None) or returns the existing owner
        (the task_id of the task that won the race). Available since
        Valkey/Redis 7.0+. Eliminates the TOCTOU gap of a
        SETNX-then-GET two-step.

        Returns:
          None — NX granted, caller owns the flag for ttl_ms.
          str  — Existing owner task_id; caller should treat as duplicate.

        Raises BackendConnectionError on Valkey transport/timeout failure.
        R3.5 callers (Client.schedule, Scheduler._fire_one) propagate this
        as a hard failure — they cannot proceed without knowing dedup state.
        """
        try:
            coro = self._client.set(
                self._unique_key(dedup_hash),
                task_id,
                nx=True,
                get=True,
                px=ttl_ms,
            )
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError as exc:
            raise BackendConnectionError(
                f"setnx_unique timed out after {op_timeout}s"
            ) from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

        # valkey-py returns bytes on GET; None means NX was granted.
        if result is None:
            return None
        if isinstance(result, bytes):
            return result.decode("utf-8")
        return str(result)

    async def release_unique(
        self, dedup_hash: str, task_id: str,
        op_timeout: float | None = None,
    ) -> bool:
        """Release dedup flag only if *task_id* matches the stored owner.

        Uses unique_release.lua (GET-and-compare-then-DEL atomic in one
        EVAL). Protects against TTL-expiry-race where a second task
        claimed the flag after TTL expiry and the first task's completion
        release would otherwise DEL the wrong owner's flag.

        Returns:
          True  — flag released (we were the owner).
          False — flag not released (foreign owner, TTL-expired, or
                  never set). Non-exceptional outcome.

        Failures are logged and swallowed (like clear_cancel_flag) —
        an orphan flag self-heals via TTL, and a DEL failure must not
        break the worker's terminal-path pipeline.
        """
        try:
            script = self._ensure_unique_release_script()
            coro = script(keys=[self._unique_key(dedup_hash)], args=[task_id])
            if op_timeout is not None:
                result = await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                result = await coro
        except TimeoutError:
            logger.warning(
                "unique_release_timed_out",
                dedup_hash=dedup_hash,
                task_id=task_id,
                error=f"timed out after {op_timeout}s",
            )
            return False
        except BackendConnectionError as exc:
            # Pre-connect path (caller passed a non-connected backend).
            # Swallow to match clear_cancel_flag: terminal-path callers
            # must not crash on a release failure.
            logger.warning(
                "unique_release_backend_error",
                dedup_hash=dedup_hash,
                task_id=task_id,
                error=str(exc),
            )
            return False
        except (ValkeyError, OSError) as exc:
            logger.warning(
                "unique_release_backend_error",
                dedup_hash=dedup_hash,
                task_id=task_id,
                error=str(exc),
            )
            return False
        return int(result) == 1

    async def clear_cancel_flag(
        self, task_id: str, op_timeout: float | None = None,
    ) -> None:
        """Delete the cancel flag for *task_id*.

        *op_timeout* (seconds) bounds the call; timeout is logged
        like any other failure (REV-P1-3). Failures are logged and
        swallowed — an orphan flag will self-heal
        via TTL, and a DEL failure must not break the worker pipeline
        (plan Unit 3 / Unit 6 Flag-DEL-Failure rule).
        """
        try:
            coro = self._client.delete(self._cancel_key(task_id))
            if op_timeout is not None:
                await asyncio.wait_for(coro, timeout=op_timeout)
            else:
                await coro
        except TimeoutError:
            logger.warning(
                "cancel_flag_delete_failed",
                task_id=task_id,
                error=f"timed out after {op_timeout}s",
            )
        except (ValkeyError, OSError) as exc:
            logger.warning(
                "cancel_flag_delete_failed", task_id=task_id, error=str(exc)
            )

    # -----------------------------------------------------------------------
    # Scheduler persistence operations (Unit 5)
    # -----------------------------------------------------------------------

    async def zadd_schedule(self, schedule_name: str, fire_ts_ms: int) -> None:
        """Insert / update the fire-time of a schedule in the sorted-set."""
        try:
            await self._client.zadd(
                self._sched_zset_key(), {schedule_name: fire_ts_ms}
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def zrem_schedule(self, schedule_name: str) -> None:
        """Drop a schedule from the zset. No-op when it is not present."""
        try:
            await self._client.zrem(self._sched_zset_key(), schedule_name)
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def zrangebyscore_schedules(
        self, max_score_ms: int, limit: int
    ) -> list[bytes]:
        """Return schedule names whose fire-ts is <= *max_score_ms*.

        Read-only; does NOT claim/remove. The Lua commit-script is the
        only mutation path (see Unit 6).
        """
        try:
            return await self._client.zrangebyscore(
                self._sched_zset_key(), "-inf", max_score_ms,
                start=0, num=limit,
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def hset_schedule_def(
        self, schedule_name: str, def_json: bytes
    ) -> None:
        try:
            await self._client.hset(
                self._sched_defs_key(), schedule_name, def_json
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def hsetnx_schedule_def(
        self, schedule_name: str, def_json: bytes
    ) -> bool:
        """Atomic insert-if-absent. Returns True when the field was
        newly created, False when an entry already existed. Closes the
        TOCTOU gap between ``hget`` + ``hset`` in ``add_schedule``
        (Unit 8 / M-14)."""
        try:
            result = await self._client.hsetnx(
                self._sched_defs_key(), schedule_name, def_json
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return int(result) == 1

    async def hget_schedule_def(self, schedule_name: str) -> bytes | None:
        try:
            return await self._client.hget(
                self._sched_defs_key(), schedule_name
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def hdel_schedule_def(self, schedule_name: str) -> None:
        try:
            await self._client.hdel(self._sched_defs_key(), schedule_name)
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def hgetall_schedule_defs(self) -> dict[bytes, bytes]:
        try:
            return await self._client.hgetall(self._sched_defs_key())
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def set_schedfire_dedup(
        self, schedfire_key: str, ttl_ms: int
    ) -> bool:
        """Consumer-side "completed" dedup for scheduled-fire messages.

        Sets a long-TTL marker AFTER the task has finished executing
        (Unit 1 / H-2). Returns True when this was the first write
        (the task is now marked completed and any redelivery must be
        skipped), False when another worker already wrote the same
        completed marker (race-loser; idempotent).

        TTL must be configured to cover the retry horizon
        (``scheduler_dedup_ttl >= retry_max_delay``, enforced at
        config validation, Review-Finding P1-1).
        """
        try:
            result = await self._client.set(
                self._sched_fired_key(schedfire_key), "1",
                nx=True, px=ttl_ms,
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return bool(result)

    def _sched_inflight_key(self, schedfire_key: str) -> str:
        """Separate namespace from the completed marker so the two
        TTLs can coexist without collision. A task in-flight has
        ``sq:sched:inflight:<key>`` set; once completed, that expires
        naturally and ``sq:sched:fired:<key>`` takes over as the
        long-lived block."""
        return f"sq:sched:inflight:{schedfire_key}"

    async def set_schedfire_inflight(
        self, schedfire_key: str, ttl_ms: int
    ) -> bool:
        """Claim the in-flight slot for a scheduled-fire message.

        Pre-run SETNX with a short TTL (typically ``scheduler_inflight_ttl
        * 1000`` ms). Returns True when this caller is the first to
        claim the slot — it should proceed to run the task and write
        the long-TTL completed marker via :meth:`set_schedfire_dedup`
        in its finally block. Returns False when another worker holds
        the slot already — ack + skip.

        If the worker that won the slot crashes before writing the
        completed marker, the in-flight key expires on the short TTL
        and the next redelivery can re-run (restoring at-least-once).
        This is the fix for the pre-run-only-SETNX silent-drop hole
        (Unit 1 / H-2 / reliability R-002).
        """
        try:
            result = await self._client.set(
                self._sched_inflight_key(schedfire_key), "1",
                nx=True, px=ttl_ms,
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return bool(result)

    async def is_schedfire_completed(self, schedfire_key: str) -> bool:
        """Fast check whether the long-TTL completed marker already
        exists for this key. Used BEFORE the in-flight SETNX so that
        a task which finished successfully and whose broker-level ack
        was lost (causing RabbitMQ to redeliver) stays ack+skipped
        for the full retry-horizon window."""
        try:
            result = await self._client.exists(
                self._sched_fired_key(schedfire_key)
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return result == 1

    async def set_schedfire_handover_marker(
        self, owner_token: str, ttl_ms: int
    ) -> None:
        """Leader marker set right before graceful release.

        Incoming leader observing this key decides it is a clean HANDOVER
        rather than a FAILOVER (Review-Finding P2-1). TTL 5s is a narrow
        window — long enough for a standby to see it in its next poll
        cycle, short enough that a stale marker cannot mislead a later
        acquire.
        """
        try:
            await self._client.set(
                self._sched_handover_key(owner_token), "1", px=ttl_ms,
            )
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def get_and_del_schedfire_handover_marker(self) -> bool:
        """Consume any handover marker set by a previous leader.

        Returns ``True`` if at least one ``sq:sched:handover:*`` key
        existed (and was deleted), ``False`` otherwise. The outgoing
        leader sets the marker under its own owner-token, so the key
        name is not known ahead of time — we scan and delete.
        """
        found_any = False
        try:
            async for key in self._client.scan_iter(
                match="sq:sched:handover:*", count=100,
            ):
                await self._client.delete(key)
                found_any = True
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return found_any

    async def subscribe_and_wait(self, channel: str, timeout: float) -> bool:
        """Subscribe to a Pub/Sub channel and wait for a message.

        Returns True if a message was received, False on timeout or error.
        Always cleans up (unsubscribe + close) in finally.
        """
        pubsub = self.create_pubsub()
        try:
            await pubsub.subscribe(channel)
            # Poll with short intervals to catch messages reliably.
            # A single get_message() call can miss messages that arrive
            # between subscribe and the call.
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while loop.time() < deadline:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.05,
                )
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

    async def get_scheduler_leader(self) -> str | None:
        """Return the current scheduler-lock holder, or ``None`` if unlocked.

        Thin, read-only accessor to ``SCHEDULER_LOCK_KEY`` that the CLI
        (``swarmq schedule status``) consumes instead of reaching into
        ``backend._client``. Plan 2026-04-23-001 Unit 4 / KP-5.

        Does NOT swallow connection errors (Decision E4): callers can
        distinguish "no leader elected" (``None`` return) from
        "backend unreachable" (``ValkeyConnectionError`` /
        ``ValkeyTimeoutError`` propagates). Ops visibility for
        ``schedule status`` remains intact.
        """
        from swarmq.scheduler import SCHEDULER_LOCK_KEY

        raw = await self._client.get(SCHEDULER_LOCK_KEY)
        if raw is None:
            return None
        # ``errors='replace'`` keeps ``schedule status`` working even if a
        # rogue writer stored non-UTF8 bytes under SCHEDULER_LOCK_KEY —
        # the operator gets a (possibly mojibake) leader string rather
        # than a raw ``UnicodeDecodeError`` traceback.
        result = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
        # Treat empty string the same as "no leader" — defensive against
        # an upstream writer that SET an empty value (the lock contract
        # only ever stores the holder ID).
        return result if result else None
