"""Valkey result backend for SwarmQ."""

import asyncio

import structlog
import valkey.asyncio
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ValkeyError

from swarmq.exceptions import BackendConnectionError

logger = structlog.get_logger()


class ValkeyBackend:
    def __init__(self, url: str) -> None:
        self._url = url
        self._client: valkey.asyncio.Valkey | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        try:
            self._client = valkey.asyncio.Valkey.from_url(self._url)
            await self._client.ping()
        except (ValkeyConnectionError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    async def close(self) -> None:
        if self._client is not None:
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
