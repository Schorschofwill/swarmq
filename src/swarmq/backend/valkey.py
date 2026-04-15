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

    async def store_result(self, task_id: str, result_data: bytes, ttl_ms: int) -> None:
        try:
            await self._client.set(self._key(task_id), result_data, px=ttl_ms)
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
