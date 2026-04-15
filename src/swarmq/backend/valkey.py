"""Valkey result backend for SwarmQ."""

import valkey.asyncio
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.exceptions import BackendConnectionError


class ValkeyBackend:
    def __init__(self, url: str) -> None:
        self._url = url
        self._client: valkey.asyncio.Valkey | None = None

    async def connect(self) -> None:
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
        await self._client.set(self._key(task_id), result_data, px=ttl_ms)

    async def get_result(self, task_id: str) -> bytes | None:
        return await self._client.get(self._key(task_id))

    async def delete_result(self, task_id: str) -> None:
        await self._client.delete(self._key(task_id))
