"""Queue-Tiefen-Sampler (Plan U21): synchroner Sampler über eine
``BrokerAdmin``-Factory aus der CLI — ``process_manager`` importiert
keine Broker-Implementierung mehr (KTD6)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

import structlog

logger = structlog.get_logger()


SAMPLE_TIMEOUT_S = 5.0


class SamplerBroker(Protocol):
    """Struktureller Ausschnitt der ``BrokerAdmin``-Factory (U21): der
    Sampler braucht Verbindungsaufbau/-abbau plus die Vertragsmethode
    ``queue_depth`` — keine Broker-Implementierung im Paket (KTD6)."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def queue_depth(self, queue: str) -> int: ...


AdminFactory = Callable[[], SamplerBroker]


def _make_queue_depth_sampler(
    admin_factory: AdminFactory, queues: list[str],
) -> Callable[[], int]:
    """Build a **synchronous** sampler the ProcessManager can call.

    The supervisor has no asyncio loop; our broker API is async. We
    open a throwaway connection each call, sum depth across all
    configured queues, and close. 10s cadence makes this cheap.
    Wrapped in :data:`SAMPLE_TIMEOUT_S` so a stuck broker does not
    hang the supervisor.
    """
    import asyncio

    def sample() -> int:
        async def _run() -> int:
            broker = admin_factory()
            try:
                async with asyncio.timeout(SAMPLE_TIMEOUT_S):
                    await broker.connect()
                    total = 0
                    for q in queues:
                        total += await broker.queue_depth(q)
                    return total
            finally:
                # Close may itself hang under broker failure — bound
                # it separately so the outer timeout does not leak
                # into the next sampler call.
                try:
                    async with asyncio.timeout(1.0):
                        await broker.close()
                except (TimeoutError, Exception):
                    pass

        return asyncio.run(_run())

    return sample
