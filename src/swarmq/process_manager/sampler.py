"""Queue-Tiefen-Sampler (Plan U21): synchroner Sampler über eine
``BrokerAdmin``-Factory aus der CLI — ``process_manager`` importiert
keine Broker-Implementierung mehr (KTD6)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from swarmq.ready_depth import ready_depth_or_none

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

SAMPLE_TIMEOUT_S = 5.0


class SamplerBroker(Protocol):
    """Struktureller Ausschnitt der ``BrokerAdmin``-Factory (U21): der
    Sampler braucht Verbindungsaufbau/-abbau plus die Vertragsmethode
    ``queue_depth`` — keine Broker-Implementierung im Paket (KTD6)."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def queue_depth(self, queue: str) -> int: ...


AdminFactory = Callable[[], SamplerBroker]


async def _ready_depth(broker: SamplerBroker, queue: str) -> int:
    """Der Autoscaler zaehlt nur die Haupt-Queue (Plan 0.12.0, MR 10, F9):
    ``ready_depth``; ohne sie ``queue_depth`` wie bis 0.11 (Q15).

    Scheitert ``ready_depth`` mit einer Ausnahme, folgt ``queue_depth`` als
    zweiter Roundtrip auf derselben Verbindung: bewusst in Kauf genommen,
    der Sampler laeuft alle 10 s und der Rueckfall ist der sichere Wert
    (Review MR 10, Hinweis)."""
    ready = await ready_depth_or_none(broker, queue)
    return ready if ready is not None else await broker.queue_depth(queue)


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
                        total += await _ready_depth(broker, q)
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
