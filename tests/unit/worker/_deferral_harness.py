"""Zustellung durch die Rückstellungs-Gates mit gestörtem Broker (F4-17, F4-18).

Echter ``Worker`` mit echtem Ausführungsschutz und echter Unique-Semantik
(``InMemoryBackend``); nur der Broker ist ein Mock. ``retry_later`` wirft die
gegebene Ausnahme, ``redeliver`` (das ``nack``) gelingt oder scheitert wie
bei einem toten Kanal mit ``BrokerConnectionError``.

Gates:

- ``rate_limit``: der Limiter verweigert (``retry_after_ms`` 500).
- ``lock``: der Lock ist von einem anderen gehalten (Konflikt, Zurückstellung).
- ``guard``: dieselbe Ausführung läuft anderswo (Ausführungsschutz stellt zurück).
- ``backend_defer``: der Limiter scheitert am Backend-Ausfall (Zurückstellung
  bei Backend-Ausfall aus dem Rate-Limit-Gate).

Weitere Requeue-Pfade im ``try`` der Pipeline (``redeliver_in_place`` ohne
Kopie, F4-18):

- ``rate_limit_error``: der Limiter wirft einen Fehler, der kein Ausfall ist
  (``rate_limit_check_failed``).
- ``lock_error``: das Lock-Acquire wirft einen Fehler, der kein Ausfall ist
  (``lock_acquire_failed``).
- ``guard_error``: der Claim des Ausführungsschutzes bekommt eine
  Fehlerantwort (``task_gate_backend_error``).
- ``shutdown``: der Worker fährt herunter (Requeue im Shutdown-Zweig).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import RateLimitDecision
from swarmq.exceptions import BackendConnectionError, BackendResponseError, BrokerConnectionError
from swarmq.identity import execution_identity
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit._log_capture import capture_swarmq_logs
from tests.unit.worker._helpers import _default_payload

UNIQUE_HASH = "e" * 64
TASK_ID = "t-1"
GATES = ("rate_limit", "lock", "guard", "backend_defer")
REQUEUE_PATHS = ("rate_limit_error", "lock_error", "guard_error", "shutdown")


class RateLimitedTask(Task):
    task_info = TaskInfo(name="deferral_rate_limited", queues=["default"], rate_limit="1/s")

    @override
    async def run(self) -> None:
        return None


class LockedTask(Task):
    task_info = TaskInfo(name="deferral_locked", queues=["default"], lock="resource", hard_timeout=60)

    @override
    async def run(self) -> None:
        return None


class PlainTask(Task):
    task_info = TaskInfo(name="deferral_plain", queues=["default"])

    @override
    async def run(self) -> None:
        return None


_TASKS: dict[str, type[Task]] = {
    "rate_limit": RateLimitedTask,
    "lock": LockedTask,
    "guard": PlainTask,
    "backend_defer": RateLimitedTask,
    "rate_limit_error": RateLimitedTask,
    "lock_error": LockedTask,
    "guard_error": PlainTask,
    "shutdown": PlainTask,
}


@dataclass
class DeferralRun:
    broker: MagicMock
    release_unique: AsyncMock
    flag_owner: str | None
    logs: list[dict[str, Any]] = field(default_factory=list)

    def events(self, level: str) -> list[str]:
        return [e["event"] for e in self.logs if e["log_level"] == level]


def _broker(publish_exc: BaseException | None, nack_exc: BaseException | None) -> MagicMock:
    broker = MagicMock(spec=RabbitMQBroker)
    for name in (
        "connect",
        "declare_queue",
        "consume",
        "ack",
        "dead_letter",
        "route_via_dlx",
        "close",
        "cancel_consume",
    ):
        setattr(broker, name, AsyncMock())
    broker.retry_later = AsyncMock(side_effect=publish_exc)
    broker.redeliver = AsyncMock(side_effect=nack_exc)
    return broker


def channel_lost() -> BrokerConnectionError:
    """Wie ``settle()`` einen Kanalverlust beim ``nack`` meldet (B22)."""
    return BrokerConnectionError("redeliver failed, channel or connection lost: channel closed")


async def run_deferral(
    gate: str,
    unique_until: str,
    *,
    publish_exc: BaseException | None,
    nack_exc: BaseException | None,
) -> DeferralRun:
    """Eine Zustellung mit beanspruchtem Unique-Flag durch *gate*."""
    backend = InMemoryBackend(FakeClock())
    await backend.connect()
    try:
        broker = _broker(publish_exc, nack_exc)
        task = _TASKS[gate]
        registry = TaskRegistry()
        registry.register(task)
        config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", concurrency=2, queues=["default"])
        worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
        worker._guard_store = backend.execution_guard
        if gate == "rate_limit":
            limiter = MagicMock()
            limiter.check = AsyncMock(return_value=RateLimitDecision(allowed=False, remaining=0, retry_after_ms=500))
            worker._rate_limiter = limiter
        elif gate == "backend_defer":
            limiter = MagicMock()
            limiter.check = AsyncMock(side_effect=BackendConnectionError("valkey down"))
            worker._rate_limiter = limiter
        elif gate == "lock":
            locks = MagicMock()
            locks.acquire = AsyncMock(return_value=None)
            locks.current_owner = AsyncMock(return_value="other")
            locks.pttl = AsyncMock(return_value=30_000)
            worker._lock_manager = locks
        elif gate == "rate_limit_error":
            limiter = MagicMock()
            limiter.check = AsyncMock(side_effect=RuntimeError("limiter bug"))
            worker._rate_limiter = limiter
        elif gate == "lock_error":
            locks = MagicMock()
            locks.acquire = AsyncMock(side_effect=RuntimeError("lock store bug"))
            worker._lock_manager = locks
        elif gate == "guard_error":
            backend.execution_guard.claim = AsyncMock(  # type: ignore[method-assign] # Fehlerantwort einspielen
                side_effect=BackendResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
            )
        elif gate == "shutdown":
            worker._shutdown_event.set()
        message = make_message(
            _default_payload(task_id=TASK_ID, name=task.task_info.name, args=[], kwargs={}),
            headers={"x-swarmq-unique-hash": UNIQUE_HASH, "x-swarmq-unique-until": unique_until},
        )
        if gate == "guard":
            await backend.execution_guard.claim(
                execution_identity(message.headers, TASK_ID),
                owner="other",
                retry_count=0,
                ttl_ms=60_000,
                state_ttl_ms=600_000,
                max_orphans=3,
            )
        assert await backend.claim_unique(UNIQUE_HASH, TASK_ID, 600_000) is None
        release_unique = AsyncMock(wraps=backend.release_unique)
        backend.release_unique = release_unique  # type: ignore[method-assign] # Spion auf der echten Freigabe
        with capture_swarmq_logs() as logs:
            await worker._handle_message(message)
        # ``claim_unique`` liefert den Halter, wenn das Flag noch steht.
        owner = await backend.claim_unique(UNIQUE_HASH, "probe", 600_000)
        return DeferralRun(broker=broker, release_unique=release_unique, flag_owner=owner, logs=logs)
    finally:
        await backend.close()
