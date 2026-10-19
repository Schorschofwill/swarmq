"""Zustellung durch die Rückstellungs-Gates mit gestörtem Broker (F4-17, F4-18).

Echter ``Worker`` mit echtem Ausführungsschutz, echter Unique-Semantik und
echtem Rate-Limit- und Lock-Store (``InMemoryBackend``). Der Broker ist ein
``create_autospec`` von ``RabbitMQBroker``: ``retry_later`` wirft die
gegebene Ausnahme, ``redeliver`` (das ``nack``) gelingt oder scheitert wie
bei einem toten Kanal mit ``BrokerConnectionError``. Nur die Fehlerpfade
ersetzen einen Store durch ein ``create_autospec`` seines Vertrags
(``RateLimitStore``, ``LockStore``) bzw. die ``claim``-Methode des echten
Ausführungsschutzes.

Szenarien der Rückstellungs-Gates:

- ``rate_limit``: das Limit ist vorher ausgeschöpft, der echte Limiter
  verweigert.
- ``lock``: der Lock ist im echten Store von einem anderen gehalten
  (Konflikt, Zurückstellung).
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
from unittest.mock import AsyncMock, create_autospec

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockStore, RateLimitStore
from swarmq.exceptions import BackendConnectionError, BackendResponseError, BrokerConnectionError
from swarmq.identity import execution_identity
from swarmq.locking import compute_lock_ttl_ms
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
    broker: Any
    release_unique: AsyncMock
    flag_owner: str | None
    logs: list[dict[str, Any]] = field(default_factory=list)

    def events(self, level: str) -> list[str]:
        return [e["event"] for e in self.logs if e["log_level"] == level]


def _broker(publish_exc: BaseException | None, nack_exc: BaseException | None) -> Any:
    broker = create_autospec(RabbitMQBroker, instance=True)
    broker.retry_later.side_effect = publish_exc
    broker.redeliver.side_effect = nack_exc
    return broker


def channel_lost() -> BrokerConnectionError:
    """Wie ``settle()`` einen Kanalverlust beim ``nack`` meldet (B22)."""
    return BrokerConnectionError("redeliver failed, channel or connection lost: channel closed")


async def run_deferral(
    scenario: str,
    unique_until: str,
    *,
    publish_exc: BaseException | None,
    nack_exc: BaseException | None,
) -> DeferralRun:
    """Eine Zustellung mit beanspruchtem Unique-Flag durch *scenario*."""
    backend = InMemoryBackend(FakeClock())
    await backend.connect()
    try:
        broker = _broker(publish_exc, nack_exc)
        task = _TASKS[scenario]
        registry = TaskRegistry()
        registry.register(task)
        config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", concurrency=2, queues=["default"])
        worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
        # Wie ``Worker.start`` (lifecycle): Stores aus dem Backend.
        worker._guard_store = backend.execution_guard
        worker._rate_limiter = backend.rate_limit
        worker._lock_manager = backend.locks
        if scenario == "rate_limit":
            # ``1/s``: der erste Check verbraucht das Fenster.
            assert (await backend.rate_limit.check(task.task_info.name, max_requests=1, window_ms=1000)).allowed
        elif scenario == "lock":
            ttl_ms = compute_lock_ttl_ms(task.task_info.hard_timeout)
            assert await backend.locks.acquire("resource", owner="other", ttl_ms=ttl_ms, max_concurrency=1)
        elif scenario in ("backend_defer", "rate_limit_error"):
            limiter = create_autospec(RateLimitStore, instance=True)
            limiter.check.side_effect = (
                BackendConnectionError("valkey down") if scenario == "backend_defer" else RuntimeError("limiter bug")
            )
            worker._rate_limiter = limiter
        elif scenario == "lock_error":
            locks = create_autospec(LockStore, instance=True)
            locks.acquire.side_effect = RuntimeError("lock store bug")
            worker._lock_manager = locks
        elif scenario == "guard_error":
            backend.execution_guard.claim = AsyncMock(  # type: ignore[method-assign] # Fehlerantwort einspielen
                side_effect=BackendResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
            )
        elif scenario == "shutdown":
            worker._shutdown_event.set()
        message = make_message(
            _default_payload(task_id=TASK_ID, name=task.task_info.name, args=[], kwargs={}),
            headers={"x-swarmq-unique-hash": UNIQUE_HASH, "x-swarmq-unique-until": unique_until},
        )
        if scenario == "guard":
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
