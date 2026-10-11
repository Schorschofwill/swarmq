"""Worker-Start bei verdraengender Valkey-Policy (Plan 0.12.0, MR 14, BZ-04).

Das Backend prueft die Policy beim Verbinden und legt das Ergebnis ab
(``eviction_check``); der Worker entscheidet nach
``SWARMQ_VALKEY_EVICTION_CHECK``, bevor er Scheduler und Koordination
verdrahtet. Grenze gemockt: Broker und Backend.

Test-Liste:
- [x] ``strict`` + unsichere Policy -> ``ConfigurationError`` (Exit 78), kein Scheduler
- [x] ``warn`` + unsichere Policy -> Start laeuft weiter
- [x] ``strict`` + nicht pruefbar -> Start laeuft weiter (nur Warnung des Backends)
- [x] ``strict`` + sichere Policy -> Start laeuft weiter
- [x] Backend ohne ``eviction_check`` (Fremd-Backend, ``MagicMock``) -> Start laeuft weiter (Q15)
- [x] ``off`` + unsichere Policy -> Start laeuft weiter
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError
from swarmq.registry import TaskRegistry
from swarmq.valkey_policy import EvictionCheck
from swarmq.worker import Worker
from swarmq.worker.eviction_gate import enforce_eviction_policy

pytestmark = pytest.mark.timeout(5)


def _broker() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.close = AsyncMock()
    return b


def _backend(check: EvictionCheck | None) -> MagicMock:
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.close = AsyncMock()
    be.eviction_check = check
    return be


def _worker(backend: MagicMock, mode: str) -> Worker:
    config = SwarmQConfig(broker_url="amqp://localhost", backend_url="valkey://localhost", valkey_eviction_check=mode)
    return Worker(broker=_broker(), backend=backend, registry=TaskRegistry(), config=config)


async def test_strict_refuses_to_start_under_an_evicting_policy() -> None:
    worker = _worker(_backend(EvictionCheck(status="unsafe", policy="volatile-lru", maxmemory=1024)), "strict")

    with pytest.raises(ConfigurationError, match="volatile-lru"):
        await worker.start()

    assert worker._scheduler is None


_UNSAFE = EvictionCheck(status="unsafe", policy="allkeys-lru", maxmemory=None)


@pytest.mark.parametrize(
    ("check", "mode"),
    [
        (_UNSAFE, "warn"),
        (_UNSAFE, "off"),
        (EvictionCheck(status="unknown"), "strict"),
        (EvictionCheck(status="safe", policy="noeviction", maxmemory=0), "strict"),
        (None, "strict"),
    ],
    ids=["warn", "off", "unknown", "safe", "unchecked"],
)
def test_start_goes_on_unless_strict_meets_a_proven_evicting_policy(check: EvictionCheck | None, mode: str) -> None:
    enforce_eviction_policy(_backend(check), mode)


def test_backend_without_check_result_does_not_block() -> None:
    """Q15: ein ``MagicMock`` liefert fuer jedes Attribut einen Mock, kein Ergebnis."""
    enforce_eviction_policy(MagicMock(), "strict")


def test_unknown_maxmemory_counts_as_unsafe_and_says_so() -> None:
    with pytest.raises(ConfigurationError, match="maxmemory unknown"):
        enforce_eviction_policy(_backend(_UNSAFE), "strict")
