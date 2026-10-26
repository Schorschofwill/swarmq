"""Nachweis DP-01, Teil 2 im Gate: Die Rückstellungs-Kopie einer abgewiesenen
Nachricht wartet ``retry_after_ms`` plus Jitter.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-01). Die Streuung selbst
prüft ``tests/unit/test_befund_dp_01_rate_limit_jitter.py``; hier geht es
darum, dass das Gate sie mit dem Zähler der Nachricht anwendet und das Signal
weiter das unveränderte ``retry_after_ms`` meldet.

Testliste:
# [x] 10/m, erste Abweisung, Zufall oben: delay_ms = 2500 + 2 · 6000
# [x] zweite Abweisung (Zähler 1 im Header): Streuung 4 · 6000
"""

from __future__ import annotations

import random
from typing import override
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.contracts.coordination import RateLimitDecision
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = [pytest.mark.timeout(5), pytest.mark.befund("DP-01")]


class _Limited(Task):
    task_info = TaskInfo(name="dp01_limited", queues=["default"], rate_limit="10/m")

    @override
    async def run(self) -> None:
        raise AssertionError("must not run while rate-limited")


@pytest.fixture
def _jitter_at_the_top(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(random, "uniform", lambda lo, hi: hi)


@pytest.mark.usefixtures("_jitter_at_the_top")
@pytest.mark.parametrize(("previous_bounces", "spread_ms"), [(0, 12_000), (1, 24_000)])
async def test_the_deferral_copy_waits_retry_after_plus_jitter(
    broker, backend, config, previous_bounces: int, spread_ms: int
) -> None:
    registry = TaskRegistry()
    registry.register(_Limited)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._rate_limiter = MagicMock()
    worker._rate_limiter.check = AsyncMock(
        return_value=RateLimitDecision(allowed=False, remaining=0, retry_after_ms=2500)
    )
    headers = {"x-swarmq-rate-limit-count": previous_bounces} if previous_bounces else None

    with patch.object(_Limited, "run", AsyncMock()):
        await worker._handle_message(
            make_message(_default_payload(name="dp01_limited", args=[], kwargs={}), headers=headers)
        )

    _, kwargs = broker.retry_later.call_args
    assert kwargs["delay_ms"] == 2500 + spread_ms
