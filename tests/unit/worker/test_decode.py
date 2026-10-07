"""Tests fuer swarmq.worker.decode -- Logging des Dekodierpfads.

Plan 2026-10-06-0831, U9 (R2, R17).
"""

from __future__ import annotations

# Test list:
# 1. Ein nicht dekodierbarer Body loggt ``deserialize_failed`` auf ERROR ueber
#    den Logger ``swarmq.worker.decode`` mit ``error_type`` und ``error``.
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.worker.decode import decode_and_validate
from swarmq.worker.gates import GateDeps

pytestmark = [pytest.mark.timeout(5)]


def _deps() -> GateDeps:
    broker = MagicMock()
    broker.route_via_dlx = AsyncMock()
    return GateDeps(
        broker=broker, backend=MagicMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        signals=MagicMock(dispatch=AsyncMock()), registry=MagicMock(),
        rate_limiter=lambda: None, lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(), maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )


async def test_undecodable_body_logs_deserialize_failed_with_error_fields(swarmq_logs):
    message = MagicMock(body=b'{"task": {"id": "t1"}}', headers={}, queue="q")

    assert await decode_and_validate(_deps(), message) is None

    [entry] = [e for e in swarmq_logs if e["event"] == "deserialize_failed"]
    assert entry["logger"] == "swarmq.worker.decode"
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "KeyError"
    assert entry["error"] == "'queue'"
