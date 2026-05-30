"""U2: Valkey backend workflow-state, cancel-flag, and notification ops.

Integration tests against real Valkey. The autouse ``flush_valkey``
fixture clears state between tests; ``check_docker`` skips the module
when Valkey is unreachable.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


@pytest.fixture
async def backend(valkey_url):
    be = ValkeyBackend(valkey_url)
    await be.connect()
    yield be
    await be.close()


def _wf_id() -> str:
    return f"wf-{uuid.uuid4().hex[:12]}"


# --------------------------------------------------------------------------
# Workflow state — R10, R14
# --------------------------------------------------------------------------


async def test_store_and_get_workflow_state_roundtrip(backend):
    wf_id = _wf_id()
    payload = b'{"status": "running", "steps": ["a", "b"]}'
    await backend.store_workflow_state(wf_id, payload, ttl_ms=60_000)
    assert await backend.get_workflow_state(wf_id) == payload


async def test_get_workflow_state_unknown_returns_none(backend):
    # Feeds the WorkflowNotFound mapping in U4.
    assert await backend.get_workflow_state(_wf_id()) is None


async def test_workflow_state_expires(backend):
    wf_id = _wf_id()
    await backend.store_workflow_state(wf_id, b"x", ttl_ms=50)
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        if await backend.get_workflow_state(wf_id) is None:
            break
        await asyncio.sleep(0.02)
    assert await backend.get_workflow_state(wf_id) is None


async def test_store_workflow_state_overwrites(backend):
    wf_id = _wf_id()
    await backend.store_workflow_state(wf_id, b"v1", ttl_ms=60_000)
    await backend.store_workflow_state(wf_id, b"v2", ttl_ms=60_000)
    assert await backend.get_workflow_state(wf_id) == b"v2"


# --------------------------------------------------------------------------
# Workflow-level cancel flag — R17
# --------------------------------------------------------------------------


async def test_set_workflow_cancel_flag_newly_set(backend):
    wf_id = _wf_id()
    assert await backend.set_workflow_cancel_flag(wf_id, ttl_ms=60_000) is True
    assert await backend.is_workflow_cancelled(wf_id) is True


async def test_set_workflow_cancel_flag_idempotent(backend):
    wf_id = _wf_id()
    assert await backend.set_workflow_cancel_flag(wf_id, ttl_ms=60_000) is True
    # Second set is a no-op (NX) — returns False.
    assert await backend.set_workflow_cancel_flag(wf_id, ttl_ms=60_000) is False


async def test_is_workflow_cancelled_false_when_unset(backend):
    assert await backend.is_workflow_cancelled(_wf_id()) is False


# --------------------------------------------------------------------------
# Workflow notification — R16 liveness
# --------------------------------------------------------------------------


async def test_publish_workflow_notification_reaches_subscriber(backend):
    wf_id = _wf_id()
    channel = backend.workflow_notify_channel(wf_id)
    waiter = asyncio.create_task(backend.subscribe_and_wait(channel, timeout=3.0))
    # Give the subscriber a moment to establish before publishing.
    await asyncio.sleep(0.1)
    await backend.publish_workflow_notification(wf_id)
    assert await waiter is True


async def test_publish_workflow_notification_best_effort_when_no_subscriber(backend):
    # No subscriber: publish must not raise (best-effort, like per-task notify).
    await backend.publish_workflow_notification(_wf_id())


async def test_op_timeout_wraps_as_backend_error(backend):
    # A 0-second op_timeout forces asyncio.wait_for to time out, which the
    # method maps to BackendConnectionError (same discipline as store_result).
    with pytest.raises(BackendConnectionError):
        await backend.store_workflow_state(
            _wf_id(), b"x", ttl_ms=1000, op_timeout=0.0
        )
