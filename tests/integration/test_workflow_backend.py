"""U2: Valkey backend workflow-state, cancel-flag, and notification ops.

Integration tests against real Valkey. The autouse ``flush_valkey``
fixture clears state between tests; ``check_docker`` skips the module
when Valkey is unreachable.
"""

from __future__ import annotations

import asyncio
import contextlib
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


async def test_notify_workflow_reaches_subscriber(backend):
    wf_id = _wf_id()
    channel = backend.workflow_notify_channel(wf_id)
    subscribed = asyncio.Event()

    async def _subscribe_and_wait() -> bool:
        pubsub = backend._pubsub()  # rohes Pub/Sub ist seit U15 nicht mehr public
        try:
            await pubsub.subscribe(channel)
            subscribed.set()  # signal readiness instead of a sleep
            deadline = asyncio.get_running_loop().time() + 3.0
            while asyncio.get_running_loop().time() < deadline:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.05,
                )
                if msg is not None:
                    return True
            return False
        finally:
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()

    waiter = asyncio.create_task(_subscribe_and_wait())
    # Wait for the subscriber to be fully established before publishing.
    await subscribed.wait()
    await backend.notify_workflow(wf_id)
    assert await waiter is True


async def test_notify_workflow_best_effort_when_no_subscriber(backend):
    # No subscriber: publish must not raise (best-effort, like per-task notify).
    await backend.notify_workflow(_wf_id())


async def test_op_timeout_wraps_as_backend_error(backend):
    # A 0-second op_timeout forces asyncio.wait_for to time out, which the
    # method maps to BackendConnectionError (same discipline as store_result).
    with pytest.raises(BackendConnectionError):
        await backend.store_workflow_state(
            _wf_id(), b"x", ttl_ms=1000, op_timeout=0.0
        )


# --------------------------------------------------------------------------
# B10 — verschachtelte Topologie im Zustand (Plan 2026-08-30-002, U4)
# --------------------------------------------------------------------------


async def test_nested_topology_state_roundtrips_unchanged(backend):
    """Die Korrektur leitet die Gliedkennung aus der Topologie ab — also muss
    die Topologie den Weg durch Serialisierung und Speicher unveraendert
    ueberstehen, auch mit zusammengesetzten Gliedern.

    Prueft zugleich R9: das Zustandsdokument traegt weiterhin genau die Felder
    aus ``build_initial_workflow_state``, kein zusaetzliches Aggregat-Feld.
    Ein Entwurf mit eigenem Zustandseintrag haette hier ein weiteres Feld
    hinterlassen — und die Golden-Fixture gebrochen.
    """
    from swarmq.serialization import decode_json, encode_json
    from swarmq.task import Task, TaskInfo
    from swarmq.workflow import chain, chord, group
    from swarmq.workflow_orchestration import build_initial_workflow_state

    class Noop(Task):
        task_info = TaskInfo(name="noop_nested", queues=["default"])

        async def run(self, *args) -> None:
            return None

    wf_id = _wf_id()
    topology = group(
        Noop.s("a"),
        chain(Noop.s("b"), Noop.s("c")),
        group(Noop.s("d"), chain(Noop.s("e"), Noop.s("f"))),
        chord(group(Noop.s("g")), Noop.s()),
    )
    state = build_initial_workflow_state(wf_id, topology.to_dict())
    assert set(state) == {
        "v",
        "workflow_id",
        "status",
        "topology",
        "final_result_task_id",
        "error",
        "step_tasks",
    }

    await backend.store_workflow_state(wf_id, encode_json(state), ttl_ms=60_000)
    raw = await backend.get_workflow_state(wf_id)
    assert raw is not None
    assert decode_json(raw)["topology"] == topology.to_dict()
