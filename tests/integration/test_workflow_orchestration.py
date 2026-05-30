"""U4: Client.apply persistence + frontier enqueue + reattach + cancel.

Against real RabbitMQ + Valkey. Full advancement (chain progressing,
chord firing) is covered by the worker units (U5/U6) e2e tests — here we
verify the apply-time contract: state persisted, initial frontier
published with workflow headers, reattach, and workflow-level cancel.
"""

from __future__ import annotations

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.exceptions import WorkflowNotFound
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group
from swarmq.workflow_orchestration import STATUS_RUNNING

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

_QUEUE = "wf_apply_test"


class WA(Task):
    task_info = TaskInfo(name="wa", queues=[_QUEUE])

    async def run(self, x: str = "") -> str:
        return f"a:{x}"


class WB(Task):
    task_info = TaskInfo(name="wb", queues=[_QUEUE])

    async def run(self, x: str = "") -> str:
        return f"b:{x}"


@pytest.fixture
async def client(rabbitmq_url, valkey_url):
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    await broker.declare_queue(_QUEUE)
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    registry = TaskRegistry()
    registry.register(WA)
    registry.register(WB)
    c = Client(broker, backend, registry=registry)
    yield c, broker, backend
    await broker.close()
    await backend.close()


async def test_apply_persists_state_and_topology(client):
    c, _broker, backend = client
    handle = await c.apply(chain(WA.s("x"), WB.s()))
    raw = await backend.get_workflow_state(handle.workflow_id)
    assert raw is not None
    state = decode_json(raw)
    assert state["status"] == STATUS_RUNNING
    assert state["topology"]["kind"] == "chain"
    # Chain frontier is the first step only.
    assert list(state["step_tasks"].keys()) == ["/0"]


async def test_apply_chain_enqueues_only_first_step(client):
    c, broker, _backend = client
    before = await broker.get_queue_depth(_QUEUE)
    await c.apply(chain(WA.s("x"), WB.s()))
    after = await broker.get_queue_depth(_QUEUE)
    assert after - before == 1


async def test_apply_group_enqueues_all_members(client):
    c, broker, _backend = client
    before = await broker.get_queue_depth(_QUEUE)
    await c.apply(group(WA.s("a"), WA.s("b"), WB.s("c")))
    after = await broker.get_queue_depth(_QUEUE)
    assert after - before == 3


async def test_apply_chord_enqueues_header_members_only(client):
    c, broker, _backend = client
    before = await broker.get_queue_depth(_QUEUE)
    await c.apply(chord(group(WA.s("a"), WA.s("b")), WB.s()))
    after = await broker.get_queue_depth(_QUEUE)
    # Header members enqueued; body waits for fan-in (U5).
    assert after - before == 2


async def test_reattach_returns_handle(client):
    c, _broker, _backend = client
    handle = await c.apply(chain(WA.s("x"), WB.s()))
    reattached = await c.workflow(handle.workflow_id)
    assert reattached.workflow_id == handle.workflow_id
    assert await reattached.status() == STATUS_RUNNING


async def test_reattach_unknown_raises(client):
    c, _broker, _backend = client
    with pytest.raises(WorkflowNotFound):
        await c.workflow("wf-does-not-exist")


async def test_cancel_sets_workflow_flag(client):
    c, _broker, backend = client
    handle = await c.apply(chain(WA.s("x"), WB.s()))
    assert await backend.is_workflow_cancelled(handle.workflow_id) is False
    assert await handle.cancel() is True
    assert await backend.is_workflow_cancelled(handle.workflow_id) is True
