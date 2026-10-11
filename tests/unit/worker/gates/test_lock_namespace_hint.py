"""Lock-Hinweise nennen den Key mit Namensraum (Plan 0.12.0, MR 15, BZ-10).

Die Diagnose ``recover a wedged lock via valkey-cli DEL <key>`` muss den Key
nennen, unter dem das Lock tatsaechlich liegt; mit ``SWARMQ_NAMESPACE`` ist
das ``<ns>:sq:lock:<name>``.

Test-Liste:
- [x] Wartefrist erreicht: DLQ-Traceback nennt ``app1:sq:lock:resource``
- [x] der Namensraum des Backends gilt vor der Konfiguration (Review MR 15, E1/P1-1)
"""

from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, lock_wait_exceeded_headers

pytestmark = pytest.mark.timeout(5)


class _NsLockedTask(Task):
    task_info = TaskInfo(name="ns_locked", queues=["default"], lock="resource", hard_timeout=60)


async def test_wait_limit_hint_names_the_namespaced_lock_key(broker, backend, config) -> None:
    config = dataclasses.replace(config, namespace="app1")
    reg = TaskRegistry()
    reg.register(_NsLockedTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)
    w._lock_manager = MagicMock()
    w._lock_manager.acquire = AsyncMock(return_value=None)
    msg = make_message(_default_payload(name="ns_locked", args=[], kwargs={}))
    msg.headers = lock_wait_exceeded_headers(config)

    await w._handle_message(msg)

    _, kwargs = broker.dead_letter.call_args
    assert "valkey-cli DEL app1:sq:lock:resource" in kwargs["headers"]["x-swarmq-traceback"]


async def test_hint_uses_the_backend_namespace(broker, backend, config) -> None:
    # Review MR 15, E1/P1-1: der Namensraum des Backends gilt ueberall, auch
    # wenn die Konfiguration (direkt gebauter Worker) keinen traegt.
    backend.namespace = "app9"
    reg = TaskRegistry()
    reg.register(_NsLockedTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)
    w._lock_manager = MagicMock()
    w._lock_manager.acquire = AsyncMock(return_value=None)
    msg = make_message(_default_payload(name="ns_locked", args=[], kwargs={}))
    msg.headers = lock_wait_exceeded_headers(config)

    await w._handle_message(msg)

    _, kwargs = broker.dead_letter.call_args
    assert "valkey-cli DEL app9:sq:lock:resource" in kwargs["headers"]["x-swarmq-traceback"]
