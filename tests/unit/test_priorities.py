"""Unit tests for R4.3 Task Priorities (high/normal/low routing)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.registry import TaskRegistry
from swarmq.signals import SignalDispatcher
from swarmq.task import Task, TaskInfo, resolve_priority_queue


class TestPriorityQueueResolver:
    def test_normal_passes_through(self):
        assert resolve_priority_queue("tasks", "normal") == "tasks"

    def test_high_appends_suffix(self):
        assert resolve_priority_queue("tasks", "high") == "tasks.high"

    def test_low_appends_suffix(self):
        assert resolve_priority_queue("orders", "low") == "orders.low"


class TestTaskInfoPriority:
    def test_default_is_normal(self):
        ti = TaskInfo(name="t", queues=["q"])
        assert ti.priority == "normal"

    def test_explicit_high(self):
        ti = TaskInfo(name="t", queues=["q"], priority="high")
        assert ti.priority == "high"

    def test_explicit_low(self):
        ti = TaskInfo(name="t", queues=["q"], priority="low")
        assert ti.priority == "low"

    def test_invalid_rejected(self):
        with pytest.raises(ValueError, match="priority must be"):
            TaskInfo(name="t", queues=["q"], priority="urgent")  # type: ignore[arg-type]


class TestClientPriorityRouting:
    async def _make_client_with_task(self, priority: str):
        Task._clear_registered_tasks()

        class P(Task):
            task_info = TaskInfo(
                name="p_task", queues=["work"], priority=priority,  # type: ignore[arg-type]
            )

            async def run(self) -> str:
                return "ok"

        broker = AsyncMock()
        broker.publish = AsyncMock()
        backend = AsyncMock()
        registry = TaskRegistry()
        registry.register(P)
        client = Client(
            broker=broker, backend=backend, registry=registry,
            signal_dispatcher=SignalDispatcher(),
        )
        return client, broker

    async def test_normal_priority_publishes_to_base_queue(self):
        client, broker = await self._make_client_with_task("normal")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work"
        finally:
            Task._clear_registered_tasks()

    async def test_high_priority_publishes_to_high_suffix(self):
        client, broker = await self._make_client_with_task("high")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work.high"
        finally:
            Task._clear_registered_tasks()

    async def test_low_priority_publishes_to_low_suffix(self):
        client, broker = await self._make_client_with_task("low")
        try:
            await client.schedule("p_task")
            call = broker.publish.await_args
            assert call.args[0] == "work.low"
        finally:
            Task._clear_registered_tasks()
