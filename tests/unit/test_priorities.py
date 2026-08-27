"""Unit tests for R4.3 Task Priorities (high/normal/low routing)."""

from __future__ import annotations

import pytest

from swarmq.task import TaskInfo, resolve_priority_queue


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
