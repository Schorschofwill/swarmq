"""Tests for swarmq.task — Task base class and TaskInfo."""

import pytest

from swarmq.task import DeliveryMode, Task, TaskInfo


def test_taskinfo_with_all_fields():
    info = TaskInfo(
        name="my.task",
        queues=["default", "high"],
        soft_timeout=30,
        hard_timeout=60,
        delivery=DeliveryMode.FIRE_AND_FORGET,
        result_ttl=3600,
    )
    assert info.name == "my.task"
    assert info.queues == ("default", "high")
    assert info.soft_timeout == 30
    assert info.hard_timeout == 60
    assert info.delivery == DeliveryMode.FIRE_AND_FORGET
    assert info.result_ttl == 3600


def test_taskinfo_with_only_required_fields():
    info = TaskInfo(name="simple", queues=["default"])
    assert info.name == "simple"
    assert info.queues == ("default",)
    assert info.soft_timeout is None
    assert info.hard_timeout is None
    assert info.delivery == DeliveryMode.AT_LEAST_ONCE
    assert info.result_ttl is None


def test_delivery_mode_values():
    assert DeliveryMode.AT_LEAST_ONCE.value == "at_least_once"
    assert DeliveryMode.FIRE_AND_FORGET.value == "fire_and_forget"


def test_taskinfo_empty_name_raises():
    with pytest.raises(ValueError, match="name"):
        TaskInfo(name="", queues=["default"])


def test_taskinfo_name_too_long_raises():
    with pytest.raises(ValueError, match="name"):
        TaskInfo(name="x" * 256, queues=["default"])


def test_taskinfo_empty_queues_raises():
    with pytest.raises(ValueError, match="queues"):
        TaskInfo(name="task", queues=[])


def test_taskinfo_hard_timeout_less_than_soft_raises():
    with pytest.raises(ValueError, match="hard_timeout"):
        TaskInfo(name="task", queues=["q"], soft_timeout=60, hard_timeout=30)


def test_taskinfo_is_frozen():
    info = TaskInfo(name="task", queues=["q"])
    with pytest.raises(AttributeError):
        info.name = "other"


async def test_task_subclass_with_run():
    class AddTask(Task):
        task_info = TaskInfo(name="add", queues=["math"])

        async def run(self, a, b):
            return a + b

    t = AddTask()
    assert t.task_info.name == "add"
    assert await t.run(1, 2) == 3


async def test_base_task_run_raises_not_implemented():
    class BareTask(Task):
        task_info = TaskInfo(name="bare", queues=["q"])

    t = BareTask()
    with pytest.raises(NotImplementedError):
        await t.run()


async def test_pre_and_post_execution_are_async_nops():
    class SimpleTask(Task):
        task_info = TaskInfo(name="simple", queues=["q"])

        async def run(self):
            pass

    t = SimpleTask()
    result_pre = await t.pre_execution()
    result_post = await t.post_execution()
    assert result_pre is None
    assert result_post is None


def test_auto_registration_of_task_subclass():
    Task._clear_registered_tasks()

    class RegTask(Task):
        task_info = TaskInfo(name="reg", queues=["q"])

        async def run(self):
            pass

    assert RegTask in Task._registered_tasks
    assert len(Task._registered_tasks) == 1


def test_multiple_subclasses_all_registered():
    Task._clear_registered_tasks()

    class TaskA(Task):
        task_info = TaskInfo(name="a", queues=["q"])

        async def run(self):
            pass

    class TaskB(Task):
        task_info = TaskInfo(name="b", queues=["q"])

        async def run(self):
            pass

    assert TaskA in Task._registered_tasks
    assert TaskB in Task._registered_tasks
    assert len(Task._registered_tasks) == 2


def test_abstract_intermediate_not_registered():
    Task._clear_registered_tasks()

    class BaseWorker(Task):
        """Intermediate class without task_info — should NOT register."""

        async def run(self):
            pass

    class ConcreteWorker(BaseWorker):
        task_info = TaskInfo(name="concrete", queues=["q"])

    assert BaseWorker not in Task._registered_tasks
    assert ConcreteWorker in Task._registered_tasks
    assert len(Task._registered_tasks) == 1


def test_clear_registered_tasks_resets():
    # Ensure there's something registered first.
    class TempTask(Task):
        task_info = TaskInfo(name="temp", queues=["q"])

        async def run(self):
            pass

    assert len(Task._registered_tasks) >= 1
    Task._clear_registered_tasks()
    assert Task._registered_tasks == []


# ---------------------------------------------------------------------------
# Retry fields (Unit 2.2)
# ---------------------------------------------------------------------------


def test_taskinfo_retry_fields_defaults():
    """TaskInfo retry fields have correct defaults."""
    info = TaskInfo(name="task", queues=["q"])
    assert info.max_retries == 0
    assert info.retry_backoff == "exponential"
    assert info.retry_exceptions is None
    assert info.retry_base_delay == 1.0


def test_taskinfo_retry_fields_explicit():
    """TaskInfo retry fields can be set explicitly."""
    info = TaskInfo(
        name="task",
        queues=["q"],
        max_retries=5,
        retry_backoff="linear",
        retry_exceptions=[ValueError, ConnectionError],
        retry_base_delay=2.5,
    )
    assert info.max_retries == 5
    assert info.retry_backoff == "linear"
    assert info.retry_exceptions == (ValueError, ConnectionError)
    assert info.retry_base_delay == 2.5


def test_taskinfo_invalid_retry_backoff_raises():
    """Invalid retry_backoff strategy raises ValueError."""
    with pytest.raises(ValueError, match="retry_backoff"):
        TaskInfo(name="task", queues=["q"], retry_backoff="invalid")
