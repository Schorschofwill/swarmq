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


def test_taskinfo_rate_limit_default_is_none():
    """Existing tasks without rate_limit keep rate_limit=None."""
    info = TaskInfo(name="task", queues=["q"])
    assert info.rate_limit is None


def test_taskinfo_rate_limit_accepts_valid_string():
    """Valid Celery-style rate_limit strings are accepted verbatim."""
    info = TaskInfo(name="task", queues=["q"], rate_limit="100/m")
    assert info.rate_limit == "100/m"


def test_taskinfo_rate_limit_rejects_unsupported_unit():
    """Invalid rate_limit fails loud at construction, not at execution."""
    with pytest.raises(ValueError, match="unsupported unit"):
        TaskInfo(name="task", queues=["q"], rate_limit="100/day")


def test_taskinfo_rate_limit_rejects_missing_separator():
    with pytest.raises(ValueError, match="missing unit separator"):
        TaskInfo(name="task", queues=["q"], rate_limit="100")


def test_taskinfo_rate_limit_rejects_zero_count():
    with pytest.raises(ValueError, match="count must be >= 1"):
        TaskInfo(name="task", queues=["q"], rate_limit="0/m")


# ---------------------------------------------------------------------------
# Task locking fields (R3.4 / Phase 3b.2)
# ---------------------------------------------------------------------------


def test_taskinfo_lock_default_is_none():
    info = TaskInfo(name="task", queues=["q"])
    assert info.lock is None
    assert info.max_concurrency == 1


def test_taskinfo_lock_with_hard_timeout_accepted():
    info = TaskInfo(name="task", queues=["q"], lock="resource", hard_timeout=60)
    assert info.lock == "resource"
    assert info.max_concurrency == 1


def test_taskinfo_lock_semaphore_with_hard_timeout_accepted():
    info = TaskInfo(
        name="task", queues=["q"], lock="resource", max_concurrency=3, hard_timeout=60
    )
    assert info.lock == "resource"
    assert info.max_concurrency == 3


def test_taskinfo_lock_without_hard_timeout_raises():
    with pytest.raises(ValueError, match="lock requires hard_timeout"):
        TaskInfo(name="task", queues=["q"], lock="resource")


def test_taskinfo_max_concurrency_without_lock_raises():
    with pytest.raises(ValueError, match="max_concurrency requires lock"):
        TaskInfo(name="task", queues=["q"], max_concurrency=3)


def test_taskinfo_max_concurrency_zero_raises():
    with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
        TaskInfo(
            name="task", queues=["q"], lock="resource", max_concurrency=0,
            hard_timeout=60,
        )


def test_taskinfo_max_concurrency_negative_raises():
    with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
        TaskInfo(
            name="task", queues=["q"], lock="resource", max_concurrency=-1,
            hard_timeout=60,
        )


def test_taskinfo_format_string_lock_accepted_without_warning():
    """Format-string locks are rendered at acquire-time via inspect.signature.
    No warning at TaskInfo construction — render-failures surface in the
    worker as DLQ entries with a clear error header."""
    info = TaskInfo(
        name="task", queues=["q"], lock="user-{user_id}", hard_timeout=60
    )
    assert info.lock == "user-{user_id}"


def test_taskinfo_empty_lock_string_rejected():
    """lock="" would render to the bare 'sq:lock:' Valkey key and silently
    collide with every other empty-locked task. Reject at construction."""
    with pytest.raises(ValueError, match="lock must not be empty"):
        TaskInfo(name="task", queues=["q"], lock="", hard_timeout=60)


def test_taskinfo_whitespace_only_lock_rejected():
    """Same trap as empty: whitespace-only locks render to a key that's
    indistinguishable in operator commands from the namespace prefix."""
    with pytest.raises(ValueError, match="lock must not be empty"):
        TaskInfo(name="task", queues=["q"], lock="   ", hard_timeout=60)


# ---------------------------------------------------------------------------
# Cron + catchup (Unit 3)
# ---------------------------------------------------------------------------


def test_taskinfo_cron_default_none():
    info = TaskInfo(name="task", queues=["q"])
    assert info.cron is None
    assert info.catchup is False


def test_taskinfo_cron_5_field_standard_accepted():
    info = TaskInfo(name="task", queues=["q"], cron="*/5 * * * *")
    assert info.cron == "*/5 * * * *"


def test_taskinfo_cron_daily_at_3am_accepted():
    info = TaskInfo(name="task", queues=["q"], cron="0 3 * * *")
    assert info.cron == "0 3 * * *"


def test_taskinfo_cron_every_minute_accepted():
    info = TaskInfo(name="task", queues=["q"], cron="* * * * *")
    assert info.cron == "* * * * *"


def test_taskinfo_cron_invalid_syntax_raises():
    with pytest.raises(ValueError, match="cron"):
        TaskInfo(name="task", queues=["q"], cron="not-a-cron")


def test_taskinfo_cron_extended_L_syntax_rejected():
    """Extended cron syntax (L, W, #) is rejected at construction."""
    with pytest.raises(ValueError, match="cron"):
        TaskInfo(name="task", queues=["q"], cron="0 3 L * *")


def test_taskinfo_cron_extended_W_syntax_rejected():
    with pytest.raises(ValueError, match="cron"):
        TaskInfo(name="task", queues=["q"], cron="0 3 15W * *")


def test_taskinfo_cron_extended_hash_syntax_rejected():
    with pytest.raises(ValueError, match="cron"):
        TaskInfo(name="task", queues=["q"], cron="0 3 * * 1#2")


def test_taskinfo_catchup_true_raises_not_implemented():
    """catchup=True is reserved for a future release; fail fast at
    construction (Review-Finding P2-3)."""
    with pytest.raises(NotImplementedError, match="catchup"):
        TaskInfo(name="task", queues=["q"], cron="* * * * *", catchup=True)


def test_taskinfo_catchup_true_without_cron_also_raises():
    """catchup=True is rejected regardless of cron (reserved keyword)."""
    with pytest.raises(NotImplementedError, match="catchup"):
        TaskInfo(name="task", queues=["q"], catchup=True)


def test_taskinfo_catchup_false_with_cron_accepted():
    info = TaskInfo(name="task", queues=["q"], cron="* * * * *", catchup=False)
    assert info.catchup is False
    assert info.cron == "* * * * *"


# ---------------------------------------------------------------------------
# Task registration delay/eta signature-guard (P1-3)
# ---------------------------------------------------------------------------


def test_task_subclass_with_delay_parameter_raises():
    """Task.run accepting 'delay' as a parameter would silently lose its
    payload once Client.schedule(delay=) is called. Guard at registration."""
    Task._clear_registered_tasks()
    with pytest.raises(ValueError, match="delay"):
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, delay):  # reserved name
                return delay


def test_task_subclass_with_eta_parameter_raises():
    Task._clear_registered_tasks()
    with pytest.raises(ValueError, match="eta"):
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, eta):
                return eta


def test_task_subclass_with_delay_in_kwargs_raises():
    """Keyword-only delay parameter is equally a conflict."""
    Task._clear_registered_tasks()
    with pytest.raises(ValueError, match="delay"):
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, *, delay=None):
                return delay


def test_task_subclass_with_non_reserved_params_still_registers():
    """Baseline — non-reserved names pass the guard."""
    Task._clear_registered_tasks()

    class OkTask(Task):
        task_info = TaskInfo(name="ok", queues=["q"])

        async def run(self, a, b, c):
            return a + b + c

    assert OkTask in Task._registered_tasks


def test_task_subclass_without_run_method_allowed():
    """A subclass without its own run() inherits the base — no signature
    to inspect, no conflict possible."""
    Task._clear_registered_tasks()

    class BareTask(Task):
        task_info = TaskInfo(name="bare", queues=["q"])

    assert BareTask in Task._registered_tasks
