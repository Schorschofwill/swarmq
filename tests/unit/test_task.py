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


def test_taskinfo_catchup_true_raises_value_error():
    """catchup=True is reserved for a future release; fail fast at
    construction (Review-Finding P2-3). ValueError (not
    NotImplementedError) — the field is a reserved API value, not
    an abstract-method stub (api-contract AC-03)."""
    with pytest.raises(ValueError, match="catchup"):
        TaskInfo(name="task", queues=["q"], cron="* * * * *", catchup=True)


def test_taskinfo_catchup_true_without_cron_also_raises():
    """catchup=True is rejected regardless of cron (reserved keyword)."""
    with pytest.raises(ValueError, match="catchup"):
        TaskInfo(name="task", queues=["q"], catchup=True)


def test_taskinfo_catchup_false_with_cron_accepted():
    info = TaskInfo(name="task", queues=["q"], cron="* * * * *", catchup=False)
    assert info.catchup is False
    assert info.cron == "* * * * *"


# ---------------------------------------------------------------------------
# Cron-Zeitzone (Plan 2026-10-08-1541, U1, R6, KTD1)
#
# Test-Liste:
# - cron mit IANA-Zone wird angenommen, Zone bleibt erhalten
# - ohne Angabe ist die Zone None
# - unbekannte Zone -> ValueError mit dem Namen
# - timezone ohne cron -> ValueError
# - "UTC" und "Etc/UTC" werden zu None normalisiert
# - leerer String -> ValueError (kein stilles UTC)
# ---------------------------------------------------------------------------


def test_taskinfo_cron_with_timezone_accepted():
    info = TaskInfo(
        name="task", queues=["q"], cron="0 3 * * *", timezone="Europe/Berlin"
    )
    assert info.timezone == "Europe/Berlin"


def test_taskinfo_unknown_timezone_raises_with_name():
    with pytest.raises(ValueError, match="Europe/Atlantis"):
        TaskInfo(
            name="task", queues=["q"], cron="0 3 * * *", timezone="Europe/Atlantis"
        )


def test_taskinfo_malformed_timezone_key_raises():
    """Leerer oder pfadartiger Schlüssel ist kein stilles UTC."""
    for key in ("", "../etc/passwd"):
        with pytest.raises(ValueError, match="timezone"):
            TaskInfo(name="task", queues=["q"], cron="0 3 * * *", timezone=key)


def test_taskinfo_timezone_without_cron_raises():
    with pytest.raises(ValueError, match="timezone requires cron"):
        TaskInfo(name="task", queues=["q"], timezone="Europe/Berlin")


def test_taskinfo_utc_timezone_normalized_to_none():
    """KTD1: "UTC", "Etc/UTC" und keine Angabe sind gleichwertig; der
    Fingerabdruck einer gespeicherten Definition ändert sich beim
    Upgrade nicht."""
    for key in ("UTC", "Etc/UTC", None):
        info = TaskInfo(name="task", queues=["q"], cron="0 3 * * *", timezone=key)
        assert info.timezone is None


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


# -----------------------------------------------------------------------
# R3.5 Unique Tasks — TaskInfo.unique_for / unique_until validation
# -----------------------------------------------------------------------


def test_taskinfo_unique_defaults_are_none_and_completion():
    t = TaskInfo(name="t", queues=["q"])
    assert t.unique_for is None
    assert t.unique_until == "completion"


def test_taskinfo_unique_for_positive_accepted():
    t = TaskInfo(name="t", queues=["q"], unique_for=60)
    assert t.unique_for == 60


def test_taskinfo_unique_until_start_accepted():
    t = TaskInfo(name="t", queues=["q"], unique_for=60, unique_until="start")
    assert t.unique_until == "start"


def test_taskinfo_unique_for_zero_raises():
    """origin CD6: unique_for=0 is a fail-fast, not silent normalize to None."""
    with pytest.raises(ValueError, match="unique_for=0"):
        TaskInfo(name="t", queues=["q"], unique_for=0)


def test_taskinfo_unique_for_negative_raises():
    with pytest.raises(ValueError, match="unique_for must be > 0"):
        TaskInfo(name="t", queues=["q"], unique_for=-5)


def test_taskinfo_unique_until_invalid_literal_raises():
    with pytest.raises(ValueError, match="unique_until must be"):
        TaskInfo(
            name="t",
            queues=["q"],
            unique_for=60,
            unique_until="invalid",  # type: ignore[arg-type]
        )


def test_taskinfo_unique_until_without_unique_for_is_accepted():
    """origin CD6: unique_until set without unique_for does NOT raise;
    it's silently ignored (dedup inactive). Avoids sentinel discrimination
    on the frozen dataclass."""
    t = TaskInfo(name="t", queues=["q"], unique_until="start")
    assert t.unique_for is None
    assert t.unique_until == "start"  # preserved but unused


def test_task_subclass_reserved_unique_for_raises():
    Task._clear_registered_tasks()

    with pytest.raises(ValueError, match="unique_for"):
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, *, unique_for=None):
                return unique_for


def test_task_subclass_reserved_unique_until_raises():
    Task._clear_registered_tasks()

    with pytest.raises(ValueError, match="unique_until"):
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, *, unique_until=None):
                return unique_until


def test_task_subclass_reserved_param_error_mentions_all_reserved():
    """Migration hint in error message includes all four reserved names
    so users who hit delay/eta today still see the full picture."""
    Task._clear_registered_tasks()

    with pytest.raises(ValueError) as excinfo:
        class BadTask(Task):
            task_info = TaskInfo(name="bad", queues=["q"])

            async def run(self, *, delay=None):
                return delay

    msg = str(excinfo.value)
    assert "unique_for" in msg
    assert "unique_until" in msg


# ---------------------------------------------------------------------------
# R3.7 — Task.update_progress
# ---------------------------------------------------------------------------


class TestUpdateProgress:
    """Direct-call semantics of Task.update_progress (R3.7 Unit 3)."""

    @pytest.mark.timeout(5)
    async def test_noop_without_worker_wiring(self):
        """No backend / no task_id → silently skip. Keeps unit-test
        call-sites portable with direct instantiation."""
        t = Task()
        # Should not raise.
        await t.update_progress(5, 10, "halfway")

    @pytest.mark.timeout(5)
    async def test_writes_hash_and_publishes_on_first_call(self):
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "abc-1"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic() - 2.0
        t._result_ttl_ms = 60_000

        await t.update_progress(3, 10, "three of ten")

        t._backend.set_progress.assert_awaited_once()
        call_args = t._backend.set_progress.await_args
        assert call_args.args[0] == "abc-1"
        fields = call_args.args[1]
        assert fields["current"] == "3"
        assert fields["total"] == "10"
        assert fields["percent"] == "30.00"
        assert fields["message"] == "three of ten"
        # ETA sanity: 2s elapsed at 3/10 → ~4.67s remaining.
        assert int(fields["eta_seconds"]) in {4, 5}
        t._backend.publish_progress.assert_awaited_once()

    @pytest.mark.timeout(5)
    async def test_throttles_second_call_within_window(self):
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "abc-1"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic()

        await t.update_progress(1, 100)
        await t.update_progress(2, 100)  # within 500ms → skipped

        assert t._backend.set_progress.await_count == 1

    @pytest.mark.timeout(5)
    async def test_final_update_always_flushes(self):
        """current == total bypasses throttling (R3.7 R4 — force-flush)."""
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "abc-1"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic()

        await t.update_progress(1, 10)
        await t.update_progress(10, 10, "done")  # final → must flush

        assert t._backend.set_progress.await_count == 2
        last_fields = t._backend.set_progress.await_args.args[1]
        assert last_fields["current"] == "10"
        assert last_fields["percent"] == "100.00"

    @pytest.mark.timeout(5)
    async def test_zero_total_noop(self):
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        await t.update_progress(5, 0)
        t._backend.set_progress.assert_not_awaited()

    @pytest.mark.timeout(5)
    async def test_negative_current_noop(self):
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        await t.update_progress(-1, 10)
        t._backend.set_progress.assert_not_awaited()

    @pytest.mark.timeout(5)
    async def test_first_update_eta_is_empty_when_current_zero(self):
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic()

        await t.update_progress(0, 10, "starting")
        fields = t._backend.set_progress.await_args.args[1]
        assert fields["eta_seconds"] == ""

    @pytest.mark.timeout(5)
    async def test_backend_hset_failure_swallowed(self, swarmq_logs):
        """Valkey blip must NOT crash the task body (R3.7 best-effort contract).

        Plan 2026-10-06-0831, U9: geloggt ueber ``swarmq.task`` mit
        ``error_type``/``error`` (R17)."""
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        t._backend.set_progress = AsyncMock(side_effect=RuntimeError("valkey down"))
        t._task_start_time = _time.monotonic()
        # Should not raise.
        await t.update_progress(1, 10)
        # Publish is skipped when HSET failed.
        t._backend.publish_progress.assert_not_awaited()
        [entry] = [
            e for e in swarmq_logs
            if e["event"] == "update_progress_hset_failed" and e["logger"] == "swarmq.task"
        ]
        assert entry["log_level"] == "warning"
        assert entry["task_id"] == "x"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "valkey down"

    @pytest.mark.timeout(5)
    async def test_publish_failure_swallowed(self, swarmq_logs):
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        t._backend.publish_progress = AsyncMock(
            side_effect=RuntimeError("pubsub down")
        )
        t._task_start_time = _time.monotonic()
        # Should not raise.
        await t.update_progress(1, 10)
        t._backend.set_progress.assert_awaited_once()
        [entry] = [
            e for e in swarmq_logs
            if e["event"] == "update_progress_publish_failed" and e["logger"] == "swarmq.task"
        ]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "pubsub down"

    @pytest.mark.timeout(5)
    async def test_final_flush_emitted_only_once(self):
        """Repeated update_progress(total, total) must not bypass
        the throttle forever (ADV-002 regression guard)."""
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic()

        for _ in range(5):
            await t.update_progress(10, 10, "done")

        assert t._backend.set_progress.await_count == 1

    @pytest.mark.timeout(5)
    async def test_throttle_resets_after_interval(self):
        """A third call after the 500ms floor has elapsed must flush.
        Pins the throttle-expiry contract so a refactor that never
        resets _last_progress_update is caught (KP-TG-03)."""
        import time as _time
        from unittest.mock import AsyncMock
        t = Task()
        t._task_id = "x"
        t._backend = AsyncMock()
        t._task_start_time = _time.monotonic()

        await t.update_progress(1, 100)
        await t.update_progress(2, 100)  # throttled
        # Rewind the last-update timestamp past the throttle window.
        t._last_progress_update = _time.monotonic() - 1.0
        await t.update_progress(3, 100)  # must flush

        assert t._backend.set_progress.await_count == 2
