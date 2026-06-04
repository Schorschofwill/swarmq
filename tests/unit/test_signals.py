"""Tests for swarmq.signals — Signal Enum and SignalDispatcher."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
import structlog.testing

from swarmq.signals import Signal, SignalDispatcher


# ---------------------------------------------------------------------------
# 1. Signal Enum has all 14 expected values
# ---------------------------------------------------------------------------

def test_signal_enum_has_task_enqueued():
    assert Signal.TASK_ENQUEUED.value == "task_enqueued"


def test_signal_enum_has_task_started():
    assert Signal.TASK_STARTED.value == "task_started"


def test_signal_enum_has_task_completed():
    assert Signal.TASK_COMPLETED.value == "task_completed"


def test_signal_enum_has_task_failed():
    assert Signal.TASK_FAILED.value == "task_failed"


def test_signal_enum_has_task_retrying():
    assert Signal.TASK_RETRYING.value == "task_retrying"


def test_signal_enum_has_task_cancelled():
    assert Signal.TASK_CANCELLED.value == "task_cancelled"


def test_signal_enum_task_revoked_removed():
    """Safety check: old name is gone to prevent accidental reintroduction."""
    assert not hasattr(Signal, "TASK_REVOKED")


def test_signal_enum_has_task_expired():
    assert Signal.TASK_EXPIRED.value == "task_expired"


def test_signal_enum_has_task_locked():
    assert Signal.TASK_LOCKED.value == "task_locked"


def test_signal_enum_has_task_rate_limited():
    assert Signal.TASK_RATE_LIMITED.value == "task_rate_limited"


def test_signal_enum_has_worker_online():
    assert Signal.WORKER_ONLINE.value == "worker_online"


def test_signal_enum_has_worker_shutdown():
    assert Signal.WORKER_SHUTDOWN.value == "worker_shutdown"


def test_signal_enum_has_worker_heartbeat():
    assert Signal.WORKER_HEARTBEAT.value == "worker_heartbeat"


def test_signal_enum_has_scheduler_leader():
    assert Signal.SCHEDULER_LEADER.value == "scheduler_leader"


def test_signal_enum_has_scheduler_standby():
    assert Signal.SCHEDULER_STANDBY.value == "scheduler_standby"


def test_signal_enum_has_scheduler_heartbeat():
    assert Signal.SCHEDULER_HEARTBEAT.value == "scheduler_heartbeat"


def test_signal_enum_has_scheduler_leader_handover():
    assert Signal.SCHEDULER_LEADER_HANDOVER.value == "scheduler_leader_handover"


def test_signal_enum_has_scheduler_leader_failover():
    assert Signal.SCHEDULER_LEADER_FAILOVER.value == "scheduler_leader_failover"


def test_signal_enum_contains_all_required_values():
    """Presence-check (not count-check): additive new signals must not
    break this test. Count-based coupling was brittle — api-contract
    AC-07."""
    required = {
        "TASK_ENQUEUED", "TASK_STARTED", "TASK_COMPLETED", "TASK_FAILED",
        "TASK_RETRYING", "TASK_CANCELLED", "TASK_EXPIRED", "TASK_LOCKED",
        "TASK_RATE_LIMITED",
        "WORKER_ONLINE", "WORKER_SHUTDOWN", "WORKER_HEARTBEAT",
        "SCHEDULER_LEADER", "SCHEDULER_STANDBY", "SCHEDULER_HEARTBEAT",
        "SCHEDULER_LEADER_HANDOVER", "SCHEDULER_LEADER_FAILOVER",
    }
    existing = {s.name for s in Signal}
    missing = required - existing
    assert not missing, f"required signals missing: {missing}"


# ---------------------------------------------------------------------------
# 2. SignalDispatcher.connect registers handler
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_registers_handler():
    dispatcher = SignalDispatcher()
    handler = AsyncMock()

    dispatcher.connect(Signal.TASK_COMPLETED, handler)

    await dispatcher.dispatch(Signal.TASK_COMPLETED)
    handler.assert_awaited_once()


# ---------------------------------------------------------------------------
# 3. @on_signal decorator registers handler
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_signal_decorator_registers_handler():
    dispatcher = SignalDispatcher()
    calls: list[str] = []

    @dispatcher.on_signal(Signal.TASK_STARTED)
    async def handler(**kwargs):
        calls.append("called")

    await dispatcher.dispatch(Signal.TASK_STARTED)
    assert calls == ["called"]


@pytest.mark.asyncio
async def test_on_signal_decorator_returns_original_handler():
    """The decorator should not replace the function — it should return it."""
    dispatcher = SignalDispatcher()

    @dispatcher.on_signal(Signal.TASK_STARTED)
    async def handler(**kwargs):
        return "ok"

    # The decorated name must still be callable as the original coroutine
    result = await handler()
    assert result == "ok"


# ---------------------------------------------------------------------------
# 4. dispatch calls all registered handlers for the signal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_calls_registered_handler():
    dispatcher = SignalDispatcher()
    handler = AsyncMock()
    dispatcher.connect(Signal.TASK_FAILED, handler)

    await dispatcher.dispatch(Signal.TASK_FAILED)

    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_only_calls_handlers_for_that_signal():
    """Handlers for other signals must not fire."""
    dispatcher = SignalDispatcher()
    completed_handler = AsyncMock()
    failed_handler = AsyncMock()

    dispatcher.connect(Signal.TASK_COMPLETED, completed_handler)
    dispatcher.connect(Signal.TASK_FAILED, failed_handler)

    await dispatcher.dispatch(Signal.TASK_COMPLETED)

    completed_handler.assert_awaited_once()
    failed_handler.assert_not_awaited()


# ---------------------------------------------------------------------------
# 5. dispatch with kwargs → handlers receive kwargs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_passes_kwargs_to_handler():
    dispatcher = SignalDispatcher()
    handler = AsyncMock()
    dispatcher.connect(Signal.TASK_COMPLETED, handler)

    await dispatcher.dispatch(
        Signal.TASK_COMPLETED, task_id="abc-123", duration_ms=42.5
    )

    handler.assert_awaited_once_with(task_id="abc-123", duration_ms=42.5)


# ---------------------------------------------------------------------------
# 6. Multiple handlers for same signal → all called in registration order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multiple_handlers_called_in_registration_order():
    dispatcher = SignalDispatcher()
    calls: list[str] = []

    async def first(**kwargs):
        calls.append("first")

    async def second(**kwargs):
        calls.append("second")

    async def third(**kwargs):
        calls.append("third")

    dispatcher.connect(Signal.WORKER_ONLINE, first)
    dispatcher.connect(Signal.WORKER_ONLINE, second)
    dispatcher.connect(Signal.WORKER_ONLINE, third)

    await dispatcher.dispatch(Signal.WORKER_ONLINE)

    assert calls == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# 7. Handler raises exception → logged, next handler still runs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handler_exception_is_logged_and_next_runs():
    dispatcher = SignalDispatcher()
    calls: list[str] = []

    async def failing(**kwargs):
        calls.append("failing")
        raise RuntimeError("boom")

    async def succeeding(**kwargs):
        calls.append("succeeding")

    dispatcher.connect(Signal.TASK_FAILED, failing)
    dispatcher.connect(Signal.TASK_FAILED, succeeding)

    with structlog.testing.capture_logs() as logs:
        await dispatcher.dispatch(Signal.TASK_FAILED)

    # Both handlers ran — exception did not abort the dispatch loop
    assert calls == ["failing", "succeeding"]

    # A warning was logged; the failing handler's error must be captured
    warning_entries = [
        entry for entry in logs if entry.get("log_level") == "warning"
    ]
    assert warning_entries, "expected a WARNING log entry for handler exception"
    # The failing handler's exception details must be present
    entry = warning_entries[0]
    assert entry["event"] == "signal_handler_error"
    assert entry["signal"] == Signal.TASK_FAILED.value
    assert "RuntimeError" in entry["error"]


@pytest.mark.asyncio
async def test_handler_exception_does_not_propagate():
    """dispatch itself must not raise when a handler fails."""
    dispatcher = SignalDispatcher()

    async def failing(**kwargs):
        raise ValueError("nope")

    dispatcher.connect(Signal.TASK_FAILED, failing)

    # Must not raise
    await dispatcher.dispatch(Signal.TASK_FAILED)


# ---------------------------------------------------------------------------
# 8. dispatch for signal with no handlers → no-op (no error)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_with_no_handlers_is_noop():
    dispatcher = SignalDispatcher()

    # No handlers connected — must not raise
    await dispatcher.dispatch(Signal.SCHEDULER_STANDBY)


@pytest.mark.asyncio
async def test_dispatch_with_no_handlers_accepts_kwargs():
    dispatcher = SignalDispatcher()

    # kwargs with no handlers — also a safe no-op
    await dispatcher.dispatch(Signal.SCHEDULER_STANDBY, foo="bar", n=1)


# -------------------------------------------------------------------
# R3.5: TASK_DUPLICATE_DETECTED signal
# -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_duplicate_detected_dispatches_with_payload():
    """Signal fires with expected payload keys (existing_task_id,
    task_name, dedup_hash, source)."""
    dispatcher = SignalDispatcher()
    received: list[dict] = []

    @dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
    async def handler(**kwargs):
        received.append(kwargs)

    await dispatcher.dispatch(
        Signal.TASK_DUPLICATE_DETECTED,
        existing_task_id="t-123",
        task_name="send_email",
        dedup_hash="abc",
        source="client.schedule",
    )

    assert len(received) == 1
    assert received[0] == {
        "existing_task_id": "t-123",
        "task_name": "send_email",
        "dedup_hash": "abc",
        "source": "client.schedule",
    }


def test_task_duplicate_detected_is_member_of_signal_enum():
    assert Signal.TASK_DUPLICATE_DETECTED.value == "task_duplicate_detected"
    assert Signal.TASK_DUPLICATE_DETECTED in Signal


# ---------------------------------------------------------------------------
# Härtung R2 P1-9/P2: handler_timeout + CancelledError-Durchreichung
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_handler_timeout_bounds_hanging_handler():
    """Härtung R2 P1-9: ein blockierender (nicht werfender) Handler
    hängte den Dispatch unbegrenzt — als allererste Aktion in
    _execute_shutdown blockierte das den gesamten Graceful Shutdown
    bis zum SIGKILL des Orchestrators. Mit handler_timeout wird der
    Handler abgebrochen, geloggt und der nächste läuft."""
    import asyncio

    dispatcher = SignalDispatcher()
    ran: list[str] = []

    @dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
    async def hanging(**kwargs):
        await asyncio.sleep(60)

    @dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
    async def second(**kwargs):
        ran.append("second")

    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.wait_for(
        dispatcher.dispatch(
            Signal.WORKER_SHUTDOWN, handler_timeout=0.1, worker_id="w",
        ),
        timeout=5,
    )
    elapsed = loop.time() - started

    assert elapsed < 4, f"hanging handler not bounded ({elapsed:.1f}s)"
    assert ran == ["second"], "subsequent handlers must still run"


@pytest.mark.asyncio
async def test_dispatch_without_timeout_keeps_legacy_behaviour():
    dispatcher = SignalDispatcher()
    received: list[dict] = []

    @dispatcher.on_signal(Signal.WORKER_ONLINE)
    async def handler(**kwargs):
        received.append(kwargs)

    await dispatcher.dispatch(Signal.WORKER_ONLINE, worker_id="w")
    assert received == [{"worker_id": "w"}]


@pytest.mark.asyncio
async def test_dispatch_reraises_cancelled_error():
    """Härtung R2 P2: ``except Exception`` fängt CancelledError nicht —
    das ist KORREKT (Shutdown-Cancellation muss propagieren), war aber
    undokumentiert und ungetestet. Festgenagelt: ein im Handler
    aufschlagendes CancelledError verlässt dispatch()."""
    import asyncio

    dispatcher = SignalDispatcher()

    @dispatcher.on_signal(Signal.TASK_COMPLETED)
    async def handler(**kwargs):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t")
