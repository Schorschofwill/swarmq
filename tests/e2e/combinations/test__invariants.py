"""Unit-style tests for the universal invariant helpers.

These tests do not touch RabbitMQ or Valkey — they exercise the helpers
themselves so regressions in invariant logic surface immediately, not
hidden behind real-infrastructure flakes.
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.signals import Signal, SignalDispatcher

from ._invariants import (
    FAFStageObservation,
    SignalRecorder,
    assert_at_least_once_held,
    assert_faf_stage_invariants,
    assert_no_data_loss,
    assert_no_deadlock,
    assert_signal_sequence,
)

# ---------------------------------------------------------------------------
# SignalRecorder + assert_signal_sequence
# ---------------------------------------------------------------------------


async def test_signal_sequence_happy_path() -> None:
    """Dispatcher emits the expected signals in order — strict prefix match."""
    dispatcher = SignalDispatcher()
    recorder = SignalRecorder.attach(
        dispatcher,
        signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED],
    )
    await dispatcher.dispatch(Signal.TASK_STARTED, task_id="t1", task_name="x")
    await dispatcher.dispatch(
        Signal.TASK_COMPLETED,
        task_id="t1",
        task_name="x",
        duration_ms=12,
    )
    await assert_signal_sequence(
        recorder,
        [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
        timeout_s=1.0,
    )


async def test_signal_sequence_wrong_order_fails() -> None:
    """Emit order reversed — strict-prefix assertion must raise."""
    dispatcher = SignalDispatcher()
    recorder = SignalRecorder.attach(dispatcher)
    await dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="x", duration_ms=1)
    await dispatcher.dispatch(Signal.TASK_STARTED, task_id="t", task_name="x")
    with pytest.raises(AssertionError, match="signal sequence mismatch"):
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
            timeout_s=0.3,
        )


async def test_signal_sequence_timeout() -> None:
    """Expected signals never emitted — assertion times out cleanly."""
    dispatcher = SignalDispatcher()
    recorder = SignalRecorder.attach(dispatcher)
    with pytest.raises(AssertionError, match="signal sequence mismatch|incomplete"):
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED],
            timeout_s=0.2,
        )


async def test_signal_sequence_non_strict_set_membership() -> None:
    """``strict_order=False`` only requires set-membership, any order ok."""
    dispatcher = SignalDispatcher()
    recorder = SignalRecorder.attach(dispatcher)
    await dispatcher.dispatch(Signal.TASK_COMPLETED, task_id="t", task_name="x", duration_ms=1)
    await dispatcher.dispatch(Signal.TASK_STARTED, task_id="t", task_name="x")
    await assert_signal_sequence(
        recorder,
        [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
        timeout_s=1.0,
        strict_order=False,
    )


# ---------------------------------------------------------------------------
# assert_no_deadlock
# ---------------------------------------------------------------------------


async def test_no_deadlock_releases_in_time() -> None:
    """Lock-check returns truthy initially, falsy after delay — assertion passes."""
    released_at = asyncio.get_event_loop().time() + 0.3

    def _check() -> bool:
        return asyncio.get_event_loop().time() < released_at

    await assert_no_deadlock(_check, max_wait_s=2.0, poll_interval_s=0.05)


async def test_no_deadlock_times_out() -> None:
    """Lock-check stays truthy — assertion raises with deadlock message."""
    with pytest.raises(AssertionError, match="deadlock"):
        await assert_no_deadlock(lambda: True, max_wait_s=0.3, poll_interval_s=0.05)


async def test_no_deadlock_supports_async_check() -> None:
    """Lock-check may be async; awaited result is the truthiness signal."""

    async def _check() -> bool:
        return False  # already released

    await assert_no_deadlock(_check, max_wait_s=0.5)


# ---------------------------------------------------------------------------
# assert_no_data_loss
# ---------------------------------------------------------------------------


async def test_no_data_loss_at_least_once_allows_duplicates() -> None:
    """delivered > expected is fine for at-least-once (dup, not drop)."""
    await assert_no_data_loss(3, expected_count=1, at_least_once=True)


async def test_no_data_loss_at_least_once_rejects_drop() -> None:
    """delivered < expected violates at-least-once."""
    with pytest.raises(AssertionError, match="data loss under at-least-once"):
        await assert_no_data_loss(0, expected_count=1, at_least_once=True)


async def test_no_data_loss_at_most_once_rejects_duplicate() -> None:
    """delivered > expected is duplicate violation under at-most-once."""
    with pytest.raises(AssertionError, match="duplicate"):
        await assert_no_data_loss(2, expected_count=1, at_least_once=False)


async def test_no_data_loss_at_most_once_rejects_drop() -> None:
    """delivered < expected is drop violation under at-most-once."""
    with pytest.raises(AssertionError, match="drop"):
        await assert_no_data_loss(0, expected_count=1, at_least_once=False)


# ---------------------------------------------------------------------------
# assert_at_least_once_held
# ---------------------------------------------------------------------------


def test_at_least_once_held_passes_when_handler_ran() -> None:
    assert_at_least_once_held(1, expected_min=1)
    assert_at_least_once_held(5, expected_min=1)


def test_at_least_once_held_fails_on_silent_drop() -> None:
    with pytest.raises(AssertionError, match="silent drop"):
        assert_at_least_once_held(0, expected_min=1)


# ---------------------------------------------------------------------------
# FAF-4 stage invariants combinator
# ---------------------------------------------------------------------------


def test_faf_stage_none_fallthrough_passes() -> None:
    """Stage was None, handler ran — FAF-1 satisfied."""
    obs = FAFStageObservation(
        stage_executed=True,
        nacked_with_requeue=False,
        silently_dropped=False,
        requeue_count=0,
    )
    assert_faf_stage_invariants(obs, scenario_kind="stage_none")


def test_faf_stage_none_with_no_execution_fails() -> None:
    """Stage was None but handler didn't run — FAF-1 violation."""
    obs = FAFStageObservation(
        stage_executed=False,
        nacked_with_requeue=False,
        silently_dropped=False,
        requeue_count=0,
    )
    with pytest.raises(AssertionError, match="FAF-1"):
        assert_faf_stage_invariants(obs, scenario_kind="stage_none")


def test_faf_backend_fault_nacks_correctly() -> None:
    """Backend faulted, nack(requeue=True), single requeue — all four invariants ok."""
    obs = FAFStageObservation(
        stage_executed=False,
        nacked_with_requeue=True,
        silently_dropped=False,
        requeue_count=1,
    )
    assert_faf_stage_invariants(obs, scenario_kind="backend_fault")


def test_faf_silent_drop_detected() -> None:
    """Backend fault but no nack and no DLQ — FAF-2 violation."""
    obs = FAFStageObservation(
        stage_executed=False,
        nacked_with_requeue=False,
        silently_dropped=True,
        requeue_count=0,
    )
    with pytest.raises(AssertionError, match="silently dropped"):
        assert_faf_stage_invariants(obs, scenario_kind="backend_fault")


def test_faf_requeue_storm_caught() -> None:
    """Backend fault, nack happened, but requeue_count > max_requeues — FAF-4 violation."""
    obs = FAFStageObservation(
        stage_executed=False,
        nacked_with_requeue=True,
        silently_dropped=False,
        requeue_count=50,
    )
    with pytest.raises(AssertionError, match="requeue storm"):
        assert_faf_stage_invariants(
            obs,
            scenario_kind="backend_fault",
            max_requeues=10,
        )


def test_faf_faf_backend_fault_no_silent_drop_either() -> None:
    """FAF + backend fault — FAF-3 (FAF doesn't loosen pre-handler safety)."""
    obs = FAFStageObservation(
        stage_executed=False,
        nacked_with_requeue=True,
        silently_dropped=False,
        requeue_count=1,
    )
    assert_faf_stage_invariants(obs, scenario_kind="faf_backend_fault")
