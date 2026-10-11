"""Universal invariants for combinations tests (Plan 2026-05-26-002 Unit 3).

These helpers are the assertion vocabulary shared across the four risk-
category test files. Each combo test re-uses one or more of them, then
adds combo-specific assertions on top.

Design notes:
- The FAF-4-stage combinator encodes the four invariants from
  ``docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md``
  so every broker/backend mid-op-drop test exercises the same regression
  surface.
- ``assert_no_reader_leak`` exists because of the learning at
  ``docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md``
  — after a SIGKILL, ``RobustConnection.is_closed`` lies; reader-task
  counts are the truthful signal.
- Helpers raise ``AssertionError`` with diff-style messages so pytest's
  default reporter shows the failure shape inline.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import psutil

from swarmq.signals import Signal, SignalDispatcher

# ---------------------------------------------------------------------------
# SignalRecorder — connects to a dispatcher, captures emit order + payloads
# ---------------------------------------------------------------------------


@dataclass
class _RecordedSignal:
    """One captured signal emission: which signal, when, and its payload."""

    signal: Signal
    timestamp: float
    payload: dict[str, Any]


@dataclass
class SignalRecorder:
    """Captures Signal emissions in order for ``assert_signal_sequence``.

    Usage::

        recorder = SignalRecorder.attach(dispatcher, signals=[Signal.TASK_STARTED, ...])
        # run task ...
        await assert_signal_sequence(recorder, [Signal.TASK_STARTED, Signal.TASK_COMPLETED])

    The recorder uses ``dispatcher.connect`` for each requested signal,
    which means it observes the same emit order the production code sees
    (handler exceptions don't disrupt subsequent handlers — see
    ``SignalDispatcher.dispatch``).
    """

    events: list[_RecordedSignal] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @classmethod
    def attach(
        cls,
        dispatcher: SignalDispatcher,
        *,
        signals: list[Signal] | None = None,
    ) -> SignalRecorder:
        """Connect a recorder to ``dispatcher``. If ``signals`` is None,
        attach to every Signal — convenient for combo tests that don't
        know up-front which signals matter."""
        recorder = cls()
        targets = signals if signals is not None else list(Signal)
        for sig in targets:
            dispatcher.connect(sig, recorder._make_handler(sig))
        return recorder

    def _make_handler(self, sig: Signal) -> Callable[..., Any]:
        async def _handler(**payload: Any) -> None:
            async with self._lock:
                self.events.append(
                    _RecordedSignal(
                        signal=sig,
                        timestamp=time.monotonic(),
                        payload=dict(payload),
                    ),
                )

        return _handler

    @property
    def emitted_signals(self) -> list[Signal]:
        """Just the signal enum values, in emit order."""
        return [e.signal for e in self.events]


# ---------------------------------------------------------------------------
# assert_signal_sequence — used by combos where emit-order is contractual
# ---------------------------------------------------------------------------


async def assert_signal_sequence(
    recorder: SignalRecorder,
    expected: list[Signal],
    *,
    timeout_s: float = 5.0,
    strict_order: bool = True,
    poll_interval_s: float = 0.05,
) -> None:
    """Wait until all signals in ``expected`` are recorded; assert order.

    Used for combos #9 (soft_timeout + cancel race), #17 / #18 (middleware
    raise + signal-handler ordering), and any other combo where the
    SwarmQ contract specifies a specific emit order.

    ``strict_order=True`` requires the recorded sequence to start with
    ``expected`` exactly (extra trailing signals are fine — operators
    routinely add custom signals downstream).
    ``strict_order=False`` only checks set-membership.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        recorded = recorder.emitted_signals
        if strict_order:
            if len(recorded) >= len(expected) and recorded[: len(expected)] == expected:
                return
        else:
            if set(expected) <= set(recorded):
                return
        await asyncio.sleep(poll_interval_s)

    recorded = recorder.emitted_signals
    if strict_order:
        raise AssertionError(
            f"signal sequence mismatch after {timeout_s}s\n"
            f"  expected (strict prefix): {[s.value for s in expected]}\n"
            f"  recorded:                 {[s.value for s in recorded]}"
        )
    missing = set(expected) - set(recorded)
    raise AssertionError(
        f"signal-set incomplete after {timeout_s}s\n"
        f"  expected (set):  {[s.value for s in expected]}\n"
        f"  recorded:        {[s.value for s in recorded]}\n"
        f"  missing:         {[s.value for s in missing]}"
    )


# ---------------------------------------------------------------------------
# assert_no_deadlock — Tier-3 combos (#27, #29) need lock-TTL-recovery
# ---------------------------------------------------------------------------


async def assert_no_deadlock(
    lock_check: Callable[[], Any],
    *,
    max_wait_s: float = 10.0,
    poll_interval_s: float = 0.2,
) -> None:
    """Poll a lock-check callable until it reports no held lock, or fail.

    ``lock_check`` is a zero-arg async-or-sync callable that returns a
    truthy value while the lock is held and a falsy value once released
    (e.g. ``lambda: backend._client.exists(lock_key)``). Decoupling lets
    callers point at backend internals without ``assert_no_deadlock``
    knowing about Valkey.

    Combos #4 (lock + hard_timeout), #27 (SIGKILL + lock + at-least-once),
    #29 (process_manager + crash + lock) are the primary callers.
    """
    deadline = time.monotonic() + max_wait_s
    while time.monotonic() < deadline:
        result = lock_check()
        if asyncio.iscoroutine(result):
            result = await result
        if not result:
            return
        await asyncio.sleep(poll_interval_s)
    raise AssertionError(f"deadlock: lock still held after {max_wait_s}s — lock-TTL recovery did not fire")


# ---------------------------------------------------------------------------
# assert_no_data_loss — at-least-once / at-most-once delivery contract
# ---------------------------------------------------------------------------


async def assert_no_data_loss(
    delivered_count: int,
    expected_count: int,
    *,
    at_least_once: bool = True,
) -> None:
    """Compare delivered vs expected message count against the configured
    delivery semantic.

    - ``at_least_once=True``: delivered >= expected (duplicates allowed,
      drop not allowed)
    - ``at_least_once=False``: delivered == expected (at-most-once; both
      drop and duplicate are violations)

    Callers compute ``delivered_count`` themselves — they know which
    queue + DLQ to sum. This helper just makes the contract explicit.
    """
    if at_least_once:
        if delivered_count < expected_count:
            raise AssertionError(
                f"data loss under at-least-once: delivered={delivered_count}, expected>={expected_count}"
            )
    else:
        if delivered_count != expected_count:
            raise AssertionError(
                f"delivery violation under at-most-once: "
                f"delivered={delivered_count}, expected=={expected_count} "
                f"({'duplicate' if delivered_count > expected_count else 'drop'})"
            )


# ---------------------------------------------------------------------------
# assert_at_least_once_held — silent-drop detector
# ---------------------------------------------------------------------------


def assert_at_least_once_held(
    handler_call_count: int,
    *,
    expected_min: int = 1,
) -> None:
    """Trivial but load-bearing: a Tier-1 invariant that any failure-injected
    combo must not silently drop the task. If the handler ran 0 times, the
    failure path swallowed the task — which is always a bug.

    Synchronous because it's just an int compare. Async helpers reach for
    the broker / backend; this one is for after-the-fact handler-side
    bookkeeping (e.g. a list of seen task_ids has expected length)."""
    if handler_call_count < expected_min:
        raise AssertionError(f"silent drop: handler ran {handler_call_count} time(s), expected at least {expected_min}")


# ---------------------------------------------------------------------------
# assert_no_reader_leak — SIGKILL aftermath check
# ---------------------------------------------------------------------------


def _broker_port(child: psutil.Process) -> int | None:
    """Port of ``SWARMQ_BROKER_URL`` in the child's environment."""
    try:
        url = child.environ().get("SWARMQ_BROKER_URL")
    except psutil.NoSuchProcess, psutil.AccessDenied:
        return None
    if not url:
        return None
    return urlsplit(url).port or 5672


async def assert_no_reader_leak(
    process: subprocess.Popen[str] | None,
    *,
    max_broker_connections: int = 2,
    min_broker_connections: int = 1,
    observe_s: float = 0.5,
) -> None:
    """Check the SURVIVOR worker after a predecessor's crash: it stays alive
    and holds no leaked broker connections.

    ``process`` is the supervisor started by ``spawn_worker``; since R20 the
    worker loop runs in its ``multiprocessing`` child. The check:

    1. the supervisor and at least one worker child are alive — before and
       after ``observe_s``;
    2. each worker child holds between ``min_broker_connections`` and
       ``max_broker_connections`` TCP connections to the broker port (taken
       from the child's ``SWARMQ_BROKER_URL``), and at least one child was
       actually counted -- otherwise (no broker URL in the child's
       environment, ``AccessDenied``) the check would silently degrade to a
       liveness check. A healthy worker holds one; aiormq reader
       leaks (learning aiormq-reader-leak-and-stale-connection-recovery-
       2026-04-16) show up as extra, stale connections — and
       ``RobustConnection.is_closed`` is no truthful signal for them.

    Earlier versions sent SIGUSR1 for a faulthandler dump; swarmq never
    registers that handler, so the default action terminated the process
    (rc=-10) and the check could never pass.
    """
    if process is None:
        return
    if process.poll() is not None:
        raise AssertionError(f"survivor supervisor is dead (rc={process.returncode})")

    def _children() -> list[psutil.Process]:
        try:
            tree = psutil.Process(process.pid).children(recursive=True)
        except psutil.NoSuchProcess:
            return []
        workers: list[psutil.Process] = []
        for child in tree:
            try:
                if any("spawn_main" in part for part in child.cmdline()):
                    workers.append(child)
            except psutil.NoSuchProcess, psutil.AccessDenied:
                continue
        return workers

    before = _children()
    if not before:
        raise AssertionError("survivor supervisor runs no worker child")
    # Deliberate observation window (negative check: nothing may die during
    # it) -- there is no event to wait for.
    await asyncio.sleep(observe_s)
    if process.poll() is not None:
        raise AssertionError(f"survivor supervisor died during observation (rc={process.returncode})")
    after = _children()
    dead = {c.pid for c in before} - {c.pid for c in after}
    if dead:
        raise AssertionError(f"worker child(ren) {sorted(dead)} died during observation")

    counted = 0
    for child in after:
        port = _broker_port(child)
        if port is None:
            continue
        try:
            conns = [
                c
                for c in child.net_connections(kind="tcp")
                if c.raddr and c.raddr.port == port and c.status == psutil.CONN_ESTABLISHED
            ]
        except psutil.NoSuchProcess, psutil.AccessDenied:
            continue
        counted += 1
        if len(conns) > max_broker_connections:
            raise AssertionError(
                f"worker child {child.pid} holds {len(conns)} broker connections "
                f"(max {max_broker_connections}) — leaked aiormq connection/reader"
            )
        if len(conns) < min_broker_connections:
            raise AssertionError(
                f"worker child {child.pid} holds {len(conns)} broker connections "
                f"(min {min_broker_connections}) — survivor lost its broker connection"
            )
    if not counted:
        raise AssertionError(
            "no worker child's broker connections could be counted (no SWARMQ_BROKER_URL, "
            "AccessDenied) — the check would only prove liveness"
        )


# ---------------------------------------------------------------------------
# FAF-4 combinator — the four pipeline-stage invariants in one helper
# ---------------------------------------------------------------------------


@dataclass
class FAFStageObservation:
    """What a broker/backend mid-op-drop scenario observed for one task."""

    # Did the stage that should have run actually run (None = stage was
    # ``None`` and the message fell through to handler)?
    stage_executed: bool

    # On stage failure, did the broker see a nack(requeue=True)?
    nacked_with_requeue: bool

    # Was the message silently dropped (no handler, no nack, no DLQ)?
    silently_dropped: bool

    # How many times was the message requeued? Useful for verifying
    # bounded-retry contract.
    requeue_count: int


def assert_faf_stage_invariants(
    observation: FAFStageObservation,
    *,
    scenario_kind: str,
    max_requeues: int = 10,
) -> None:
    """The four FAF-pipeline-stage invariants from learning
    worker-pipeline-stage-ack-ordering-2026-04-16, asserted as one block:

    1. Stage ``None`` → fallthrough to handler (no stage-introduced drop)
    2. Backend/Broker fault during stage → ``nack(requeue=True)``,
       NOT silent drop
    3. FAF + stage fault → still no silent drop (FAF doesn't change the
       safety contract for stages BEFORE the handler)
    4. Requeue counter bounded — a single fault should not produce an
       unbounded retry storm

    ``scenario_kind`` is one of:
      - ``"stage_none"`` — stage was None, expect fallthrough
      - ``"backend_fault"`` — backend raised mid-stage
      - ``"faf_backend_fault"`` — FAF message + backend raised mid-stage
      - ``"broker_drop"`` — broker connection dropped mid-stage

    Helper is sync because all observations are post-hoc.
    """
    if scenario_kind == "stage_none":
        if not observation.stage_executed:
            raise AssertionError("FAF-1 (stage_none fallthrough): handler did not run despite stage=None")
        return

    # All fault scenarios: must NOT silently drop.
    if observation.silently_dropped:
        raise AssertionError(f"FAF-2/3 ({scenario_kind}): message silently dropped (no handler, no nack, no DLQ)")
    if not observation.nacked_with_requeue:
        raise AssertionError(f"FAF-2/3 ({scenario_kind}): expected nack(requeue=True) after stage fault, none observed")
    if observation.requeue_count > max_requeues:
        raise AssertionError(
            f"FAF-4 ({scenario_kind}): requeue storm — count {observation.requeue_count} exceeded bound {max_requeues}"
        )
