"""Tests for swarmq.middleware — Middleware base class and MiddlewareChain."""

from __future__ import annotations

# Test list:
# 1. Middleware base class has 6 async no-op methods (callable without override)
# 2. MiddlewareChain with empty list -> all run_* methods are no-ops
# 3. pre_execute order: 3 middlewares registered -> called in registration order (A, B, C)
# 4. post_execute reverse order: 3 middlewares -> called in reverse (C, B, A)
# 5. on_error reverse order
# 6. on_retry receives retry_count and delay
# 7. Error in post_execute-hook -> logged, next hook still runs (ueber den
#    Logger ``swarmq.middleware``; Plan 2026-10-06-0831, U9)
# 7b. Der Logeintrag traegt ``error_type`` und ``error`` (R17)
# 8. Error in pre_enqueue -> propagates (raises)
# 9. Integration: mock middleware with Counter tracks calls to verify order
# 10. U11: workflow_step_message_context liefert einen MessageContext mit
#     festem delivery_mode="at_least_once" und den Workflow-Schluesseln
import inspect

import pytest

from swarmq.middleware import Middleware, MiddlewareChain

# ---------------------------------------------------------------------------
# 1. Middleware base class has 6 async no-op methods
# ---------------------------------------------------------------------------


async def test_middleware_base_class_has_six_async_no_op_methods():
    """Base Middleware exposes 6 async hooks which are callable no-ops."""
    mw = Middleware()

    hook_names = [
        "pre_enqueue",
        "post_enqueue",
        "pre_execute",
        "post_execute",
        "on_error",
        "on_retry",
    ]

    for name in hook_names:
        method = getattr(mw, name)
        assert inspect.iscoroutinefunction(method), f"{name} must be async"

    # Each hook must be callable without raising, returning None.
    message = {"task": {"id": "t1"}}
    exc = RuntimeError("boom")

    assert await mw.pre_enqueue(message) is None
    assert await mw.post_enqueue(message) is None
    assert await mw.pre_execute(message) is None
    assert await mw.post_execute(message, result="ok") is None
    assert await mw.on_error(message, exc) is None
    assert await mw.on_retry(message, exc, retry_count=1, delay=0.5) is None


# ---------------------------------------------------------------------------
# 2. MiddlewareChain with empty list -> all run_* methods are no-ops
# ---------------------------------------------------------------------------


async def test_empty_chain_all_run_methods_are_no_ops():
    """Empty chain returns None for every run_* method without raising."""
    chain = MiddlewareChain([])

    message = {"task": {"id": "t1"}}
    exc = RuntimeError("boom")

    assert await chain.run_pre_enqueue(message) is None
    assert await chain.run_post_enqueue(message) is None
    assert await chain.run_pre_execute(message) is None
    assert await chain.run_post_execute(message, result="ok") is None
    assert await chain.run_on_error(message, exception=exc) is None
    assert await chain.run_on_retry(message, exception=exc, retry_count=1, delay=0.5) is None


# ---------------------------------------------------------------------------
# Helper: recording middleware for order-based tests
# ---------------------------------------------------------------------------


class _RecordingMiddleware(Middleware):
    """Records hook invocations to a shared list as (label, hook_name) tuples."""

    def __init__(self, label: str, calls: list[tuple[str, str]]) -> None:
        self.label = label
        self.calls = calls

    async def pre_enqueue(self, message: dict) -> None:
        self.calls.append((self.label, "pre_enqueue"))

    async def post_enqueue(self, message: dict) -> None:
        self.calls.append((self.label, "post_enqueue"))

    async def pre_execute(self, message: dict) -> None:
        self.calls.append((self.label, "pre_execute"))

    async def post_execute(self, message: dict, result=None) -> None:
        self.calls.append((self.label, "post_execute"))

    async def on_error(self, message: dict, exception: BaseException) -> None:
        self.calls.append((self.label, "on_error"))

    async def on_retry(
        self,
        message: dict,
        exception: BaseException,
        retry_count: int,
        delay: float,
    ) -> None:
        self.calls.append(
            (self.label, "on_retry", retry_count, delay)  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# 3. pre_execute order: 3 middlewares registered -> called in registration order
# ---------------------------------------------------------------------------


async def test_run_pre_execute_calls_in_registration_order():
    calls: list[tuple[str, str]] = []
    a = _RecordingMiddleware("A", calls)
    b = _RecordingMiddleware("B", calls)
    c = _RecordingMiddleware("C", calls)

    chain = MiddlewareChain([a, b, c])
    await chain.run_pre_execute({"task": {"id": "t1"}})

    assert calls == [
        ("A", "pre_execute"),
        ("B", "pre_execute"),
        ("C", "pre_execute"),
    ]


# ---------------------------------------------------------------------------
# 4. post_execute reverse order: 3 middlewares -> called in reverse (C, B, A)
# ---------------------------------------------------------------------------


async def test_run_post_execute_calls_in_reverse_order():
    calls: list[tuple[str, str]] = []
    a = _RecordingMiddleware("A", calls)
    b = _RecordingMiddleware("B", calls)
    c = _RecordingMiddleware("C", calls)

    chain = MiddlewareChain([a, b, c])
    await chain.run_post_execute({"task": {"id": "t1"}}, result=42)

    assert calls == [
        ("C", "post_execute"),
        ("B", "post_execute"),
        ("A", "post_execute"),
    ]


# ---------------------------------------------------------------------------
# 5. on_error reverse order
# ---------------------------------------------------------------------------


async def test_run_on_error_calls_in_reverse_order():
    calls: list[tuple[str, str]] = []
    a = _RecordingMiddleware("A", calls)
    b = _RecordingMiddleware("B", calls)
    c = _RecordingMiddleware("C", calls)

    chain = MiddlewareChain([a, b, c])
    await chain.run_on_error({"task": {"id": "t1"}}, exception=RuntimeError("boom"))

    assert calls == [
        ("C", "on_error"),
        ("B", "on_error"),
        ("A", "on_error"),
    ]


# ---------------------------------------------------------------------------
# 6. on_retry receives retry_count and delay
# ---------------------------------------------------------------------------


async def test_run_on_retry_forwards_retry_count_and_delay():
    calls: list = []
    a = _RecordingMiddleware("A", calls)
    b = _RecordingMiddleware("B", calls)

    chain = MiddlewareChain([a, b])
    exc = RuntimeError("boom")
    await chain.run_on_retry({"task": {"id": "t1"}}, exception=exc, retry_count=3, delay=2.5)

    # Reverse order and payload carries retry_count + delay.
    assert calls == [
        ("B", "on_retry", 3, 2.5),
        ("A", "on_retry", 3, 2.5),
    ]


# ---------------------------------------------------------------------------
# 7. Error in post_execute-hook -> logged, next hook still runs
# ---------------------------------------------------------------------------


class _RaisingMiddleware(Middleware):
    """Always raises inside a named hook."""

    def __init__(self, hook: str, exc: BaseException) -> None:
        self.hook = hook
        self.exc = exc

    async def pre_enqueue(self, message: dict) -> None:
        if self.hook == "pre_enqueue":
            raise self.exc

    async def post_execute(self, message: dict, result=None) -> None:
        if self.hook == "post_execute":
            raise self.exc


async def test_error_in_post_execute_is_swallowed_and_next_hook_runs(swarmq_logs):
    calls: list[tuple[str, str]] = []
    good_a = _RecordingMiddleware("A", calls)
    good_b = _RecordingMiddleware("B", calls)
    raiser = _RaisingMiddleware("post_execute", RuntimeError("middleware-boom"))

    # Registration order: [A, raiser, B].
    # post_execute runs in reverse, so order is: B (ok), raiser (raises -> logged),
    # then A (still runs).
    chain = MiddlewareChain([good_a, raiser, good_b])

    await chain.run_post_execute({"task": {"id": "t1"}}, result=7)
    captured = [e for e in swarmq_logs if e["logger"] == "swarmq.middleware"]

    assert calls == [
        ("B", "post_execute"),
        ("A", "post_execute"),
    ]
    # The raiser's failure must have been logged at error level with the hook name.
    errors = [e for e in captured if e.get("log_level") == "error"]
    assert any(e.get("event") == "middleware_hook_failed" and e.get("hook") == "post_execute" for e in errors), (
        f"expected middleware_hook_failed log, got: {captured}"
    )


@pytest.mark.timeout(5)
async def test_middleware_hook_failure_logs_error_fields(swarmq_logs):
    """R17: Typ und Text des Fehlers als eigene Felder."""
    chain = MiddlewareChain([_RaisingMiddleware("post_execute", RuntimeError("middleware-boom"))])

    await chain.run_post_execute({"task": {"id": "t1"}}, result=7)

    [entry] = [e for e in swarmq_logs if e["event"] == "middleware_hook_failed"]
    assert entry["log_level"] == "error"
    assert entry["middleware"] == "_RaisingMiddleware"
    assert entry["error_type"] == "RuntimeError"
    assert entry["error"] == "middleware-boom"


# ---------------------------------------------------------------------------
# 8. Error in pre_enqueue -> propagates (raises)
# ---------------------------------------------------------------------------


async def test_error_in_pre_enqueue_propagates():
    calls: list[tuple[str, str]] = []
    good_a = _RecordingMiddleware("A", calls)
    raiser = _RaisingMiddleware("pre_enqueue", RuntimeError("block-enqueue"))
    good_b = _RecordingMiddleware("B", calls)

    chain = MiddlewareChain([good_a, raiser, good_b])

    with pytest.raises(RuntimeError, match="block-enqueue"):
        await chain.run_pre_enqueue({"task": {"id": "t1"}})

    # A must have been called before the raiser; B must NOT have been called.
    assert calls == [("A", "pre_enqueue")]


# ---------------------------------------------------------------------------
# 9. Integration: mock middleware with Counter tracks calls to verify order
# 10. U11: workflow_step_message_context liefert einen MessageContext mit
#     festem delivery_mode="at_least_once" und den Workflow-Schluesseln
# ---------------------------------------------------------------------------


async def test_integration_counter_tracks_full_hook_lifecycle():
    """Walk through every chain method and verify counters + per-hook ordering."""
    from collections import Counter

    calls: list[tuple[str, str]] = []
    a = _RecordingMiddleware("A", calls)
    b = _RecordingMiddleware("B", calls)
    c = _RecordingMiddleware("C", calls)

    chain = MiddlewareChain([a, b, c])
    message = {"task": {"id": "t1"}}
    exc = RuntimeError("boom")

    await chain.run_pre_enqueue(message)
    await chain.run_post_enqueue(message)
    await chain.run_pre_execute(message)
    await chain.run_post_execute(message, result="ok")
    await chain.run_on_error(message, exception=exc)
    await chain.run_on_retry(message, exception=exc, retry_count=2, delay=1.0)

    # Each of 3 middlewares was called once per hook -> 3 * 6 = 18.
    assert len(calls) == 18

    # Each (label, hook_name) pair appears exactly once.
    pairs = Counter((c[0], c[1]) for c in calls)
    for label in ("A", "B", "C"):
        for hook in (
            "pre_enqueue",
            "post_enqueue",
            "pre_execute",
            "post_execute",
            "on_error",
            "on_retry",
        ):
            assert pairs[(label, hook)] == 1, f"{label}.{hook} should be invoked exactly once"

    # Forward hooks: pre_enqueue, pre_execute -> A, B, C.
    pre_enqueue_order = [c[0] for c in calls if c[1] == "pre_enqueue"]
    assert pre_enqueue_order == ["A", "B", "C"]
    pre_execute_order = [c[0] for c in calls if c[1] == "pre_execute"]
    assert pre_execute_order == ["A", "B", "C"]

    # Reverse hooks: post_enqueue, post_execute, on_error, on_retry -> C, B, A.
    for hook in ("post_enqueue", "post_execute", "on_error", "on_retry"):
        order = [c[0] for c in calls if c[1] == hook]
        assert order == ["C", "B", "A"], f"{hook} should run in reverse"


async def test_workflow_step_message_context_pins_delivery_mode_and_workflow_keys():
    """R15/R16 (U11): Ein Workflow-Schritt laeuft immer als at-least-once, auch
    wenn der Task selbst FIRE_AND_FORGET deklariert — sonst gaebe es keinen
    Advance-Seam und der Workflow haengte. ``workflow_id``/``step_path`` sind
    zusaetzliche, optionale Schluessel: Handler mit fester Signatur bleiben
    funktionsfaehig."""
    from swarmq.middleware import workflow_step_message_context

    ctx = workflow_step_message_context(
        task_id="t1",
        task_name="echo",
        queue="default",
        args=[1, 2],
        kwargs={"k": "v"},
        workflow_id="wf-1",
        step_path="/1/g0",
    )
    assert ctx["delivery_mode"] == "at_least_once"
    assert ctx["task_id"] == "t1"
    assert ctx["args"] == [1, 2]
    assert ctx["kwargs"] == {"k": "v"}
    assert ctx["workflow_id"] == "wf-1"
    assert ctx["step_path"] == "/1/g0"
