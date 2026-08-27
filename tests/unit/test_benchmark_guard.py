"""AST-level guards that keep benchmarks/bench.py free of the channel /
iterator leak patterns we already spent two debugging sessions on.

These are pure AST checks — no RabbitMQ, no Docker, no network. They
run in every unit pass and fail fast if a new scenario is added with
the legacy `async for message in queue:` idiom or a bare
`connection.channel()` that is not wrapped in `_bench_channel()`.

If one of these tests fails, the fix is documented at the top of
benchmarks/bench.py (search for ``_bench_channel``): migrate the
offending call to ``async with _bench_channel(conn) as ch:`` and the
async-for to ``async with queue.iterator() as it: async for ... in it``.
"""

from __future__ import annotations

import ast
from pathlib import Path

BENCH_PATH = Path(__file__).resolve().parents[2] / "benchmarks" / "bench.py"


def _load_bench_module() -> ast.Module:
    source = BENCH_PATH.read_text(encoding="utf-8")
    return ast.parse(source, filename=str(BENCH_PATH))


def test_benchmark_file_exists() -> None:
    """Guard the guards: if the file moves, the other tests would
    silently pass against an empty parse tree."""
    assert BENCH_PATH.is_file(), f"bench.py moved: {BENCH_PATH}"


def test_no_bare_async_for_on_queue_variable() -> None:
    """Catch the original hang pattern: ``async for message in queue:``
    without a surrounding ``async with queue.iterator()`` context.

    The original bug (aiormq _reader task leak) was exactly this shape.
    aio-pika's Queue.__aiter__ opens a broker-side consumer that is NOT
    torn down on task.cancel(); only the iterator context manager
    guarantees cleanup.
    """
    tree = _load_bench_module()
    offenders: list[tuple[int, str]] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            # Stack of enclosing `async with` items that look like
            # ``queue.iterator()`` or ``<anything>.iterator()``. Any
            # AsyncFor found inside such a scope is considered safe.
            self._iterator_context_depth = 0

        def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
            has_iterator_ctx = any(
                isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Attribute)
                and item.context_expr.func.attr == "iterator"
                for item in node.items
            )
            if has_iterator_ctx:
                self._iterator_context_depth += 1
            try:
                self.generic_visit(node)
            finally:
                if has_iterator_ctx:
                    self._iterator_context_depth -= 1

        def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
            # Only flag when iterating over a variable that looks like
            # an aio-pika Queue (named 'queue' or ending in '_queue') —
            # Valkey's `pubsub.listen()` is a legitimate async-for.
            iter_target = node.iter
            flag = False
            if isinstance(iter_target, ast.Name):  # noqa: SIM102
                if iter_target.id == "queue" or iter_target.id.endswith("_queue"):
                    flag = True
            if flag and self._iterator_context_depth == 0:
                offenders.append((node.lineno, ast.unparse(iter_target)))
            self.generic_visit(node)

    Visitor().visit(tree)
    assert not offenders, (
        "bench.py contains `async for <msg> in queue:` not wrapped in "
        "`async with queue.iterator() as it:` — this is the exact leak "
        "pattern that hung full_pipeline. Offenders: "
        + ", ".join(f"line {ln} over {expr!r}" for ln, expr in offenders)
    )


def test_all_channel_opens_use_bench_channel() -> None:
    """Catch the other half of the leak pattern: bare
    ``connection.channel()`` calls outside an ``async with
    _bench_channel(conn)`` block.

    The probe call inside ``RabbitMQBench._ensure_connection`` is the
    only exception — it has its own try/close because its semantics are
    different (test the socket, don't use it).
    """
    tree = _load_bench_module()
    offenders: list[tuple[int, str]] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self._current_func: str | None = None

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            outer, self._current_func = self._current_func, node.name
            try:
                self.generic_visit(node)
            finally:
                self._current_func = outer

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            # nested sync helpers don't matter, but keep the stack honest
            outer, self._current_func = self._current_func, node.name
            try:
                self.generic_visit(node)
            finally:
                self._current_func = outer

        def visit_Await(self, node: ast.Await) -> None:
            call = node.value
            if (  # noqa: SIM102
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "channel"
            ):
                # Two legitimate exceptions:
                # * `_bench_channel` itself IS the wrapper — its own
                #   `await connection.channel()` is exactly what the
                #   helper abstracts over.
                # * `_ensure_connection` opens a throwaway probe channel
                #   with its own try/close; semantics differ from usage.
                if self._current_func not in ("_bench_channel", "_ensure_connection"):
                    offenders.append((node.lineno, ast.unparse(call)))
            self.generic_visit(node)

    Visitor().visit(tree)
    assert not offenders, (
        "bench.py opens a channel via `await ...channel()` outside "
        "`async with _bench_channel(conn)` — this reintroduces the "
        "channel leak. Offenders: "
        + ", ".join(f"line {ln}: {expr}" for ln, expr in offenders)
    )
