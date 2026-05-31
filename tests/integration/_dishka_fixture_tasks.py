"""Fixture module for the Dishka DI CLI integration test (plan U6).

Builds an AsyncContainer at module level and registers it via
``setup_dishka`` so each spawned worker subprocess rebuilds it fresh on
re-import. The task injects an APP-scoped ``Greeter`` and a REQUEST-scoped
value derived from the per-task ``TaskContext`` — proving both scopes wire up
end-to-end through the real worker.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterable

from dishka import FromDishka, Provider, Scope, make_async_container, provide

from swarmq import SwarmQProvider, TaskContext, setup_dishka
from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "cli_dishka_default")


class Greeter:
    """APP-scoped singleton (built once per worker process)."""

    def greet(self, value: str) -> str:
        return f"di:{value}"


class AppProvider(Provider):
    greeter = provide(Greeter, scope=Scope.APP)

    @provide(scope=Scope.REQUEST)
    async def queue_tag(self, ctx: TaskContext) -> AsyncIterable[str]:
        # REQUEST-scoped value carrying the task's queue, proving the
        # per-task TaskContext is available to providers.
        yield ctx.queue


container = make_async_container(AppProvider(), SwarmQProvider())
setup_dishka(container)


class DiEchoTask(Task):
    task_info = TaskInfo(name="cli_di_echo", queues=[_QUEUE])

    async def run(
        self,
        value: str,
        greeter: FromDishka[Greeter],
        queue_tag: FromDishka[str],
    ) -> str:
        return f"{greeter.greet(value)}@{queue_tag}"
