"""Enqueue-Seam der Workflow-Schritte (U11, KTD7).

Worker und Client reihen Workflow-Schritte durch dieselbe Klasse ein, damit
die beiden Wege nicht auseinanderlaufen: ein Schritt laeuft durch dieselbe
Middleware-Kette wie ``Client.schedule`` und loest dieselben Signale aus.
``workflow_orchestration`` re-exportiert die Namen (U12).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from swarmq.contracts import PublishReceipt
from swarmq.middleware import MessageContext, workflow_step_message_context
from swarmq.serialization import encode_json
from swarmq.signals import Signal


@dataclass(frozen=True)
class WorkflowStep:
    """Ein zu veröffentlichender Workflow-Schritt (U11, KTD7).

    Der Advancer reicht Nachrichten-Felder, nicht Bytes: der Adapter baut
    daraus den Broker-Body *und* den ``MessageContext`` der Middleware, ohne
    den Body wieder zu dekodieren.
    """

    workflow_id: str
    step_path: str
    task_id: str
    task_name: str
    queue: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, Any] = field(default_factory=dict)

    def to_message(self) -> dict[str, Any]:
        """Die Wire-Form des Schritts — eine Quelle für Worker und Client,
        damit die beiden Enqueue-Pfade nicht auseinanderlaufen."""
        return {
            "task": {"id": self.task_id, "queue": self.queue, "name": self.task_name},
            "args": list(self.args),
            "kwargs": self.kwargs,
        }


class WorkflowStepPublisher(Protocol):
    """Enqueue-Seam des Advancers (KTD7). ``pre_enqueue`` ist ein Gate über
    die ganze Front: wirft es, wird kein Schritt der Front publiziert.
    ``publish`` veröffentlicht einen Schritt und feuert danach
    ``post_enqueue`` sowie die Signale — der Advancer markiert erst nach dem
    ``PublishReceipt`` (publish-then-mark, AE5)."""

    async def pre_enqueue(self, steps: Sequence[WorkflowStep]) -> None: ...

    async def publish(self, step: WorkflowStep) -> PublishReceipt: ...


class WorkflowStepEnqueuer:
    """Enqueue-Adapter für Workflow-Schritte (U11, KTD7) — von Worker und
    Client geteilt, damit beide Wege dieselbe Behandlung geben.

    Ein Schritt läuft durch dieselbe Middleware-Kette wie ``Client.schedule``
    (``pre_enqueue`` → Publish → ``post_enqueue``) und löst danach
    ``TASK_ENQUEUED`` mit unverändertem Payload sowie
    ``WORKFLOW_STEP_ENQUEUED`` mit der Workflow-Identität aus.

    Der ``MessageContext`` einer Front wird zwischen ``pre_enqueue`` und
    ``publish`` behalten: eine Middleware, die dort ein Feld ergänzt, sieht es
    in ``post_enqueue`` wieder (dieselbe Eigenschaft wie im Client, wo beide
    Hooks dasselbe Dict sehen). Eingetragen wird erst, wenn ``pre_enqueue``
    durchlief, und beim Publish wieder entnommen — ein abgelehnter oder
    scheiternder Schritt lässt nichts zurück.
    """

    def __init__(
        self,
        *,
        publish: Callable[[str, bytes, dict[str, Any]], Awaitable[PublishReceipt]],
        dispatch: Callable[..., Awaitable[Any]],
        get_chain: Callable[[], Any],
    ) -> None:
        self._publish = publish
        self._dispatch = dispatch
        self._get_chain = get_chain
        self._contexts: dict[tuple[str, str], MessageContext] = {}

    @staticmethod
    def _context(step: WorkflowStep) -> MessageContext:
        return workflow_step_message_context(
            task_id=step.task_id,
            task_name=step.task_name,
            queue=step.queue,
            args=list(step.args),
            kwargs=step.kwargs,
            workflow_id=step.workflow_id,
            step_path=step.step_path,
        )

    async def pre_enqueue(self, steps: Sequence[WorkflowStep]) -> None:
        chain = self._get_chain()
        contexts = [(step, self._context(step)) for step in steps]
        for _step, ctx in contexts:
            # Wirft ein Hook, propagiert die Ausnahme, bevor irgendetwas
            # publiziert oder gemerkt ist (R15/R17).
            await chain.run_pre_enqueue(ctx)
        for step, ctx in contexts:
            self._contexts[(step.workflow_id, step.step_path)] = ctx

    async def publish(self, step: WorkflowStep) -> PublishReceipt:
        ctx = self._contexts.pop(
            (step.workflow_id, step.step_path), None
        ) or self._context(step)
        receipt = await self._publish(
            step.queue, encode_json(step.to_message()), step.headers
        )
        chain = self._get_chain()
        await chain.run_post_enqueue(ctx)
        await self._dispatch(
            Signal.TASK_ENQUEUED,
            task_id=step.task_id,
            task_name=step.task_name,
            queue=step.queue,
        )
        await self._dispatch(
            Signal.WORKFLOW_STEP_ENQUEUED,
            task_id=step.task_id,
            task_name=step.task_name,
            queue=step.queue,
            workflow_id=step.workflow_id,
            step_path=step.step_path,
        )
        return receipt


__all__ = ["WorkflowStep", "WorkflowStepEnqueuer", "WorkflowStepPublisher"]
