"""Worker-side workflow orchestration helpers (U3, U5, U6, U7).

This module holds the worker-side machinery that advances workflows —
distinct from the infrastructure-free topology layer in ``workflow.py``
(client/producer side). The ``ChordStore`` contract wraps the atomic
``chord_complete.lua`` script; the :class:`WorkflowAdvancer` and its
enqueue/fan-in helpers live here too.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, override

from swarmq.contracts import PublishReceipt
from swarmq.contracts.coordination import ChordStore
from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
    WorkflowNotFound,
    WorkflowResultExpired,
    WorkflowStateConflict,
    WorkflowValidationError,
)
from swarmq.headers import (
    HEADER_ENQUEUED_AT,
    HEADER_WORKFLOW_ID,
    HEADER_WORKFLOW_STEP,
)
from swarmq.logging import get_logger
from swarmq.serialization import decode_json
from swarmq.workflow import (
    Chain,
    Chord,
    Group,
    Signature,
    WorkflowNode,
    _node_from_dict,
)

# U12 (KTD13): Adressierung (``workflow_topology``) und Enqueue-Seam
# (``workflow_enqueue``) liegen in eigenen Modulen. Die ``X as X``-Form ist
# der ausdrueckliche Re-Export (PEP 484): bestehende Importwege
# (``client/workflows.py``, ``worker/*``, ``app.py``, ``__init__.py``)
# bleiben unveraendert, und kein Linter haelt sie fuer toten Import.
from swarmq.workflow_enqueue import WorkflowStep as WorkflowStep
from swarmq.workflow_enqueue import WorkflowStepEnqueuer as WorkflowStepEnqueuer
from swarmq.workflow_enqueue import WorkflowStepPublisher as WorkflowStepPublisher
from swarmq.workflow_failure import WorkflowFailure
from swarmq.workflow_state import (
    _TERMINAL_STATUSES,
    _StaleWorkflowState,
    _WorkflowAlreadyTerminal,
)
from swarmq.workflow_state import STATUS_CANCELLED as STATUS_CANCELLED
from swarmq.workflow_state import STATUS_COMPLETED as STATUS_COMPLETED
from swarmq.workflow_state import STATUS_FAILED as STATUS_FAILED
from swarmq.workflow_state import STATUS_RUNNING as STATUS_RUNNING
from swarmq.workflow_state import WORKFLOW_CAS_MAX_ROUNDS as WORKFLOW_CAS_MAX_ROUNDS
from swarmq.workflow_state import WORKFLOW_STATE_VERSION as WORKFLOW_STATE_VERSION
from swarmq.workflow_state import build_initial_workflow_state as build_initial_workflow_state
from swarmq.workflow_topology import _frontier
from swarmq.workflow_topology import aggregate_result_id as aggregate_result_id
from swarmq.workflow_topology import initial_frontier as initial_frontier
from swarmq.workflow_topology import inject_result as inject_result
from swarmq.workflow_topology import node_at_path as node_at_path
from swarmq.workflow_topology import split_path as split_path
from swarmq.workflow_topology import workflow_step_task_id as workflow_step_task_id

logger = get_logger(__name__)

if TYPE_CHECKING:
    from swarmq.contracts import ResultBackend


class WorkflowHandle:
    """Reattachable handle to a fire-and-forget workflow (R13, R14, R15).

    Constructed by ``Client.apply()`` and reconstructed by
    ``Client.workflow(workflow_id)``. Carries no in-process workflow state
    beyond the id — everything is read from Valkey, so the handle survives
    the originating client process (R10).
    """

    def __init__(
        self,
        backend: ResultBackend,
        workflow_id: str,
        unpack: Callable[[bytes], Any],
        cancel_ttl_ms: int,
    ) -> None:
        self._backend = backend
        self.workflow_id = workflow_id
        self._unpack = unpack
        self._cancel_ttl_ms = cancel_ttl_ms

    async def _load_state(self) -> dict[str, Any]:
        raw = await self._backend.get_workflow_state(self.workflow_id)
        if raw is None:
            raise WorkflowNotFound(self.workflow_id)
        return decode_json(raw)  # type: ignore[no-any-return]  # decode_json liefert Any aus JSON

    async def status(self) -> str:
        state = await self._load_state()
        return str(state["status"])

    async def children(self) -> dict[str, Any]:
        """Per-member status/result introspection (R15), reconstructed at
        read time. Returns a map of every enqueued step's topology path ->
        ``{"task_id", "status", "result"?}``, where ``status`` is
        ``"running"`` (no result stored yet), ``"completed"`` (result
        present), or ``"failed"`` (result present but errored/cancelled).
        Empty before any step is enqueued.

        Read-time reconstruction (topology + ``step_tasks`` + result
        existence) avoids holding a separate per-member status field
        transactionally in sync with the chord set."""
        state = await self._load_state()
        step_tasks: dict[str, str] = state.get("step_tasks", {})
        if not step_tasks:
            return {}
        paths = list(step_tasks)
        task_ids = [step_tasks[p] for p in paths]
        raws = await self._backend.get_results(task_ids)
        out: dict[str, Any] = {}
        for path, tid, raw in zip(paths, task_ids, raws, strict=True):
            if raw is None:
                out[path] = {"task_id": tid, "status": "running"}
                continue
            entry: dict[str, Any] = {"task_id": tid, "status": "completed"}
            try:
                entry["result"] = self._unpack(raw)
            except Exception:  # noqa: BLE001 - gespeichertes Fehlerergebnis meldet Status failed
                # Stored result is an error/cancel — report failed status
                # rather than propagating the exception from an introspection
                # call; get() is the surface that raises.
                entry["status"] = "failed"
            out[path] = entry
        return out

    async def cancel(self) -> bool:
        """Set the workflow-level cancel flag and wake any waiter (R17).

        Effect: ``get()`` raises promptly; steps not
        yet picked up — including already-enqueued group/chord members —
        are stopped at worker pickup and finished as cancelled; a step
        already mid-execution runs to completion (cooperative model, no
        hard kill), and its ``advance()`` then refuses to enqueue
        downstream steps.
        """
        newly = await self._backend.set_workflow_cancel_flag(self.workflow_id, ttl_ms=self._cancel_ttl_ms)
        await self._backend.notify_workflow(self.workflow_id)
        return newly

    async def get(self, timeout: float = 30.0) -> Any:
        """Block until the workflow reaches a terminal status, then return
        the final result (R13). Raises on failed/cancelled; raises
        ResultNotReadyError on timeout.

        Waits on the stable workflow notification channel (woken by U5
        completion / U6 fail-mark) with a state-key poll fallback — the
        notification is best-effort, the poll guarantees liveness."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while True:
            state = await self._load_state()
            if state["status"] in _TERMINAL_STATUSES:
                return await self._resolve_terminal(state)
            # Cancel liveness (R17): a cancel() between steps (or while a
            # step is slow) sets the flag but no advance() has run yet to
            # write STATUS_CANCELLED. Surface it here so get() returns
            # promptly instead of hanging until timeout. Checking the flag
            # (not writing state) avoids a write race with a concurrent
            # advance() that may be completing the workflow.
            if await self._backend.is_workflow_cancelled(self.workflow_id):
                raise TaskCancelledError(self.workflow_id)
            remaining = deadline - loop.time()
            if not remaining > 0:  # nan zählt als abgelaufen (F3-43)
                raise ResultNotReadyError(f"workflow {self.workflow_id} did not complete within {timeout}s")
            # Vertragsmethode (U15): wartet bis zur Benachrichtigung
            # oder zum Slice-Ende; der Poll oben trägt die Liveness.
            try:
                await self._backend.wait_for_workflow(self.workflow_id, timeout=min(0.2, remaining))
            except Exception:  # noqa: BLE001 - Pub/Sub nicht verfuegbar, Poll-Fallback
                # Pub/Sub nicht verfügbar — Poll-Fallback.
                await asyncio.sleep(min(0.05, max(0.0, remaining)))

    async def _resolve_terminal(self, state: dict[str, Any]) -> Any:
        status = state["status"]
        if status == STATUS_CANCELLED:
            raise TaskCancelledError(self.workflow_id)
        if status == STATUS_FAILED:
            err: dict[str, Any] = state.get("error") or {}
            # ``class`` fehlt in Zustaenden aelterer Worker (P9, Q2).
            raise TaskError(err.get("message", "workflow failed"), error_class=err.get("class"))
        # completed — resolve the final result via the terminal task id.
        final_id = state.get("final_result_task_id")
        if final_id is None:
            return None
        raw = await self._backend.get_result(final_id)
        if raw is None:
            # State says completed but the final result is gone → it expired
            # (R6): surface it rather than returning a misleading None.
            raise WorkflowResultExpired(
                f"workflow {self.workflow_id} completed but its final result "
                "expired before retrieval; raise the result/workflow TTL"
            )
        return self._unpack(raw)


# --------------------------------------------------------------------------
# Topology addressing & frontier navigation (shared by U4 apply + U5 advance)
# --------------------------------------------------------------------------
#
# Each node in a persisted topology is addressed by a path string built from
# the root: chain step i -> ``/{i}``; group member i -> ``/g{i}``; chord
# header -> ``/h`` (its Group members are ``/h/g{i}``); chord body -> ``/b``.
# The path is carried on each step message (HEADER_WORKFLOW_STEP) so the
# worker can navigate the topology to decide what to enqueue next.


class WorkflowAdvancer(WorkflowFailure):
    """Worker-side workflow advancement (U5). Called at the success seam
    after a workflow-step's result is stored. Drives the topology forward:
    next chain step (with result injection), chord/group fan-in via the
    atomic ``ChordStore``, and terminal completion.

    Collaborators are injected so the DAG logic is unit-testable with fakes.
    ``publisher`` is the enqueue seam (:class:`WorkflowStepPublisher` —
    Middleware-Kette und Signale liegen dort, U11), ``resolve_queue`` maps a
    task name to its queue, ``make_task_id`` mints a DETERMINISTIC task id
    from ``(workflow_id, step_path)`` (see :func:`workflow_step_task_id` —
    determinism keeps redelivered steps idempotent), ``unpack`` decodes a
    stored result to its value.
    """

    def __init__(
        self,
        backend: ResultBackend,
        chord_manager: ChordStore,
        *,
        publisher: WorkflowStepPublisher,
        resolve_queue: Callable[[str], str],
        make_task_id: Callable[[str, str], str],
        unpack: Callable[[bytes], Any],
        ttl_ms: int,
    ) -> None:
        self._backend = backend
        self._chord = chord_manager
        self._publisher = publisher
        self._resolve_queue = resolve_queue
        self._make_task_id = make_task_id
        self._unpack = unpack
        self._ttl_ms = ttl_ms

    async def advance(self, wf_id: str, completed_path: str, completed_task_id: str, result: Any) -> None:
        """Entry point: a workflow step at *completed_path* finished with
        *result*. Decide and enqueue what runs next, or mark the workflow
        complete. Idempotent against redelivery via the chord set + the
        per-step ``next-enqueued`` markers."""
        raw = await self._backend.get_workflow_state(wf_id)
        if raw is None:
            # State gone (expired/cancelled-cleaned) — nothing to advance.
            # BZ-13: say so; the chain ends here, and without this line it
            # ended without a trace.
            logger.warning(
                "workflow_state_missing",
                workflow_id=wf_id,
                step_path=completed_path,
                task_id=completed_task_id,
            )
            return
        state: dict[str, Any] = decode_json(raw)
        if state["status"] != STATUS_RUNNING:
            return  # already terminal
        try:
            if await self._backend.is_workflow_cancelled(wf_id):
                await self._mark_terminal(wf_id, state, STATUS_CANCELLED, None, None)
                return

            topology = _node_from_dict(state["topology"])
            await self._on_leaf_complete(wf_id, state, topology, completed_path, completed_task_id, result)
        except _StaleWorkflowState:
            await self._reconcile(wf_id, state)
        except _WorkflowAlreadyTerminal:
            return

    @override
    async def _reconcile(self, wf_id: str, local: dict[str, Any]) -> None:
        """Konfliktbehandlung (R23): den frischen Zustand lesen und die lokalen
        Aenderungen hineinmergen statt den Advance zu wiederholen -- publiziert
        und den Chord-Zaehler bewegt hat dieser Advance bereits.
        ``step_tasks`` wird additiv vereinigt (Bestand gewinnt); ein lokaler
        Terminalstatus bringt ``status``, ``final_result_task_id`` und
        ``error`` mit. Ist der frische Zustand terminal, endet der Advance
        ohne Schreibung. Nach ``WORKFLOW_CAS_MAX_ROUNDS`` verlorenen Runden
        (die erste zaehlt mit): :class:`WorkflowStateConflict`."""
        local_terminal = local["status"] in _TERMINAL_STATUSES
        for _round in range(WORKFLOW_CAS_MAX_ROUNDS - 1):
            raw = await self._backend.get_workflow_state(wf_id)
            if raw is None:
                return
            fresh: dict[str, Any] = decode_json(raw)
            if fresh["status"] in _TERMINAL_STATUSES:
                return
            for path, task_id in local["step_tasks"].items():
                fresh["step_tasks"].setdefault(path, task_id)
            if local_terminal:
                fresh["status"] = local["status"]
                fresh["final_result_task_id"] = local.get("final_result_task_id")
                fresh["error"] = local.get("error")
            try:
                await self._persist(wf_id, fresh)
            except _StaleWorkflowState:
                continue
            if local_terminal:
                await self._backend.notify_workflow(wf_id)
            return
        raise WorkflowStateConflict(
            f"workflow {wf_id}: state compare-and-set lost {WORKFLOW_CAS_MAX_ROUNDS} "
            "rounds in a row; the step is not finished and must be redelivered"
        )

    # --- core recursion -----------------------------------------------------

    async def _on_leaf_complete(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        path: str,
        task_id: str,
        result: Any,
    ) -> None:
        if path == "":
            # The whole workflow is a single signature. Sonst fehlt der
            # Pfad-Header -- nie still abschliessen (S-009).
            if not isinstance(topology, Signature):
                raise WorkflowValidationError(f"step without path in a {type(topology).__name__} workflow")
            await self._mark_terminal(wf_id, state, STATUS_COMPLETED, task_id, None)
            return

        parent_path, last = split_path(path)
        parent = node_at_path(topology, parent_path)

        if isinstance(parent, Chain):
            i = int(last)
            if i + 1 < len(parent.steps):
                await self._enqueue_node(
                    wf_id,
                    state,
                    parent.steps[i + 1],
                    f"{parent_path}/{i + 1}",
                    inject=result,
                )
                await self._persist(wf_id, state)
            else:
                # Last chain step done → the chain itself completed.
                await self._on_container_complete(
                    wf_id,
                    state,
                    topology,
                    parent_path,
                    result_task_id=task_id,
                    result_value=None,
                )
        elif isinstance(parent, Group):
            await self._on_group_member(wf_id, state, topology, parent, parent_path, task_id, result)
        elif isinstance(parent, Chord):
            # Only the body resolves to a chord-child leaf ("/b"); header
            # members live under "/h/g{i}" and hit the Group branch.
            await self._on_container_complete(
                wf_id,
                state,
                topology,
                parent_path,
                result_task_id=task_id,
                result_value=None,
            )
        else:
            raise WorkflowValidationError(f"step path {path!r} lies under a {type(parent).__name__}")

    async def _on_group_member(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        group: Group,
        group_path: str,
        member_task_id: str,
        result: Any,  # noqa: ARG002 - einheitliche Signatur der Member-Callbacks
    ) -> None:
        expected_n = len(group.members)
        fired = await self._chord.record_member(
            wf_id, group_path, member_task_id, expected=expected_n, ttl_ms=self._ttl_ms
        )
        if not fired:
            return  # not the last member (or a redelivery) — nothing to do
        # Group complete: gather member results in DEFINITION order (not the
        # unordered chord set), R5. Der Zustand stammt vom Eintritt in
        # advance() und kann veraltet sein -- vor dem Bau der Liste nachziehen.
        await self._refresh_step_tasks(wf_id, state)
        member_task_ids = self._ordered_member_ids(wf_id, group, group_path)
        ordered_results = await self._gather_results(member_task_ids)

        # Is this group a chord header? Then enqueue the chord body with the
        # ordered results. Otherwise the group's result is the list itself.
        gp_parent, gp_last = split_path(group_path)
        if gp_last == "h":
            chord_node = node_at_path(topology, gp_parent)
            if not isinstance(chord_node, Chord):
                raise WorkflowValidationError(f"expected Chord at {gp_parent!r}, got {type(chord_node).__name__}")
            await self._enqueue_node(
                wf_id,
                state,
                chord_node.body,
                f"{gp_parent}/b",
                inject=ordered_results,
            )
            await self._persist(wf_id, state)
        else:
            await self._on_container_complete(
                wf_id,
                state,
                topology,
                group_path,
                result_task_id=None,
                result_value=ordered_results,
            )

    async def _on_container_complete(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        node_path: str,
        *,
        result_task_id: str | None,
        result_value: Any,
    ) -> None:
        """A composite node (chain/group/chord) at *node_path* finished with
        a result (a stored task id, or an in-memory aggregate value).
        Propagate to its container, or finish the workflow at the root."""
        if node_path == "":
            await self._mark_terminal(wf_id, state, STATUS_COMPLETED, result_task_id, result_value)
            return

        parent_path, last = split_path(node_path)
        parent = node_at_path(topology, parent_path)

        # Materialise a result value to inject / propagate.
        if result_task_id is not None:
            value = await self._result_value(result_task_id)
        else:
            value = result_value

        if isinstance(parent, Chain):
            i = int(last)
            if i + 1 < len(parent.steps):
                await self._enqueue_node(
                    wf_id,
                    state,
                    parent.steps[i + 1],
                    f"{parent_path}/{i + 1}",
                    inject=value,
                )
                await self._persist(wf_id, state)
            else:
                await self._on_container_complete(
                    wf_id,
                    state,
                    topology,
                    parent_path,
                    result_task_id=result_task_id,
                    result_value=value,
                )
        elif isinstance(parent, Group):
            # A composite that is itself a group member: treat its completion
            # as that member completing (fan-in counts it once via a synthetic
            # member id derived from the node path).
            synthetic_id = await self._materialise(wf_id, node_path, result_task_id, value)
            await self._on_group_member(wf_id, state, topology, parent, parent_path, synthetic_id, value)
        elif isinstance(parent, Chord):
            await self._on_container_complete(
                wf_id,
                state,
                topology,
                parent_path,
                result_task_id=result_task_id,
                result_value=value,
            )
        else:
            raise WorkflowValidationError(f"node path {node_path!r} lies under a {type(parent).__name__}")

    # --- enqueue + state helpers -------------------------------------------

    async def _enqueue_node(
        self,
        wf_id: str,
        state: dict[str, Any],
        node: WorkflowNode,
        node_path: str,
        *,
        inject: Any,
    ) -> None:
        """Publish the frontier of *node* (a chain step / chord body) as
        workflow-step messages, injecting *inject* into mutable leaves.
        publish-then-mark: each step is published (on a publisher-confirms
        channel) before its step-task id is recorded, so a crash never loses
        the step (at worst a rare idempotent double-enqueue). Seit U20 ist die
        Reihenfolge strukturell erzwungen: :meth:`mark_enqueued` verlangt den
        ``PublishReceipt`` aus ``Publisher.publish`` (AE5)."""
        enqueued_at = int(time.time())
        steps: list[WorkflowStep] = []
        for step_path, signature in _frontier(node, node_path):
            # next-enqueued marker (publish-then-mark): skip if already done.
            if step_path in state["step_tasks"]:
                continue
            injected = inject_result(signature, inject)
            task_id = self._make_task_id(wf_id, step_path)
            queue = self._resolve_queue(injected.task_name)
            steps.append(
                WorkflowStep(
                    workflow_id=wf_id,
                    step_path=step_path,
                    task_id=task_id,
                    task_name=injected.task_name,
                    queue=queue,
                    args=list(injected.args),
                    kwargs=injected.kwargs,
                    headers={
                        HEADER_WORKFLOW_ID: wf_id,
                        HEADER_WORKFLOW_STEP: step_path,
                        HEADER_ENQUEUED_AT: enqueued_at,
                    },
                )
            )
        if not steps:
            return
        # R15: ``pre_enqueue`` ist ein Gate ueber die GANZE Front. Liefe es je
        # Schritt, koennte eine ablehnende Middleware eine Gruppe halb
        # eingereiht hinterlassen — der Fan-in erreichte N nie und der
        # Workflow haengte bis zum TTL.
        await self._publisher.pre_enqueue(steps)
        for step in steps:
            receipt = await self._publisher.publish(step)
            self.mark_enqueued(state, step.step_path, step.task_id, receipt)

    def mark_enqueued(self, state: dict[str, Any], step_path: str, task_id: str, receipt: PublishReceipt) -> None:
        """publish-then-mark (AE5): die Markierung verlangt den Beleg der
        Veröffentlichung. Ohne ``PublishReceipt`` — den nur
        ``Publisher.publish`` erzeugt — ist die Markierung ein Typfehler
        (mypy) und zur Laufzeit ein ``TypeError``."""
        if not isinstance(receipt, PublishReceipt):  # pyright: ignore[reportUnnecessaryIsInstance]  # Laufzeitschutz fuer ungetypte Aufrufer (AE5)
            raise TypeError(
                f"mark_enqueued requires the PublishReceipt returned by Publisher.publish, got {type(receipt).__name__}"
            )
        state["step_tasks"][step_path] = task_id

    async def _refresh_step_tasks(self, wf_id: str, state: dict[str, Any]) -> None:
        """Die zuletzt persistierten Schritt-Kennungen in *state* nachziehen.

        ``advance()`` liest den Zustand einmal beim Eintritt; bis der Fan-in
        laeuft, koennen nebenlaeufige Advances weitere Schritte markiert haben.
        Vereinigt wird rein additiv -- ``step_tasks`` wird nirgends geloescht
        und die Kennungen sind deterministisch, ein Konflikt kann also nicht
        entstehen.

        Seit KTD11 schliesst das Compare-and-Set in :meth:`_persist` das
        verlorene Zustands-Update. Die Gliedkennungen des Fan-ins liest diese
        Lesung nicht mehr: :meth:`_ordered_member_ids` leitet sie ab (E-073),
        weil ein Schritt publiziert wird, BEVOR sein Marker persistiert ist,
        und schneller enden kann als die Persistenz (F4-16). Die Lesung bleibt
        für drei Dinge: Sie honoriert einen frischen Terminalstatus (R24),
        dann endet der Advance ohne Schreibung
        (:class:`_WorkflowAlreadyTerminal`); sie übernimmt die Version als
        Erwartung des Compare-and-Set; und sie zieht die Marker nach, mit denen
        :meth:`_enqueue_node` schon eingereihte Schritte überspringt.
        """
        raw = await self._backend.get_workflow_state(wf_id)
        if raw is None:
            return
        persisted: dict[str, Any] = decode_json(raw)
        if persisted["status"] in _TERMINAL_STATUSES:
            raise _WorkflowAlreadyTerminal(wf_id)
        step_tasks: dict[str, str] = state["step_tasks"]
        for path, task_id in persisted.get("step_tasks", {}).items():
            step_tasks.setdefault(path, task_id)
        # Bis hierher hat dieser Advance nichts Lokales veraendert (der Fan-in
        # publiziert erst danach), der lokale Zustand ist also nach dem
        # Nachziehen dem persistierten gleich -- dessen Version darf das CAS
        # in _persist als Erwartung nehmen.
        state["version"] = int(persisted.get("version", 0))

    def _terminal_result_id(
        self,
        wf_id: str,
        node: WorkflowNode,
        path: str,
        member_path: str | None = None,
    ) -> str:
        """Kennung des Ergebnisses, das *node* an *path* als Ganzes liefert.

        Spiegelt die beiden Zweige von :meth:`_materialise`: ein Blatt, eine
        Kette und ein Chord laufen auf die **echte** Schritt-Kennung ihres
        Abschlussschritts hinaus, eine Gruppe auf die Aggregatkennung (beide
        ueber :func:`aggregate_result_id`, damit sie nicht auseinanderdriften).
        Die Schritt-Kennung eines Blatts wird abgeleitet
        (``self._make_task_id(wf_id, path)``, dieselbe Funktion, mit der
        :meth:`_enqueue_node` und ``apply()`` das Blatt einreihen), nicht in
        ``step_tasks`` nachgeschlagen: Der Marker kann beim Fan-in noch fehlen,
        das Ergebnis liegt dann trotzdem vor (F4-16, E-073).

        ``member_path`` ist der Pfad des GLIEDS, mit dem die Rekursion begann;
        sie reicht ihn unveraendert durch. Das ist load-bearing:
        ``_materialise`` laeuft genau einmal je Glied, im ``Group``-Zweig von
        :meth:`_on_container_complete` und mit dem GLIEDPFAD --
        ``result_task_id=None`` wandert aus einer inneren Gruppe durch die
        Ketten- und Chord-Ebenen nach oben, ohne unterwegs etwas abzulegen.
        ``chain(a, group(b, c))`` hat sein Aggregat deshalb unter
        ``<gruppe>/g<i>``, nicht unter dem tiefsten Pfad. Der erste Anlauf
        nahm den tiefsten Pfad und lief in ein ``WorkflowResultExpired``, das
        faelschlich die TTL beschuldigte.

        Rein lesend und rekursiv; die Tiefengrenze setzt ``_validate`` schon
        bei der Konstruktion (``DEFAULT_MAX_WORKFLOW_DEPTH``).
        """
        if member_path is None:
            member_path = path
        if isinstance(node, Signature):
            return self._make_task_id(wf_id, path)
        if isinstance(node, Chain):
            last = len(node.steps) - 1
            return self._terminal_result_id(wf_id, node.steps[last], f"{path}/{last}", member_path)
        if isinstance(node, Chord):
            return self._terminal_result_id(wf_id, node.body, f"{path}/b", member_path)
        if isinstance(node, Group):  # pyright: ignore[reportUnnecessaryIsInstance]  # defensiv fuer ungetypte Aufrufer
            return aggregate_result_id(wf_id, member_path)
        raise WorkflowValidationError(f"unknown node type {type(node).__name__}")

    def _ordered_member_ids(self, wf_id: str, group: Group, group_path: str) -> list[str]:
        """Gliedkennungen in Definitionsreihenfolge (R5).

        Bis 0.3.0 wurde hier stur ``<gruppe>/g<i>`` in ``step_tasks``
        nachgeschlagen und ein fehlender Schluessel **wortlos uebersprungen**.
        Ein zusammengesetztes Glied legt dort nie einen Schluessel an, sodass
        die Liste still kuerzer wurde, als die Gruppe Glieder hat: B10. Von
        0.4.0 bis 0.12 schlug ein Guard bei unvollständiger Liste laut an --
        auch dann, wenn nur der Marker eines Blatts verspätet war (F4-16).

        Seit E-073 wird jede Gliedkennung abgeleitet
        (:meth:`_terminal_result_id`), auch die eines Blatts. Die Liste hat
        damit immer genau so viele Einträge, wie die Gruppe Glieder hat; die
        Vollständigkeit der Ergebnisse belegt die Fan-in-Zählung
        (``chord_complete.lua``). Fehlt ein Ergebnis trotzdem (abgelaufen,
        verloren), meldet :meth:`_gather_results` ``WorkflowResultExpired`` --
        kein Loch, keine gekürzte Liste.
        """
        return [self._terminal_result_id(wf_id, member, f"{group_path}/g{i}") for i, member in enumerate(group.members)]
