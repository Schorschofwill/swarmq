"""Ein Fire des Schedulers (Plan 0.12.0, MR 0; verbatim aus ``loop.py``).

``_fire_one`` baut die Nachricht, setzt fuer statische Crons den Dedup-Claim,
veroeffentlicht und schreibt das Fire owner-geprueft fest. Mixin ueber dem
``SchedulerLoop``-Zustand. Uhr und Logger kommen ueber ``self._wall_time`` und
``self._log`` aus ``swarmq.scheduler.loop`` (Q17): ``patch("swarmq.scheduler.loop.time")``
und der Logger-Name ``swarmq.scheduler.loop`` gelten unveraendert, ohne
Rueckimport.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields
from swarmq.scheduler.definition import ScheduleDefinition, make_schedfire_key
from swarmq.serialization import compute_dedup_hash, encode_json
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts import Publisher
    from swarmq.scheduler.definition import ScheduleStore
    from swarmq.signals import SignalDispatcher


class SchedulerFire:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    if TYPE_CHECKING:
        _backend: Any  # ``loop.SchedulerBackend``; kein Import, sonst Zyklus
        _broker: Publisher
        _config: SwarmQConfig
        _store: ScheduleStore
        _signals: SignalDispatcher
        _owner_token: str
        _state: str
        _reported_names: frozenset[str]

        @property
        def _log(self) -> Any: ...

        def _wall_time(self) -> float: ...

        def _plan_next_fire(self, schedule_name: str, definition: ScheduleDefinition, now_ms: int) -> int | None: ...

        async def _park_unplannable(
            self, schedule_name: str, score_ms: int, definition: ScheduleDefinition
        ) -> None: ...

        async def _advanced_definition(
            self,
            schedule_name: str,
            score_ms: int,
            definition: ScheduleDefinition,
            next_fire_ts_ms: int,
            now_ms: int,
        ) -> ScheduleDefinition | None: ...

    async def _fire_one(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
    ) -> None:
        # Use the authoritative zset score, not definition.fire_ts_ms
        # (Unit 7). The two can diverge briefly during a static-
        # cron update where update_static_schedule writes a new
        # fire_ts_ms into defs before the ZADD lands, or if an operator
        # hand-edits the zset. The dedup key must match what will
        # actually be published so a redelivered message hits the
        # same SETNX cell.
        schedfire_key = make_schedfire_key(schedule_name, score_ms)

        now_ms = int(self._wall_time() * 1000)
        next_fire_ts_ms = self._plan_next_fire(schedule_name, definition, now_ms)
        if next_fire_ts_ms is None:
            await self._park_unplannable(schedule_name, score_ms, definition)
            return

        # Build the task message body — same shape as Client.schedule.
        task_id = str(uuid.uuid7())
        body = encode_json(
            {
                "task": {
                    "id": task_id,
                    "queue": definition.queue,
                    "name": definition.task_name,
                },
                "args": list(definition.args),
                "kwargs": definition.kwargs,
            }
        )

        # --- R3.5 Unit 9: pre-publish SETNX for static crons, header
        # propagation for client-originated deferred (cli_unique) ---
        # Stamp HEADER_ENQUEUED_AT so R4.4 max_age
        # works for scheduler-fired tasks (cron + delay + eta). Without
        # this the worker's _check_max_age_expired saw no header and
        # treated every scheduled message as non-expiring.
        headers: dict[str, Any] = {
            "x-swarmq-schedfire": schedfire_key,
            "x-swarmq-enqueued-at": int(self._wall_time()),
        }
        dup_detected = False

        if definition.source == "cli_unique" and definition.dedup_hash:
            # Client already SETNX'd this task at schedule(delay=/eta=) time.
            # Propagate the hash + intended mode so the worker releases
            # at the right point. unique_until is persisted on the
            # definition so post-deploy TaskInfo
            # changes cannot silently flip release semantics.
            headers["x-swarmq-unique-hash"] = definition.dedup_hash
            headers["x-swarmq-unique-until"] = definition.unique_until
        else:
            # Static cron or plain CLI — resolve TaskInfo and SETNX here.
            task_info = self._resolve_task_info(definition.task_name)
            if task_info is None and schedule_name not in self._reported_names:
                # A schedule whose task was renamed or
                # removed previously published into the void with no
                # trace. Warn-only, deliberately NOT a skip: in multi-
                # module deployments the task may live in ANOTHER
                # worker's registry — a hard skip would break those.
                # Crons a live worker reports (U12, KTD10) belong to such
                # a registry and therefore do not warn.
                self._log.warning(
                    "scheduler_task_not_in_local_registry",
                    schedule_name=schedule_name,
                    task_name=definition.task_name,
                    hint=(
                        "the leader's registry does not know this task; "
                        "if no other worker fleet registers it, the "
                        "message lands unconsumed — remove the schedule "
                        "or fix the task name"
                    ),
                )
            if task_info is not None and task_info.unique_for is not None:
                dedup_hash = compute_dedup_hash(
                    definition.task_name,
                    tuple(definition.args),
                    dict(definition.kwargs),
                )
                try:
                    existing = await self._backend.claim_unique(
                        dedup_hash,
                        task_id,
                        task_info.unique_for * 1000,
                        op_timeout=self._config.backend_op_timeout,
                    )
                except BackendConnectionError as exc:
                    self._log.warning(
                        "scheduler_unique_setnx_failed",
                        schedule_name=schedule_name,
                        **_error_fields(exc),
                    )
                    return  # Retry on next poll; commit not called.

                if existing is not None:
                    await self._emit_duplicate_detected(
                        existing_task_id=existing,
                        task_name=definition.task_name,
                        dedup_hash=dedup_hash,
                    )
                    self._log.info(
                        "dedup_detected",
                        existing_task_id=existing,
                        task_name=definition.task_name,
                        dedup_hash=dedup_hash,
                        source="scheduler.fire",
                    )
                    dup_detected = True
                else:
                    headers["x-swarmq-unique-hash"] = dedup_hash
                    headers["x-swarmq-unique-until"] = task_info.unique_until

        if not dup_detected:
            try:
                await self._broker.publish(
                    definition.queue,
                    body,
                    headers=headers,
                )
            except Exception as exc:
                self._log.warning(
                    "scheduler_publish_failed",
                    schedule_name=schedule_name,
                    **_error_fields(exc),
                )
                return  # Do NOT commit — retry on the next poll tick.

        # The commit runs even on dup-skip: commit_fire MUST advance the
        # zset (otherwise the same schedule re-fires on every poll tick
        # forever — see plan Unit 9 Risks "Double-SETNX-Race"
        # commit-on-fail rationale).
        updated_def = await self._advanced_definition(
            schedule_name,
            score_ms,
            definition,
            next_fire_ts_ms,
            now_ms,
        )

        # fired_ts_ms: the commit only lands while the zset still holds the
        # score we fired. A `schedule remove` (disable/permanent) or a
        # re-plan that raced this fire keeps its change instead of being
        # overwritten with the pre-poll definition (0.9.0).
        try:
            committed = await self._store.commit_fire(
                schedule_name,
                next_fire_ts_ms,
                updated_def,
                self._owner_token,
                fired_ts_ms=score_ms,
            )
        except BackendConnectionError as exc:
            self._log.warning(
                "scheduler_commit_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
                dup_detected=dup_detected,
            )
            # On dup: next poll will re-observe the same score and re-emit
            # the dup signal. Self-heals when Valkey recovers — signal
            # noise is the accepted v1 trade-off for operator simplicity.
            return

        if not committed:
            # Owner-check rejected — we lost the lock between publish and
            # commit. The new leader will handle the next poll.
            self._log.warning(
                "scheduler_commit_owner_mismatch",
                schedule_name=schedule_name,
            )
            self._state = "standby"

    def _resolve_task_info(self, task_name: str) -> TaskInfo | None:
        """Find the registered TaskInfo for *task_name* (R3.5 Unit 9).

        Scheduler has no direct registry reference — it walks the global
        ``Task._registered_tasks`` list. Linear scan is O(n); with n
        typically < 100 tasks this is well below the per-fire
        ScheduleStore + broker round-trip cost, so no index is introduced.
        """
        for task_class in Task._registered_tasks:
            ti: TaskInfo | None = getattr(task_class, "task_info", None)
            if ti is not None and ti.name == task_name:
                return ti
        return None

    async def _emit_duplicate_detected(
        self,
        *,
        existing_task_id: str,
        task_name: str,
        dedup_hash: str,
    ) -> None:
        """Dispatch TASK_DUPLICATE_DETECTED with source='scheduler.fire'.

        Wrapped here because :meth:`_emit` is specialised for
        scheduler-state signals (accepts only ``worker_id``). Handler
        exceptions are swallowed via the same SignalDispatcher contract.
        """
        try:
            await self._signals.dispatch(
                Signal.TASK_DUPLICATE_DETECTED,
                existing_task_id=existing_task_id,
                task_name=task_name,
                dedup_hash=dedup_hash,
                source="scheduler.fire",
            )
        except Exception as exc:
            self._log.warning(
                "scheduler_duplicate_signal_dispatch_error",
                task_name=task_name,
                **_error_fields(exc),
            )
