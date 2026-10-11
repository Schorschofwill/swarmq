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

from swarmq.exceptions import BackendConnectionError, PublishIndeterminate, UnroutableMessage
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

        async def _report_unroutable_cron(
            self,
            schedule_name: str,
            definition: ScheduleDefinition,
            exc: BaseException,
        ) -> None: ...

        async def _defer_unroutable_oneshot(
            self,
            schedule_name: str,
            score_ms: int,
            definition: ScheduleDefinition,
            now_ms: int,
            task_id: str,
        ) -> None: ...

        def _note_publish_ok(self) -> None: ...

        async def _note_publish_failure(self, exc: BaseException) -> None: ...

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
        task_id = _fire_task_id(schedule_name, definition)
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
        # BZ-09/B7: Hash eines Unique-Claims, den genau dieses Fire genommen hat.
        own_claim: str | None = None

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
            # BZ-09: Traegt die Definition ``unique_for`` (gemeldet vom Worker,
            # der den Task kennt), gelten beide Werte aus ihr; sonst beide aus
            # der Registry des Leaders, nie gemischt.
            unique_for, unique_until = definition.unique_for, definition.unique_until
            if unique_for is None and task_info is not None:
                unique_for, unique_until = task_info.unique_for, task_info.unique_until
            if unique_for is not None:
                dedup_hash = compute_dedup_hash(
                    definition.task_name,
                    tuple(definition.args),
                    dict(definition.kwargs),
                )
                try:
                    existing = await self._backend.claim_unique(
                        dedup_hash,
                        task_id,
                        unique_for * 1000,
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
                    own_claim = dedup_hash
                    headers["x-swarmq-unique-hash"] = dedup_hash
                    headers["x-swarmq-unique-until"] = unique_until

        if not dup_detected and not await self._publish_fire(
            schedule_name,
            score_ms,
            definition,
            body,
            headers,
            now_ms,
            task_id,
            own_claim,
        ):
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

    async def _publish_fire(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        body: bytes,
        headers: dict[str, Any],
        now_ms: int,
        task_id: str,
        own_claim: str | None = None,
    ) -> bool:
        """Publish eines Fires; ``True``, wenn ``_fire_one`` festschreiben soll.
        *own_claim* ist der Unique-Hash, den dieses Fire beansprucht hat; ein
        Fehlschlag gibt ihn frei, damit der naechste Poll nicht als Duplikat
        endet (BZ-09/B7)."""
        try:
            await self._broker.publish(
                definition.queue,
                body,
                headers=headers,
            )
        except UnroutableMessage as exc:
            # BZ-03: die Queue fehlt. Ein Oneshot wird zurueckgestellt; ein
            # Cron-Fire ist verloren und wird festgeschrieben, sonst
            # feuerte er in jedem Takt erneut.
            # Nichts wurde zugestellt: das eigene Flag freigeben, sonst endete
            # der naechste Versuch (Oneshot) bzw. Termin (Cron) als Duplikat.
            await self._release_own_claim(schedule_name, own_claim, task_id)
            if definition.cron is None:
                await self._defer_unroutable_oneshot(schedule_name, score_ms, definition, now_ms, task_id)
                return False
            await self._report_unroutable_cron(schedule_name, definition, exc)
        except Exception as exc:
            self._log.warning(
                "scheduler_publish_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
            )
            # Q14: bei ``PublishIndeterminate`` kann die Nachricht angekommen
            # sein; das Flag bleibt.
            if not isinstance(exc, PublishIndeterminate):
                await self._release_own_claim(schedule_name, own_claim, task_id)
            # BZ-10: wiederholte Fehler kosten die Fuehrung (``stepdown.py``).
            await self._note_publish_failure(exc)
            return False
        else:
            self._note_publish_ok()
        return True

    async def _release_own_claim(self, schedule_name: str, own_claim: str | None, task_id: str) -> None:
        """Gibt den Unique-Claim frei, den dieses Fire genommen hat (BZ-09/B7);
        Owner-geprueft, ein fremder Claim bleibt stehen. Best effort (Q15):
        scheitert die Freigabe, laeuft das Flag ueber seine TTL ab."""
        if own_claim is None:
            return
        try:
            await self._backend.release_unique(own_claim, task_id, op_timeout=self._config.backend_op_timeout)
        except Exception as exc:
            self._log.warning(
                "scheduler_unique_release_failed",
                schedule_name=schedule_name,
                task_id=task_id,
                dedup_hash=own_claim,
                **_error_fields(exc),
            )

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


_ONESHOT_PREFIX = "oneshot:"


def _fire_task_id(schedule_name: str, definition: ScheduleDefinition) -> str:
    """task_id eines Fires (BZ-02): ein Oneshot (delay/eta) feuert unter der
    ID, die ``schedule()`` geliefert hat (Feld ``task_id``, sonst Suffix von
    ``oneshot:<id>`` für Einträge eines 0.11-Producers); ein Cron bekommt je
    Fire eine neue."""
    if definition.cron is None and definition.task_id:
        return definition.task_id
    if definition.cron is None and schedule_name.startswith(_ONESHOT_PREFIX):
        return schedule_name.removeprefix(_ONESHOT_PREFIX)
    return str(uuid.uuid7())
