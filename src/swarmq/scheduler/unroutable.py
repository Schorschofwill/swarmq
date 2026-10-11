"""Unroutbarer Fire des Leaders (Plan 0.12.0, MR 6, BZ-03).

Ein Publish an eine fehlende Queue kommt als ``UnroutableMessage`` zurueck.
Ein Cron-Fire ist dann verloren: ``_fire_one`` schreibt ihn wie einen
erfolgreichen fest, hier wird er gezaehlt und gedrosselt geloggt. Mixin ueber
dem ``SchedulerLoop``-Zustand; Uhr und Logger wie in ``fire.py`` ueber
``self._wall_time`` und ``self._log`` (Logger-Name ``swarmq.scheduler.loop``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import msgspec

from swarmq.cancellation import build_error_result_dict
from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields
from swarmq.serialization import encode_result
from swarmq.signals import Signal

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore


#: Abstand der erneuten Versuche eines unroutbaren Oneshots; haelt den Poll frei.
_ONESHOT_RETRY_MS = 60_000
#: Hoechstens ein Log je Schedule in diesem Fenster (Alarmmuedigkeit, R8).
_LOG_WINDOW_S = 600.0
#: Ab dieser Groesse werden abgelaufene Drossel-Eintraege entfernt; Oneshot-
#: Namen sind einmalig, die Tabelle wuechse sonst ohne Grenze.
_LOG_TABLE_PRUNE_AT = 1024


_SchedulerState = Literal["standby", "leader", "stopped"]
"""Zustand des Schedulers; hier definiert, damit Mixin und Loop denselben Typ teilen."""


class SchedulerUnroutable:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    if TYPE_CHECKING:

        @property
        def _log(self) -> Any: ...

        def _wall_time(self) -> float: ...

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

        def _resolve_task_info(self, task_name: str) -> Any: ...

    _reported_names: frozenset[str]
    _store: ScheduleStore
    _backend: Any
    _config: SwarmQConfig
    _owner_token: str
    _state: _SchedulerState

    #: Letztes Unroutable-Log je Schedule (Wanduhr in s); ``SchedulerLoop.__init__``.
    _unroutable_logged_at: dict[str, float]

    def _unroutable_log_due(self, schedule_name: str) -> bool:
        """``True`` hoechstens einmal je Schedule und ``_LOG_WINDOW_S``."""
        table = self._unroutable_logged_at
        now = self._wall_time()
        last = table.get(schedule_name)
        if last is not None and now - last < _LOG_WINDOW_S:
            return False
        if len(table) >= _LOG_TABLE_PRUNE_AT:
            for name, at in list(table.items()):
                if now - at >= _LOG_WINDOW_S:
                    del table[name]
        table[schedule_name] = now
        return True

    async def _report_unroutable_cron(
        self,
        schedule_name: str,
        definition: ScheduleDefinition,
        exc: BaseException,
    ) -> None:
        """Zaehlt einen verlorenen Cron-Fire (Signal, daraus die Metrik) und
        loggt ihn gedrosselt."""
        if self._unroutable_log_due(schedule_name):
            self._log.warning(
                "scheduler_fire_unroutable",
                schedule_name=schedule_name,
                queue=definition.queue,
                task_name=definition.task_name,
                **_error_fields(exc),
                hint=(
                    "no queue with this name exists; this cron fire is lost — start a "
                    "worker for the queue (workers declare their queues) or fix the queue"
                ),
            )
            if schedule_name in self._reported_names and self._resolve_task_info(definition.task_name) is None:
                # Gemeldet von einem Worker mit eigener Registry, dessen Queue
                # es auf diesem Broker nicht gibt (anderer vhost/Broker bei
                # gemeinsamem Valkey, BZ-10).
                self._log.warning(
                    "scheduler_fire_unroutable_foreign",
                    schedule_name=schedule_name,
                    queue=definition.queue,
                    task_name=definition.task_name,
                    hint=(
                        "this cron is reported by a worker of another app whose queue does not "
                        "exist on this leader's broker; apps sharing this Valkey must use the "
                        "same RabbitMQ vhost, or each app sets its own SWARMQ_NAMESPACE "
                        "(required with separate vhosts)"
                    ),
                )
        await self._emit(
            Signal.SCHEDULER_FIRE_UNROUTABLE,
            schedule_name=schedule_name,
            queue=definition.queue,
            kind="cron_lost",
        )

    async def _defer_unroutable_oneshot(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        now_ms: int,
        task_id: str,
    ) -> None:
        """Stellt einen unroutbaren Oneshot um 60 s zurueck; ``unroutable_since``
        bleibt beim ersten Versuch (gemessen ab dort, nicht ab der Faelligkeit).
        Nach ``scheduler_unroutable_oneshot_max_s`` wird er verworfen."""
        since = definition.unroutable_since if definition.unroutable_since is not None else now_ms
        if now_ms - since >= self._config.scheduler_unroutable_oneshot_max_s * 1000:
            await self._drop_unroutable_oneshot(schedule_name, score_ms, definition, task_id, since)
            return
        next_ms = now_ms + _ONESHOT_RETRY_MS
        if self._unroutable_log_due(schedule_name):
            self._log.warning(
                "scheduler_oneshot_unroutable",
                schedule_name=schedule_name,
                task_id=task_id,
                task_name=definition.task_name,
                queue=definition.queue,
                unroutable_since=since,
                hint=(
                    "no queue with this name exists; the delayed task is retried every 60s and "
                    "dropped after scheduler_unroutable_oneshot_max_s — start a worker for the queue"
                ),
            )
        updated = msgspec.structs.replace(definition, fire_ts_ms=next_ms, unroutable_since=since)
        await self._commit_unroutable(schedule_name, score_ms, next_ms, updated)

    async def _drop_unroutable_oneshot(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        task_id: str,
        since_ms: int,
    ) -> None:
        """Entfernt den Oneshot (owner-geprueft); nur wenn das griff: Event
        und Signal (daraus die Metrik)."""
        if not await self._commit_unroutable(schedule_name, score_ms, -1, None):
            return
        self._log.error(
            "scheduler_oneshot_unroutable_dropped",
            schedule_name=schedule_name,
            task_id=task_id,
            task_name=definition.task_name,
            queue=definition.queue,
            unroutable_since=since_ms,
            max_s=self._config.scheduler_unroutable_oneshot_max_s,
            hint="no queue with this name appeared in time; the delayed task was dropped with an error result",
        )
        await self._emit(
            Signal.SCHEDULER_FIRE_UNROUTABLE,
            schedule_name=schedule_name,
            queue=definition.queue,
            kind="oneshot_dropped",
        )
        await self._store_unroutable_result(definition, task_id)
        if definition.source == "cli_unique" and definition.dedup_hash:
            await self._release_unroutable_unique(definition.dedup_hash, task_id)

    async def _release_unroutable_unique(self, dedup_hash: str, task_id: str) -> None:
        """Gibt die Unique-Sperre frei, die ``schedule()`` fuer diesen Oneshot
        nahm (Owner = ``task_id``); sonst blockierte sie bis zu ihrem TTL."""
        try:
            await self._backend.release_unique(dedup_hash, task_id, op_timeout=self._config.backend_op_timeout)
        except Exception as exc:  # noqa: BLE001 - best effort, Sperre läuft über TTL ab
            self._log.warning(
                "scheduler_unroutable_unique_release_failed",
                task_id=task_id,
                dedup_hash=dedup_hash,
                **_error_fields(exc),
            )

    async def _store_unroutable_result(self, definition: ScheduleDefinition, task_id: str) -> None:
        """Fehlerergebnis unter *task_id*, damit ``get_result()`` endet statt
        in den Timeout zu laufen. Best effort; ein Backend ohne die Methoden
        (Fake, Q15) faellt still auf "kein Ergebnis" zurueck."""
        ttl_s = self._config.result_ttl if self._config.result_ttl is not None else 86400  # type: ignore[redundant-expr]  # pyright: ignore[reportUnnecessaryComparison]  # Fallback fuer untypisiert gesetztes None
        if ttl_s <= 0:
            return
        data = encode_result(
            build_error_result_dict(
                task_id=task_id,
                queue=definition.queue,
                task_name=definition.task_name,
                error_message=(
                    f"dropped by the scheduler: queue {definition.queue!r} stayed unroutable "
                    f"for {self._config.scheduler_unroutable_oneshot_max_s:g}s (no queue with this name)"
                ),
            )
        )
        try:
            await self._backend.store_result(
                task_id,
                data,
                int(ttl_s * 1000),
                op_timeout=self._config.backend_op_timeout,
            )
            await self._backend.notify_result(task_id)
        except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, best effort
            self._log.warning("scheduler_unroutable_result_store_failed", task_id=task_id, **_error_fields(exc))

    async def _commit_unroutable(
        self,
        schedule_name: str,
        score_ms: int,
        next_ms: int,
        updated: ScheduleDefinition | None,
    ) -> bool:
        """Owner-gepruefter Commit wie in ``_fire_one``; ``True`` nur, wenn er griff."""
        try:
            committed = await self._store.commit_fire(
                schedule_name,
                next_ms,
                updated,
                self._owner_token,
                fired_ts_ms=score_ms,
            )
        except BackendConnectionError as exc:
            self._log.warning("scheduler_commit_failed", schedule_name=schedule_name, **_error_fields(exc))
            return False
        if not committed:
            self._log.warning("scheduler_commit_owner_mismatch", schedule_name=schedule_name)
            self._state = "standby"
            return False
        return True
