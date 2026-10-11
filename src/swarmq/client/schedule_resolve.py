"""Aufloesung beim Einplanen (Plan 0.12.0, MR 0; verbatim aus ``schedule.py``):
Queue und Delivery-Mode, Unique-Parameter und die Vorbereitung von
``schedule_many``. Dazu der ``_MISSING``-Sentinel; ``schedule.py``
re-exportiert ihn unveraendert. Mixin ueber dem Client-Zustand.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal, override

from swarmq.exceptions import TaskNotFoundError
from swarmq.logging import get_logger
from swarmq.serialization import compute_dedup_hash

logger = get_logger(__name__)

# BZ-24: so viele Task-Namen merkt sich ein Client fuer die einmalige
# Warnung; dynamische Namen fuellen den Speicher sonst unbegrenzt.
_MAX_UNRESOLVED_NAMES = 1000

if TYPE_CHECKING:
    from swarmq.registry import TaskRegistry
    from swarmq.task import UniqueUntil


# R3.5 Unique Tasks: module-private sentinel that distinguishes
# "caller did not pass a value" from "caller explicitly passed None".
# None means "dedup explicitly disabled for this call"; _MISSING means
# "fall through to TaskInfo default (or None in Client-Modus)".
# Not exported — do NOT add to __init__.py.
#
# Dedicated singleton class so type-checkers narrow int | None | _MissingType
# unions at call sites (plain Any would collapse to Any). __slots__ keeps
# the object tiny; __new__ enforces exactly one instance so `is _MISSING`
# identity checks stay cheap.
class _MissingType:
    """Private singleton for the `_MISSING` sentinel. See module docstring."""

    __slots__ = ()
    _instance: _MissingType | None = None

    def __new__(cls) -> _MissingType:  # noqa: PYI034 - Singleton liefert die gespeicherte Instanz
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @override
    def __repr__(self) -> str:
        return "<_MISSING>"

    def __bool__(self) -> bool:
        # Defensive: treat the sentinel as falsy in `if value:` style checks
        # that might be written before reading the docstring. Proper checks
        # use `value is _MISSING`.
        return False


_MISSING: _MissingType = _MissingType()


class ClientScheduleResolve:
    # Mixin über dem Client-Zustand; die Attribute setzt ``Client.__init__``.
    if TYPE_CHECKING:
        _registry: TaskRegistry | None
        _unresolved_task_names: set[str]

    def _resolve_queue_and_delivery_mode(self, task_name: str, queue: str | None) -> tuple[str, str]:
        """Resolve the target queue and delivery mode for *task_name*.

        Uses the registered TaskInfo when available; falls back to the
        explicit *queue* argument and ``at_least_once`` delivery otherwise.
        Raises ``ValueError`` if the queue cannot be resolved.
        """
        resolved_queue = queue
        delivery_mode = "at_least_once"
        priority: str = "normal"
        if self._registry is not None:
            entry = self._registry.get_entry(task_name)
            if resolved_queue is None:
                resolved_queue = entry.task_info.queues[0]
            delivery_mode = entry.task_info.delivery.value
            priority = entry.task_info.priority
        if resolved_queue is None:
            raise ValueError("No queue specified and no registry available to resolve one")
        if self._registry is None:
            self._warn_task_defaults_unresolved(task_name)
        # R4.3: apply priority suffix (.high / .low). 'normal' passes
        # through unchanged. Only applied when the registry knows the
        # task — a bare client.schedule(name, queue=...) without a
        # registered task stays on the explicit queue.
        if priority != "normal":
            resolved_queue = f"{resolved_queue}.{priority}"
        return resolved_queue, delivery_mode

    def _warn_task_defaults_unresolved(self, task_name: str) -> None:
        """BZ-24: einmal je Task-Name und Instanz, hoechstens fuer
        ``_MAX_UNRESOLVED_NAMES`` Namen."""
        names = self._unresolved_task_names
        if task_name in names or len(names) >= _MAX_UNRESOLVED_NAMES:
            return
        names.add(task_name)
        logger.warning(
            "client_task_defaults_unresolved",
            task_name=task_name,
            hint=(
                "client has no registry: unique_for, priority and delivery mode "
                "from the task's TaskInfo are not applied"
            ),
        )

    def _resolve_unique(
        self,
        task_name: str,
        unique_for: int | _MissingType | None,
        unique_until: Literal["start", "completion"] | _MissingType,
    ) -> tuple[int | None, UniqueUntil]:
        """Resolve effective unique_for / unique_until for a schedule() call.

        Rules (origin CD1):
          - unique_for is _MISSING: look up TaskInfo default if registry
            is available; otherwise None (dedup off) in Client-Modus.
          - unique_for is None (explicit): dedup off for this call,
            even if TaskInfo sets a default.
          - unique_for is int: use this value.
        unique_until follows the same pattern with default "completion".
        """
        final_for: int | None
        final_until: UniqueUntil = "completion"

        if isinstance(unique_for, _MissingType):
            final_for = None
            if self._registry is not None and self._registry.has(task_name):
                entry = self._registry.get_entry(task_name)
                final_for = entry.task_info.unique_for
                final_until = entry.task_info.unique_until
        else:
            final_for = unique_for

        if not isinstance(unique_until, _MissingType):
            final_until = unique_until
        # sonst: bereits aus TaskInfo gesetzt (oder "completion"-Default
        # im Client-Modus).

        return final_for, final_until

    def _batch_prepare(
        self,
        tasks: list[tuple[str, tuple[Any, ...], dict[str, Any]]],
        queue: str | None,
        unique_for: int | _MissingType | None,
        unique_until: Literal["start", "completion"] | _MissingType,
    ) -> list[dict[str, Any]]:
        """Pass 0 (sync): validate + generate task_ids + compute dedup
        parameters per element. Any validation error aborts before a single
        network call, so no rollback is needed yet."""
        prepped: list[dict[str, Any]] = []
        for task_name, args, kwargs in tasks:
            if self._registry is not None and not self._registry.has(task_name):
                raise TaskNotFoundError(task_name)
            resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(
                task_name,
                queue,
            )
            resolved_unique_for, resolved_unique_until = self._resolve_unique(
                task_name,
                unique_for,
                unique_until,
            )
            if resolved_unique_for is not None:
                dedup_hash_precomp: str | None = compute_dedup_hash(
                    task_name,
                    args,
                    kwargs,
                )
                ttl_ms: int | None = resolved_unique_for * 1000
            else:
                dedup_hash_precomp = None
                ttl_ms = None
            prepped.append(
                {
                    "task_name": task_name,
                    "args": args,
                    "kwargs": kwargs,
                    "task_id": str(uuid.uuid7()),
                    "queue": resolved_queue,
                    "delivery_mode": delivery_mode,
                    "unique_until": resolved_unique_until,
                    "dedup_hash": dedup_hash_precomp,
                    "dedup_ttl_ms": ttl_ms,
                }
            )
        return prepped
