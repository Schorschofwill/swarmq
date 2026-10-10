"""Task-cancellation primitives for SwarmQ.

Shared helpers consumed by :mod:`swarmq.worker` (cancel-pre-start
check, running-cancel monitor, hard-timeout-vs-cancel race
discrimination). Pure functions and type aliases live here so the
worker module stays scoped to message-handling pipeline control
flow; the cancel-specific dict construction and reason enum do not
need any worker state and benefit from a single source of truth.
"""

from __future__ import annotations

from typing import Any, Literal

# Cancel-reason discriminators for the CancelledError branch of the
# worker run loop. Named constants + Literal type so static type-
# checkers catch typos at call-sites instead of letting a misspelled
# reason silently fall into the hard-timeout path.
CancelReason = Literal["cancel", "timeout"]
CANCEL_REASON_CANCEL: CancelReason = "cancel"
CANCEL_REASON_TIMEOUT: CancelReason = "timeout"

# BZ-23 (Plan 0.12.0, MR 5): Restlaufzeit des Cancel-Flags, nachdem der
# Worker die stornierte Zustellung abgeschlossen hat. Das Flag wird nicht
# gelöscht, sondern per ``PEXPIRE … LT`` auf höchstens diese Dauer verkürzt:
# ein Duplikat derselben Nachricht (at-least-once) bleibt storniert, ein
# kürzer gesetztes Flag wird nie verlängert.
CANCEL_FLAG_SPENT_TTL_MS = 3_600_000


def build_result_dict(
    *,
    task_id: str,
    queue: str,
    task_name: str,
    is_error: bool,
    is_cancelled: bool,
    error_message: str = "",
    result: Any = None,
) -> dict[str, Any]:
    """Build the canonical worker-result payload.

    Consolidates four previously-duplicated result-dict constructions
    in :mod:`swarmq.worker` into one helper. Keyword-only
    arguments so a future field addition stays explicit at every
    call-site.

    The shape is fixed by ``Client._unpack`` (which reads via ``.get()``
    for pre-R3.6 forward-compat): the five keys below must always be
    present. ``is_cancelled`` is priority-read before ``is_error``, so
    a cancelled-with-error task surfaces as :class:`TaskCancelledError`,
    not :class:`TaskError` (R3.6 contract).
    """
    return {
        "task": {"id": task_id, "queue": queue, "name": task_name},
        "is_error": is_error,
        "is_cancelled": is_cancelled,
        "error_message": error_message,
        "result": result,
    }


def build_cancel_result_dict(
    *,
    task_id: str,
    queue: str,
    task_name: str,
) -> dict[str, Any]:
    """Shorthand for the cancel-specific result shape (is_error=False,
    is_cancelled=True, result=None, error_message="")."""
    return build_result_dict(
        task_id=task_id,
        queue=queue,
        task_name=task_name,
        is_error=False,
        is_cancelled=True,
    )


def build_error_result_dict(
    *,
    task_id: str,
    queue: str,
    task_name: str,
    error_message: str,
) -> dict[str, Any]:
    """Shorthand for the error-only result shape (is_error=True,
    is_cancelled=False, result=None)."""
    return build_result_dict(
        task_id=task_id,
        queue=queue,
        task_name=task_name,
        is_error=True,
        is_cancelled=False,
        error_message=error_message,
    )
