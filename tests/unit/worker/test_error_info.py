"""Fehlerklasse eines Fehlerergebnisses (Plan 2026-10-12-1105, MR 3, P9).

Test-Liste (T12, T17, E5 A, E7 A):

1. ``error_class_of(ValueError())`` -> ``"builtins.ValueError"``.
2. Eigene Klasse -> ``"modul.Qualname"``; verschachtelt -> ``"modul.Aussen.Innen"``.
3. ``RemoteTaskError(remote_type="x.Y")`` -> ``"x.Y"``;
   ``RemoteTaskError(remote_type="unknown")`` -> ``None``.
4. Markierter Worker-Fehler (``mark_worker_error``) -> ``None``.
5. Tabelle der Gate-Klassen: ``TaskNotFoundError``, ``LockWaitExceeded``,
   ``RedeliveryLimitError`` -> ``swarmq.exceptions.*``; ``WorkerHungError``,
   ``ValueError`` -> ``None``.
"""

from __future__ import annotations

import pytest

from swarmq.worker.compute.envelope import RemoteTaskError
from swarmq.worker.error_info import error_class_of, gate_error_class, mark_worker_error

pytestmark = [pytest.mark.timeout(5)]


class _OwnError(Exception):
    pass


class _Outer:
    class Inner(Exception):
        pass


def test_class_name_is_module_and_qualname():
    assert error_class_of(ValueError()) == "builtins.ValueError"
    assert error_class_of(_OwnError("x")) == f"{__name__}._OwnError"
    assert error_class_of(_Outer.Inner()) == f"{__name__}._Outer.Inner"


def test_remote_task_error_reports_the_class_from_the_child():
    assert error_class_of(RemoteTaskError("boom", remote_type="x.Y")) == "x.Y"


def test_remote_task_error_without_detail_has_no_class():
    assert error_class_of(RemoteTaskError("no detail", remote_type="unknown")) is None


def test_marked_worker_error_has_no_class():
    exc = RuntimeError("hard timeout after 1s")
    assert mark_worker_error(exc) is exc
    assert error_class_of(exc) is None
    assert error_class_of(RuntimeError("unmarked")) == "builtins.RuntimeError"


@pytest.mark.parametrize(
    ("error_type", "expected"),
    [
        ("TaskNotFoundError", "swarmq.exceptions.TaskNotFoundError"),
        ("LockWaitExceeded", "swarmq.exceptions.LockWaitExceeded"),
        ("RedeliveryLimitError", "swarmq.exceptions.RedeliveryLimitError"),
        ("WorkerHungError", None),
        ("ValueError", None),
    ],
)
def test_gate_error_types_map_to_public_swarmq_classes_only(error_type, expected):
    assert gate_error_class(error_type) == expected
