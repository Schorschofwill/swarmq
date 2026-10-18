"""Fuzz: Fehlerklasse eines Fehlerergebnisses (Fehlersuche Runde 4, U16, R20, KTD8).

Ziel ist ``swarmq.worker.error_info`` (``error_class_of``,
``mark_worker_error``, ``gate_error_class``). Zusagen (Quelle: Modul- und
Funktions-Docstrings in ``src/swarmq/worker/error_info.py``, Plan
2026-10-12-1105 MR 3 P9/T12/T17; Unit-Tests
``tests/unit/worker/test_error_info.py``):

* ``error_class_of`` liefert ``modul.qualname`` der Klasse als ``str``;
  bei ``RemoteTaskError`` den gemeldeten ``remote_type`` (``"unknown"`` ->
  ``None``); für markierte Worker-Fehler ``None``.
* ``mark_worker_error`` markiert jede Exception und gibt sie zurück.
* ``gate_error_class`` bildet einen ``error_type`` auf eine öffentliche
  swarmq-Klasse ab oder liefert ``None``.

Exception-Typen: alle eingebauten, die sich mit einem Argument bauen lassen,
dazu dynamische Unterklassen mit beliebigem Namen, ``__module__`` (auch
``None``), ``__qualname__``, ``__slots__``, ``__str__``/``__repr__``, die
werfen, ``__getattr__``, das wirft, und nicht serialisierbaren Attributen
(Lambda, Lock, Generator, rekursive Liste).

Allow-List: leer. Keine der drei Funktionen sagt eine Ausnahme zu.

Testliste:
# [x] unmarkierte Exception -> str "modul.qualname", nie Ausnahme
# [x] markierte Exception -> None; mark_worker_error gibt dasselbe Objekt zurück
# [x] RemoteTaskError(remote_type=x) -> x bzw. None für "unknown"
# [x] gate_error_class(beliebiger Text) -> None oder swarmq.exceptions.*
"""

from __future__ import annotations

import builtins
import threading
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.worker.compute.envelope import RemoteTaskError
from swarmq.worker.error_info import error_class_of, gate_error_class, mark_worker_error

pytestmark = [pytest.mark.timeout(60)]


def _constructible(cls: type[BaseException]) -> bool:
    try:
        cls("x")
    except Exception:
        return False
    return True


# ``*ExceptionGroup`` braucht eine Liste von Exceptions; die übrigen nehmen
# ein Argument.
_BUILTIN_ERRORS = sorted(
    (
        value
        for value in vars(builtins).values()
        if isinstance(value, type) and issubclass(value, BaseException) and _constructible(value)
    ),
    key=lambda cls: cls.__name__,
)


def _raise(*_args: object) -> Any:
    raise RuntimeError("kaputt")


def _recursive_list() -> list[Any]:
    items: list[Any] = []
    items.append(items)
    return items


_odd_attributes = st.dictionaries(
    st.from_regex(r"[a-z][a-z0-9_]{0,8}", fullmatch=True),
    st.sampled_from(["lambda", "lock", "generator", "recursive", "object", "bytes"]),
    max_size=3,
)


def _attribute(kind: str) -> object:
    match kind:
        case "lambda":
            return lambda: None
        case "lock":
            return threading.Lock()
        case "generator":
            return (i for i in range(1))
        case "recursive":
            return _recursive_list()
        case "object":
            return object()
        case _:
            return b"\xff\xfe"


@st.composite
def _exceptions(draw: st.DrawFn) -> BaseException:
    base = draw(st.sampled_from(_BUILTIN_ERRORS))
    if draw(st.booleans()):
        return base(draw(st.text(max_size=8)))
    namespace: dict[str, Any] = {}
    namespace["__module__"] = draw(st.one_of(st.none(), st.text(max_size=16)))
    if draw(st.booleans()):
        namespace["__qualname__"] = draw(st.text(max_size=16))
    if draw(st.booleans()):
        namespace["__slots__"] = ()
    for dunder in ("__str__", "__repr__", "__getattr__"):
        if draw(st.booleans()):
            namespace[dunder] = _raise
    name = draw(st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,12}", fullmatch=True))
    cls = type(name, (base,), namespace)
    exc: BaseException = cls(draw(st.text(max_size=8)))
    if "__slots__" not in namespace:
        for attr, kind in draw(_odd_attributes).items():
            setattr(exc, attr, _attribute(kind))
    return exc


@given(exc=_exceptions())
@settings(max_examples=500, deadline=None)
def test_unmarked_exception_has_its_qualified_class_name(exc: BaseException) -> None:
    cls = type(exc)

    assert error_class_of(exc) == f"{cls.__module__}.{cls.__qualname__}"


@given(exc=_exceptions())
@settings(max_examples=500, deadline=None)
def test_marked_exception_has_no_class(exc: BaseException) -> None:
    marked = mark_worker_error(exc)

    assert marked is exc
    assert error_class_of(marked) is None


@given(remote_type=st.text(max_size=32), message=st.text(max_size=16))
@example(remote_type="unknown", message="")
@example(remote_type="", message="")
@settings(max_examples=500, deadline=None)
def test_remote_task_error_reports_the_remote_type(remote_type: str, message: str) -> None:
    expected = None if remote_type == "unknown" else remote_type

    assert error_class_of(RemoteTaskError(message, remote_type=remote_type)) == expected


@given(
    error_type=st.one_of(st.text(max_size=32), st.sampled_from(["TaskNotFoundError", "WorkerHungError", "ValueError"]))
)
@settings(max_examples=500, deadline=None)
def test_gate_error_class_is_a_swarmq_class_or_none(error_type: str) -> None:
    error_class = gate_error_class(error_type)

    assert error_class is None or error_class == f"swarmq.exceptions.{error_type}"
