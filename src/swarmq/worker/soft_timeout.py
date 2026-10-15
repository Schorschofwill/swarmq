"""Soft-Timeout um ``run()`` (E-045; FS-05, KTD14).

Ist ``soft_timeout`` gesetzt, laeuft ``run()`` unter ``asyncio.wait_for``.
Laeuft die Frist ab, kommt im Task eine ``CancelledError`` an, und der Worker
wertet den Ablauf als ``SoftTimeoutError``. Eine ``TimeoutError`` aus dem Task
selbst (``asyncio.timeout``, ``socket.timeout``, aiohttp) bleibt dagegen, was
sie ist: Eine innere Huelle reicht sie verpackt durch ``wait_for``, sodass eine
``TimeoutError`` aus ``wait_for`` sicher der eigene Ablauf ist.

Leichtgewichtig, damit das Rechenkind (``compute/child.py``) es lazy laden kann.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from swarmq.exceptions import SoftTimeoutError


class _TaskTimeoutError(Exception):
    """Traegt eine ``TimeoutError`` des Tasks unerkannt durch ``wait_for``."""

    def __init__(self, original: TimeoutError) -> None:
        super().__init__(original)
        self.original = original


async def _unwrapped_timeouts[T](coro: Coroutine[Any, Any, T]) -> T:
    try:
        return await coro
    except TimeoutError as exc:
        raise _TaskTimeoutError(exc) from None


async def run_with_soft_timeout[T](coro: Coroutine[Any, Any, T], soft_timeout: float | None) -> T:
    """``coro`` ausfuehren, bei gesetztem ``soft_timeout`` unter ``wait_for``.

    Nur der Ablauf der eigenen Frist wird ``SoftTimeoutError`` ("soft timeout
    after Xs"). Eine ``TimeoutError`` des Tasks wird unveraendert (dasselbe
    Objekt, eigener Traceback) weitergeworfen. Ein Abbruch von aussen bleibt
    ``CancelledError``.
    """
    if soft_timeout is None:
        return await coro
    original: TimeoutError
    try:
        return await asyncio.wait_for(_unwrapped_timeouts(coro), timeout=soft_timeout)
    except _TaskTimeoutError as wrapped:
        original = wrapped.original
    except TimeoutError as exc:
        # Frist <= 0: ``wait_for`` bricht ab, bevor die Huelle ``coro`` startet.
        # Schliessen, damit ``run()`` nicht als "never awaited" liegen bleibt;
        # eine beendete Coroutine laesst ``close()`` unberuehrt.
        coro.close()
        raise SoftTimeoutError(f"soft timeout after {soft_timeout}s") from exc
    # Ausserhalb des ``except``: sonst haenge die Huelle als ``__context__`` an.
    raise original
