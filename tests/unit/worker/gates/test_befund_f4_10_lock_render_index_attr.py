"""Nachweis F4-10: Ein Lock-Template mit Index- oder Attributzugriff wirft
``IndexError``/``AttributeError`` am Lock-Gate vorbei.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-10; Verdacht
R4F-08; Gegenprüfung ``6-gegenpruefung-F.md``; Fuzz-Treffer in
``tests/fuzz/test_lock_template_fuzz.py``, geschrumpft
``template="{key[0]}", key=[]`` und ``template="{key.id}", key=None``).

``render_lock_name`` wandelt nur ``TypeError``/``KeyError`` in ``ValueError``;
``LockGate.apply`` fängt nur ``ValueError``. Ein Template ``"{user.id}"``
scheitert bei jeder Nachricht (kwargs kommen JSON-dekodiert als ``dict``),
``"{items[0]}"`` bei leerer Liste. Statt DLQ mit ``x-swarmq-lock-render-error``
bleibt die Nachricht bis ``consumer_timeout`` unbestätigt, erst das
Zustelllimit beendet die Schleife mit ``WorkerHungError``.

Erwartet: Docstring ``render_lock_name`` („Raises ``ValueError`` … on render
failure — caller routes the message to DLQ“), Modulkopf KTD3 und Kommentar in
``apply`` (Render-Fehler gehen an das Dead-Letter-Ziel),
``locking-and-dedup.md:174-180`` („Lock-render failures“), ADV-009.

Testliste:
# [x] render_lock_name: {key[0]} mit [], {key.id} mit None und mit {"id": 1} -> ValueError
# [x] LockGate.apply mit denselben Templates -> Dead-Letter, Redelivered, kein acquire
"""

from __future__ import annotations

import uuid
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import LockGate, render_lock_name
from swarmq.worker.outcome import Redelivered

pytestmark = pytest.mark.timeout(5)

_CASES = pytest.mark.parametrize(
    ("template", "key"),
    [("{key[0]}", []), ("{key.id}", None), ("{key.id}", {"id": 1})],
    ids=["index-empty-list", "attr-none", "attr-json-dict"],
)


class F410Locked(Task):
    task_info = TaskInfo(name="f4_10_locked", queues=["q"], lock="{key}", hard_timeout=60)

    @override
    async def run(self, key: object) -> None:
        raise AssertionError("darf nicht laufen")


class _Delivery:
    headers: dict[str, Any] = {}  # noqa: RUF012 - Fake-Zustellung ohne Header
    queue = "q"


@pytest.mark.befund("F4-10")
@_CASES
def test_render_failure_with_index_or_attribute_access_is_a_value_error(template: str, key: object) -> None:
    error: BaseException | None = None
    try:
        render_lock_name(template, F410Locked, [key], {})
    except Exception as exc:
        error = exc

    assert isinstance(error, ValueError), f"{template!r} mit {key!r}: statt ValueError {error!r}"


@pytest.mark.befund("F4-10")
@_CASES
async def test_lock_gate_dead_letters_a_render_failure_with_index_or_attribute_access(
    template: str, key: object
) -> None:
    store = MagicMock()
    store.acquire = AsyncMock()
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x")
    deps.backend = MagicMock(namespace=None)
    deps.lock_manager = lambda: store
    deps.detach = None
    deps.keep_lock = None
    dead_letter = AsyncMock()
    gate = LockGate(deps, dead_letter=dead_letter)
    ctx = GateContext(
        task_id="t",
        task_name="f4_10_locked",
        queue="q",
        task_info=TaskInfo(name="f4_10_locked", queues=["q"], lock=template, hard_timeout=60),
        task_class=F410Locked,
        args=[key],
        kwargs={},
        handler_id=uuid.uuid4(),
    )

    outcome: object
    try:
        outcome = await gate.apply(_Delivery(), ctx)
    except Exception as exc:
        outcome = exc

    assert outcome is Redelivered, f"{template!r} mit {key!r}: statt Dead-Letter {outcome!r}"
    dead_letter.assert_awaited_once()
    store.acquire.assert_not_awaited()
