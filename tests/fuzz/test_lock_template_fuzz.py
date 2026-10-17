"""Fuzz: Lock-Templates (Fehlersuche Runde 4, U16, R20, KTD8).

Ziele: ``worker.gates.lock.render_lock_name`` (öffentliche Modulfunktion)
und ``LockGate.apply`` (öffentliche Gate-Schnittstelle) mit Templates im
Stil von ``TaskInfo(lock="user-{user_id}")`` und Argumenten, wie sie aus dem
Nachrichtenrumpf kommen (JSON-/msgpack-Werte: ``None``, ``bool``, ``int``,
``float``, ``str``, Listen, Dicts). Hintergrund: ``lock_key("")`` ergibt den
nackten Präfix ``sq:lock:`` als globalen Mutex
(docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md).

Zusagen:

* ``render_lock_name``: "Raises ``ValueError`` ... on render failure --
  caller routes the message to DLQ with an explanatory header" (Docstring).
* ``LockGate.apply``: Render-Fehler und leerer Render gehen an den
  Dead-Letter des Workers und enden ``Redelivered`` (Modulkopf, KTD3); ein
  leerer oder nur aus Leerraum bestehender Name erreicht ``acquire`` nie
  (Kommentar "Empty-rendered lock guard").
* Ein Lock-Name aus Nutzdaten belegt keinen internen Key unter dem
  Lock-Präfix (``KeySpace.scheduler_lock`` = ``sq:lock:{sched}:leader``);
  Quelle der Erwartung: Namensraum-Trennung in ``swarmq.keyspace``
  ("Hash-Tags legen zusammen genutzte Keys in einen Slot") und
  ``locking.SCHEDULER_LOCK_KEY``.

Allow-List: ``ValueError`` für ``render_lock_name``; ``LockGate.apply``
darf nicht werfen.

Treffer (Bericht docs/befunde/fehlersuche-runde-4-recherche/5-fuzz.md):

* F4-08 / R4F-08: Templates mit Index- oder Attributzugriff
  (``{items[0]}``, ``{user.id}``) werfen ``IndexError``/``AttributeError``
  statt ``ValueError``; ``LockGate.apply`` reicht sie durch. Ausgeklammert:
  keine Index-/Attributzugriffe im Template.
* F4-09 / R4F-09: ein Argument ``"{sched}:leader"`` mit Template ``"{key}"``
  ergibt den Key des Scheduler-Leader-Locks. Ausgeklammert: dieser Wert
  kommt in den Argumenten nicht vor.

Testliste:
# [x] render_lock_name: str oder ValueError
# [x] LockGate.apply: nie Ausnahme; leer gerendert -> Dead-Letter, kein acquire
# [x] LockGate.apply: acquire nie mit dem Namen des Scheduler-Leader-Locks
"""

from __future__ import annotations

import asyncio
import re
import uuid
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockToken
from swarmq.keyspace import KeySpace
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import LockGate, render_lock_name
from swarmq.worker.outcome import Continue, Redelivered

pytestmark = [pytest.mark.timeout(60)]

_SCHEDULER_LOCK_NAME = KeySpace().scheduler_lock().removeprefix(KeySpace().lock_prefix)

# F4-09 (R4F-09): der Name des Scheduler-Leader-Locks ist als Argument ausgeklammert.
# Treffer (geschrumpft): @example(template="{key}", key="{sched}:leader", other=None)
_text_args = st.one_of(
    st.text(max_size=12),
    st.sampled_from(["", " ", "\t", "\u200b", "{", "}", "{key}", "sched", "{sched}", "leader", "*", ":"]),
).filter(lambda text: text != _SCHEDULER_LOCK_NAME)
_payload = st.recursive(
    st.one_of(st.none(), st.booleans(), st.integers(), st.floats(), _text_args),
    lambda inner: st.one_of(
        st.lists(inner, max_size=3),
        st.dictionaries(st.sampled_from(["id", "name", "0", ""]) | st.text(max_size=4), inner, max_size=3),
    ),
    max_leaves=5,
)

# Platzhalter wie in Task-Definitionen; F4-08 (R4F-08): ohne ``[<int>]`` und ``.attr``.
# Treffer (geschrumpft): @example(template="{key[0]}", key=[], other=None)
#                       @example(template="{key.id}", key=None, other=None)
# F4-10 (R4F-10): eine Formatbreite aus den Nutzdaten (``{key:{other}}``) mit
# fünf und mehr Ziffern ist ausgeklammert; sie allokiert Gigabytes.
# Treffer (geschrumpft): @example(template="{key:{other}}", key=0.0, other=98077398153.0)  -> MemoryError
_other_payload = _payload.filter(lambda value: re.search(r"\d{5,}", str(value)) is None)

_placeholder = st.sampled_from(
    [
        "{key}",
        "{other}",
        "{key!s}",
        "{key!r}",
        "{key[id]}",
        "{key[name]}",
        "{key:>3}",
        "{key:d}",
        "{key:.2f}",
        "{missing}",
        "{}",
        "{0}",
        "{{",
        "}}",
        "{",
        "}",
        "{key:{other}}",
    ]
)
_literal = st.sampled_from(["", "user-", "tenant:", " ", "-", "lock", "sq:lock:", "{sched}"])


@st.composite
def _templates(draw: st.DrawFn) -> str:
    parts = draw(st.lists(st.one_of(_placeholder, _literal), min_size=1, max_size=4))
    return "".join(parts)


class _LockedTask(Task):
    task_info = TaskInfo(name="fuzz_lock_template", queues=["q"], lock="{key}", hard_timeout=60)

    @override
    async def run(self, key: object, other: object = "d") -> None:
        raise AssertionError("darf nicht laufen")


@given(template=_templates(), key=_payload, other=_other_payload)
@example(template="{key}", key="", other=None)
@example(template="{key[id]}", key=[], other=None)
@settings(max_examples=500, deadline=None)
def test_render_lock_name_returns_text_or_value_error(template: str, key: object, other: object) -> None:
    try:
        rendered = render_lock_name(template, _LockedTask, [key], {"other": other})
    except ValueError:
        return
    assert isinstance(rendered, str)


def _gate(acquired: list[str]) -> tuple[LockGate, AsyncMock]:
    async def acquire(name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken:
        acquired.append(name)
        return LockToken(name=name, owner=owner, max_concurrency=max_concurrency)

    store = MagicMock()
    store.acquire = acquire
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x")
    deps.backend = MagicMock(namespace=None)
    deps.lock_manager = lambda: store
    deps.detach = None
    deps.keep_lock = None
    dead_letter = AsyncMock()
    return LockGate(deps, dead_letter=dead_letter), dead_letter


class _Delivery:
    headers: dict[str, Any] = {}  # noqa: RUF012 - Fake-Zustellung ohne Header
    queue = "q"


@given(template=_templates().filter(lambda text: text.strip()), key=_payload, other=_other_payload)
@example(template="{key}", key="   ", other=None)
@example(template="{key}{other}", key="", other="")
@settings(max_examples=500, deadline=None)
def test_lock_gate_never_acquires_an_empty_or_internal_name(template: str, key: object, other: object) -> None:
    task_info = TaskInfo(name="fuzz_lock_template", queues=["q"], lock=template, hard_timeout=60)
    acquired: list[str] = []
    gate, dead_letter = _gate(acquired)
    ctx = GateContext(
        task_id="t",
        task_name="fuzz_lock_template",
        queue="q",
        task_info=task_info,
        task_class=_LockedTask,
        args=[key],
        kwargs={"other": other},
        handler_id=uuid.uuid4(),
    )

    outcome = asyncio.run(gate.apply(_Delivery(), ctx))

    if acquired:
        assert outcome is Continue
        (name,) = acquired
        assert name.strip()
        assert KeySpace().lock(name) != KeySpace().scheduler_lock()
        dead_letter.assert_not_awaited()
    else:
        assert outcome is Redelivered
        dead_letter.assert_awaited_once()
