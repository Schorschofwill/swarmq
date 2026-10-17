"""Nachweis F4-03: Ein per ``app.workflow(id)`` neu gebautes Workflow-Handle
setzt das Cancel-Flag fest auf 24 h.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-03; Verdacht
R4M-E-01, Variante 1; Gegenprüfung ``6-gegenpruefung-M.md``).

``Client.workflow`` baut das Handle mit ``cancel_ttl_ms=_DEFAULT_WORKFLOW_TTL_MS``
(24 h), ``SwarmQ.apply`` reicht ``config.result_ttl * 1000`` durch. Bei
``result_ttl`` über 24 h (der Rat der Doku für lange Workflows) läuft das
Flag eines Abbruchs aus einem anderen Prozess vor dem Zustand ab: wartende
Group-/Chord-Glieder laufen doch, ``advance()`` reiht weiter ein.

Erwartet: Plan 2026-05-30-001 ``:104`` („Cancel-Flag … TTL =
``config.result_ttl``“), Kommentar ``client/workflows.py:42-45`` („app.apply()
passes config.result_ttl * 1000 through“) und ``api.md:761-763`` („a handle
can be rebuilt in any process“): Das neu gebaute Handle setzt das Flag so
lange wie das Handle aus ``apply()``.

Variante 2 (Flag lebt nur ``result_ttl``, auch unter 24 h) ist eine
ENTSCHEIDUNG des Plans (Produktfrage PF-5) und gehört nicht hierher.

Testliste:
# [x] result_ttl 7 d: Cancel über das Reattach-Handle setzt dieselbe TTL wie über das Apply-Handle
"""

from __future__ import annotations

from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from swarmq.workflow import group

pytestmark = pytest.mark.timeout(5)

_WEEK_S = 7 * 86_400


class F403Step(Task):
    task_info = TaskInfo(name="f4_03_step", queues=["f4_03"])

    @override
    async def run(self, x: str) -> str:
        return x


def _app(set_flag: AsyncMock, result_ttl: int) -> SwarmQ:
    broker = MagicMock(spec=RabbitMQBroker)
    broker._url = "amqp://guest:guest@localhost/"
    broker.publish = AsyncMock()
    backend = MagicMock(spec=ValkeyBackend)
    backend._url = "valkey://localhost:6379/0"
    backend.store_workflow_state = AsyncMock()
    backend.get_workflow_state = AsyncMock(return_value=encode_json({"status": "running", "version": 0}))
    backend.set_workflow_cancel_flag = set_flag
    backend.notify_workflow = AsyncMock()
    app = SwarmQ(broker, backend, result_ttl=result_ttl)
    if not app.registry.has("f4_03_step"):
        app.registry.register(F403Step)
    return app


def _ttl_ms(set_flag: AsyncMock) -> int:
    call = set_flag.await_args
    assert call is not None
    return int(call.kwargs["ttl_ms"])


@pytest.mark.befund("F4-03")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-03: offen - Reattach-Handle setzt das Workflow-Cancel-Flag fest 24 h statt result_ttl",
)
async def test_reattached_handle_sets_the_cancel_flag_as_long_as_the_apply_handle() -> None:
    set_flag = AsyncMock(return_value=True)
    app = _app(set_flag, result_ttl=_WEEK_S)

    original = await app.apply(group(F403Step.s("a"), F403Step.s("b")))
    await original.cancel()
    ttl_original = _ttl_ms(set_flag)

    reattached = await app.workflow(original.workflow_id)
    await reattached.cancel()
    ttl_reattached = _ttl_ms(set_flag)

    assert ttl_original == _WEEK_S * 1000
    assert ttl_reattached == ttl_original, (
        f"Reattach-Handle: Cancel-Flag {ttl_reattached} ms, Apply-Handle {ttl_original} ms (result_ttl 7 d)"
    )
