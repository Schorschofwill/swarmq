"""Nachweis DP-02: ``schedule_many()`` übernimmt In-place-Änderungen einer
Middleware an ``kwargs`` aus ``pre_enqueue`` nicht.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-02, Verdacht V2).

``schedule_many`` kodierte jede Nachricht (``encode_json``) vor
``chain.run_pre_enqueue``; veröffentlicht wurden die vorher kodierten Bytes.
``schedule()`` kodiert nach ``pre_enqueue``, der verzögerte Pfad und die
Workflow-Schritte ebenso.

Erwartet laut E-059: Jeder Enqueue über den Client, auch ``schedule_many``,
läuft durch ``pre_enqueue``; Nachtrag 2026-10-15: In-place-Änderungen an
``kwargs`` werden wie im sofortigen Pfad übernommen, Änderungen an ``args``
nicht.

Testliste:
# [x] kwargs-Ergänzung der Middleware kommt in der veröffentlichten Nachricht an
# [x] eine Änderung an args wirkt nicht (Grenze aus E-059)
"""

from __future__ import annotations

from typing import Any, override

import pytest

from swarmq.client import Client
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.serialization import decode_json

pytestmark = [pytest.mark.timeout(5), pytest.mark.befund("DP-02")]


class _Tenant(Middleware):
    @override
    async def pre_enqueue(self, message: MessageContext) -> None:
        message["kwargs"]["tenant"] = "t1"
        message["args"].append(99)


def _published_bodies(broker: Any) -> list[dict[str, Any]]:
    return [decode_json(call.args[1]) for call in broker.publish.await_args_list]


async def test_schedule_many_publishes_kwargs_changed_in_pre_enqueue(broker, backend, registry) -> None:
    client = Client(broker=broker, backend=backend, registry=registry, middleware_chain=MiddlewareChain([_Tenant()]))

    await client.schedule_many([("add", (1,), {"x": 1}), ("add", (2,), {"x": 2})], queue="math", unique_for=None)

    bodies = _published_bodies(broker)
    assert [body["kwargs"] for body in bodies] == [{"x": 1, "tenant": "t1"}, {"x": 2, "tenant": "t1"}]
    assert [body["args"] for body in bodies] == [[1], [2]]
