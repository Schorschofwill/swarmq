"""Nachweis F4-05: ``dlq list`` meldet „nicht gekürzt“ und ``dlq inspect``
„nicht gefunden“ ohne Scan-Limit-Hinweis, obwohl das Lesen am Limit stoppte.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-05; Verdacht
R4M-F-03; Gegenprüfung ``6-gegenpruefung-M.md``).

``_drain_dlq_messages`` (``broker/rabbitmq/admin.py``) bestätigt abgelaufene
Einträge beim Lesen ohne Kopie (E-028) und zählt sie nicht in ``scanned``
(zugesagt in ``dead-letter-queues.md:271-274``). Die CLI leitet die Kürzung aber
aus ``scanned >= --limit`` bzw. ``--scan-limit`` ab (``cli/dlq_cmd.py``):
Verbrauchen abgelaufene Einträge Lesungen, meldet ``dlq list`` ``truncated:
false``, obwohl weitere Einträge in der XQ liegen, und ``dlq inspect <id>``
meldet „not found“ mit ``scan_exhausted: false``, obwohl der Task hinter dem
Limit liegt (Exit 3).

Erwartet: ``cli.md:430-431`` („shows ``(truncated)`` when ``--limit`` is hit;
``--format json`` includes a ``truncated: bool`` envelope“),
``dead-letter-queues.md:131-132``, Docstrings ``list_dead_letters`` („stopped at
limit“) und ``inspect_dead_letter`` („scan stopped at limit before the match“).

Echte XQ gegen RabbitMQ: ein abgelaufener Eintrag (Zeitstempel zwei Stunden
alt, Frist eine Stunde, ohne AMQP-``expiration``, wie Einträge aus dem DLX)
vor drei lebenden.

Testliste:
# [x] dlq list --limit 2 --format json: zwei Lesungen, ein Abgelaufener, zwei lebende bleiben -> truncated true
# [x] dlq inspect t-1 --scan-limit 2 --format json: Abgelaufener und t-0 gelesen -> gefunden oder scan_exhausted
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from collections.abc import AsyncIterator

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.cli import dlq_cmd
from swarmq.headers import HEADER_DEAD_LETTER_TTL_MS, HEADER_DEAD_LETTERED_AT
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]

_HOUR_MS = 3_600_000


def _body(task_id: str, queue: str) -> bytes:
    return json.dumps({"task": {"id": task_id, "queue": queue, "name": "t"}, "args": [], "kwargs": {}}).encode()


@pytest.fixture
async def dlq(rabbitmq_url: str, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[str]:
    """``{queue}.XQ`` mit einem abgelaufenen und drei lebenden Einträgen."""
    queue = f"f405-{uuid.uuid4().hex[:10]}"
    setup = RabbitMQBroker(rabbitmq_url)
    await setup.connect()
    await setup.declare_queue(queue)
    expired = {HEADER_DEAD_LETTERED_AT: int(time.time() * 1000) - 2 * _HOUR_MS, HEADER_DEAD_LETTER_TTL_MS: _HOUR_MS}
    await setup.publish(f"{queue}.XQ", _body("old", queue), expired)
    for i in range(3):
        await setup.publish(f"{queue}.XQ", _body(f"t-{i}", queue))
    monkeypatch.setenv("SWARMQ_BROKER_URL", rabbitmq_url)
    yield queue
    await delete_queue_family(rabbitmq_url, queue)
    await setup.close()


@pytest.mark.befund("F4-05")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-05: offen - abgelaufene DLQ-Eintraege verbrauchen Lesungen, die Kuerzung wird nicht gemeldet",
)
async def test_list_reports_truncation_when_expired_entries_used_up_the_limit(
    dlq: str, capsys: pytest.CaptureFixture[str]
) -> None:
    await dlq_cmd._run_dlq_list(argparse.Namespace(queue=dlq, limit=2, format="json"))
    out = json.loads(capsys.readouterr().out)

    assert [entry["task_id"] for entry in out["entries"]] == ["t-0"]
    assert out["truncated"] is True, f"Limit 2 erreicht, t-1 und t-2 liegen noch in der XQ: {out}"


@pytest.mark.befund("F4-05")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-05: offen - abgelaufene DLQ-Eintraege verbrauchen Lesungen, die Kuerzung wird nicht gemeldet",
)
async def test_inspect_reports_the_scan_limit_when_expired_entries_used_it_up(
    dlq: str, capsys: pytest.CaptureFixture[str]
) -> None:
    try:
        await dlq_cmd._run_dlq_inspect(argparse.Namespace(queue=dlq, task_id="t-1", scan_limit=2, format="json"))
    except dlq_cmd._DLQNotFound:
        pass
    out = json.loads(capsys.readouterr().out)

    assert out.get("found") is True or out.get("scan_exhausted") is True, (
        f"t-1 liegt hinter dem Scan-Limit, gemeldet ist 'not found' ohne Hinweis: {out}"
    )
