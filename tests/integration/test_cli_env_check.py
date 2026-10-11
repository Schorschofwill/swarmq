"""``swarmq check`` gegen den Test-Valkey (Plan 0.12.0, MR 14, BZ-04).

Nur lesend (``CONFIG GET``, ``INFO``); der Test-Valkey laeuft mit
``noeviction`` ohne Speichergrenze. Die Faelle mit verdraengender Policy
stehen als Unit-Tests (``tests/unit/test_cli_env_check.py``) und als
Chaos-Test mit eigenem Wegwerf-Container (``tests/chaos/test_valkey_eviction_policy.py``).

Test-Liste:
- [x] Test-Valkey: Exit 0, Policy ``noeviction``, Version >= 8, Werte gelesen
- [x] Broker nicht erreichbar -> Exit 1 (Entscheidung Georg 2026-10-11)
- [x] mit ``SWARMQ_BROKER_URL``: Abschnitt ``queues`` nennt die verwaiste
      gemeldete Queue und die fehlende ``.XQ`` (Plan 0.12.0, MR 18, BZ-14)
"""

from __future__ import annotations

import asyncio
import json
import uuid

import aio_pika
import pytest

from swarmq.cli import main

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


def test_check_reads_the_test_valkey(
    valkey_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)

    code = main(["check", "--format", "json"])

    report = json.loads(capsys.readouterr().out)
    assert code == 0
    valkey_section = report["valkey"]
    assert valkey_section["maxmemory_policy"] == "noeviction"
    assert valkey_section["eviction"] == "safe"
    assert int(valkey_section["version"].split(".")[0]) >= 8
    assert isinstance(valkey_section["used_memory"], int)
    assert valkey_section["aof_enabled"] in (True, False)
    assert valkey_section["cluster_enabled"] is False


def test_check_reports_orphaned_queues_and_missing_dead_letter_queues(
    valkey_url: str, rabbitmq_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from tests.support.queues import delete_queue_family

    suffix = uuid.uuid4().hex[:10]
    orphaned, without_xq = f"chk-old-{suffix}", f"chk-noxq-{suffix}"

    async def _prepare() -> None:
        broker = RabbitMQBroker(rabbitmq_url)
        await broker.connect()
        try:
            await broker.declare_queue(orphaned)
            await broker.declare_queue(without_xq)
            await broker.publish(orphaned, b"{}")
        finally:
            await broker.close()
        connection = await aio_pika.connect(rabbitmq_url)
        try:
            channel = await connection.channel()
            await channel.queue_delete(f"{without_xq}.XQ")
        finally:
            await connection.close()
        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        try:
            await backend.schedules.report_queues([orphaned, without_xq], ttl_ms=60_000)
        finally:
            await backend.close()

    asyncio.run(_prepare())
    monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)
    monkeypatch.setenv("SWARMQ_BROKER_URL", rabbitmq_url)
    # Lebendigkeitsfenster 0 ms: die eben geschriebene Meldung gilt schon als alt.
    monkeypatch.setenv("SWARMQ_SCHEDULER_SEEN_INTERVAL_S", "0.0001")
    try:
        code = main(["check", "--format", "json"])
        report = json.loads(capsys.readouterr().out)
    finally:
        asyncio.run(delete_queue_family(rabbitmq_url, orphaned, without_xq))

    assert code == 0
    queues = report["queues"]
    assert queues["checked"] is True
    assert [entry["queue"] for entry in queues["orphaned"]] == [orphaned]
    assert queues["missing_dead_letter_queues"] == [without_xq]
    codes = sorted(f["code"] for f in report["findings"] if f["section"] == "queues")
    assert codes == ["dead_letter_queue_missing", "orphaned_queue"]


def test_unreachable_broker_exits_1(
    valkey_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Entscheidung Georg 2026-10-11: wie Valkey -> Exit 1. Port 1 lehnt ab.
    monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)
    monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@127.0.0.1:1/")

    code = main(["check", "--format", "json"])

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "swarmq check:" in captured.err and "127.0.0.1:1" in captured.err
