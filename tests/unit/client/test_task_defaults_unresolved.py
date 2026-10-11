"""WARNING ``client_task_defaults_unresolved`` (BZ-24, Plan 0.12.0 MR 2).

Ein Producer ohne TaskInfo (keine Registry) loest weder Dedup
(``unique_for``) noch Prioritaet noch den Liefermodus aus der TaskInfo des
Workers auf. Das wird einmal je Task-Name und Client-Instanz sichtbar.

Testliste:
# [x] erster Aufruf warnt (Nachweis test_befund_bz24_registry_less_producer.py)
# [x] zweiter Aufruf mit demselben Task-Namen warnt nicht
# [x] ein anderer Task-Name warnt erneut, eine andere Client-Instanz auch
# [x] mit Registry keine Warnung
# [x] die Menge der gemeldeten Namen ist auf 1000 gedeckelt
# [x] schedule_many warnt ebenso einmal je Name
# [x] ohne queue= scheitert der Aufruf (ValueError) und warnt nicht; ein
#     spaeterer gueltiger Aufruf warnt dann einmal
"""

from __future__ import annotations

import pytest

from swarmq.client import Client
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)

_EVENT = "client_task_defaults_unresolved"


def _warnings(logs: list[dict]) -> list[dict]:
    return [e for e in logs if e["event"] == _EVENT]


async def test_second_schedule_of_the_same_task_does_not_warn_again(broker, backend) -> None:
    producer = Client(broker=broker, backend=backend)

    with capture_swarmq_logs() as logs:
        await producer.schedule("billing.send_invoice", 1, queue="billing")
        await producer.schedule("billing.send_invoice", 2, queue="billing")

    [entry] = _warnings(logs)
    assert entry["log_level"] == "warning"
    assert entry["task_name"] == "billing.send_invoice"


async def test_each_task_name_and_each_client_instance_warns_once(broker, backend) -> None:
    first = Client(broker=broker, backend=backend)
    second = Client(broker=broker, backend=backend)

    with capture_swarmq_logs() as logs:
        await first.schedule("billing.send_invoice", queue="billing")
        await first.schedule("billing.refund", queue="billing")
        await second.schedule("billing.send_invoice", queue="billing")

    assert [e["task_name"] for e in _warnings(logs)] == [
        "billing.send_invoice",
        "billing.refund",
        "billing.send_invoice",
    ]


async def test_client_with_registry_does_not_warn(client) -> None:
    with capture_swarmq_logs() as logs:
        await client.schedule("add", 1, 2)

    assert _warnings(logs) == []


async def test_reported_task_names_are_capped_at_1000(broker, backend) -> None:
    """Ein Producer mit dynamischen Task-Namen darf den Speicher nicht
    unbegrenzt fuellen: ueber 1000 gemeldete Namen hinaus wird nicht mehr
    gemerkt (und nicht mehr gewarnt)."""
    producer = Client(broker=broker, backend=backend)

    with capture_swarmq_logs() as logs:
        for i in range(1005):
            await producer.schedule(f"dyn.task_{i}", queue="dyn")
        await producer.schedule("dyn.task_1004", queue="dyn")

    assert len(_warnings(logs)) == 1000


async def test_failed_schedule_without_queue_does_not_use_up_the_warning(broker, backend) -> None:
    producer = Client(broker=broker, backend=backend)

    with capture_swarmq_logs() as logs:
        with pytest.raises(ValueError, match="No queue specified"):
            await producer.schedule("billing.send_invoice")
        assert _warnings(logs) == []
        await producer.schedule("billing.send_invoice", queue="billing")

    assert [e["task_name"] for e in _warnings(logs)] == ["billing.send_invoice"]


async def test_schedule_many_warns_once_per_task_name(broker, backend) -> None:
    producer = Client(broker=broker, backend=backend)

    with capture_swarmq_logs() as logs:
        await producer.schedule_many(
            [("billing.send_invoice", (1,), {}), ("billing.send_invoice", (2,), {})], queue="billing"
        )

    assert [e["task_name"] for e in _warnings(logs)] == ["billing.send_invoice"]
