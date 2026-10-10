"""Fire des Schedulers: task_id je Fire (Plan 0.12.0, MR 5, BZ-02).

Test-Liste:
- [x] Oneshot ``oneshot:<id>`` feuert unter ``<id>`` (Nachweis BZ-02)
- [x] Cron bekommt je Fire eine neue ID, auch wenn sein Name wie ein Oneshot aussieht
- [x] Oneshot mit gesetztem ``ScheduleDefinition.task_id`` feuert unter diesem Feld
- [x] Oneshot ohne ``oneshot:``-Präfix und ohne Feld bekommt eine neue ID
"""

from __future__ import annotations

import msgspec
import pytest

from swarmq.scheduler import ScheduleDefinition
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]


def _published_task_ids(broker) -> list[str]:
    return [msgspec.json.decode(call.args[1])["task"]["id"] for call in broker.publish.await_args_list]


def _definition(name: str, *, cron: str | None, **fields: object) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron=cron,
        fire_ts_ms=1_700_000_000_000,
        source="cli",
        **fields,
    )


@pytest.fixture
def loop(backend, broker, config, lock_manager, store, signal_dispatcher):
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    return loop


async def test_cron_gets_a_new_task_id_per_fire_even_with_oneshot_like_name(loop, broker):
    definition = _definition("oneshot:looks-like-id", cron="*/5 * * * *")

    await loop._fire_one("oneshot:looks-like-id", 1_700_000_000_000, definition)
    await loop._fire_one("oneshot:looks-like-id", 1_700_000_300_000, definition)

    first, second = _published_task_ids(broker)
    assert first != second
    assert "looks-like-id" not in (first, second)


async def test_oneshot_fires_under_its_task_id_field(loop, broker):
    definition = _definition("oneshot:name-part", cron=None, task_id="field-id")

    await loop._fire_one("oneshot:name-part", 1_700_000_000_000, definition)

    assert _published_task_ids(broker) == ["field-id"]


async def test_oneshot_without_prefix_or_field_gets_a_fresh_task_id(loop, broker):
    definition = _definition("plain-name", cron=None)

    await loop._fire_one("plain-name", 1_700_000_000_000, definition)

    (task_id,) = _published_task_ids(broker)
    assert task_id not in ("plain-name", "")
