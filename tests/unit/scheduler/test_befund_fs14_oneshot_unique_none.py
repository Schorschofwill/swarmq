"""Nachweis FS-14: ``schedule(delay=/eta=, unique_for=None)`` wird beim Fire
trotzdem dedupliziert und ggf. still verworfen.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-14; Quelle MC-1,
Skript ``mc/mc_unique_none_oneshot.py``).

``unique_for=None`` schaltet die Dedup fuer einen Aufruf ab
(``_resolve_unique``: "None -> dedup off"). Der Client nimmt kein Flag und
schreibt die Definition mit ``source="cli"`` und ``unique_for=None`` -- genau
wie bei "nicht angegeben". Beim Fire sieht der Leader ``unique_for is None``,
zieht ``TaskInfo.unique_for`` aus seiner Registry nach und ruft
``claim_unique`` (``scheduler/fire.py``). Haelt ein anderer Lauf mit denselben
Argumenten das Flag, verwirft der Leader den Oneshot per ``commit_fire(-1)``:
kein Publish, kein Ergebnis unter der von ``schedule()`` gelieferten ID,
``get_result`` laeuft in den Timeout.

Offene Produktfrage 3 (Register): ob ``cli``-Oneshots ohne Opt-out
dedupliziert werden. Dieser Nachweis prueft nur den ausdruecklichen Opt-out
(``unique_for=None``), der laut ``_resolve_unique`` und den
Unique-Anforderungen nie dedupliziert werden darf.

Behoben (Plan 2026-10-14-0748, U13, KTD10): Der Client speichert den Opt-out
als ``source="cli_unique"`` ohne ``dedup_hash``; der Leader faellt bei
``cli_unique`` nie auf seine Registry zurueck.

Testliste:
# [x] schedule(delay=60, unique_for=None) bei TaskInfo(unique_for=3600), Flag
#     haelt ein anderer Lauf -> der Leader publiziert unter der gelieferten
#     task_id und nimmt keinen Unique-Claim
"""

from __future__ import annotations

from typing import override
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest

from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.scheduler import ScheduleDefinition
from swarmq.signals import SignalDispatcher
from swarmq.task import Task, TaskInfo
from tests.unit.scheduler._helpers import _make_loop  # pyright: ignore[reportUnknownVariableType] - ungetypter Bestandshelfer

pytestmark = [pytest.mark.timeout(5)]


async def _deferred_definition(
    monkeypatch: pytest.MonkeyPatch, registry: TaskRegistry
) -> tuple[str, ScheduleDefinition]:
    """Die Definition, die ``Client.schedule(..., delay=60, unique_for=None)`` schreibt."""
    from swarmq.client import schedule as client_schedule

    schedule_store = MagicMock()
    schedule_store.add_schedule = AsyncMock()

    def _store_factory(_backend: object) -> MagicMock:
        return schedule_store

    monkeypatch.setattr(client_schedule, "ScheduleStore", _store_factory)
    client_backend = AsyncMock()
    client_backend.claim_unique = AsyncMock(return_value=None)
    client = Client(broker=AsyncMock(), backend=client_backend, registry=registry)

    task_id = await client.schedule("fs14_ping", 7, queue="default", delay=60.0, unique_for=None)

    client_backend.claim_unique.assert_not_awaited()  # Voraussetzung: Opt-out beim Client wirksam
    definition: ScheduleDefinition = schedule_store.add_schedule.await_args.args[0]
    return task_id, definition


@pytest.mark.befund("FS-14")
@pytest.mark.usefixtures("_cleanup_tasks")
async def test_oneshot_with_unique_for_none_is_published_despite_a_held_flag(
    monkeypatch: pytest.MonkeyPatch,
    backend: MagicMock,
    broker: MagicMock,
    config: SwarmQConfig,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    class Fs14Ping(Task):
        task_info = TaskInfo(name="fs14_ping", queues=["default"], unique_for=3600)

        @override
        async def run(self, value: int) -> int:
            return value

    registry = TaskRegistry()
    registry.register(Fs14Ping)
    task_id, definition = await _deferred_definition(monkeypatch, registry)
    # Ein anderer Lauf mit denselben Argumenten haelt das Flag.
    backend.claim_unique = AsyncMock(return_value="fs14-other-run")
    backend.store_result = AsyncMock()
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"

    await loop._fire_one(definition.schedule_name, definition.fire_ts_ms or 0, definition)

    published = [msgspec.json.decode(call.args[1])["task"]["id"] for call in broker.publish.await_args_list]
    assert published == [task_id], (
        f"Oneshot mit unique_for=None verworfen (claim_unique {backend.claim_unique.await_count}x, "
        f"commit_fire {store.commit_fire.await_args}); get_result({task_id!r}) laeuft in den Timeout"
    )
    backend.claim_unique.assert_not_awaited()
