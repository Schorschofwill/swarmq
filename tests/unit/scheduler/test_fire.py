"""Fire des Schedulers: task_id je Fire (Plan 0.12.0, MR 5, BZ-02).

Test-Liste:
- [x] Oneshot ``oneshot:<id>`` feuert unter ``<id>`` (Nachweis BZ-02)
- [x] Cron bekommt je Fire eine neue ID, auch wenn sein Name wie ein Oneshot aussieht
- [x] Oneshot mit gesetztem ``ScheduleDefinition.task_id`` feuert unter diesem Feld
- [x] Oneshot ohne ``oneshot:``-Präfix und ohne Feld bekommt eine neue ID

Unique beim Fire (Plan 0.12.0, MR 9, BZ-09):
- [x] Cron mit ``unique_for`` in der Definition, Task nicht in der Registry
      → kein Überlappen (Nachweis BZ-09)
- [x] Alt-Definition ohne ``unique_for``, Registry mit ``unique_until="start"``
      → beide Werte aus der Registry
- [x] Definition mit ``unique_for`` schlägt die Registry (beide Werte)
- [x] Publish-Fehler nach eigenem Claim → Flag frei, der nächste Poll feuert
- [x] ``PublishIndeterminate`` → Flag bleibt (Nachricht evtl. zugestellt, Q14)
- [x] ``cli_unique`` (fremder Claim des Producers) → keine Freigabe beim Publish-Fehler
- [x] unroutbarer Cron-Fire nach eigenem Claim → Flag frei
- [x] unroutbarer Oneshot (Claim des Leaders aus der Registry) → Flag frei vor dem Zurückstellen
- [x] Freigabe scheitert oder fehlt (MagicMock-Backend, Q15) → geloggt, Fire endet normal
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import msgspec
import pytest

from swarmq.exceptions import PublishIndeterminate, UnroutableMessage
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.serialization import compute_dedup_hash
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]


def _published_task_ids(broker) -> list[str]:
    return [msgspec.json.decode(call.args[1])["task"]["id"] for call in broker.publish.await_args_list]


def _definition(name: str, *, cron: str | None, **fields: object) -> ScheduleDefinition:
    values: dict[str, object] = {"fire_ts_ms": 1_700_000_000_000, "source": "cli", **fields}
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron=cron,
        **values,  # type: ignore[arg-type]
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


def _register_ping(**unique) -> None:
    class Ping(Task):
        task_info = TaskInfo(name="ping", queues=["default"], **unique)

        async def run(self) -> None:
            return None


def _unique_headers(broker) -> tuple[str, str]:
    headers = broker.publish.await_args.kwargs["headers"]
    return headers["x-swarmq-unique-hash"], headers["x-swarmq-unique-until"]


async def test_old_definition_takes_both_unique_values_from_the_registry(loop, backend, broker, _cleanup_tasks):
    """BZ-09: Eine Definition aus 0.11 (ohne ``unique_for``) mischt nicht den
    Default ``unique_until`` der Definition mit ``unique_for`` der Registry."""
    _register_ping(unique_for=3600, unique_until="start")
    backend.claim_unique = AsyncMock(return_value=None)

    await loop._fire_one("nightly", 1_700_000_000_000, _definition("nightly", cron="*/5 * * * *"))

    assert backend.claim_unique.await_args.args[2] == 3_600_000
    assert _unique_headers(broker)[1] == "start"


async def test_definition_with_unique_for_beats_the_registry(loop, backend, broker, _cleanup_tasks):
    _register_ping(unique_for=60, unique_until="completion")
    backend.claim_unique = AsyncMock(return_value=None)
    definition = _definition("nightly", cron="*/5 * * * *", unique_for=3600, unique_until="start")

    await loop._fire_one("nightly", 1_700_000_000_000, definition)

    assert backend.claim_unique.await_args.args[2] == 3_600_000
    assert _unique_headers(broker)[1] == "start"


@pytest.fixture
async def memory_loop(broker, config, lock_manager, signal_dispatcher):
    """Leader auf einem In-Memory-Backend mit einem faelligen Unique-Cron
    (``unique_for`` in der Definition, Task nicht in der Registry)."""
    memory = InMemoryBackend(FakeClock())
    await memory.connect()
    await memory.locks.acquire(SCHEDULER_LOCK_NAME, owner="test-owner", ttl_ms=60_000, max_concurrency=1)
    definition = _definition("nightly", cron="*/5 * * * *", fire_ts_ms=1_000, unique_for=3600)
    await memory.schedules.add("nightly", msgspec.json.encode(definition), 1_000)
    loop = _make_loop(memory, broker, config, lock_manager, ScheduleStore(memory), signal_dispatcher)
    loop._state = "leader"
    return loop


async def test_publish_failure_releases_the_own_claim_so_the_next_poll_fires(memory_loop, broker):
    """BZ-09/B7: Ohne Freigabe haelt der eigene Claim des gescheiterten
    Fires den Wiederholungsversuch als Duplikat auf; der Termin ginge verloren."""
    broker.publish.side_effect = [ConnectionError("broker gone"), None]

    await memory_loop._poll_tick()  # Publish scheitert, kein Commit
    await memory_loop._poll_tick()  # derselbe Termin erneut

    assert broker.publish.await_count == 2


async def test_indeterminate_publish_keeps_the_claim(memory_loop, broker):
    """Q14: Bei ``PublishIndeterminate`` kann die Nachricht angekommen sein;
    das Flag bleibt, der Wiederholungsversuch gilt als Duplikat."""
    broker.publish.side_effect = [PublishIndeterminate("confirm deadline"), None]

    await memory_loop._poll_tick()
    await memory_loop._poll_tick()

    assert broker.publish.await_count == 1


async def test_blocked_publish_releases_the_own_claim(memory_loop, broker):
    """Q14 (Plan 0.12.0, MR 16, BZ-16): ``BrokerPublishBlocked`` heisst, es
    wurde nichts gesendet; das Flag weicht, der naechste Poll feuert."""
    from swarmq.exceptions import BrokerPublishBlocked

    broker.publish.side_effect = [BrokerPublishBlocked("resource alarm"), None]

    await memory_loop._poll_tick()
    await memory_loop._poll_tick()

    assert broker.publish.await_count == 2


async def test_publish_failure_keeps_a_cli_unique_claim_of_the_producer(loop, backend, broker):
    """B7: Das Flag eines ``cli_unique``-Oneshots hat der Producer genommen;
    der Leader gibt es bei einem Publish-Fehler nicht frei."""
    backend.release_unique = AsyncMock(return_value=True)
    broker.publish.side_effect = ConnectionError("broker gone")
    definition = _definition("oneshot:abc", cron=None, source="cli_unique", dedup_hash="producer-hash", unique_for=3600)

    await loop._fire_one("oneshot:abc", 1_700_000_000_000, definition)

    backend.release_unique.assert_not_awaited()


async def test_unroutable_cron_fire_releases_the_own_claim(memory_loop, broker):
    """Ein unroutbarer Cron-Fire ist verloren und festgeschrieben (BZ-03);
    sein Flag blockierte sonst den naechsten Termin bis ``unique_for``."""
    broker.publish.side_effect = UnroutableMessage("no queue 'default'")

    await memory_loop._poll_tick()

    flag_owner = await memory_loop._backend.claim_unique(compute_dedup_hash("ping", (), {}), "probe", 60_000)
    assert flag_owner is None


async def test_deferred_unroutable_oneshot_releases_the_own_claim(loop, backend, broker, _cleanup_tasks):
    """Ein Oneshot behaelt seine task_id (BZ-02); haelt der Leader sein
    eigenes Flag fest, endet der Versuch nach 60 s als Duplikat und der
    Oneshot laeuft nie."""
    _register_ping(unique_for=3600)
    backend.claim_unique = AsyncMock(return_value=None)
    backend.release_unique = AsyncMock(return_value=True)
    broker.publish.side_effect = UnroutableMessage("no queue 'default'")

    await loop._fire_one("oneshot:abc", 1_700_000_000_000, _definition("oneshot:abc", cron=None))

    backend.release_unique.assert_awaited_once_with(
        compute_dedup_hash("ping", (), {}), "abc", op_timeout=loop._config.backend_op_timeout
    )


async def test_failed_release_is_logged_and_the_lost_cron_fire_still_commits(
    loop, backend, broker, store, swarmq_logs, _cleanup_tasks
):
    """Q15: ``release_unique`` scheitert oder fehlt (hier ein ``MagicMock``-
    Backend, dessen Methode nicht awaitable ist); das Fire endet normal, das
    Flag laeuft ueber seine TTL ab."""
    _register_ping(unique_for=3600)
    backend.claim_unique = AsyncMock(return_value=None)
    broker.publish.side_effect = UnroutableMessage("no queue 'default'")

    await loop._fire_one("nightly", 1_700_000_000_000, _definition("nightly", cron="*/5 * * * *"))

    store.commit_fire.assert_awaited_once()
    [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_unique_release_failed"]
    assert entry["logger"] == "swarmq.scheduler.loop"
    assert entry["schedule_name"] == "nightly"
    assert entry["error_type"] == "TypeError"
