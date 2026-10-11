"""Unroutbarer Fire des Leaders (Plan 0.12.0, MR 6, BZ-03).

Ein Cron-Fire an eine fehlende Queue ist verloren: Commit, Zaehler, Log
gedrosselt einmal je Cron und 10 min. Ein Oneshot wird mit Score
``now + 60 s`` zurueckgestellt (``unroutable_since`` ab dem ersten Versuch)
und nach ``scheduler_unroutable_oneshot_max_s`` verworfen: Event, Metrik,
Fehlerergebnis unter ``task_id``, bei ``cli_unique`` die Unique-Freigabe.

Test-Liste:
- [x] Cron unroutbar: Commit auf den naechsten Termin
- [x] Cron unroutbar: Signal ``SCHEDULER_FIRE_UNROUTABLE`` (kind ``cron_lost``)
- [x] Cron unroutbar: WARNING ``scheduler_fire_unroutable``, gedrosselt je Cron und 10 min
- [x] Drossel-Tabelle bleibt begrenzt (abgelaufene Eintraege fallen weg)
- [x] fremd gemeldeter Cron: zusaetzlich ``scheduler_fire_unroutable_foreign``
- [x] dessen Hinweis nennt vhost und ``SWARMQ_NAMESPACE`` (BZ-10, MR 15)
- [x] anderer Publish-Fehler: weiterhin kein Commit
- [x] Oneshot unroutbar: Commit mit Score ``now + 60 s`` und ``unroutable_since = now``
- [x] Oneshot erneut unroutbar: ``unroutable_since`` bleibt beim ersten Versuch
- [x] Oneshot ueber der Frist: entfernt, ERROR-Event, Signal ``oneshot_dropped``
- [x] verworfener Oneshot: Fehlerergebnis unter ``task_id``
- [x] verworfener ``cli_unique``-Oneshot: Unique-Freigabe mit ``task_id`` als Owner
- [x] Backend ohne ``store_result`` (MagicMock, Q15): Verwerfen laeuft trotzdem
- [x] Owner-Check lehnt das Verwerfen ab: kein Ergebnis, Standby
- [x] zurueckgestellter Oneshot: WARNING ``scheduler_oneshot_unroutable``, gedrosselt wie beim Cron
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate, UnroutableMessage
from swarmq.scheduler import ScheduleDefinition
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]

_NOW_S = 1_700_000_100.0
_NOW_MS = int(_NOW_S * 1000)
_SCORE_MS = 1_700_000_000_000


def _cron(name: str = "cron_a", **fields: Any) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="missing",
        cron="*/5 * * * *",
        fire_ts_ms=_SCORE_MS,
        source="static",
        **fields,
    )


@pytest.fixture
def loop(backend, broker, config, lock_manager, store, signal_dispatcher, monkeypatch):
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    loop.clock_s = _NOW_S
    monkeypatch.setattr(loop, "_wall_time", lambda: loop.clock_s)
    broker.publish = AsyncMock(side_effect=UnroutableMessage("no queue 'missing'"))
    return loop


async def test_unroutable_cron_fire_is_committed_to_the_next_occurrence(loop, store):
    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    store.commit_fire.assert_awaited_once()
    name, next_fire_ts_ms, updated, owner = store.commit_fire.await_args.args
    assert (name, owner) == ("cron_a", "test-owner")
    assert next_fire_ts_ms > _NOW_MS
    assert updated.fire_ts_ms == next_fire_ts_ms
    assert store.commit_fire.await_args.kwargs["fired_ts_ms"] == _SCORE_MS


def _recorded(signal_dispatcher, signal: Signal) -> list[dict[str, object]]:
    seen: list[dict[str, object]] = []

    async def _handler(**kw: object) -> None:
        seen.append(kw)

    signal_dispatcher.connect(signal, _handler)
    return seen


async def test_unroutable_cron_fire_emits_the_unroutable_signal(loop, signal_dispatcher):
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_FIRE_UNROUTABLE)

    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    assert seen == [{"worker_id": "w-1", "schedule_name": "cron_a", "queue": "missing", "kind": "cron_lost"}]


def _loop_events(logs, event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["logger"] == "swarmq.scheduler.loop" and e["event"] == event]


async def test_unroutable_cron_warning_is_throttled_per_cron_and_ten_minutes(loop, swarmq_logs):
    await loop._fire_one("cron_a", _SCORE_MS, _cron())
    loop.clock_s += 599
    await loop._fire_one("cron_a", _SCORE_MS, _cron())
    await loop._fire_one("cron_b", _SCORE_MS, _cron("cron_b"))
    loop.clock_s += 1
    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    warnings = _loop_events(swarmq_logs, "scheduler_fire_unroutable")
    assert [(w["schedule_name"], w["log_level"]) for w in warnings] == [
        ("cron_a", "warning"),
        ("cron_b", "warning"),
        ("cron_a", "warning"),
    ]
    assert warnings[0]["queue"] == "missing"
    assert _loop_events(swarmq_logs, "scheduler_publish_failed") == []


async def test_unroutable_log_table_stays_bounded(loop):
    for i in range(1100):
        await loop._fire_one(f"cron_{i}", _SCORE_MS, _cron(f"cron_{i}"))
    loop.clock_s += 600

    await loop._fire_one("cron_late", _SCORE_MS, _cron("cron_late"))

    assert len(loop._unroutable_logged_at) == 1


async def test_unroutable_cron_reported_by_another_worker_logs_foreign_hint(loop, swarmq_logs):
    loop._reported_names = frozenset({"cron_a"})

    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    [foreign] = _loop_events(swarmq_logs, "scheduler_fire_unroutable_foreign")
    assert (foreign["schedule_name"], foreign["queue"], foreign["log_level"]) == ("cron_a", "missing", "warning")


async def test_foreign_hint_names_vhost_and_namespace(loop, swarmq_logs):
    """BZ-10 (MR 15): getrennte vhosts brauchen je App einen Namensraum."""
    loop._reported_names = frozenset({"cron_a"})

    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    [foreign] = _loop_events(swarmq_logs, "scheduler_fire_unroutable_foreign")
    assert "vhost" in foreign["hint"]
    assert "SWARMQ_NAMESPACE" in foreign["hint"]


@pytest.mark.parametrize("error", [BrokerConnectionError("down"), PublishIndeterminate("deadline")])
async def test_other_publish_errors_still_skip_the_commit(loop, broker, store, error):
    """Q14: nur ``UnroutableMessage`` gilt als verlorener Fire."""
    broker.publish = AsyncMock(side_effect=error)

    await loop._fire_one("cron_a", _SCORE_MS, _cron())

    store.commit_fire.assert_not_awaited()


def _oneshot(name: str = "oneshot:t-1", **fields: Any) -> ScheduleDefinition:
    values: dict[str, Any] = {"source": "cli", "task_id": "t-1", **fields}
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="missing",
        cron=None,
        fire_ts_ms=_SCORE_MS,
        **values,
    )


async def test_unroutable_oneshot_is_deferred_by_sixty_seconds_from_now(loop, store):
    await loop._fire_one("oneshot:t-1", _SCORE_MS, _oneshot())

    store.commit_fire.assert_awaited_once()
    name, next_fire_ts_ms, updated, _owner = store.commit_fire.await_args.args
    assert (name, next_fire_ts_ms) == ("oneshot:t-1", _NOW_MS + 60_000)
    assert updated.unroutable_since == _NOW_MS
    assert updated.fire_ts_ms == _NOW_MS + 60_000
    assert store.commit_fire.await_args.kwargs["fired_ts_ms"] == _SCORE_MS


async def test_repeatedly_unroutable_oneshot_keeps_its_first_unroutable_since(loop, store):
    first_ms = _NOW_MS - 3_600_000

    await loop._fire_one("oneshot:t-1", _SCORE_MS, _oneshot(unroutable_since=first_ms))

    _name, next_fire_ts_ms, updated, _owner = store.commit_fire.await_args.args
    assert next_fire_ts_ms == _NOW_MS + 60_000
    assert updated.unroutable_since == first_ms


async def test_oneshot_unroutable_past_the_limit_is_dropped(loop, store, config, signal_dispatcher, swarmq_logs):
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_FIRE_UNROUTABLE)
    since_ms = _NOW_MS - int(config.scheduler_unroutable_oneshot_max_s * 1000)

    await loop._fire_one("oneshot:t-1", _SCORE_MS, _oneshot(unroutable_since=since_ms))

    store.commit_fire.assert_awaited_once()
    assert store.commit_fire.await_args.args[:3] == ("oneshot:t-1", -1, None)
    assert store.commit_fire.await_args.kwargs["fired_ts_ms"] == _SCORE_MS
    [dropped] = _loop_events(swarmq_logs, "scheduler_oneshot_unroutable_dropped")
    assert dropped["log_level"] == "error"
    assert (dropped["task_id"], dropped["queue"], dropped["unroutable_since"]) == ("t-1", "missing", since_ms)
    assert seen == [{"worker_id": "w-1", "schedule_name": "oneshot:t-1", "queue": "missing", "kind": "oneshot_dropped"}]


def _expired(config, **fields: Any) -> ScheduleDefinition:
    return _oneshot(unroutable_since=_NOW_MS - int(config.scheduler_unroutable_oneshot_max_s * 1000), **fields)


async def test_dropped_oneshot_stores_an_error_result_under_its_task_id(loop, backend, config):
    backend.store_result = AsyncMock()
    backend.notify_result = AsyncMock()

    await loop._fire_one("oneshot:t-1", _SCORE_MS, _expired(config))

    backend.store_result.assert_awaited_once()
    task_id, raw, ttl_ms = backend.store_result.await_args.args
    assert (task_id, ttl_ms) == ("t-1", config.result_ttl * 1000)
    result = decode_result(raw)
    assert result["is_error"] is True
    assert result["task"] == {"id": "t-1", "queue": "missing", "name": "ping"}
    assert "unroutable" in result["error_message"]
    backend.notify_result.assert_awaited_once_with("t-1")


async def test_dropped_cli_unique_oneshot_releases_its_unique_claim(loop, backend, config):
    backend.release_unique = AsyncMock(return_value=True)

    await loop._fire_one(
        "oneshot:t-1",
        _SCORE_MS,
        _expired(config, source="cli_unique", dedup_hash="h-1"),
    )

    backend.release_unique.assert_awaited_once_with("h-1", "t-1", op_timeout=config.backend_op_timeout)


async def test_dropped_plain_oneshot_releases_nothing(loop, backend, config):
    backend.release_unique = AsyncMock(return_value=True)

    await loop._fire_one("oneshot:t-1", _SCORE_MS, _expired(config))

    backend.release_unique.assert_not_awaited()


async def test_drop_survives_a_backend_without_result_methods(loop, backend, config, swarmq_logs):
    """Q15: ``MagicMock`` liefert nicht-awaitbare Methoden; das Verwerfen
    endet trotzdem und meldet nur das fehlende Ergebnis."""
    backend.store_result = MagicMock()
    backend.release_unique = MagicMock()

    await loop._fire_one(
        "oneshot:t-1",
        _SCORE_MS,
        _expired(config, source="cli_unique", dedup_hash="h-1"),
    )

    assert _loop_events(swarmq_logs, "scheduler_oneshot_unroutable_dropped")
    assert _loop_events(swarmq_logs, "scheduler_unroutable_result_store_failed")
    assert _loop_events(swarmq_logs, "scheduler_unroutable_unique_release_failed")


async def test_drop_rejected_by_the_owner_check_stores_nothing_and_steps_down(loop, store, backend, config):
    store.commit_fire = AsyncMock(return_value=False)
    backend.store_result = AsyncMock()

    await loop._fire_one("oneshot:t-1", _SCORE_MS, _expired(config))

    backend.store_result.assert_not_awaited()
    assert loop.state == "standby"


async def test_deferred_oneshot_warning_is_throttled(loop, swarmq_logs):
    await loop._fire_one("oneshot:t-1", _SCORE_MS, _oneshot())
    loop.clock_s += 60
    await loop._fire_one("oneshot:t-1", _SCORE_MS, _oneshot(unroutable_since=_NOW_MS))

    [warning] = _loop_events(swarmq_logs, "scheduler_oneshot_unroutable")
    assert (warning["task_id"], warning["queue"], warning["log_level"]) == ("t-1", "missing", "warning")
