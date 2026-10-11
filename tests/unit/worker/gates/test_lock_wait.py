"""Lock-Konflikte warten zeitbasiert (Plan 0.12.0, MR 13, BZ-05).

Kein Deckel 10 mehr: Ein Konflikt stellt die Nachricht mit Backoff zurueck,
gemessen ab dem ersten Konflikt (``x-swarmq-lock-wait-since``). In die DLQ
(``LockWaitExceeded``) geht sie erst, wenn derselbe Halter laenger haelt als
die beobachtete PTTL + 60 s (Stillstand) oder ``SWARMQ_LOCK_WAIT_MAX_S``
erreicht ist; ``max_age`` bleibt die harte Obergrenze (Expiry-Gate davor).
Der Legacy-Zaehler ``x-swarmq-lock-conflict-count`` wird nicht mehr erhoeht
(0.11-Pods derselben App schickten wartende 0.12-Kopien sonst ab 10 in die
DLQ); der neue Header ``x-swarmq-lock-conflicts`` speist
``lock_conflict_count``.

Test-Liste:
- [x] erste Konflikt-Kopie: ``lock-conflicts`` = 1, ``lock-wait-since`` = jetzt,
      ``lock-name``; der Legacy-Zaehler wird nicht gesetzt
- [x] spaetere Kopie: Zaehler +1, ``wait-since`` bleibt, Legacy-Zaehler bleibt
      unveraendert (kein Deckel)
- [x] ``TASK_LOCKED`` traegt ``lock_conflict_count`` aus dem neuen Header
- [x] Halter-Fingerprint: erste Beobachtung setzt ``lock-holder``,
      ``lock-holder-since`` = jetzt, ``lock-holder-ttl-ms`` = PTTL
- [x] derselbe Halter: ``holder-since`` bleibt; neuer Halter: neu gesetzt
- [x] Stillstand (derselbe Halter laenger als PTTL + 60 s) -> DLQ mit
      ``LockWaitExceeded``, Fehlerergebnis, ``TASK_DLQ``
- [x] knapp unter der Stillstandsgrenze -> weiter zurueckstellen
- [x] PTTL -2 (Key weg): kein Stillstand, Halter-Header entfallen
- [x] PTTL -1 (ohne Frist): die eigene Lock-TTL ist die Basis
- [x] Obergrenze ``lock_wait_max_s`` ab dem ersten Konflikt -> DLQ
- [x] Fingerprint nicht messbar (Store ohne ``observe``, Q15): weiter
      zurueckstellen bis zur Obergrenze, einmal WARNING
      ``lock_observe_unsupported``
- [x] Review MR 13, P2-5: ``AsyncMock``-Store und Protocol-Subklasse ohne
      eigenes ``observe`` (liefert kein ``LockObservation``) -> nicht messbar,
      einmal WARNING ``lock_observe_unsupported``, weiter zurueckstellen
- [x] ``observe`` wirft (Backend-Ausfall): weiter zurueckstellen, WARNING
      ``lock_observe_failed``
- [x] ``task_lock_conflict`` nur beim ersten Konflikt und dann einmal je
      Minute (ab Konflikt 12 bei 60-s-Deckel)
- [x] Backoff min(100 ms * 2^(n-1), 60 s) aus dem neuen Zaehler
- [x] Q6: eine Rate-Limit-Kopie behaelt die Lock-Zustands-Header; die
      Obergrenze greift auch mit Kopien aus anderen Gates
- [x] die fachliche Retry-Kopie entfernt die Lock-Zustands-Header
      (beide Q6-Faelle in ``tests/unit/worker/test_lock_wait_state_headers.py``)
- [x] Review MR 13, P2-4: eine Neuzustellung durch den Broker
      (``x-delivery-count`` > 0: SIGKILL/OOM nach erfolgreichem Acquire,
      Shutdown-Requeue) beginnt eine neue Wartezeit -- der alte
      ``wait-since`` fuehrt nicht sofort zu ``LockWaitExceeded``
- [x] Review MR 13, P2-4 (Mischbetrieb): eine fachliche Retry-Kopie eines
      0.11-Workers behaelt die neuen Header (0.11 kennt sie nicht), zaehlt
      aber ``x-swarmq-retry-count`` hoch -- auch sie beginnt eine neue
      Wartezeit; ebenso eine neue Replay-ID (``dlq retry`` von 0.11)
- [x] Gauges: wartende Konflikte und aeltester Wartender je Worker
      (``tests/unit/worker/test_lock_wait_tracker.py``)
"""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.contracts.coordination import LockObservation
from swarmq.headers import (
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_SINCE,
)
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates.lock import holder_fingerprint, lock_conflict_delay_ms
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_NOW_S = 1_700_000_000.0
_NOW_MS = int(_NOW_S * 1000)
_LEGACY = "x-swarmq-lock-conflict-count"


class Locked(Task):
    task_info = TaskInfo(name="bz05_locked", queues=["default"], lock="resource", hard_timeout=60)

    async def run(self) -> None:
        raise AssertionError("darf bei Lock-Konflikt nicht laufen")


@pytest.fixture
def clock():
    with patch("swarmq.worker.gates.lock.time") as fake:
        fake.time.return_value = _NOW_S
        yield fake


def _worker(
    broker: Any, backend: Any, config: Any, *, observation: LockObservation | None = None, **cfg: Any
) -> Worker:
    registry = TaskRegistry()
    registry.register(Locked)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=dataclasses.replace(config, **cfg))
    store = MagicMock()
    store.acquire = AsyncMock(return_value=None)  # Konflikt
    store.release = AsyncMock(return_value=True)
    if observation is not None:
        store.observe = AsyncMock(return_value=observation)
    worker._lock_manager = store
    return worker


def _message(**headers: Any) -> Any:
    message = make_message(_default_payload(task_id="bz05-1", name="bz05_locked", args=[], kwargs={}), headers=headers)
    message.queue = "default"
    return message


def _copy(broker: Any) -> dict[str, Any]:
    return broker.retry_later.await_args.kwargs["headers"]


async def test_first_conflict_copy_carries_the_wait_state(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))

    await worker._handle_message(_message())

    headers = _copy(broker)
    assert headers[HEADER_LOCK_CONFLICTS] == 1
    assert headers[HEADER_LOCK_WAIT_SINCE] == _NOW_MS
    assert headers["x-swarmq-lock-name"] == "resource"
    assert _LEGACY not in headers
    broker.dead_letter.assert_not_awaited()


async def test_later_copy_counts_up_and_keeps_the_first_conflict(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))

    await worker._handle_message(
        _message(**{HEADER_LOCK_CONFLICTS: 4, HEADER_LOCK_WAIT_SINCE: _NOW_MS - 3_000, _LEGACY: 9})
    )

    headers = _copy(broker)
    assert headers[HEADER_LOCK_CONFLICTS] == 5
    assert headers[HEADER_LOCK_WAIT_SINCE] == _NOW_MS - 3_000
    assert headers[_LEGACY] == 9  # unveraendert, kein Deckel
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 1_600  # 100 ms * 2^4
    broker.dead_letter.assert_not_awaited()


async def test_task_locked_reports_the_new_counter(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))
    seen: list[dict[str, Any]] = []

    @worker._signal_dispatcher.on_signal(Signal.TASK_LOCKED)
    async def _on_locked(**kwargs: Any) -> None:
        seen.append(kwargs)

    await worker._handle_message(_message(**{HEADER_LOCK_CONFLICTS: 2, _LEGACY: 7}))

    assert [s["lock_conflict_count"] for s in seen] == [3]


async def test_first_observation_records_the_holder(broker, backend, config, clock) -> None:
    owners = frozenset({"h1", "h2"})
    worker = _worker(broker, backend, config, observation=LockObservation(owners, 42_000))

    await worker._handle_message(_message())

    headers = _copy(broker)
    assert headers[HEADER_LOCK_HOLDER] == holder_fingerprint(owners)
    assert headers[HEADER_LOCK_HOLDER_SINCE] == _NOW_MS
    assert headers[HEADER_LOCK_HOLDER_TTL_MS] == 42_000


def test_fingerprint_is_the_owner_set_only() -> None:
    assert holder_fingerprint(frozenset({"a", "b"})) == holder_fingerprint(frozenset({"b", "a"}))
    assert holder_fingerprint(frozenset({"a"})) != holder_fingerprint(frozenset({"a", "b"}))


async def test_same_holder_keeps_since_new_holder_resets(broker, backend, config, clock) -> None:
    held = {
        HEADER_LOCK_HOLDER: holder_fingerprint(frozenset({"h1"})),
        HEADER_LOCK_HOLDER_SINCE: _NOW_MS - 30_000,
        HEADER_LOCK_HOLDER_TTL_MS: 90_000,
    }
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 20_000))
    await worker._handle_message(_message(**held))
    same = _copy(broker)
    assert same[HEADER_LOCK_HOLDER_SINCE] == _NOW_MS - 30_000
    assert same[HEADER_LOCK_HOLDER_TTL_MS] == 90_000

    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h2"}), 70_000))
    await worker._handle_message(_message(**held))
    changed = _copy(broker)
    assert changed[HEADER_LOCK_HOLDER] == holder_fingerprint(frozenset({"h2"}))
    assert changed[HEADER_LOCK_HOLDER_SINCE] == _NOW_MS
    assert changed[HEADER_LOCK_HOLDER_TTL_MS] == 70_000


def _stalled(held_ms: int, ttl_ms: int = 90_000, waited_ms: int | None = None) -> dict[str, Any]:
    """Derselbe Halter seit ``held_ms``; die Wartezeit liegt (Standard) unter
    der Obergrenze, damit nur der Stillstand entscheidet."""
    return {
        HEADER_LOCK_HOLDER: holder_fingerprint(frozenset({"h1"})),
        HEADER_LOCK_HOLDER_SINCE: _NOW_MS - held_ms,
        HEADER_LOCK_HOLDER_TTL_MS: ttl_ms,
        HEADER_LOCK_WAIT_SINCE: _NOW_MS - (min(held_ms, 3_600_000) if waited_ms is None else waited_ms),
        HEADER_LOCK_CONFLICTS: 30,
    }


async def test_stalled_holder_dead_letters_with_lock_wait_exceeded(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 85_000))
    dlq: list[dict[str, Any]] = []

    @worker._signal_dispatcher.on_signal(Signal.TASK_DLQ)
    async def _on_dlq(**kwargs: Any) -> None:
        dlq.append(kwargs)

    await worker._handle_message(_message(**_stalled(90_000 + 60_000 + 1)))

    broker.retry_later.assert_not_awaited()
    assert broker.dead_letter.await_count == 1
    traceback = broker.dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"]
    assert traceback.startswith("LockWaitExceeded: lock holder stalled")
    assert [c.args[0] for c in backend.store_result.await_args_list] == ["bz05-1"]
    assert [d["task_id"] for d in dlq] == ["bz05-1"]


async def test_just_below_the_stall_limit_keeps_waiting(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 85_000))

    await worker._handle_message(_message(**_stalled(90_000 + 60_000)))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()


async def test_lock_gone_is_no_stall_and_drops_the_holder(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset(), -2))

    await worker._handle_message(_message(**_stalled(10 * 3_600_000)))

    broker.dead_letter.assert_not_awaited()
    headers = _copy(broker)
    assert not {HEADER_LOCK_HOLDER, HEADER_LOCK_HOLDER_SINCE, HEADER_LOCK_HOLDER_TTL_MS} & set(headers)


async def test_key_without_ttl_uses_the_own_lock_ttl(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), -1))

    await worker._handle_message(_message())

    assert _copy(broker)[HEADER_LOCK_HOLDER_TTL_MS] == 90_000  # hard_timeout 60 s + 30 s


async def test_wait_limit_dead_letters(broker, backend, config, clock) -> None:
    worker = _worker(
        broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000), lock_wait_max_s=600
    )

    await worker._handle_message(_message(**{HEADER_LOCK_WAIT_SINCE: _NOW_MS - 600_000}))

    assert broker.dead_letter.await_count == 1
    assert broker.dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"].startswith(
        "LockWaitExceeded: lock wait limit reached"
    )


async def test_store_without_observe_waits_until_the_limit_and_warns_once(
    broker, backend, config, clock, swarmq_logs
) -> None:
    class _LegacyStore:  # Dritt-Store ohne ``observe`` (Q15)
        acquire = AsyncMock(return_value=None)
        release = AsyncMock(return_value=True)

    worker = _worker(broker, backend, config)
    worker._lock_manager = _LegacyStore()  # type: ignore[assignment]

    for _ in range(2):
        await worker._handle_message(_message(**_stalled(10 * 3_600_000 - 1, ttl_ms=1)))

    broker.dead_letter.assert_not_awaited()  # kein Deckel, kein Stillstand
    assert broker.retry_later.await_count == 2
    warned = [e for e in swarmq_logs if e["event"] == "lock_observe_unsupported"]
    assert len(warned) == 1
    assert warned[0]["log_level"] == "warning"

    await worker._handle_message(_message(**{HEADER_LOCK_WAIT_SINCE: _NOW_MS - 21_600_000}))
    assert broker.dead_letter.await_count == 1


async def test_magicmock_store_is_treated_as_not_measurable(broker, backend, config, clock, swarmq_logs) -> None:
    """Q15: ein ``MagicMock``-Store hat jedes Attribut; ``observe`` liefert
    kein Awaitable -- das zaehlt als nicht messbar, nicht als Fehler."""
    worker = _worker(broker, backend, config)

    await worker._handle_message(_message(**_stalled(10 * 3_600_000 - 1, ttl_ms=1)))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    events = {e["event"] for e in swarmq_logs}
    assert "lock_observe_unsupported" in events
    assert "lock_observe_failed" not in events


async def test_asyncmock_store_is_treated_as_not_measurable(broker, backend, config, clock, swarmq_logs) -> None:
    """Review MR 13, P2-5 (Q15): ein ``AsyncMock``-Store liefert aus
    ``observe`` ein ``MagicMock`` statt ``LockObservation`` -- das zaehlt als
    nicht messbar, nicht als Stillstand oder Fehler."""
    worker = _worker(broker, backend, config)
    store = AsyncMock()
    store.acquire = AsyncMock(return_value=None)
    worker._lock_manager = store

    await worker._handle_message(_message(**_stalled(10 * 3_600_000 - 1, ttl_ms=1)))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    events = {e["event"] for e in swarmq_logs}
    assert "lock_observe_unsupported" in events
    assert "lock_observe_failed" not in events


async def test_protocol_subclass_without_observe_warns_unsupported(broker, backend, config, clock, swarmq_logs) -> None:
    """Review MR 13, P2-5 (Q15): eine Subklasse des ``LockStore``-Protocols
    erbt ``observe`` mit leerem Rumpf (liefert ``None``) -- nicht messbar,
    und das wird wie ein fehlendes ``observe`` einmal gemeldet."""
    from swarmq.contracts.coordination import LockStore

    class _SubclassStore(LockStore):
        async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> None:
            return None

    worker = _worker(broker, backend, config)
    worker._lock_manager = _SubclassStore()  # type: ignore[assignment]

    for _ in range(2):
        await worker._handle_message(_message(**_stalled(10 * 3_600_000 - 1, ttl_ms=1)))

    broker.dead_letter.assert_not_awaited()
    assert broker.retry_later.await_count == 2
    warned = [e for e in swarmq_logs if e["event"] == "lock_observe_unsupported"]
    assert len(warned) == 1
    assert warned[0]["log_level"] == "warning"


async def test_observe_failure_keeps_waiting(broker, backend, config, clock, swarmq_logs) -> None:
    worker = _worker(broker, backend, config)
    worker._lock_manager.observe = AsyncMock(side_effect=ConnectionError("valkey away"))

    await worker._handle_message(_message(**_stalled(10 * 3_600_000 - 1, ttl_ms=1)))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    [failed] = [e for e in swarmq_logs if e["event"] == "lock_observe_failed"]
    assert failed["log_level"] == "warning"
    assert failed["lock_name"] == "resource"


@pytest.mark.parametrize(("prior", "logged"), [(0, True), (1, False), (10, False), (11, True), (30, True)])
async def test_conflict_log_only_first_and_then_once_a_minute(
    broker, backend, config, clock, swarmq_logs, prior, logged
) -> None:
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))

    await worker._handle_message(_message(**{HEADER_LOCK_CONFLICTS: prior}))

    assert bool([e for e in swarmq_logs if e["event"] == "task_lock_conflict"]) is logged


@pytest.mark.parametrize(("conflicts", "delay_ms"), [(1, 100), (2, 200), (10, 51_200), (11, 60_000), (500, 60_000)])
def test_backoff_from_the_new_counter(conflicts, delay_ms) -> None:
    assert lock_conflict_delay_ms(conflicts) == delay_ms


async def test_gate_tracks_waiting_messages_until_acquired(broker, backend, config, clock) -> None:
    from swarmq.contracts.coordination import LockToken

    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))
    await worker._handle_message(_message(**{HEADER_LOCK_WAIT_SINCE: _NOW_MS - 20_000}))
    assert worker._lock_gate.waits.snapshot(now_ms=_NOW_MS) == (1, 20.0)

    worker._lock_manager.acquire = AsyncMock(
        side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(name, owner, max_concurrency)
    )
    with patch.object(Locked, "run", AsyncMock(return_value=None)):
        await worker._handle_message(_message(**{HEADER_LOCK_WAIT_SINCE: _NOW_MS - 20_000}))
    assert worker._lock_gate.waits.snapshot(now_ms=_NOW_MS) == (0, 0.0)


async def test_broker_redelivery_after_acquire_starts_a_new_wait(broker, backend, config, clock) -> None:
    """Review MR 13, P2-4: die Kopie hat den Lock erworben und lief; der
    Worker starb (SIGKILL/OOM) oder gab sie beim Shutdown zurueck. Der
    Broker stellt sie mit ``x-delivery-count`` neu zu -- ihr ``wait-since``
    gehoert zur alten Wartezeit und darf nicht sofort in die DLQ fuehren."""
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h2"}), 50_000))
    stale = {**_stalled(7 * 3_600_000, waited_ms=7 * 3_600_000), "x-delivery-count": 1}

    await worker._handle_message(_message(**stale))

    broker.dead_letter.assert_not_awaited()
    headers = _copy(broker)
    assert headers[HEADER_LOCK_WAIT_SINCE] == _NOW_MS
    assert headers[HEADER_LOCK_CONFLICTS] == 1
    assert headers[HEADER_LOCK_HOLDER] == holder_fingerprint(frozenset({"h2"}))
    assert headers[HEADER_LOCK_HOLDER_SINCE] == _NOW_MS


@pytest.mark.parametrize("next_generation", [{"x-swarmq-retry-count": 1}, {"x-swarmq-replay-id": "r" * 32}])
async def test_legacy_copy_of_a_new_generation_starts_a_new_wait(
    broker, backend, config, clock, next_generation
) -> None:
    """Review MR 13, P2-4 (Mischbetrieb): 0.11 entfernt die Lock-Zustands-
    Header weder in seiner fachlichen Retry-Kopie noch bei ``dlq retry``.
    Eine Kopie einer spaeteren Ausfuehrung (Retry-Zaehler, Replay-ID) erbt
    sie unveraendert und darf nicht nach der alten Wartezeit enden."""
    worker = _worker(broker, backend, config, observation=LockObservation(frozenset({"h1"}), 50_000))
    await worker._handle_message(_message())  # 0.12: erster Konflikt
    legacy_copy = {**_copy(broker), **next_generation}  # 0.11: Acquire, Lauf, Retry-Kopie
    clock.time.return_value = _NOW_S + 7 * 3_600

    await worker._handle_message(_message(**legacy_copy))

    broker.dead_letter.assert_not_awaited()
    headers = _copy(broker)
    assert headers[HEADER_LOCK_WAIT_SINCE] == _NOW_MS + 7 * 3_600_000
    assert headers[HEADER_LOCK_CONFLICTS] == 1
