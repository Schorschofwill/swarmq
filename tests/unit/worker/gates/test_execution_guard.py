"""Ausführungsschutz (Plan 2026-10-08-1541, U8, KTD5): Gate, Lease-Register
und Verlängerung gegen den In-Memory-Store (echte Semantik, Fake-Uhr).

Test-Liste (Gate):
1. Erste Zustellung → Claim auf die Task-ID, ``Continue``; eine zweite
   Zustellung sähe den Running-Marker.
2. Done-Marker → bestätigen und überspringen (``duplicate_delivery_skipped``),
   ``Terminal.never_release()``.
3. Superseded-Marker mit größerem Zähler → ebenso.
4. Fremder Running-Marker → ``retry_later`` mit ``republish_headers`` und
   ``x-swarmq-guard-defer-count`` 1, Retry-Zähler unverändert, ``Redelivered``.
5. Folge-Zurückstellung: Zähler +1, Wartezeit wächst.
6. Zurückstellung scheitert am Publish → ``redeliver_in_place``.
7. Waisen-Grenze → Dead-Letter als ``WorkerHungError``.
8. Valkey nicht erreichbar → Zurückstellung wie ``backend_defer``.
9. FIRE_AND_FORGET → keine Store-Aufrufe.
10. ``execution_guard=False`` → keine Store-Aufrufe.
11. ``x-swarmq-replay-id`` → eigene Identität (``dlq retry`` läuft erneut).
12. Ohne Store (Worker nicht gestartet) → ``Continue``.
13. Begrenzter Claim: ein hängender Store zählt nach ``backend_op_timeout``
    als Ausfall.
14. ``settle`` (``finally``): eine nicht abgeschlossene Lease nur freigeben.
15. Kopien anderer Wege erben ``x-swarmq-guard-defer-count`` nicht.

Test-Liste (Verlängerung, Fake-Uhr):
16. Der Takt hält den eigenen Marker über mehrere TTLs.
17. Ein verlorener (abgelaufener, fremd übernommener) Marker wird weder
    verlängert noch gelöscht; WARNING + Metrik-Ereignis, einmal.
18. Ein Fehler beim Verlängern wird geloggt, der Takt läuft weiter.
19. Der Takt läuft alle ``ttl/3``, nur solange Leases bestehen.
20. Eine während des Takts vom Verb abgeschlossene Lease ist kein Verlust.
21. Ereignisse für die Metrik: übersprungen, zurückgestellt, Waisen-Grenze.
22. Ein Takt verlängert alle Leases nebenläufig (ein gesammelter Stapel der
    ScriptPipeline), nicht eine nach der anderen.

Review Phase 2 (2026-10-09):
23. Claim scheitert clientseitig (Timeout), lief aber im Store: ein
    begrenztes Release im Hintergrund gibt den eigenen Marker frei -- die
    nächste Zustellung zählt keine Waise.
24. Dieses Release ist an den Owner gebunden: einen fremden Marker lässt es
    stehen.
25. Überschreitet die Summe der Zurückstellungen ``consumer_timeout_s``
    (nominelle Wartezeiten ohne Jitter), meldet die Zurückstellung, die die
    Grenze überschreitet, WARNING ``execution_guard_defer_stalled`` und das
    Metrik-Ereignis ``defer_stalled`` -- und stellt weiter zurück (keine DLQ).
26. Vor und nach dem Überschreiten: keine Meldung (einmal je Nachricht).

Gescheiterter Abschluss (``execution_guard_finish_failed``), Nachholen im
Hintergrund (2026-10-09):
27. Scheitert das Freigeben (kurzer Valkey-Ausfall), holt ein Versuch im
    Hintergrund es nach: kein Marker bis zur TTL, keine Waise.
28. Scheitert der Done-Marker (Ack ausgeführt), setzt das Nachholen den
    Done-Zustand -- die Neuzustellung wird übersprungen (KTD5).
29. Scheitert der Superseded-Marker, setzt das Nachholen ihn mit dem Zähler
    der Retry-Kopie.
30. Begrenzt: scheitert der Store dauerhaft, gibt das Nachholen nach etwa
    einer Marker-TTL (nominelle Wartezeiten) auf, WARNING
    ``execution_guard_finish_gave_up``.
31. ``drain`` (Shutdown): wartende Nachholversuche probieren sofort ein
    letztes Mal; ``drain`` kehrt zurück.
32. ``drain`` mit hängendem Store kehrt spätestens nach
    ``backend_op_timeout`` zurück (der letzte Versuch ist begrenzt), WARNING
    ``execution_guard_finish_gave_up``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.task import DeliveryMode, TaskInfo
from swarmq.worker.gates import GateContext, republish_headers
from swarmq.worker.gates.execution_guard import ExecutionGuard
from swarmq.worker.outcome import Continue, Redelivered, Terminal
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def backend(clock: FakeClock) -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(clock)
    await impl.connect()
    yield impl
    await impl.close()


@pytest.fixture
def deps() -> MagicMock:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker.ack = AsyncMock()
    deps.broker.redeliver = AsyncMock()
    deps.broker.retry_later = AsyncMock()
    deps.broker.dead_letter = AsyncMock()
    return deps


@pytest.fixture
def dead_letter() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def guard(deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock) -> ExecutionGuard:
    return ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter)


def _ctx(task_id: str = "t-1", **info: object) -> GateContext:
    return GateContext(
        task_id=task_id, task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"], **info),  # type: ignore[arg-type]
    )


async def _other_claim(
    backend: InMemoryBackend, identity: str = "t-1", retry_count: int = 0, owner: str = "other",
) -> str:
    claim = await backend.execution_guard.claim(
        identity, owner=owner, retry_count=retry_count, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3,
    )
    return claim.status


async def test_first_delivery_claims_and_continues(guard: ExecutionGuard, backend: InMemoryBackend) -> None:
    assert await guard.apply(make_message(headers={}), _ctx()) is Continue
    assert await _other_claim(backend) == "running"


async def test_done_marker_acks_and_skips(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    await _other_claim(backend)
    await backend.execution_guard.mark_done("t-1", owner="other", state_ttl_ms=600_000)
    delivery = make_message(headers={})

    with capture_swarmq_logs() as logs:
        outcome = await guard.apply(delivery, _ctx())

    assert outcome == Terminal.never_release()
    deps.broker.ack.assert_awaited_once_with(delivery)
    [skipped] = [e for e in logs if e["event"] == "duplicate_delivery_skipped"]
    assert skipped["reason"] == "done" and skipped["task_id"] == "t-1"


async def test_attempt_overtaken_by_a_retry_acks_and_skips(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    await backend.execution_guard.mark_superseded("t-1", owner="other", retry_count=2, state_ttl_ms=600_000)
    stale = make_message(headers={"x-swarmq-retry-count": 1})

    with capture_swarmq_logs() as logs:
        outcome = await guard.apply(stale, _ctx())

    assert outcome == Terminal.never_release()
    deps.broker.ack.assert_awaited_once_with(stale)
    assert [e["reason"] for e in logs if e["event"] == "duplicate_delivery_skipped"] == ["superseded"]
    assert await guard.apply(make_message(headers={"x-swarmq-retry-count": 2}), _ctx()) is Continue


async def test_foreign_running_marker_defers_with_backoff_without_counting_a_retry(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    await _other_claim(backend)
    delivery = make_message(
        headers={"x-swarmq-retry-count": 0, "x-swarmq-unique-hash": "h", "x-delivery-count": 2},
    )

    outcome = await guard.apply(delivery, _ctx())

    assert outcome is Redelivered
    deps.broker.ack.assert_not_awaited()
    deps.broker.retry_later.assert_awaited_once()
    call = deps.broker.retry_later.await_args
    assert call.args == (delivery,)
    assert call.kwargs["headers"] == {
        "x-swarmq-retry-count": 0, "x-swarmq-unique-hash": "h", "x-swarmq-guard-defer-count": 1,
    }
    assert 800 <= call.kwargs["delay_ms"] <= 1200


async def test_successive_deferral_counts_up_and_waits_longer(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    await _other_claim(backend)

    with capture_swarmq_logs() as logs:
        await guard.apply(make_message(headers={"x-swarmq-guard-defer-count": 3}), _ctx())

    call = deps.broker.retry_later.await_args
    assert call.kwargs["headers"]["x-swarmq-guard-defer-count"] == 4
    assert 6400 <= call.kwargs["delay_ms"] <= 9600
    [deferred] = [e for e in logs if e["event"] == "execution_guard_deferred"]
    assert deferred["defer_count"] == 4 and deferred["delay_ms"] == call.kwargs["delay_ms"]


async def test_failed_deferral_publish_falls_back_to_redeliver(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    await _other_claim(backend)
    deps.broker.retry_later.side_effect = BrokerConnectionError("publish failed")
    delivery = make_message(headers={})

    assert await guard.apply(delivery, _ctx()) is Redelivered
    deps.broker.redeliver.assert_awaited_once_with(delivery)


async def _crash_three_times(backend: InMemoryBackend, clock: FakeClock, identity: str = "t-1") -> None:
    for owner in ("a", "b", "c"):
        await backend.execution_guard.claim(
            identity, owner=owner, retry_count=0, ttl_ms=1_000, state_ttl_ms=600_000, max_orphans=3,
        )
        await clock.advance(2)


async def test_orphan_limit_dead_letters_as_worker_hung_error(
    guard: ExecutionGuard, backend: InMemoryBackend, clock: FakeClock, dead_letter: AsyncMock,
) -> None:
    await _crash_three_times(backend, clock)
    delivery = make_message(headers={})

    with capture_swarmq_logs() as logs:
        outcome = await guard.apply(delivery, _ctx())

    assert outcome == Terminal.already_released()
    dead_letter.assert_awaited_once()
    args = dead_letter.await_args.args
    assert args[0] is delivery and args[1].task_id == "t-1"
    assert "WorkerHungError" in args[2] and args[3] == "WorkerHungError"
    [limit] = [e for e in logs if e["event"] == "execution_guard_orphan_limit"]
    assert limit["orphans"] == 3 and limit["log_level"] == "error"


async def test_unreachable_backend_defers_like_backend_defer(deps: MagicMock, dead_letter: AsyncMock) -> None:
    store = MagicMock()
    store.claim = AsyncMock(side_effect=BackendConnectionError("connection refused"))
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    outcome = await guard.apply(make_message(headers={}), _ctx())

    assert outcome is Redelivered
    assert deps.broker.retry_later.await_args.kwargs["headers"]["x-swarmq-backend-defer-count"] == 1


async def test_hanging_claim_counts_as_outage_after_backend_op_timeout(dead_letter: AsyncMock) -> None:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", backend_op_timeout=0.01)
    deps.broker.retry_later = AsyncMock()
    never = asyncio.Event()

    async def _hang(*_args: object, **_kwargs: object) -> None:
        await never.wait()

    store = MagicMock()
    store.claim = _hang
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    assert await guard.apply(make_message(headers={}), _ctx()) is Redelivered
    assert deps.broker.retry_later.await_args.kwargs["headers"]["x-swarmq-backend-defer-count"] == 1


async def test_fire_and_forget_is_not_guarded(deps: MagicMock, dead_letter: AsyncMock) -> None:
    store = MagicMock()
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    ctx = _ctx(delivery=DeliveryMode.FIRE_AND_FORGET)
    assert await guard.apply(make_message(headers={}), ctx) is Continue
    assert store.mock_calls == []


async def test_disabled_guard_makes_no_store_calls(deps: MagicMock, dead_letter: AsyncMock) -> None:
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard=False)
    store = MagicMock()
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    assert await guard.apply(make_message(headers={}), _ctx()) is Continue
    assert store.mock_calls == []


async def test_without_store_the_guard_is_inactive(deps: MagicMock, dead_letter: AsyncMock) -> None:
    """Vor ``Worker.start()`` (oder mit einem Backend ohne Ausführungsschutz)
    gibt es keinen Store -- das Gate lässt durch."""
    guard = ExecutionGuard(deps, store=lambda: None, dead_letter=dead_letter)
    assert await guard.apply(make_message(headers={}), _ctx()) is Continue


async def test_replay_id_is_a_new_execution_identity(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock,
) -> None:
    """``dlq retry`` (bzw. die Wiederholung eines Workflow-Schritts mit
    deterministischer Task-ID) innerhalb der Done-TTL läuft erneut; eine
    Neuzustellung dieser Wiederholung wird wieder erkannt."""
    await _other_claim(backend)
    await backend.execution_guard.mark_done("t-1", owner="other", state_ttl_ms=600_000)

    replay = make_message(headers={"x-swarmq-replay-id": "r1"})
    assert await guard.apply(replay, _ctx()) is Continue
    assert await guard.apply(make_message(headers={"x-swarmq-replay-id": "r1"}), _ctx()) is Redelivered


async def test_broker_dead_lettered_copy_is_a_new_execution_identity(
    guard: ExecutionGuard, backend: InMemoryBackend,
) -> None:
    """MR 12 (BZ-08): ein Replay nach einem Broker-DLX (``x-death`` fuer die
    Haupt-Queue) ist die Identitaet ``task_id#n`` -- es laeuft trotz Done-
    Marker des ersten Laufs; Tode in Delay-Queues zaehlen nicht."""
    await _other_claim(backend)
    await backend.execution_guard.mark_done("t-1", owner="other", state_ttl_ms=600_000)

    delayed = make_message(headers={"x-death": [{"queue": "q.DQ.1s", "count": 1, "reason": "expired"}]})
    assert await guard.apply(delayed, _ctx()) == Terminal.never_release()

    replay = make_message(headers={"x-death": [{"queue": "q", "count": 1, "reason": "rejected"}]})
    assert await guard.apply(replay, _ctx()) is Continue
    assert await _other_claim(backend, identity="t-1#1", owner="probe") == "running"


async def test_replay_identity_done_marker_outlives_the_dead_letter_retention(
    guard: ExecutionGuard, deps: MagicMock, clock: FakeClock,
) -> None:
    """MR 12 (A4): der Done-Marker eines Replays (H gesetzt oder n > 0) lebt
    mindestens ``dead_letter_ttl_s`` -- ein erneuter ``dlq retry`` derselben
    XQ-Nachricht am naechsten Tag laeuft sonst doppelt. Normale Identitaeten
    behalten ``execution_guard_done_ttl_s``."""
    deps.config.dead_letter_ttl_s = 2 * 86_400.0
    assert deps.config.execution_guard_done_ttl_s < 86_400
    for headers in ({"x-swarmq-replay-id": "r1"}, {}):
        delivery = make_message(headers=headers)
        assert await guard.apply(delivery, _ctx()) is Continue
        lease = guard.take_lease(delivery)
        assert lease is not None
        await guard.finish(lease)

    await clock.advance(86_400)  # ueber die Done-TTL, unter der DLQ-Frist
    assert await guard.apply(make_message(headers={}), _ctx()) is Continue
    assert await guard.apply(make_message(headers={"x-swarmq-replay-id": "r1"}), _ctx()) == Terminal.never_release()


async def test_replay_done_marker_without_a_retention_lives_the_backlog_ttl(
    guard: ExecutionGuard, deps: MagicMock, clock: FakeClock,
) -> None:
    """``dead_letter_ttl_s=None`` (unbegrenzte DLQ): statt unbegrenzt gilt
    ``execution_guard_backlog_ttl_s`` (7 d; Abschnitt 7)."""
    deps.config.dead_letter_ttl_s = None
    delivery = make_message(headers={"x-swarmq-xdeath-base": 2})
    assert await guard.apply(delivery, _ctx()) is Continue
    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.finish(lease)
    await clock.advance(6 * 86_400)
    assert await guard.apply(make_message(headers={"x-swarmq-xdeath-base": 2}), _ctx()) == Terminal.never_release()
    await clock.advance(2 * 86_400)
    assert await guard.apply(make_message(headers={"x-swarmq-xdeath-base": 2}), _ctx()) is Continue


async def test_settle_releases_a_lease_no_verb_concluded(guard: ExecutionGuard, backend: InMemoryBackend) -> None:
    """``finally`` des Runners (kein Verb, Ausnahme, Abbruch): Running frei,
    kein Done-Marker -- die Neuzustellung läuft."""
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())

    await guard.settle(delivery)

    assert await _other_claim(backend) == "claimed"


async def test_renewal_keeps_the_own_running_marker_alive(
    deps: MagicMock, backend: InMemoryBackend, clock: FakeClock, dead_letter: AsyncMock,
) -> None:
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard_ttl_s=3.0)
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter)
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())

    for _ in range(3):
        await clock.advance(2)
        await guard.renew_once()

    assert await _other_claim(backend) == "running"
    await guard.settle(delivery)


async def test_lost_marker_is_logged_counted_and_not_touched(
    deps: MagicMock, backend: InMemoryBackend, clock: FakeClock, dead_letter: AsyncMock,
) -> None:
    """Ist der eigene Marker abgelaufen und von einer anderen Zustellung
    übernommen, wird der fremde Marker weder verlängert noch gelöscht."""
    events: list[str] = []
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard_ttl_s=1.0)
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, on_event=events.append)
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())
    await clock.advance(2)
    assert await _other_claim(backend) == "claimed"  # übernommen (Waise)

    with capture_swarmq_logs() as logs:
        await guard.renew_once()
        await guard.settle(delivery)

    [lost] = [e for e in logs if e["event"] == "execution_guard_lease_lost"]
    assert lost["log_level"] == "warning" and lost["task_id"] == "t-1"
    assert events == ["lease_lost"]
    await clock.advance(30)
    # Der fremde Marker steht noch (ein Dritter sieht ihn als laufend).
    assert await _other_claim(backend, owner="third") == "running"


async def test_lost_marker_is_reported_once(
    deps: MagicMock, backend: InMemoryBackend, clock: FakeClock, dead_letter: AsyncMock,
) -> None:
    events: list[str] = []
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard_ttl_s=1.0)
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, on_event=events.append)
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())
    await clock.advance(2)

    await guard.renew_once()
    await guard.renew_once()

    assert events == ["lease_lost"]
    await guard.settle(delivery)


async def test_renewal_failure_is_logged_and_does_not_raise(
    guard: ExecutionGuard, backend: InMemoryBackend, monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())
    monkeypatch.setattr(backend.execution_guard, "renew", AsyncMock(side_effect=BackendConnectionError("down")))

    with capture_swarmq_logs() as logs:
        await guard.renew_once()

    assert [e["log_level"] for e in logs if e["event"] == "execution_guard_renew_failed"] == ["warning"]
    await guard.settle(delivery)


async def test_renewal_ticks_while_leases_exist_and_stops_without(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ein Takt für alle Leases eines Workers, alle ``ttl/3``; er beginnt mit
    der ersten Lease und endet mit der letzten (kein Dauer-Task)."""
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard_ttl_s=3.0)
    intervals: list[float] = []
    tick = asyncio.Event()
    stopped = asyncio.Event()

    next_wait = asyncio.Event()

    async def _sleep(seconds: float) -> None:
        intervals.append(seconds)
        if len(intervals) > 1:
            next_wait.set()  # der Takt ist durch und wartet auf den naechsten
        try:
            await tick.wait()
        except asyncio.CancelledError:
            stopped.set()
            raise
        tick.clear()

    renewed = asyncio.Event()
    real_renew = backend.execution_guard.renew

    async def _renew(*args: object, **kwargs: object) -> bool:
        renewed.set()
        return await real_renew(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backend.execution_guard, "renew", _renew)
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, sleep=_sleep)
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())

    tick.set()
    await asyncio.wait_for(renewed.wait(), 1)
    await asyncio.wait_for(next_wait.wait(), 1)
    await guard.settle(delivery)
    await asyncio.wait_for(stopped.wait(), 1)

    assert intervals[0] == 1.0


@pytest.mark.parametrize("scenario", ["duplicate_skipped", "deferred", "orphan_limit"])
async def test_guard_outcomes_are_counted(
    scenario: str, deps: MagicMock, backend: InMemoryBackend, clock: FakeClock, dead_letter: AsyncMock,
) -> None:
    events: list[str] = []
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, on_event=events.append)
    if scenario == "duplicate_skipped":
        await backend.execution_guard.mark_done("t-1", owner="x", state_ttl_ms=600_000)
    elif scenario == "deferred":
        await _other_claim(backend)
    else:
        await _crash_three_times(backend, clock)

    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())
    await guard.settle(delivery)

    assert events == [scenario]


def test_copies_carry_the_death_count_as_base_and_drop_x_death() -> None:
    """MR 12 (BZ-08): der Broker baut ``x-death`` beim naechsten Dead-Lettern
    neu auf (Spike S5); jede Kopie haelt n in ``x-swarmq-xdeath-base`` fest."""
    deaths = [{"queue": "q", "count": 2, "reason": "rejected"}, {"queue": "q.DQ", "count": 9, "reason": "expired"}]
    delivery = make_message(headers={"x-death": deaths, "x-swarmq-xdeath-base": 1, "x-keep": "k"})
    assert republish_headers(delivery, _ctx()) == {"x-swarmq-xdeath-base": 3, "x-keep": "k"}
    only_delayed = make_message(headers={"x-death": deaths[1:]})
    assert republish_headers(only_delayed, _ctx()) == {}


def test_other_copies_do_not_inherit_the_guard_defer_count() -> None:
    """Eine Retry- oder Requeue-Kopie lief nicht neben dem Original -- eine
    spätere Zurückstellung beginnt wieder bei der Basiswartezeit."""
    delivery = make_message(headers={"x-swarmq-guard-defer-count": 5, "x-swarmq-retry-count": 1})
    assert republish_headers(delivery, _ctx()) == {"x-swarmq-retry-count": 1}


async def test_lease_concluded_during_renewal_is_not_reported_lost(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schließt das Verb die Lease ab, während der Takt verlängert, findet
    die Verlängerung keinen Marker mehr -- das ist kein Verlust."""
    events: list[str] = []
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, on_event=events.append)
    delivery = make_message(headers={})
    await guard.apply(delivery, _ctx())

    async def _renew_racing_the_ack(*_args: object, **_kwargs: object) -> bool:
        lease = guard.take_lease(delivery)
        assert lease is not None
        await guard.finish(lease)
        return False

    monkeypatch.setattr(backend.execution_guard, "renew", _renew_racing_the_ack)
    await guard.renew_once()

    assert events == []


async def test_renewal_tick_renews_all_leases_concurrently(guard: ExecutionGuard, backend: InMemoryBackend) -> None:
    """Die Verlaengerungen eines Takts sind gleichzeitig unterwegs: erst
    wenn beide begonnen haben, kehrt der Store zurueck."""
    first, second = make_message(headers={}), make_message(headers={})
    await guard.apply(first, _ctx("t-1"))
    await guard.apply(second, _ctx("t-2"))
    started: list[str] = []
    both_started = asyncio.Event()

    async def renew(identity: str, *, owner: str, ttl_ms: int) -> bool:
        started.append(identity)
        if len(started) == 2:
            both_started.set()
        await both_started.wait()
        return True

    backend.execution_guard.renew = renew  # type: ignore[method-assign]
    await asyncio.wait_for(guard.renew_once(), timeout=1)

    assert sorted(started) == ["t-1", "t-2"]
    await guard.settle(first)
    await guard.settle(second)


async def test_claim_that_failed_on_the_client_is_released_in_the_background(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    """Der Claim lief im Store, nur die Antwort ging verloren: ohne Release
    liefe der Marker ab und zaehlte bei der Neuzustellung als Waise."""
    store = backend.execution_guard
    real_claim, real_release = store.claim, store.release
    released = asyncio.Event()

    async def claim_then_fail(*args: object, **kwargs: object) -> object:
        await real_claim(*args, **kwargs)  # type: ignore[arg-type]
        raise BackendConnectionError("reply lost")

    async def release(*args: object, **kwargs: object) -> None:
        await real_release(*args, **kwargs)  # type: ignore[arg-type]
        released.set()

    store.claim = claim_then_fail  # type: ignore[method-assign]
    store.release = release  # type: ignore[method-assign]
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    assert await guard.apply(make_message(headers={}), _ctx()) is Redelivered
    await asyncio.wait_for(released.wait(), 1)

    store.claim = real_claim  # type: ignore[method-assign]
    claim = await store.claim("t-1", owner="next", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3)
    assert (claim.status, claim.orphans) == ("claimed", 0)


async def test_background_release_after_a_failed_claim_leaves_a_foreign_marker(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    store = backend.execution_guard
    await _other_claim(backend)  # fremder Running-Marker
    real_claim, real_release = store.claim, store.release
    released = asyncio.Event()

    async def fail(*_args: object, **_kwargs: object) -> object:
        raise BackendConnectionError("timeout")

    async def release(*args: object, **kwargs: object) -> None:
        await real_release(*args, **kwargs)  # type: ignore[arg-type]
        released.set()

    store.claim = fail  # type: ignore[method-assign]
    store.release = release  # type: ignore[method-assign]
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter)

    assert await guard.apply(make_message(headers={}), _ctx()) is Redelivered
    await asyncio.wait_for(released.wait(), 1)

    store.claim = real_claim  # type: ignore[method-assign]
    assert await _other_claim(backend, owner="third") == "running"



def _stalled_guard(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock, events: list[str],
) -> ExecutionGuard:
    # Nominell 1 s, 2 s, 4 s ...: die zweite Zurueckstellung erreicht 3 s.
    deps.config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", consumer_timeout_s=3.0,
        backend_defer_base_delay=1.0, backend_defer_max_delay=60.0,
    )
    return ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=dead_letter, on_event=events.append)


async def test_deferral_crossing_the_consumer_timeout_warns_but_keeps_deferring(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    events: list[str] = []
    guard = _stalled_guard(deps, backend, dead_letter, events)
    await _other_claim(backend)

    with capture_swarmq_logs() as logs:
        outcome = await guard.apply(make_message(headers={"x-swarmq-guard-defer-count": 1}), _ctx())

    assert outcome is Redelivered
    deps.broker.retry_later.assert_awaited_once()
    dead_letter.assert_not_awaited()
    [stalled] = [e for e in logs if e["event"] == "execution_guard_defer_stalled"]
    assert stalled["log_level"] == "warning" and stalled["task_id"] == "t-1"
    assert stalled["defer_count"] == 2
    assert "defer_stalled" in events


@pytest.mark.parametrize("prior", [0, 2, 5000])
async def test_deferral_before_or_after_the_crossing_does_not_warn(
    prior: int, deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    """Einmal je Nachricht; auch nach tausenden Zurueckstellungen (Haenger
    ueber Tage) rechnet die Summe ohne Ueberlauf."""
    events: list[str] = []
    guard = _stalled_guard(deps, backend, dead_letter, events)
    await _other_claim(backend)

    with capture_swarmq_logs() as logs:
        outcome = await guard.apply(make_message(headers={"x-swarmq-guard-defer-count": prior}), _ctx())

    assert outcome is Redelivered
    assert [e for e in logs if e["event"] == "execution_guard_defer_stalled"] == []
    assert "defer_stalled" not in events


async def _no_wait(_delay: float) -> None:
    await asyncio.sleep(0)


def _fail_first(store: object, name: str, failures: int = 1) -> tuple[list[object], asyncio.Event]:
    """Lässt ``failures`` Aufrufe von ``store.<name>`` scheitern (Valkey kurz
    weg); danach ruft es die echte Methode und setzt das Event."""
    real = getattr(store, name)
    calls: list[object] = []
    done = asyncio.Event()

    async def flaky(*args: object, **kwargs: object) -> None:
        calls.append(args)
        if len(calls) <= failures:
            raise BackendConnectionError("valkey down")
        await real(*args, **kwargs)
        done.set()

    setattr(store, name, flaky)
    return calls, done


async def test_failed_release_is_retried_in_the_background(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_no_wait)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue
    _calls, released = _fail_first(store, "release")

    with capture_swarmq_logs() as logs:
        await guard.settle(delivery)
        await asyncio.wait_for(released.wait(), 1)

    claim = await store.claim("t-1", owner="next", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3)
    assert (claim.status, claim.orphans) == ("claimed", 0)
    assert [e["event"] for e in logs if e["event"].startswith("execution_guard_finish")] == [
        "execution_guard_finish_failed", "execution_guard_finish_recovered",
    ]


async def test_failed_done_marker_is_retried_as_done(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    """Das Ack lief: das Nachholen setzt den Done-Zustand, nicht nur die
    Freigabe -- die Neuzustellung wird übersprungen statt erneut ausgeführt."""
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_no_wait)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue
    _calls, marked = _fail_first(store, "mark_done")

    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.finish(lease)
    await asyncio.wait_for(marked.wait(), 1)

    assert await _other_claim(backend, owner="next") == "done"


async def test_failed_superseded_marker_is_retried_with_the_retry_count(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_no_wait)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue
    _calls, marked = _fail_first(store, "mark_superseded")

    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.supersede(lease, 1)
    await asyncio.wait_for(marked.wait(), 1)

    assert await _other_claim(backend, owner="old-attempt", retry_count=0) == "superseded"
    assert await _other_claim(backend, owner="retry-copy", retry_count=1) == "claimed"


async def _until(predicate: object) -> None:
    while not predicate():  # type: ignore[operator]
        await asyncio.sleep(0)


async def test_retrying_a_failed_marker_gives_up_after_about_one_ttl(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    """Nominell 1, 2, 4, 8 s: nach der vierten Wartezeit (Summe 15 s) ist
    die TTL von 10 s überschritten -- letzter Versuch, dann Aufgabe."""
    deps.config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", execution_guard_ttl_s=10.0,
        backend_defer_base_delay=1.0, backend_defer_max_delay=60.0,
    )
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_no_wait)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue
    calls, _released = _fail_first(store, "release", failures=1000)

    with capture_swarmq_logs() as logs:
        await guard.settle(delivery)
        await asyncio.wait_for(_until(lambda: any(e["event"] == "execution_guard_finish_gave_up" for e in logs)), 1)
        for _ in range(10):
            await asyncio.sleep(0)

    assert len(calls) == 5  # der Versuch im Verb plus vier nachgeholte
    [gave_up] = [e for e in logs if e["event"] == "execution_guard_finish_gave_up"]
    assert gave_up["log_level"] == "warning" and gave_up["marker"] == "release" and gave_up["attempts"] == 4


async def _sleep_forever(_delay: float) -> None:
    await asyncio.Event().wait()


async def test_drain_makes_a_waiting_retry_try_once_more_right_away(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    """Shutdown: der Nachholversuch wartet nicht seine Backoff-Zeit ab,
    sondern versucht es sofort ein letztes Mal, bevor das Backend schließt."""
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_sleep_forever)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue
    _calls, released = _fail_first(store, "release")
    await guard.settle(delivery)

    await asyncio.wait_for(guard.drain(), 1)

    assert released.is_set()
    assert await _other_claim(backend, owner="next") == "claimed"


async def test_drain_with_a_hanging_store_returns_after_the_bounded_last_attempt(
    deps: MagicMock, backend: InMemoryBackend, dead_letter: AsyncMock,
) -> None:
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", backend_op_timeout=0.02)
    store = backend.execution_guard
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=dead_letter, sleep=_sleep_forever)
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx()) is Continue

    async def hang(*_args: object, **_kwargs: object) -> None:
        await asyncio.Event().wait()

    store.release = hang  # type: ignore[method-assign]
    with capture_swarmq_logs() as logs:
        await guard.settle(delivery)  # erster Versuch: Zeitüberschreitung
        await asyncio.wait_for(guard.drain(), 1)

    [gave_up] = [e for e in logs if e["event"] == "execution_guard_finish_gave_up"]
    assert gave_up["attempts"] == 1
