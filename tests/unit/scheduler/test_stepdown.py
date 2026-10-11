"""Ruecktritt des Leaders nach wiederholten Publish-Fehlern (Plan 0.12.0, MR 15, BZ-10).

Ein Leader, dessen Broker scheitert, hielt bisher die Crons aller Apps an
(gemeinsamer Valkey ohne Namensraum). Er tritt jetzt zurueck, sobald
mindestens 2 Publish-Fehler ueber mindestens ``scheduler_publish_stepdown_s``
aufgelaufen sind, und bewirbt sich danach 2 x Lock-TTL nicht erneut.

Test-Liste (``PublishStepdown``, rein):
- [x] ein Fehler -> kein Ruecktritt
- [x] zwei Fehler, aber vor Ablauf der Frist -> kein Ruecktritt
- [x] zwei Fehler und Frist erreicht -> Ruecktritt
- [x] ein erfolgreicher Publish setzt die Serie zurueck
- [x] gezaehlt wird nur ``BrokerConnectionError`` ohne ``UnroutableMessage``
      und ``MessageHeadersTooLarge`` (Broker-Alarm trifft alle Kandidaten)
- [x] ``BrokerPublishBlocked`` zaehlt nicht (MR 16)
- [x] Frist 0 schaltet den Ruecktritt ab
- [x] nach dem Ruecktritt beginnt eine neue Serie
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.exceptions import BrokerConnectionError
from swarmq.signals import Signal
from tests.unit.scheduler._helpers import _def, _make_loop

pytestmark = pytest.mark.timeout(5)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _tracker(window_s: float = 30.0):  # type: ignore[no-untyped-def]
    from swarmq.scheduler.stepdown import PublishStepdown

    clock = _Clock()
    return PublishStepdown(window_s, clock=clock), clock


def test_one_failure_does_not_step_down() -> None:
    tracker, _clock = _tracker()

    assert tracker.failure(BrokerConnectionError("down")) is False


def test_two_failures_before_the_window_do_not_step_down() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 29.0

    assert tracker.failure(BrokerConnectionError("down")) is False


def test_two_failures_over_the_window_step_down() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 30.0

    assert tracker.failure(BrokerConnectionError("down")) is True


def test_a_successful_publish_resets_the_series() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 30.0
    tracker.success()

    assert tracker.failure(BrokerConnectionError("down")) is False


@pytest.mark.parametrize("name", ["UnroutableMessage", "MessageHeadersTooLarge"])
def test_errors_that_hit_every_candidate_are_not_counted(name: str) -> None:
    from swarmq import exceptions

    tracker, clock = _tracker(30.0)
    error = getattr(exceptions, name)("x")
    tracker.failure(error)
    clock.now += 30.0

    assert tracker.failure(error) is False


def test_a_blocked_broker_does_not_step_down() -> None:
    # Plan 0.12.0, MR 16 (BZ-16): ein Broker-Alarm blockiert jeden Kandidaten.
    from swarmq.exceptions import BrokerPublishBlocked

    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerPublishBlocked("alarm"))
    clock.now += 30.0

    assert tracker.failure(BrokerPublishBlocked("alarm")) is False


def test_other_errors_are_not_counted() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 30.0

    assert tracker.failure(RuntimeError("bug")) is False


def test_window_zero_disables_the_stepdown() -> None:
    tracker, clock = _tracker(0.0)
    for _ in range(5):
        clock.now += 60.0
        assert tracker.failure(BrokerConnectionError("down")) is False


def test_a_new_series_starts_after_a_stepdown() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 30.0
    assert tracker.failure(BrokerConnectionError("down")) is True

    clock.now += 1.0
    assert tracker.failure(BrokerConnectionError("down")) is False


# ---------------------------------------------------------------------------
# SchedulerLoop
# ---------------------------------------------------------------------------


class _FakeTime:
    def __init__(self) -> None:
        self.now = 5000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _FakeTime:
    from swarmq.scheduler import stepdown

    fake = _FakeTime()
    monkeypatch.setattr(stepdown, "time", fake)
    return fake


@pytest.fixture
def leader(backend, broker, config, lock_manager, store, signal_dispatcher, clock):  # type: ignore[no-untyped-def]
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    broker.publish = AsyncMock(side_effect=BrokerConnectionError("ACCESS_REFUSED"))
    return loop


def _recorded(signal_dispatcher, signal: Signal) -> list[dict[str, object]]:  # type: ignore[no-untyped-def]
    seen: list[dict[str, object]] = []

    async def _handler(**kw: object) -> None:
        seen.append(kw)

    signal_dispatcher.connect(signal, _handler)
    return seen


async def test_leader_steps_down_after_repeated_publish_failures(  # type: ignore[no-untyped-def]
    leader, clock, lock_manager, backend, signal_dispatcher, swarmq_logs
) -> None:
    standby = _recorded(signal_dispatcher, Signal.SCHEDULER_STANDBY)
    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    assert leader.state == "leader"
    clock.now += 1.0  # scheduler_lock_ttl der Fixture

    await leader._fire_one("cron_a", 1_700_000_000_000, _def())

    assert leader.state == "standby"
    lock_manager.release.assert_awaited_once()
    backend.schedules.set_handover_marker.assert_not_awaited()
    assert len(standby) == 1
    assert [e for e in swarmq_logs if e.get("event") == "scheduler_publish_failures_stepdown"]


async def test_a_successful_fire_in_between_keeps_the_leader(leader, clock, broker) -> None:  # type: ignore[no-untyped-def]
    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    broker.publish = AsyncMock()
    await leader._fire_one("cron_b", 1_700_000_000_000, _def("cron_b"))
    broker.publish = AsyncMock(side_effect=BrokerConnectionError("down"))
    clock.now += 1.0

    await leader._fire_one("cron_a", 1_700_000_000_000, _def())

    assert leader.state == "leader"


async def test_no_candidacy_during_the_cooldown(leader, clock, lock_manager) -> None:  # type: ignore[no-untyped-def]
    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    clock.now += 1.0
    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    lock_manager.acquire.reset_mock()

    clock.now += 1.9  # Abkuehlzeit 2 x scheduler_lock_ttl (1 s)
    await leader._election_tick()
    assert leader.state == "standby"
    lock_manager.acquire.assert_not_awaited()

    clock.now += 0.1
    await leader._election_tick()
    lock_manager.acquire.assert_awaited_once()


async def test_window_zero_keeps_the_leader(  # type: ignore[no-untyped-def]
    backend, broker, config, lock_manager, store, signal_dispatcher, clock
) -> None:
    import dataclasses

    loop = _make_loop(
        backend,
        broker,
        dataclasses.replace(config, scheduler_publish_stepdown_s=0.0),
        lock_manager,
        store,
        signal_dispatcher,
    )
    loop._state = "leader"
    broker.publish = AsyncMock(side_effect=BrokerConnectionError("down"))
    for _ in range(3):
        await loop._fire_one("cron_a", 1_700_000_000_000, _def())
        clock.now += 10.0

    assert loop.state == "leader"
    lock_manager.release.assert_not_awaited()


async def test_failed_release_still_steps_down(leader, clock, lock_manager) -> None:  # type: ignore[no-untyped-def]
    from swarmq.exceptions import BackendConnectionError

    lock_manager.release = AsyncMock(side_effect=BackendConnectionError("valkey gone"))
    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    clock.now += 1.0

    await leader._fire_one("cron_a", 1_700_000_000_000, _def())

    assert leader.state == "standby"
