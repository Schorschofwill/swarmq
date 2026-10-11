"""Serie der Publish-Fehler fuer den Ruecktritt (Review MR 15, P2-1, P2-2).

Test-Liste:
- [x] liegt zwischen zwei Fehlern mehr als die Frist, beginnt eine neue Serie
- [x] ein Abstand genau gleich der Frist setzt die Serie fort
- [x] eine neue Amtszeit (Lock verloren, neu gewonnen) beginnt eine neue Serie
- [x] ``PublishRejected`` (Queue-spezifische Ablehnung) zaehlt nicht
- [x] ein einzelner kaputter Cron (wiederholt ``PublishRejected``) fuehrt nicht zum Ruecktritt
- [x] ein einzelner Cron, dessen Confirms ausbleiben (``PublishIndeterminate``), loest keinen Ruecktritt aus
      (von Georg am 2026-10-11 entschieden)
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import BrokerConnectionError

pytestmark = pytest.mark.timeout(5)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _tracker(window_s: float = 30.0):
    from swarmq.scheduler.stepdown import PublishStepdown

    clock = _Clock()
    return PublishStepdown(window_s, clock=clock), clock


def test_a_gap_longer_than_the_window_starts_a_new_series() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 31.0

    assert tracker.failure(BrokerConnectionError("down")) is False
    clock.now += 30.0
    assert tracker.failure(BrokerConnectionError("down")) is True


def test_a_gap_equal_to_the_window_continues_the_series() -> None:
    tracker, clock = _tracker(30.0)
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 20.0
    tracker.failure(BrokerConnectionError("down"))
    clock.now += 30.0

    assert tracker.failure(BrokerConnectionError("down")) is True


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
def leader(backend, broker, config, lock_manager, store, signal_dispatcher, clock):
    from unittest.mock import AsyncMock

    from tests.unit.scheduler._helpers import _make_loop

    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    broker.publish = AsyncMock(side_effect=BrokerConnectionError("down"))
    return loop


async def test_a_new_term_starts_a_new_series(leader, clock, lock_manager) -> None:
    from unittest.mock import AsyncMock

    from tests.unit.scheduler._helpers import _def

    await leader._fire_one("cron_a", 1_700_000_000_000, _def())
    lock_manager.renew = AsyncMock(return_value=False)
    await leader._election_tick()  # Lock verloren -> Standby
    assert leader.state == "standby"
    lock_manager.renew = AsyncMock(return_value=True)
    await leader._election_tick()  # neu gewonnen -> neue Amtszeit
    assert leader.state == "leader"
    clock.now += 1.0  # scheduler_lock_ttl der Fixture; Abstand == Frist

    await leader._fire_one("cron_a", 1_700_000_000_000, _def())

    assert leader.state == "leader"
    lock_manager.release.assert_not_awaited()


def test_a_queue_specific_rejection_is_not_counted() -> None:
    from swarmq.exceptions import PublishRejected

    tracker, clock = _tracker(30.0)
    tracker.failure(PublishRejected("nack"))
    clock.now += 30.0

    assert issubclass(PublishRejected, BrokerConnectionError)
    assert tracker.failure(PublishRejected("nack")) is False


async def test_a_single_broken_cron_does_not_step_down(leader, clock, broker, lock_manager) -> None:
    from unittest.mock import AsyncMock

    from swarmq.exceptions import PublishRejected
    from tests.unit.scheduler._helpers import _def

    broker.publish = AsyncMock(side_effect=PublishRejected("publish to 'full' failed after 5 attempts"))
    for _ in range(5):
        await leader._fire_one("cron_full", 1_700_000_000_000, _def("cron_full"))
        clock.now += 1.0

    assert leader.state == "leader"
    lock_manager.release.assert_not_awaited()


async def test_a_single_cron_without_confirms_does_not_step_down(leader, clock, broker, lock_manager) -> None:
    # Georg, 2026-10-11: ``PublishIndeterminate`` zaehlt nicht fuer den Ruecktritt.
    from unittest.mock import AsyncMock

    from swarmq.exceptions import PublishIndeterminate
    from tests.unit.scheduler._helpers import _def

    broker.publish = AsyncMock(side_effect=PublishIndeterminate("publish to 'slow' exceeded the publish deadline"))
    for _ in range(5):
        await leader._fire_one("cron_slow", 1_700_000_000_000, _def("cron_slow"))
        clock.now += 1.0

    assert leader.state == "leader"
    lock_manager.release.assert_not_awaited()
