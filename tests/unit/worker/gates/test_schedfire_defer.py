"""Schedfire-Zurueckstellung bei fremdem Claim (FS-03 Teil 1, E-046, Plan U5, KTD5).

Eine Neuzustellung -- vom Broker (``x-delivery-count``/``x-acquired-count``
> 0) oder eine schon zurueckgestellte Kopie (``x-swarmq-schedfire-defer-count``)
-- wird bei fremdem In-Flight-Claim nicht als Duplikat bestaetigt, sondern
ueber die Delay-Queue zurueckgestellt, bis der Completed-Marker steht oder der
Claim ablaeuft. Ein frisches Doppel-Fire des Leaders bleibt ein Duplikat
(E-047). Gedeckelt auf ``max(completed_ttl, inflight_ttl +
backend_defer_max_delay)`` (``completed_ttl`` ohne Alter der Kopie), gemessen an der Summe der Wartezeiten aus dem
Defer-Zaehler; danach Stall-Log statt Lauf.

Test-Liste:
1. [x] Neuzustellung mit Zaehler, fremder Claim: ``retry_later`` mit
       Defer-Header 1, kein Ack, kein Lauf (Nachweis FS-03)
2. [x] Kopie mit Defer-Header ohne Zaehler, Claim fremd: erneut
       zurueckgestellt (Nachweis FS-03, Zaehler 2)
3. [x] Backoff waechst mit dem Defer-Zaehler
4. [x] Broker-Neuzustellung einer zurueckgestellten Kopie beginnt die
       Zaehlung neu (Zaehler 1)
5. [x] Kopie mit Defer-Header, Completed-Marker gesetzt: Ack, kein Claim
6. [x] Kopie mit Defer-Header, Claim frei (abgelaufen): Lauf
7. [x] Backend-Ausfall-Kopie einer zurueckgestellten Kopie behaelt den
       Defer-Header
8. [x] Frisches Duplikat ohne Zaehler und ohne Defer-Header: Ack (E-047)
9. [x] ``retry_later`` scheitert: ``nack`` an derselben Stelle, keine
       Freigabe des fremden Claims
10. [x] Zurueckstellung ueber den Deckel hinaus: Stall-Log und Ack statt
        Lauf und statt weiterer Zurueckstellung
11. [x] Deckel bei ``hard_timeout`` > ``completed_ttl``: die Kopie wird
        weiter zurueckgestellt (kein Stall-Log), solange der Claim noch
        leben kann -- und laeuft nach dessen Ablauf
12. [x] Die Kopie traegt die Claim-Tokens des Originals unveraendert und
        bietet kein eigenes an
13. [x] Jede Zurueckstellung loggt eine INFO-Zeile mit Zaehler und Wartezeit
14. [x] Erste Broker-Neuzustellung trifft im Completed-Check bzw. beim Claim
        auf Backend-Ausfall: die Kopie bleibt als Neuzustellung erkennbar
        (Defer-Header 0) und wird bei fremdem Claim zurueckgestellt, nicht
        bestaetigt (Review U5, P1)
15. [x] Deckel haengt nicht am Alter der Kopie (``x-swarmq-enqueued-at``):
        an der Kante greift er auch bei einer alten Kopie (Review U5, P2)
16. [x] Broker-Neuzustellung einer zurueckgestellten Kopie trifft auf
        Backend-Ausfall: die Kopie beginnt die Zaehlung neu (wie Liste 4)
"""

from __future__ import annotations

import time
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import BackendConnectionError
from swarmq.headers import (
    HEADER_BACKEND_DEFER_COUNT,
    HEADER_ENQUEUED_AT,
    HEADER_SCHEDFIRE_CLAIM,
    HEADER_SCHEDFIRE_DEFER_COUNT,
)
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_FIRE = "x-swarmq-schedfire"


class VeryLongCron(Task):
    """``hard_timeout`` weit ueber ``completed_ttl`` (Default 3.900 s)."""

    task_info = TaskInfo(name="very_long_cron", queues=["default"], hard_timeout=9000)

    @override
    async def run(self) -> None:
        return None


def _ctx(task_name: str = "add") -> GateContext:
    return GateContext(task_id="abc-123", task_name=task_name, queue="default")


def _fire(headers: dict[str, Any] | None = None, *, name: str = "add") -> MagicMock:
    return make_message(_default_payload(name=name), headers={_FIRE: "cron:s:1", **(headers or {})})


@pytest.fixture
def foreign_claim(backend):
    backend.schedules.is_fire_completed = AsyncMock(return_value=False)
    backend.schedules.claim_fire = AsyncMock(return_value=False)
    return backend


def _copy_headers(broker: MagicMock) -> dict[str, Any]:
    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    return headers


async def test_broker_redelivery_with_foreign_claim_is_deferred(worker, broker, foreign_claim):
    """Liste 1."""
    msg = _fire({"x-delivery-count": 1})

    assert await worker._schedfire_gate.apply(msg, _ctx()) is Redelivered

    broker.ack.assert_not_awaited()
    assert _copy_headers(broker)[HEADER_SCHEDFIRE_DEFER_COUNT] == 1
    assert "x-delivery-count" not in _copy_headers(broker)


async def test_backoff_grows_with_the_defer_count(worker, broker, foreign_claim):
    """Liste 3: Jitter 0,8 ... 1,2 -- Versuch 1 (1 s) und 4 (8 s) ueberlappen nicht."""
    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 0, "x-delivery-count": 1}), _ctx())
    first = broker.retry_later.await_args.kwargs["delay_ms"]
    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 3}), _ctx())
    fourth = broker.retry_later.await_args.kwargs["delay_ms"]

    assert first <= 1200
    assert fourth >= 6400


async def test_broker_redelivery_of_a_deferred_copy_restarts_the_count(worker, broker, foreign_claim):
    """Liste 4: ``prior_deferrals`` -- die Folge ist unterbrochen."""
    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 7, "x-acquired-count": 1}), _ctx())

    assert _copy_headers(broker)[HEADER_SCHEDFIRE_DEFER_COUNT] == 1


async def test_deferred_copy_acks_once_the_fire_completed(worker, broker, backend):
    """Liste 5."""
    backend.schedules.is_fire_completed = AsyncMock(return_value=True)
    msg = _fire({HEADER_SCHEDFIRE_DEFER_COUNT: 4})

    assert await worker._schedfire_gate.apply(msg, _ctx()) == Terminal.never_release()

    broker.ack.assert_awaited_once_with(msg)
    backend.schedules.claim_fire.assert_not_awaited()
    broker.retry_later.assert_not_awaited()


async def test_deferred_copy_runs_once_the_claim_expired(worker, broker, backend):
    """Liste 6."""
    backend.schedules.claim_fire = AsyncMock(return_value=True)
    ctx = _ctx()

    assert await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 4}), ctx) is Continue

    assert ctx.schedfire_claim is not None
    broker.ack.assert_not_awaited()


async def test_backend_outage_copy_of_a_deferred_copy_keeps_the_defer_header(worker, broker, backend):
    """Liste 7: ohne den Header kaeme die Kopie ohne Broker-Zaehler zurueck
    und wuerde bei noch fremdem Claim bestaetigt."""
    backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))

    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 3}), _ctx())

    headers = _copy_headers(broker)
    assert headers[HEADER_SCHEDFIRE_DEFER_COUNT] == 3
    assert headers[HEADER_BACKEND_DEFER_COUNT] == 1


async def test_fresh_duplicate_is_still_acked(worker, broker, foreign_claim):
    """Liste 8 (E-047): frisches Doppel-Fire des Leaders."""
    msg = _fire()

    assert await worker._schedfire_gate.apply(msg, _ctx()) == Terminal.never_release()

    broker.ack.assert_awaited_once_with(msg)
    broker.retry_later.assert_not_awaited()


async def test_failed_deferral_requeues_without_releasing_the_foreign_claim(worker, broker, foreign_claim, swarmq_logs):
    """Liste 9."""
    broker.retry_later.side_effect = RuntimeError("broker down")
    msg = _fire({"x-delivery-count": 1})

    assert await worker._schedfire_gate.apply(msg, _ctx()) is Redelivered

    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()
    foreign_claim.schedules.release_fire_claim.assert_not_awaited()
    foreign_claim.schedules.mark_fire_completed.assert_not_awaited()
    [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_defer_failed_redelivered"]
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "RuntimeError"


async def test_deferral_beyond_the_cap_logs_a_stall_and_acks(worker, broker, foreign_claim, swarmq_logs):
    """Liste 10: 200 Zurueckstellungen sind mindestens 0,8 x 11.703 s --
    weit ueber dem Deckel 3.900 s (Default-Konfiguration)."""
    msg = _fire({HEADER_SCHEDFIRE_DEFER_COUNT: 200})

    assert await worker._schedfire_gate.apply(msg, _ctx()) == Terminal.never_release()

    broker.ack.assert_awaited_once_with(msg)
    broker.retry_later.assert_not_awaited()
    [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_defer_stalled"]
    assert entry["log_level"] == "warning"
    assert entry["schedfire_key"] == "cron:s:1"
    assert entry["defer_count"] == 200
    assert entry["cap_s"] == 3900
    assert "claim" in entry["hint"]


async def test_long_hard_timeout_keeps_deferring_past_the_completed_ttl(
    worker, broker, backend, foreign_claim, swarmq_logs
):
    """Liste 11: 120 Zurueckstellungen (mindestens 0,8 x 6.903 s) liegen ueber
    ``completed_ttl`` (3.900 s), aber vor dem Claim-Ablauf (9.030 s) -- die
    Kopie wartet weiter und laeuft, sobald der Claim frei ist."""
    worker._registry.register(VeryLongCron)
    msg = _fire({HEADER_SCHEDFIRE_DEFER_COUNT: 120}, name="very_long_cron")

    assert await worker._schedfire_gate.apply(msg, _ctx("very_long_cron")) is Redelivered
    assert _copy_headers(broker)[HEADER_SCHEDFIRE_DEFER_COUNT] == 121
    assert not [e for e in swarmq_logs if e["event"] == "schedfire_defer_stalled"]

    backend.schedules.claim_fire = AsyncMock(return_value=True)
    assert await worker._schedfire_gate.apply(msg, _ctx("very_long_cron")) is Continue


async def test_long_hard_timeout_cap_ends_after_the_claim_could_have_expired(worker, broker, foreign_claim):
    """Liste 10/11 trianguliert: 200 Zurueckstellungen (mindestens 9.362 s)
    liegen hinter ``hard_timeout + 30 + backend_defer_max_delay`` (9.090 s)."""
    worker._registry.register(VeryLongCron)
    msg = _fire({HEADER_SCHEDFIRE_DEFER_COUNT: 200}, name="very_long_cron")

    assert await worker._schedfire_gate.apply(msg, _ctx("very_long_cron")) == Terminal.never_release()

    broker.ack.assert_awaited_once_with(msg)


async def test_deferred_copy_carries_the_original_claim_tokens(worker, broker, foreign_claim):
    """Liste 12: die Zustellung haelt keinen Claim -- die Kopie bietet weiter
    nur die Tokens an, die sie mitbrachte."""
    await worker._schedfire_gate.apply(_fire({"x-delivery-count": 1, HEADER_SCHEDFIRE_CLAIM: "tok-a"}), _ctx())

    assert _copy_headers(broker)[HEADER_SCHEDFIRE_CLAIM] == "tok-a"


async def test_each_deferral_logs_one_info_line(worker, broker, foreign_claim, swarmq_logs):
    """Liste 13."""
    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 2}), _ctx())

    [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_redelivery_deferred"]
    assert entry["log_level"] == "info"
    assert entry["schedfire_key"] == "cron:s:1"
    assert entry["defer_count"] == 3
    assert entry["delay_ms"] == broker.retry_later.await_args.kwargs["delay_ms"]
    assert entry["task_id"] == "abc-123"


@pytest.mark.parametrize("stage", ["is_fire_completed", "claim_fire"])
async def test_backend_outage_copy_of_a_first_redelivery_stays_a_redelivery(worker, broker, backend, stage):
    """Liste 14: die Backend-Ausfall-Kopie verliert die Broker-Zaehler; ohne
    Defer-Header kaeme sie als frisches Duplikat zurueck und wuerde bei noch
    fremdem Claim (dem des Toten) bestaetigt -- das Fire waere verloren."""
    backend.schedules.is_fire_completed = AsyncMock(return_value=False)
    backend.schedules.claim_fire = AsyncMock(return_value=False)
    setattr(backend.schedules, stage, AsyncMock(side_effect=BackendConnectionError("valkey down")))
    await worker._schedfire_gate.apply(_fire({"x-delivery-count": 1}), _ctx())
    copy_headers = _copy_headers(broker)
    assert "x-delivery-count" not in copy_headers
    assert copy_headers[HEADER_SCHEDFIRE_DEFER_COUNT] == 0

    backend.schedules.is_fire_completed = AsyncMock(return_value=False)
    backend.schedules.claim_fire = AsyncMock(return_value=False)
    broker.retry_later.reset_mock()
    copy = make_message(_default_payload(name="add"), headers=copy_headers)

    assert await worker._schedfire_gate.apply(copy, _ctx()) is Redelivered
    broker.ack.assert_not_awaited()
    assert _copy_headers(broker)[HEADER_SCHEDFIRE_DEFER_COUNT] == 1


async def test_backend_outage_copy_of_a_redelivered_deferred_copy_restarts_the_count(worker, broker, backend):
    """Liste 16: wie Liste 4 -- der Broker hat die Folge unterbrochen; der
    alte Zaehler zaehlte sonst gegen den Deckel der neuen Folge."""
    backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))

    await worker._schedfire_gate.apply(_fire({HEADER_SCHEDFIRE_DEFER_COUNT: 7, "x-acquired-count": 1}), _ctx())

    assert _copy_headers(broker)[HEADER_SCHEDFIRE_DEFER_COUNT] == 0


@pytest.mark.parametrize(("defer_count", "stalled"), [(86, False), (87, True)])
async def test_cap_does_not_grow_with_the_age_of_the_copy(worker, broker, foreign_claim, defer_count, stalled):
    """Liste 15: jede Kopie traegt das ``x-swarmq-enqueued-at`` des Fires; der
    Completed-Marker-TTL waechst mit diesem Alter (2 x Alter + 300). Der
    Deckel bleibt trotzdem bei 3.900 s (Default): 86 Zurueckstellungen sind
    mindestens 0,8 x 4.863 s = 3.890 s (darunter), 87 mindestens 0,8 x
    4.923 s = 3.938 s (darueber)."""
    old = int(time.time()) - 10_000
    msg = _fire({HEADER_SCHEDFIRE_DEFER_COUNT: defer_count, HEADER_ENQUEUED_AT: old})

    outcome = await worker._schedfire_gate.apply(msg, _ctx())

    if stalled:
        assert outcome == Terminal.never_release()
        broker.ack.assert_awaited_once_with(msg)
    else:
        assert outcome is Redelivered
        broker.ack.assert_not_awaited()
