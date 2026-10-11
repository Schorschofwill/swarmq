"""``redeliver_in_place``: Requeue an derselben Stelle mit Schedfire-Claim.

Ein ``nack(requeue=True)`` kann keinen Header setzen: das Original kehrt ohne
``x-swarmq-schedfire-claim`` zurueck. Stuende der Claim dann noch, bestaetigte
sich die Redelivery als Duplikat -- der Fire (bzw. der Workflow-Advance einer
AT_LEAST_ONCE-Zustellung) waere verloren. Deshalb wird der Claim vorher
freigegeben; scheitert das, geht statt des ``nack`` eine Kopie mit dem
Owner-Token ueber die Delay-Queue (sie uebernimmt den Claim).

Test-Liste (viertes Review 0.9.0):
- [x] Freigabe gelingt -> ``nack(requeue=True)`` des Originals, keine Kopie
- [x] ohne Claim -> ``nack(requeue=True)``, keine Freigabe, keine Kopie
- [x] Freigabe scheitert (Ausfall) -> Kopie ueber ``retry_later`` mit dem
      Owner-Token und Backoff (Zurueckstellungszaehler), kein ``nack``
- [x] Freigabe scheitert, Publish der Kopie scheitert auch -> ``nack`` als
      letzter Ausweg
- [x] Freigabe wirft eine fremde Ausnahme (Dritt-Store) -> die Zustellung
      bleibt nicht unbestaetigt haengen (Kopie wie beim Ausfall)
- [x] WARNING-Zeile fuer die Kopie (Fire-Key, Zaehler, Wartezeit, Fehler)
"""

from __future__ import annotations

import functools

import pytest

import swarmq.worker.gates as gates_module
from swarmq.backoff import backend_defer_delay
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError, PublishIndeterminate
from swarmq.headers import HEADER_BACKEND_DEFER_COUNT, HEADER_SCHEDFIRE_CLAIM
from swarmq.worker.gates import GateContext, SchedfireClaim, redeliver_in_place
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def no_jitter(monkeypatch):
    """Zufallsquelle des Jitters auf 1,0: die Wartezeit ist exakt pruefbar."""
    monkeypatch.setattr(
        gates_module, "backend_defer_delay",
        functools.partial(backend_defer_delay, uniform=lambda low, high: 1.0),
    )


def _claimed_ctx() -> GateContext:
    ctx = GateContext(task_id="abc-123", task_name="add", queue="default")
    ctx.schedfire_claim = SchedfireClaim(key="cron_a:1", owner="own-1")
    return ctx


def _fire() -> object:
    return make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1"})


async def test_failed_release_publishes_a_copy_carrying_the_owner_token(worker, broker, backend):
    backend.schedules.release_fire_claim.side_effect = BackendConnectionError("valkey down")
    msg = _fire()

    await redeliver_in_place(worker._gate_deps, msg, _claimed_ctx())

    broker.redeliver.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    assert broker.retry_later.await_args.args[0] is msg
    assert broker.retry_later.await_args.kwargs["headers"][HEADER_SCHEDFIRE_CLAIM] == "own-1"


@pytest.mark.parametrize(("prior", "attempt", "delay_ms"), [(None, 1, 1000), (2, 3, 4000)])
async def test_copy_after_failed_release_is_deferred_with_backoff(worker, broker, backend, prior, attempt, delay_ms):
    """Wie eine Zurueckstellung bei Backend-Ausfall: wachsende Wartezeit statt
    eines heissen Kreislaufs, solange Valkey weg ist."""
    backend.schedules.release_fire_claim.side_effect = BackendConnectionError("valkey down")
    headers = {"x-swarmq-schedfire": "cron_a:1"}
    if prior is not None:
        headers[HEADER_BACKEND_DEFER_COUNT] = prior

    await redeliver_in_place(worker._gate_deps, make_message(_default_payload(), headers=headers), _claimed_ctx())

    kwargs = broker.retry_later.await_args.kwargs
    assert kwargs["headers"][HEADER_BACKEND_DEFER_COUNT] == attempt
    assert kwargs["delay_ms"] == delay_ms


@pytest.mark.parametrize("publish_error", [BrokerConnectionError("rmq down"), PublishIndeterminate("deadline")])
async def test_failed_copy_publish_falls_back_to_nack(worker, broker, backend, publish_error):
    """Broker auch gestoert: der ``nack`` als letzter Ausweg -- die
    Zustellung bleibt nicht unbestaetigt haengen."""
    backend.schedules.release_fire_claim.side_effect = BackendConnectionError("valkey down")
    broker.retry_later.side_effect = publish_error
    msg = _fire()

    await redeliver_in_place(worker._gate_deps, msg, _claimed_ctx())

    broker.redeliver.assert_awaited_once_with(msg)


async def test_foreign_release_exception_does_not_leave_the_delivery_hanging(worker, broker, backend):
    """Ein Dritt-Store wirft etwas anderes als ``BackendConnectionError``:
    ohne Absicherung liefe die Ausnahme vor dem ``nack`` hinaus, die
    Zustellung bliebe unbestaetigt bis zum Kanalende. Der Claim kann noch
    stehen -- also wie beim Ausfall: Kopie mit Token."""
    backend.schedules.release_fire_claim.side_effect = RuntimeError("store bug")
    msg = _fire()

    await redeliver_in_place(worker._gate_deps, msg, _claimed_ctx())

    assert broker.retry_later.await_args.kwargs["headers"][HEADER_SCHEDFIRE_CLAIM] == "own-1"
    broker.redeliver.assert_not_awaited()


async def test_released_claim_is_nacked_without_copy(worker, broker, backend):
    msg = _fire()

    await redeliver_in_place(worker._gate_deps, msg, _claimed_ctx())

    backend.schedules.release_fire_claim.assert_awaited_once_with("cron_a:1", owner="own-1")
    broker.redeliver.assert_awaited_once_with(msg)
    broker.retry_later.assert_not_awaited()


async def test_without_claim_is_nacked_without_release_or_copy(worker, broker, backend):
    msg = make_message(_default_payload())

    await redeliver_in_place(worker._gate_deps, msg, GateContext(task_id="abc-123", task_name="add", queue="default"))

    backend.schedules.release_fire_claim.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.retry_later.assert_not_awaited()


async def test_copy_logs_one_warning_with_fire_key_count_and_delay(worker, broker, backend, swarmq_logs):
    backend.schedules.release_fire_claim.side_effect = BackendConnectionError("valkey down")

    await redeliver_in_place(worker._gate_deps, _fire(), _claimed_ctx())

    [entry] = [e for e in swarmq_logs if e["event"] == "task_requeued_as_copy"]
    assert entry["log_level"] == "warning"
    assert entry["logger"] == "swarmq.worker.gates"
    assert (entry["task_id"], entry["task_name"]) == ("abc-123", "add")
    assert (entry["schedfire_key"], entry["defer_count"], entry["delay_ms"]) == ("cron_a:1", 1, 1000)


async def test_failed_copy_logs_the_publish_error(worker, broker, backend, swarmq_logs):
    backend.schedules.release_fire_claim.side_effect = BackendConnectionError("valkey down")
    broker.retry_later.side_effect = BrokerConnectionError("rmq down")

    await redeliver_in_place(worker._gate_deps, _fire(), _claimed_ctx())

    [entry] = [e for e in swarmq_logs if e["event"] == "task_copy_failed_redelivered"]
    assert entry["log_level"] == "warning"
    assert entry["schedfire_key"] == "cron_a:1"
    assert (entry["error_type"], entry["error"]) == ("BrokerConnectionError", "rmq down")


async def test_failing_pause_still_redelivers(worker, broker):
    """BZ-29: Die Pause vor dem ``nack`` darf das ``nack`` nie verhindern --
    sonst bliebe die Zustellung unbestaetigt bis zum Kanalende."""

    async def broken_pause(_delay_s: float) -> None:
        raise RuntimeError("pause broken")

    worker._gate_deps.redeliver_pause = broken_pause
    msg = make_message(_default_payload())

    await redeliver_in_place(worker._gate_deps, msg, GateContext(task_id="abc-123", task_name="add", queue="default"))

    broker.redeliver.assert_awaited_once_with(msg)
