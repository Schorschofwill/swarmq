"""``delay`` und ``eta`` endlich und begrenzt (E-080, 0.13.1).

``schedule(delay=…)`` und ``schedule(eta=…)`` schreiben einen Einmal-Eintrag
mit ``fire_ts_ms`` in den Schedule-Store. Bis 0.13.0 veröffentlichte
``delay=nan`` still sofort (``nan < 0`` und ``nan > 0`` sind beide falsch),
``delay=inf`` warf einen rohen ``OverflowError``, und ein fernes ``eta`` ließ
sich speichern. Seit 0.13.1 gilt dieselbe Grenze wie für Fristen
(``MAX_DEADLINE_S``, E-063): ``ValueError`` vor jeder Nebenwirkung, also vor
``pre_enqueue``, Claim, Publish und Speichern (wie ``unique_for``, E-056).

Testliste:
# [x] delay=nan -> ValueError, keine Nebenwirkung
# [x] delay=inf -> ValueError, keine Nebenwirkung
# [x] delay=-inf -> ValueError (delay >= 0 gilt weiter)
# [x] delay=MAX_DEADLINE_S + 1 -> ValueError, keine Nebenwirkung
# [x] delay als timedelta oder String -> TypeError mit Feldnamen, keine Nebenwirkung
#     (Review 0.13.1: die Typprüfung läuft vor ``delay < 0``)
# [x] delay=MAX_DEADLINE_S erlaubt (Eintrag mit fire_ts_ms = jetzt + Grenze)
# [x] eta (aware) weiter als MAX_DEADLINE_S in der Zukunft -> ValueError, keine Nebenwirkung
# [x] eta (aware) knapp innerhalb der Grenze erlaubt
# [x] eta mit Zone +14:00: Grenze auf dem Zeitpunkt (innen gespeichert, außen abgelehnt)
# [x] eta naiv: wie bisher als Ortszeit gelesen; fern in der Zukunft -> ValueError
# [x] eta=datetime.max (naiv) -> ValueError mit Feldnamen statt rohem Jahresfehler
# [x] CLI: schedule add --at jenseits der Grenze -> ConfigurationError vor dem Backend
#     (tests/unit/test_cli.py::TestScheduleHandlerValidation::test_schedule_add_rejects_at_beyond_the_deadline_bound)

schedule_many() nimmt weder delay noch eta; Workflows haben keine Verzögerung.
"""

from __future__ import annotations

import math
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.client import Client
from swarmq.client import schedule as client_mod
from swarmq.config_validation import MAX_DEADLINE_MS, MAX_DEADLINE_S
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain

pytestmark = [pytest.mark.timeout(5), pytest.mark.entscheidung("E-080")]


class _Recorder(Middleware):
    """Schreibt ``pre_enqueue`` mit, die erste Nebenwirkung eines Aufrufs."""

    def __init__(self) -> None:
        self.seen: list[MessageContext] = []

    @override
    async def pre_enqueue(self, message: MessageContext) -> None:
        self.seen.append(message)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    instance = MagicMock()
    instance.add_schedule = AsyncMock(return_value=True)
    monkeypatch.setattr(client_mod, "ScheduleStore", lambda _backend: instance)
    return instance


@pytest.fixture
def recorder() -> _Recorder:
    return _Recorder()


@pytest.fixture
def bound_client(broker: AsyncMock, backend: AsyncMock, recorder: _Recorder) -> Client:
    backend.claim_unique = AsyncMock(return_value=None)
    return Client(broker=broker, backend=backend, middleware_chain=MiddlewareChain([recorder]))


def _assert_no_side_effect(broker: AsyncMock, backend: AsyncMock, store: MagicMock, recorder: _Recorder) -> None:
    assert recorder.seen == []
    backend.claim_unique.assert_not_awaited()
    broker.publish.assert_not_awaited()
    store.add_schedule.assert_not_awaited()


async def test_nan_delay_is_rejected_before_any_side_effect(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock, store: MagicMock, recorder: _Recorder
) -> None:
    with pytest.raises(ValueError, match=r"^delay must be a finite number of seconds, got nan"):
        await bound_client.schedule("bound.task", 1, queue="bound", delay=math.nan, unique_for=60)

    _assert_no_side_effect(broker, backend, store, recorder)


@pytest.mark.parametrize(
    ("delay", "message"),
    [
        pytest.param(math.inf, rf"^delay must be at most {MAX_DEADLINE_S} seconds, got inf", id="inf"),
        pytest.param(-math.inf, r"^delay must be >= 0, got -inf", id="neg-inf"),
        pytest.param(MAX_DEADLINE_S + 1, rf"^delay must be at most {MAX_DEADLINE_S} seconds", id="bound-plus-one"),
    ],
)
async def test_delay_outside_the_bound_is_rejected_before_any_side_effect(
    bound_client: Client,
    broker: AsyncMock,
    backend: AsyncMock,
    store: MagicMock,
    recorder: _Recorder,
    delay: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await bound_client.schedule("bound.task", 1, queue="bound", delay=delay, unique_for=60)

    _assert_no_side_effect(broker, backend, store, recorder)


@pytest.mark.parametrize("delay", [timedelta(seconds=60), "60"], ids=["timedelta", "string"])
async def test_non_number_delay_names_the_field_before_any_side_effect(
    bound_client: Client,
    broker: AsyncMock,
    backend: AsyncMock,
    store: MagicMock,
    recorder: _Recorder,
    delay: object,
) -> None:
    """Review 0.13.1: ``delay < 0`` lief vor der Typprüfung und warf einen
    Vergleichsfehler ohne Feldnamen."""
    with pytest.raises(TypeError, match=r"^delay must be a number of seconds, got "):
        await bound_client.schedule("bound.task", 1, queue="bound", delay=delay, unique_for=60)  # type: ignore[arg-type]  # Fehleingabe ist der Testfall

    _assert_no_side_effect(broker, backend, store, recorder)


async def test_delay_at_the_bound_is_stored(bound_client: Client, store: MagicMock) -> None:
    before_ms = int(time.time() * 1000)

    await bound_client.schedule("bound.task", 1, queue="bound", delay=MAX_DEADLINE_S, unique_for=60)

    definition = store.add_schedule.await_args.args[0]
    assert before_ms + MAX_DEADLINE_MS <= definition.fire_ts_ms <= int(time.time() * 1000) + MAX_DEADLINE_MS


async def test_eta_beyond_the_bound_is_rejected_before_any_side_effect(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock, store: MagicMock, recorder: _Recorder
) -> None:
    eta = datetime.now(UTC) + timedelta(seconds=MAX_DEADLINE_S + 60)

    with pytest.raises(ValueError, match=rf"^eta must be at most {MAX_DEADLINE_S} seconds in the future"):
        await bound_client.schedule("bound.task", 1, queue="bound", eta=eta, unique_for=60)

    _assert_no_side_effect(broker, backend, store, recorder)


async def test_eta_just_inside_the_bound_is_stored(bound_client: Client, store: MagicMock) -> None:
    eta = datetime.now(UTC) + timedelta(seconds=MAX_DEADLINE_S - 60)

    await bound_client.schedule("bound.task", 1, queue="bound", eta=eta, unique_for=60)

    assert store.add_schedule.await_args.args[0].fire_ts_ms == int(eta.timestamp() * 1000)


async def test_eta_with_a_far_east_zone_is_bounded_on_its_instant(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock, store: MagicMock, recorder: _Recorder
) -> None:
    """Review 0.13.1 (P3-4): die Grenze gilt auf dem Zeitpunkt, nicht auf
    der Wanduhr der Zone; ``+14:00`` liegt 14 h vor UTC."""
    zone = timezone(timedelta(hours=14))
    inside = datetime.now(zone) + timedelta(seconds=MAX_DEADLINE_S - 60)
    await bound_client.schedule("bound.task", 1, queue="bound", eta=inside, unique_for=60)
    assert store.add_schedule.await_args.args[0].fire_ts_ms == int(inside.timestamp() * 1000)
    store.add_schedule.reset_mock()
    backend.claim_unique.reset_mock()
    recorder.seen.clear()

    beyond = datetime.now(zone) + timedelta(seconds=MAX_DEADLINE_S + 60)
    with pytest.raises(ValueError, match=rf"^eta must be at most {MAX_DEADLINE_S} seconds in the future"):
        await bound_client.schedule("bound.task", 1, queue="bound", eta=beyond, unique_for=60)

    _assert_no_side_effect(broker, backend, store, recorder)


async def test_naive_eta_is_read_as_local_time_and_bounded_alike(
    bound_client: Client, broker: AsyncMock, backend: AsyncMock, store: MagicMock, recorder: _Recorder
) -> None:
    """Wie bisher liest ``eta.timestamp()`` ein naives ``eta`` als Ortszeit
    (die CLI liest ``--at`` ohne Zone dagegen als UTC); die Grenze gilt auf
    demselben Zeitstempel."""
    near = datetime.now() + timedelta(hours=1)  # noqa: DTZ005 - naive Ortszeit ist der geprüfte Fall
    await bound_client.schedule("bound.task", 1, queue="bound", eta=near)
    assert store.add_schedule.await_args.args[0].fire_ts_ms == int(near.timestamp() * 1000)
    store.add_schedule.reset_mock()
    recorder.seen.clear()

    far = datetime(9000, 1, 1)  # noqa: DTZ001 - naive Zeit ist der geprüfte Fall
    with pytest.raises(ValueError, match=rf"^eta must be at most {MAX_DEADLINE_S} seconds in the future"):
        await bound_client.schedule("bound.task", 1, queue="bound", eta=far, unique_for=60)

    _assert_no_side_effect(broker, backend, store, recorder)


@pytest.fixture(params=["UTC", "Europe/Berlin"])
def local_zone(request: pytest.FixtureRequest) -> Iterator[str]:
    """Setzt die Ortszeit des Prozesses (``TZ`` + ``tzset``) und stellt sie
    danach wieder her: ein naives ``eta`` gilt als Ortszeit."""
    previous = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    try:
        yield request.param
    finally:
        if previous is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = previous
        time.tzset()


async def test_naive_datetime_max_eta_names_the_field(
    local_zone: str,
    bound_client: Client,
    broker: AsyncMock,
    backend: AsyncMock,
    store: MagicMock,
    recorder: _Recorder,
) -> None:
    """Ein naives ``datetime.max`` liegt in jeder Ortszeit ausserhalb der
    Grenze. Oestlich von UTC scheitert ``timestamp()`` am Jahr 10000, in UTC
    nicht; die Meldung ist in beiden Faellen dieselbe und nennt ``eta``
    (CI-Lauf 2962709369 lief in UTC, lokal in Europe/Berlin)."""
    with pytest.raises(ValueError, match=rf"^eta must be at most {MAX_DEADLINE_S} seconds in the future, got "):
        await bound_client.schedule("bound.task", 1, queue="bound", eta=datetime.max, unique_for=60)  # noqa: DTZ901 - naive Zeit ist der geprüfte Fall

    _assert_no_side_effect(broker, backend, store, recorder)
