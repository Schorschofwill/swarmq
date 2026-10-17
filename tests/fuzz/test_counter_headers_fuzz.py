"""Fuzz: Leser der Zähler-Header (Fehlersuche Runde 4, U16, R20, KTD8).

Ziele (nur öffentliche Schnittstellen, AGENTS.md verbotene Aktion 6):

* ``execution_guard.retry_count_of`` direkt -- streng (E-051): nur ein
  echtes ``int`` > 0 zählt, alles andere 0.
* die nachsichtigen Leser ``gates.prior_deferrals``, ``gates.broker_redelivered``
  und ``redelivery.redeliver_pause_s`` (dieselbe Lesart wie der
  Rate-Limit-Zähler, E-051: ``int()``, nicht Lesbares und ``inf`` zählen 0,
  negative werden 0).
* ``DeliveryCountGate.apply`` mit konstruierten ``x-delivery-count`` /
  ``x-acquired-count``.
* ``LockGate.apply`` im Konfliktpfad mit konstruierten Zustands-Headern der
  Lock-Wartezeit (``x-swarmq-lock-*``) und ``lock.wait_generation``.
* ``identity.execution_identity`` (``x-death``, ``x-swarmq-xdeath-base``,
  Replay-ID).

Header-Werte folgen den AMQP-Feldtypen, die aio-pika/pamqp liefern können
(``pamqp.common.FieldValue``: ``bool``, ``bytes``, ``Decimal``, ``list``,
``dict``, ``float`` mit ``inf``/``nan``, ``int`` beliebiger Größe, ``None``,
``str``, ``datetime``), plus numerische Zeichenketten wie aus der
RabbitMQ-UI.

Allow-List: leer. E-051 sagt zu: "Ein Zähler-Header, der sich nicht als
Zähler lesen lässt, zählt wie kein Header; die Nachricht wird normal
weiterverarbeitet." Jede Ausnahme aus einem Leser oder Gate ist ein Treffer.

Treffer (Bericht docs/befunde/fehlersuche-runde-4-recherche/5-fuzz.md; IDs aus
dem Register ``docs/befunde/fehlersuche-runde-4-2026-10.md``):

* F4-07 / R4F-01 (FEHLER): ``DeliveryCountGate.apply`` wirft ``OverflowError`` für
  ``x-delivery-count``/``x-acquired-count`` = ``float("inf")`` (``_count``
  fängt nur ``TypeError``/``ValueError``). Ausgeklammert in ``_gate_counter``;
  Nachweis ``tests/unit/worker/gates/test_befund_f4_07_delivery_count_inf.py``.

Testliste:
# [x] retry_count_of streng (E-051), auch ohne Mapping-Eintrag und mit None
# [x] prior_deferrals / broker_redelivered / redeliver_pause_s nachsichtig, nie Ausnahme
# [x] DeliveryCountGate: Continue oder Terminal, nie Ausnahme (Treffer F4-07 ausgeklammert)
# [x] LockGate-Konflikt: Redelivered, Rückstellung mit gültiger Wartezeit oder DLQ, nie Ausnahme
# [x] wait_generation: "<n>:<replay>" mit n >= 0
# [x] execution_identity: nie Ausnahme, Zähler gedeckelt
"""

from __future__ import annotations

import asyncio
import math
import uuid
from collections.abc import Coroutine
from datetime import UTC
from typing import Any, cast, override
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import LockObservation
from swarmq.headers import (
    HEADER_BACKEND_DEFER_COUNT,
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_GEN,
    HEADER_LOCK_WAIT_SINCE,
    HEADER_REPLAY_ID,
    HEADER_RETRY_COUNT,
    HEADER_XDEATH_BASE,
)
from swarmq.identity import HEADER_X_DEATH, execution_identity
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext, broker_redelivered, prior_deferrals
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.gates.execution_guard import retry_count_of
from swarmq.worker.gates.lock import (
    LOCK_CONFLICT_BACKOFF_BASE_MS,
    LOCK_CONFLICT_BACKOFF_CAP_MS,
    LockGate,
    holder_fingerprint,
    wait_generation,
)
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal
from swarmq.worker.redelivery import REDELIVER_PAUSE_CAP_S, redeliver_pause_s

pytestmark = [pytest.mark.timeout(60)]

_HOLDER = "fuzz-holder"
_FINGERPRINT = holder_fingerprint(frozenset({_HOLDER}))

# Zahlen an den Grenzen: 0, ±1, Grenzen von int64/double, weit darüber.
_edge_ints = st.sampled_from([0, 1, -1, 2**31, 2**63 - 1, 2**63, 2**64, 2**70, -(2**70), 10**400])
_numeric_text = st.sampled_from(
    ["3", " 7 ", "-1", "0", "1_000", "١٢", "abc", "", "1e400", "inf", "-inf", "nan", "3.0", "9" * 5000]
)


def _amqp_scalars(*, finite_floats: bool = False) -> st.SearchStrategy[Any]:
    """AMQP-Feldwerte (pamqp ``FieldValue``) ohne Verschachtelung."""
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        _edge_ints,
        st.floats(allow_nan=not finite_floats, allow_infinity=not finite_floats),
        st.sampled_from([0.5, -0.5, 1e308] if finite_floats else [math.inf, -math.inf, math.nan, 1e308]),
        st.decimals(allow_nan=False, allow_infinity=False, places=3),
        st.text(max_size=16),
        _numeric_text,
        st.binary(max_size=8),
        st.just(b"3"),
        st.datetimes(timezones=st.just(UTC)),
    )


def _amqp_values(*, finite_floats: bool = False) -> st.SearchStrategy[Any]:
    scalars = _amqp_scalars(finite_floats=finite_floats)
    # Skalare überwiegen: Zähler-Header sind fast immer Skalare.
    return scalars | st.recursive(
        scalars,
        lambda inner: st.one_of(
            st.lists(inner, max_size=3),
            st.dictionaries(st.text(max_size=6), inner, max_size=3),
        ),
        max_leaves=6,
    )


_values = _amqp_values()


def _strict_oracle(raw: object) -> int:
    """E-051: nur ein echtes ``int`` > 0 zählt."""
    return raw if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0 else 0


def _lenient_oracle(raw: object) -> int:
    """E-051 (Rate-Limit-Lesart): ``int()``, Unlesbares und ``inf`` -> 0, negativ -> 0."""
    try:
        value = int(cast("Any", raw))  # Orakel prüft beliebige Header-Werte
    except TypeError, ValueError, OverflowError:
        return 0
    return max(value, 0)


# ---------------------------------------------------------------------------
# Retry-Zähler, streng gelesen
# ---------------------------------------------------------------------------


@given(raw=_values)
@example(raw="3")
@example(raw=True)
@example(raw=float("inf"))
@example(raw=-2)
@example(raw=2**70)
@settings(max_examples=500, deadline=None)
def test_retry_count_of_is_strict(raw: object) -> None:
    assert retry_count_of({HEADER_RETRY_COUNT: raw}) == _strict_oracle(raw)


@given(headers=st.one_of(st.none(), st.dictionaries(st.text(max_size=12), _values, max_size=4)))
@settings(max_examples=500, deadline=None)
def test_retry_count_of_without_the_header_is_zero(headers: dict[str, Any] | None) -> None:
    if headers is not None:
        headers.pop(HEADER_RETRY_COUNT, None)
    assert retry_count_of(headers) == 0


# ---------------------------------------------------------------------------
# nachsichtige Leser
# ---------------------------------------------------------------------------


@given(raw=_values)
@example(raw="4")
@example(raw=float("inf"))
@example(raw=float("nan"))
@settings(max_examples=500, deadline=None)
def test_prior_deferrals_reads_leniently(raw: object) -> None:
    assert prior_deferrals({HEADER_BACKEND_DEFER_COUNT: raw}) == _lenient_oracle(raw)


@given(delivered=_values, acquired=_values)
@example(delivered=float("inf"), acquired=None)
@settings(max_examples=500, deadline=None)
def test_broker_counters_are_read_leniently(delivered: object, acquired: object) -> None:
    headers = {"x-delivery-count": delivered, "x-acquired-count": acquired}

    redelivered = broker_redelivered(headers)
    pause = redeliver_pause_s(headers)

    assert redelivered == (max(_lenient_oracle(delivered), _lenient_oracle(acquired)) > 0)
    assert math.isfinite(pause)
    assert 0 < pause <= REDELIVER_PAUSE_CAP_S


# ---------------------------------------------------------------------------
# DeliveryCountGate (öffentliche Gate-Schnittstelle)
# ---------------------------------------------------------------------------

# F4-07 (R4F-01): ``float("inf")`` (auch ``-inf``) als x-delivery-count oder
# x-acquired-count wirft OverflowError aus DeliveryCountGate.apply. Bis zum
# Fix ausgeklammert: nur ``±inf`` (``nan`` ist kein Treffer, ``int(nan)`` wirft ValueError).
# Treffer (geschrumpft): @example(delivered=None, acquired=float("inf"), limit=0)
_gate_counter = _values.filter(lambda value: not (isinstance(value, float) and math.isinf(value)))


def _run(coro: Coroutine[Any, Any, DeliveryOutcome]) -> DeliveryOutcome:
    return asyncio.run(coro)


class _Delivery:
    def __init__(self, headers: Any, queue: str = "q") -> None:
        self.headers = headers
        self.queue = queue


@given(delivered=_gate_counter, acquired=_gate_counter, limit=st.integers(0, 50))
@example(delivered="19", acquired=None, limit=20)
@example(delivered=None, acquired=2**70, limit=20)
@settings(max_examples=500, deadline=None)
def test_delivery_count_gate_never_raises(delivered: object, acquired: object, limit: int) -> None:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", delivery_limit=max(limit, 1))
    deps.broker = MagicMock(spec=[])  # ohne effective_delivery_limit: Konfiguration gilt
    dead_letter = AsyncMock()
    gate = DeliveryCountGate(deps, dead_letter=dead_letter)
    ctx = GateContext(task_id="t", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))
    headers = {"x-delivery-count": delivered, "x-acquired-count": acquired}

    outcome = _run(gate.apply(_Delivery(headers), ctx))

    if outcome is Continue:
        dead_letter.assert_not_awaited()
    else:
        assert isinstance(outcome, Terminal)
        dead_letter.assert_awaited_once()


# ---------------------------------------------------------------------------
# LockGate im Konfliktpfad (öffentliche Gate-Schnittstelle)
# ---------------------------------------------------------------------------


class _LockedTask(Task):
    task_info = TaskInfo(name="fuzz_locked", queues=["q"], lock="resource", hard_timeout=60)

    @override
    async def run(self) -> None:
        raise AssertionError("darf nicht laufen")


_lock_state_keys = st.sampled_from(
    [
        HEADER_LOCK_CONFLICTS,
        HEADER_LOCK_WAIT_SINCE,
        HEADER_LOCK_HOLDER,
        HEADER_LOCK_HOLDER_SINCE,
        HEADER_LOCK_HOLDER_TTL_MS,
        HEADER_LOCK_WAIT_GEN,
        HEADER_RETRY_COUNT,
        HEADER_REPLAY_ID,
        HEADER_X_DEATH,
        HEADER_XDEATH_BASE,
        "x-delivery-count",
        "x-acquired-count",
    ]
)
_lock_headers = st.dictionaries(
    _lock_state_keys,
    st.one_of(_values, st.just(_FINGERPRINT), st.just("0:"), st.integers(0, 2_000_000_000_000)),
    max_size=8,
)


@given(headers=_lock_headers, pttl=st.sampled_from([-2, -1, 0, 1, 30_000, 2**62]))
@example(headers={HEADER_LOCK_CONFLICTS: float("inf"), HEADER_LOCK_WAIT_SINCE: "abc"}, pttl=30_000)
@example(headers={HEADER_LOCK_HOLDER: _FINGERPRINT, HEADER_LOCK_HOLDER_SINCE: 1, HEADER_LOCK_WAIT_GEN: "0:"}, pttl=1)
@settings(max_examples=500, deadline=None)
def test_lock_gate_conflict_never_raises(headers: dict[str, Any], pttl: int) -> None:
    store = MagicMock()
    store.acquire = AsyncMock(return_value=None)  # Konflikt
    store.observe = AsyncMock(return_value=LockObservation(owners=frozenset({_HOLDER}), pttl_ms=pttl))
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x")
    deps.backend = MagicMock(namespace=None)
    deps.broker.retry_later = AsyncMock()
    deps.signals.dispatch = AsyncMock()
    deps.lock_manager = lambda: store
    deps.detach = None
    deps.redeliver_pause = None
    deps.keep_lock = None
    dead_letter = AsyncMock()
    gate = LockGate(deps, dead_letter=dead_letter)
    ctx = GateContext(
        task_id="t",
        task_name="fuzz_locked",
        queue="q",
        task_info=_LockedTask.task_info,
        task_class=_LockedTask,
        handler_id=uuid.uuid4(),
    )

    outcome = _run(gate.apply(_Delivery(headers), ctx))

    assert outcome is Redelivered
    if deps.broker.retry_later.await_count:
        call = deps.broker.retry_later.await_args
        delay_ms = call.kwargs["delay_ms"]
        assert LOCK_CONFLICT_BACKOFF_BASE_MS <= delay_ms <= LOCK_CONFLICT_BACKOFF_CAP_MS
        copy = call.kwargs["headers"]
        assert isinstance(copy[HEADER_LOCK_CONFLICTS], int)
        assert copy[HEADER_LOCK_CONFLICTS] >= 1
    else:
        dead_letter.assert_awaited_once()


@given(headers=_lock_headers)
@settings(max_examples=500, deadline=None)
def test_wait_generation_is_a_count_and_the_replay_id(headers: dict[str, Any]) -> None:
    generation = wait_generation(headers)

    count, sep, _replay = generation.partition(":")
    assert sep
    assert count.isdigit()
    assert int(count) == _lenient_oracle(headers.get(HEADER_RETRY_COUNT))


# ---------------------------------------------------------------------------
# Identität: x-death, Base, Replay-ID
# ---------------------------------------------------------------------------

_xdeath_entry = st.one_of(
    _values,
    st.fixed_dictionaries(
        {"queue": st.one_of(st.text(max_size=12), _values), "count": st.one_of(_edge_ints, st.integers(), _values)}
    ),
)


@given(
    deaths=st.one_of(_values, st.lists(_xdeath_entry, max_size=4)),
    base=_values,
    replay=st.one_of(_values, st.text(min_size=60, max_size=70)),
)
@example(deaths=[{"queue": "q", "count": 10**400}], base=2**70, replay="r")
@settings(max_examples=500, deadline=None)
def test_execution_identity_never_raises(deaths: object, base: object, replay: object) -> None:
    identity = execution_identity({HEADER_X_DEATH: deaths, HEADER_XDEATH_BASE: base, HEADER_REPLAY_ID: replay}, "t")

    assert identity.startswith("t")
    # Deckel: n hat höchstens 12 Stellen, die Replay-ID höchstens 64 Zeichen.
    assert len(identity) <= 1 + 1 + 64 + 1 + 12
