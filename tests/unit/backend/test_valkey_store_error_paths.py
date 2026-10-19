"""Fehlerpfade der Koordinations-Stores an der Treibergrenze, einheitlich je
Store und Methode (F4-11, Testlücke Nr. 1 der Sichtung
``docs/befunde/fehlersuche-runde-4-recherche/3c-sichtung-D.md``).

Jede Methode ordnet einen Treiberfehler über ``as_backend_error`` ein:

- Ausfall (``BackendConnectionError``, nicht ``BackendResponseError``):
  Transportfehler, Zeitablauf, ``OOM`` als eigene Klasse und als
  ``ResponseError`` mit Präfix ``OOM`` (heilt durch Warten, E-005
  ``noeviction``). Das Gate stellt zurück.
- Fehlerantwort (``BackendResponseError``): ``WRONGTYPE``. Das Gate stellt bis
  zum Zustelllimit neu zu.

Bewusst nicht in der Matrix:

- ``ValkeyLockStore.current_owner``: propagiert Treiberfehler roh (Decision E4
  im Docstring: „kein Leader“ und „Backend unerreichbar“ bleiben
  unterscheidbar).

Grenze gemockt: die registrierten Skripte, ``DEL`` (bzw. die Pipeline-Verbindung
des Guard-Stores) werfen den Treiberfehler; ``TIME`` antwortet.

Testliste:
# [x] Lock-Store: acquire, release, renew, extend (Mutex und Semaphor), observe,
#     force_release (Plan 0.14, U1: ein ``DEL``)
# [x] Guard-Store: claim, renew, mark_done, mark_superseded, release
# [x] Rate-Limit-Store: check
# [x] Chord-Store: record_member, record_and_check_complete
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import OutOfMemoryError, ResponseError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from swarmq.backend.valkey.chords import ChordManager
from swarmq.backend.valkey.execution_guard import ValkeyExecutionGuardStore
from swarmq.backend.valkey.locks import ValkeyLockStore
from swarmq.backend.valkey.rate_limit import ValkeyRateLimitStore
from swarmq.contracts import LockToken
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from tests.unit.backend._pipeline_fakes import FakeScript, fake_client

pytestmark = pytest.mark.timeout(5)

_OOM_TEXT = "command not allowed when used memory > 'maxmemory'."

_OUTAGES: list[Any] = [
    pytest.param(lambda: ValkeyConnectionError("Error 111 connecting. Connection refused."), id="transport"),
    pytest.param(lambda: ValkeyTimeoutError("Timeout reading from socket"), id="timeout"),
    pytest.param(lambda: OutOfMemoryError(_OOM_TEXT), id="oom-class"),
    pytest.param(lambda: ResponseError(f"OOM {_OOM_TEXT}"), id="oom-prefix"),
]


def _wrongtype() -> BaseException:
    return ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")


Call = Callable[[BaseException], Awaitable[object]]


def _script_client(error: BaseException) -> MagicMock:
    """Client, dessen Skripte und Pipeline den Treiberfehler werfen; ``TIME`` antwortet."""
    client = MagicMock()
    client.time = AsyncMock(return_value=(1, 0))
    client.register_script = MagicMock(side_effect=lambda _source: AsyncMock(side_effect=error))
    client.delete = AsyncMock(side_effect=error)
    pipe = MagicMock()
    pipe.execute = AsyncMock(side_effect=error)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=pipe)
    context.__aexit__ = AsyncMock(return_value=False)
    client.pipeline = MagicMock(return_value=context)
    return client


def _locks(error: BaseException) -> ValkeyLockStore:
    return ValkeyLockStore(_script_client(error))


def _guard(error: BaseException) -> ValkeyExecutionGuardStore:
    client, _, _ = fake_client(lambda _argv: error)
    client.register_script = lambda source: FakeScript(sha=hashlib.sha1(source.encode()).hexdigest())
    return ValkeyExecutionGuardStore(client)


_MUTEX = LockToken(name="n", owner="o", max_concurrency=1)
_SEMAPHORE = LockToken(name="n", owner="o", max_concurrency=3)

_CALLS: list[Any] = [
    pytest.param(
        lambda e: _locks(e).acquire("n", owner="o", ttl_ms=1_000, max_concurrency=1),
        id="lock-acquire",
    ),
    pytest.param(lambda e: _locks(e).release(_MUTEX), id="lock-release"),
    pytest.param(lambda e: _locks(e).renew(_MUTEX, 1_000), id="lock-renew"),
    pytest.param(lambda e: _locks(e).extend(_MUTEX, 1_000), id="lock-extend-mutex"),
    pytest.param(lambda e: _locks(e).extend(_SEMAPHORE, 1_000), id="lock-extend-semaphore"),
    pytest.param(lambda e: _locks(e).observe("n", max_concurrency=1), id="lock-observe"),
    pytest.param(lambda e: _locks(e).force_release("n", max_concurrency=1), id="lock-force-release"),
    pytest.param(
        lambda e: _guard(e).claim("t-1", owner="o", retry_count=0, ttl_ms=1, state_ttl_ms=1, max_orphans=3),
        id="guard-claim",
    ),
    pytest.param(lambda e: _guard(e).renew("t-1", owner="o", ttl_ms=1), id="guard-renew"),
    pytest.param(lambda e: _guard(e).mark_done("t-1", owner="o", state_ttl_ms=1), id="guard-mark-done"),
    pytest.param(
        lambda e: _guard(e).mark_superseded("t-1", owner="o", retry_count=1, state_ttl_ms=1),
        id="guard-mark-superseded",
    ),
    pytest.param(lambda e: _guard(e).release("t-1", owner="o"), id="guard-release"),
    pytest.param(
        lambda e: ValkeyRateLimitStore(_script_client(e)).check("n", max_requests=1, window_ms=1_000),
        id="rate-limit-check",
    ),
    pytest.param(
        lambda e: ChordManager(_script_client(e)).record_member("wf", "/h", "m1", expected=2, ttl_ms=1_000),
        id="chord-record-member",
    ),
    pytest.param(
        lambda e: ChordManager(_script_client(e)).record_and_check_complete("wf", "/h", "m1", 2, 1_000),
        id="chord-record-and-check-complete",
    ),
]


async def _error_of(call: Awaitable[object]) -> BaseException | None:
    try:
        await call
    except Exception as exc:
        return exc
    return None


@pytest.mark.parametrize("call", _CALLS)
@pytest.mark.parametrize("make_error", _OUTAGES)
async def test_driver_outage_surfaces_as_backend_connection_error(
    call: Call, make_error: Callable[[], BaseException]
) -> None:
    cause = make_error()

    error = await _error_of(call(cause))

    assert isinstance(error, BackendConnectionError), f"{type(cause).__name__} kam als {error!r} heraus"
    assert not isinstance(error, BackendResponseError), "ein heilender Fehler ist ein Ausfall"
    assert error.__cause__ is cause


@pytest.mark.parametrize("call", _CALLS)
async def test_wrongtype_reply_surfaces_as_backend_response_error(call: Call) -> None:
    cause = _wrongtype()

    error = await _error_of(call(cause))

    assert isinstance(error, BackendResponseError), f"WRONGTYPE kam als {error!r} heraus"
    assert error.__cause__ is cause
