"""``ScriptPipeline``: Auto-Pipelining der Lua-Aufrufe des
Ausfuehrungsschutzes (Plan 2026-10-08-1541, U8, Durchsatz) -- gegen eine
gefälschte Verbindung. Gegen echtes Valkey prüft das
``tests/integration/backend/test_valkey_script_pipeline.py``.

Test-Liste:
1. Ein einzelner Aufruf liefert die Antwort seines Skripts.
2. Nebenläufige Aufrufe desselben Takts gehen in EINEM Schreibvorgang
   hinaus; jeder bekommt seine eigene Antwort.
3. Eine Fehlerantwort scheitert nur den eigenen Aufruf.
4. Ein Transportfehler scheitert alle Aufrufe des Stapels und verwirft die
   Verbindung; der nächste Aufruf läuft wieder. Ebenso, wenn schon das
   Holen der Verbindung scheitert.
5. Ein vor dem Senden abgebrochener Aufruf wird nicht gesendet.
6. Kennt Valkey das Skript nicht (NOSCRIPT), läuft der Aufruf über das
   Skript selbst (es lädt sich nach); scheitert auch das, scheitert der
   Aufruf, und der Stapel läuft weiter.
7. Während ein Stapel unterwegs ist, sammeln sich neue Aufrufe zum nächsten
   Stapel.
8. Bricht der Austausch ab (Abbruch mitten im Lesen), wird die Verbindung
   verworfen und die Wartenden scheitern statt zu hängen.
9. Die Verbindung geht nach jedem Stapel an den Pool zurück.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import NoScriptError, ResponseError

from swarmq.backend.valkey.script_pipeline import ScriptPipeline
from tests.unit.backend._pipeline_fakes import FakeConnection, FakePool, FakeScript, Reply, echo, fake_client

pytestmark = pytest.mark.timeout(5)


def _pipeline(reply: Reply = echo) -> tuple[ScriptPipeline, FakeConnection, FakePool]:
    client, connection, pool = fake_client(reply)
    return ScriptPipeline(client), connection, pool


async def test_single_call_returns_the_script_reply() -> None:
    pipeline, connection, _ = _pipeline()
    script = FakeScript()

    assert await pipeline.call(script, ["k1", "k2"], ["owner", 60_000]) == b"owner"
    assert connection.sent == [[[b"EVALSHA", script.sha.encode(), b"2", b"k1", b"k2", b"owner", b"60000"]]]


async def test_concurrent_calls_share_one_write_and_get_their_own_replies() -> None:
    pipeline, connection, _ = _pipeline()
    script = FakeScript()

    replies = await asyncio.gather(*(pipeline.call(script, ["k"], [f"o{i}"]) for i in range(5)))

    assert replies == [b"o0", b"o1", b"o2", b"o3", b"o4"]
    assert len(connection.sent) == 1
    assert len(connection.sent[0]) == 5


async def test_connection_goes_back_to_the_pool_after_each_batch() -> None:
    pipeline, _, pool = _pipeline()

    await pipeline.call(FakeScript(), ["k"], ["o"])

    assert pool.in_use == 0


async def test_error_reply_fails_only_its_own_call() -> None:
    def reply(argv: list[bytes]) -> Any:
        return ResponseError("WRONGTYPE") if argv[-1] == b"bad" else argv[-1]

    pipeline, _, _ = _pipeline(reply)
    script = FakeScript()

    results = await asyncio.gather(
        pipeline.call(script, ["k"], ["good"]), pipeline.call(script, ["k"], ["bad"]),
        pipeline.call(script, ["k"], ["fine"]), return_exceptions=True,
    )

    assert results[0] == b"good"
    assert isinstance(results[1], ResponseError)
    assert results[2] == b"fine"


async def test_transport_error_fails_the_whole_batch_and_drops_the_connection() -> None:
    broken = [True]

    def reply(argv: list[bytes]) -> Any:
        return ValkeyConnectionError("reset") if broken[0] else argv[-1]

    pipeline, connection, pool = _pipeline(reply)
    script = FakeScript()

    results = await asyncio.gather(
        pipeline.call(script, ["k"], ["a"]), pipeline.call(script, ["k"], ["b"]), return_exceptions=True,
    )

    assert all(isinstance(result, ValkeyConnectionError) for result in results)
    assert connection.disconnects == 1
    assert pool.in_use == 0
    broken[0] = False
    assert await pipeline.call(script, ["k"], ["again"]) == b"again"


async def test_failing_to_get_a_connection_fails_the_batch() -> None:
    pipeline, _, pool = _pipeline()
    pool.connect_error = ValkeyConnectionError("refused")
    script = FakeScript()

    with pytest.raises(ValkeyConnectionError):
        await pipeline.call(script, ["k"], ["a"])

    pool.connect_error = None
    assert await pipeline.call(script, ["k"], ["b"]) == b"b"


async def test_call_cancelled_before_sending_is_not_sent() -> None:
    pipeline, connection, _ = _pipeline()
    script = FakeScript()
    doomed = asyncio.create_task(pipeline.call(script, ["k"], ["doomed"]))
    kept = asyncio.create_task(pipeline.call(script, ["k"], ["kept"]))
    await asyncio.sleep(0)  # ein Takt: beide Aufrufe stehen an, der Stapel ist noch nicht gesendet
    doomed.cancel()

    assert await kept == b"kept"
    assert [argv[-1] for argv in connection.sent[0]] == [b"kept"]
    assert doomed.cancelled()


async def test_call_cancelled_while_in_flight_does_not_disturb_the_others() -> None:
    pipeline, connection, _ = _pipeline()
    connection.read_gate = asyncio.Event()
    script = FakeScript()
    doomed = asyncio.create_task(pipeline.call(script, ["k"], ["doomed"]))
    kept = asyncio.create_task(pipeline.call(script, ["k"], ["kept"]))
    await connection.sent_event.wait()  # der Stapel ist unterwegs, das Lesen wartet am Gate
    doomed.cancel()
    connection.read_gate.set()

    assert await kept == b"kept"
    assert doomed.cancelled()
    assert await pipeline.call(script, ["k"], ["next"]) == b"next"


async def test_transport_error_after_a_call_was_cancelled_in_flight_fails_the_rest() -> None:
    pipeline, connection, _ = _pipeline(lambda argv: ValkeyConnectionError("reset"))
    connection.read_gate = asyncio.Event()
    script = FakeScript()
    doomed = asyncio.create_task(pipeline.call(script, ["k"], ["doomed"]))
    kept = asyncio.create_task(pipeline.call(script, ["k"], ["kept"]))
    await connection.sent_event.wait()
    doomed.cancel()
    connection.read_gate.set()

    with pytest.raises(ValkeyConnectionError):
        await kept
    assert doomed.cancelled()


async def test_unknown_script_falls_back_to_the_script_which_loads_itself() -> None:
    pipeline, _, _ = _pipeline(lambda argv: NoScriptError("NOSCRIPT No matching script."))
    script = FakeScript()

    assert await pipeline.call(script, ["k1"], ["o", 5]) == b"direct"
    assert script.direct_calls == [(["k1"], ["o", 5])]


async def test_failing_fallback_fails_only_its_call() -> None:
    def reply(argv: list[bytes]) -> Any:
        return NoScriptError("NOSCRIPT No matching script.") if argv[-1] == b"lost" else argv[-1]

    pipeline, _, _ = _pipeline(reply)
    script = FakeScript(direct_error=ValkeyConnectionError("reset"))

    results = await asyncio.gather(
        pipeline.call(script, ["k"], ["lost"]), pipeline.call(script, ["k"], ["fine"]), return_exceptions=True,
    )

    assert isinstance(results[0], ValkeyConnectionError)
    assert results[1] == b"fine"


async def test_calls_arriving_while_a_batch_is_in_flight_form_the_next_batch() -> None:
    pipeline, connection, _ = _pipeline()
    connection.read_gate = asyncio.Event()
    script = FakeScript()
    first = asyncio.create_task(pipeline.call(script, ["k"], ["first"]))
    await connection.sent_event.wait()
    later = [asyncio.create_task(pipeline.call(script, ["k"], [f"later{i}"])) for i in range(3)]
    await asyncio.sleep(0)  # ein Takt: die drei stehen an, der erste Stapel wartet am Gate
    connection.read_gate.set()

    assert await first == b"first"
    assert await asyncio.gather(*later) == [b"later0", b"later1", b"later2"]
    assert [len(batch) for batch in connection.sent] == [1, 3]


async def test_cancelled_exchange_drops_the_connection_and_fails_the_waiters() -> None:
    def reply(argv: list[bytes]) -> Any:
        return asyncio.CancelledError() if argv[-1] == b"in-flight" else argv[-1]

    pipeline, connection, pool = _pipeline(reply)
    connection.read_gate = asyncio.Event()
    script = FakeScript()
    in_flight = asyncio.create_task(pipeline.call(script, ["k"], ["in-flight"]))
    await connection.sent_event.wait()
    waiting = asyncio.create_task(pipeline.call(script, ["k"], ["waiting"]))
    await asyncio.sleep(0)  # ein Takt: der zweite Aufruf steht an
    connection.read_gate.set()

    with pytest.raises(ValkeyConnectionError):
        await in_flight
    with pytest.raises(ValkeyConnectionError):
        await waiting
    assert connection.disconnects == 1
    assert pool.in_use == 0
