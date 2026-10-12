"""Auto-Pipelining fuer Lua-Aufrufe (Plan 2026-10-08-1541, U8, Durchsatz).

Der Ausfuehrungsschutz ruft je Zustellung zwei Skripte auf (Claim und
Abschluss). Ein einzelner Aufruf ueber valkey-py kostet vor allem
Python-Zeit (Kommando packen, Verbindung holen, Antwort lesen mit eigenem
Timeout), nicht Netzwerk. Diese Klasse sammelt die Aufrufe eines
Event-Loop-Takts und schickt sie als EIN Schreibvorgang auf EINER Verbindung
(``EVALSHA`` je Aufruf); waehrend ein Stapel unterwegs ist, sammelt sich der
naechste.

Jeder Aufruf bleibt ein eigenes, atomares Skript mit eigener Antwort: die
Semantik ist dieselbe wie bei Einzelaufrufen, nur die Roundtrips werden
geteilt. Ein Stapel ist keine Transaktion. Bausteine sind dieselben
Verbindungs-APIs, die valkey-py-Pipelines nutzen (``get_connection``,
``send_packed_command``, ``read_response``, ``release``, Retry-Politik der
Verbindung); Argumente werden UTF-8-kodiert wie vom Client-Encoder.

Messung (No-op-Worker, concurrency 50): der Mehraufwand des Schutzes je
Zustellung sinkt gegenueber Einzelaufrufen etwa auf die Haelfte.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import NoScriptError, ResponseError


@dataclass(slots=True)
class _Call:
    script: Any
    keys: Sequence[str]
    args: Sequence[str | int]
    future: asyncio.Future[Any]


def _pack(batch: Sequence[_Call]) -> bytes:
    """RESP-Arrays (``EVALSHA sha numkeys keys... args...``) eines Stapels."""
    out: list[bytes] = []
    for call in batch:
        argv = ("EVALSHA", call.script.sha, len(call.keys), *call.keys, *call.args)
        out.append(b"*%d\r\n" % len(argv))
        for arg in argv:
            data = arg if isinstance(arg, bytes) else str(arg).encode()
            out.append(b"$%d\r\n%b\r\n" % (len(data), data))
    return b"".join(out)


class ScriptPipeline:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._pending: list[_Call] = []
        self._drainer: asyncio.Task[None] | None = None

    async def call(self, script: Any, keys: Sequence[str], args: Sequence[str | int]) -> Any:
        """Fuehrt *script* mit *keys*/*args* aus; geteilter Roundtrip."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending.append(_Call(script, keys, args, future))
        if self._drainer is None:
            self._drainer = loop.create_task(self._drain())
        return await future

    async def _drain(self) -> None:
        batch: list[_Call] = []
        try:
            while self._pending:
                # Abgebrochene Aufrufe (Aufrufer hat aufgegeben, z. B.
                # ``backend_op_timeout``) gehen nicht mehr hinaus.
                batch = [call for call in self._pending if not call.future.done()]
                self._pending = []
                if batch:
                    await self._send(batch)
        except BaseException:
            # Abbruch des Sammlers (Loop-Ende): kein Wartender darf haengen.
            # Ein Ausfall -- die Aufrufer behandeln ihn wie jeden anderen.
            _fail([*batch, *self._pending], ValkeyConnectionError("script pipeline aborted"))
            self._pending = []
            raise
        finally:
            self._drainer = None

    async def _send(self, batch: list[_Call]) -> None:
        pool = self._client.connection_pool
        try:
            connection = await pool.get_connection("EVALSHA")
        except Exception as exc:  # noqa: BLE001 - Fehler geht an die wartenden Futures
            _fail(batch, exc)
            return
        try:
            # Wie valkey-py-Pipelines: Transportfehler wiederholt die
            # Retry-Politik der Verbindung mit dem ganzen Stapel.
            replies = await connection.retry.call_with_retry(
                lambda: self._exchange(connection, batch),
                _ignore_failure,
            )
        except Exception as exc:  # noqa: BLE001 - Fehler geht an die wartenden Futures
            _fail(batch, exc)
            return
        finally:
            await pool.release(connection)
        for call, reply in zip(batch, replies, strict=True):
            if isinstance(reply, NoScriptError) and not call.future.done():
                # Skript-Cache geleert (SCRIPT FLUSH, Neustart, Failover):
                # selten -- einzeln ueber das Skript, das sich nachlaedt.
                await self._direct(call)
            else:
                _settle(call.future, reply)

    async def _direct(self, call: _Call) -> None:
        try:
            reply = await call.script(keys=call.keys, args=call.args, client=self._client)
        except Exception as exc:  # noqa: BLE001 - Fehler geht an das wartende Future
            reply = exc
        _settle(call.future, reply)

    @staticmethod
    async def _exchange(connection: Any, batch: list[_Call]) -> list[Any]:
        try:
            await connection.send_packed_command(_pack(batch))
            replies: list[Any] = []
            for _ in batch:
                try:
                    replies.append(await connection.read_response())
                except ResponseError as exc:
                    # Die Fehlerantwort ist vollstaendig gelesen: die
                    # Verbindung bleibt synchron, nur dieser Aufruf scheitert.
                    replies.append(exc)
        except BaseException:
            # Protokollzustand unbekannt (Transportfehler, Abbruch mitten im
            # Lesen): die Verbindung verwerfen.
            await connection.disconnect(nowait=True)
            raise
        else:
            return replies


def _settle(future: asyncio.Future[Any], reply: Any) -> None:
    """Antwort oder Fehler zustellen -- ausser der Aufrufer hat schon
    aufgegeben (abgebrochen, z. B. ``backend_op_timeout``)."""
    if future.done():
        return
    if isinstance(reply, BaseException):
        future.set_exception(reply)
    else:
        future.set_result(reply)


def _fail(batch: list[_Call], exc: BaseException) -> None:
    for call in batch:
        _settle(call.future, exc)


async def _ignore_failure(error: BaseException) -> None:
    """``_exchange`` hat die Verbindung schon verworfen."""
