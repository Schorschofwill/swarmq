"""Valkey-Ausfuehrungsschutz (Plan 2026-10-08-1541, U8, KTD5).

Zwei Keys je Ausfuehrungs-Identitaet mit demselben Hash-Tag (ein Slot im
Cluster): der Running-Marker (String, Owner-Token, kurze TTL) und der
Zustand (Hash mit ``done``, ``sup``, ``orphans``, ``lease``; lange TTL).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.contracts import GuardClaim
from swarmq.contracts.coordination import GuardStatus
from swarmq.exceptions import BackendConnectionError
from swarmq.lua import load_script


def run_key(identity: str) -> str:
    return f"sq:guard:{{{identity}}}:run"


def state_key(identity: str) -> str:
    return f"sq:guard:{{{identity}}}:state"


def _keys(identity: str) -> list[str]:
    return [run_key(identity), state_key(identity)]


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class ValkeyExecutionGuardStore:
    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._claim_script = client.register_script(load_script("guard_claim"))
        self._finish_script = client.register_script(load_script("guard_finish"))
        # Compare-and-PEXPIRE auf dem Running-Marker: dieselbe Logik wie die
        # Verlaengerung eines Mutex-Locks (GET == Token -> PEXPIRE).
        self._renew_script = client.register_script(load_script("lock_renew"))

    async def _run(self, what: str, call: Any) -> Any:
        try:
            return await call
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"execution guard {what} failed: {exc}", exc) from exc

    async def claim(
        self, identity: str, *, owner: str, retry_count: int, ttl_ms: int, state_ttl_ms: int, max_orphans: int,
    ) -> GuardClaim:
        args: list[str | int] = [owner, ttl_ms, retry_count, state_ttl_ms, max_orphans]
        status, orphans = await self._run("claim", self._claim_script(keys=_keys(identity), args=args))
        return GuardClaim(cast("GuardStatus", _decode(status)), int(orphans))


    async def renew(self, identity: str, *, owner: str, ttl_ms: int) -> bool:
        result = await self._run("renew", self._renew_script(keys=[run_key(identity)], args=[owner, ttl_ms]))
        return int(result) == 1

    async def mark_done(self, identity: str, *, owner: str, state_ttl_ms: int) -> None:
        await self._run("finish", self._finish_script(keys=_keys(identity), args=[owner, "done", state_ttl_ms]))

    async def mark_superseded(self, identity: str, *, owner: str, retry_count: int, state_ttl_ms: int) -> None:
        await self._run(
            "finish",
            self._finish_script(keys=_keys(identity), args=[owner, "superseded", state_ttl_ms, retry_count]),
        )


    async def release(self, identity: str, *, owner: str) -> None:
        await self._run("finish", self._finish_script(keys=_keys(identity), args=[owner, "release", 0]))


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ExecutionGuardStore

    _conforms_guard: type[ExecutionGuardStore] = ValkeyExecutionGuardStore
