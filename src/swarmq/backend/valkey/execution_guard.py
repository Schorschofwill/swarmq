"""Valkey-Ausfuehrungsschutz (Plan 2026-10-08-1541, U8, KTD5).

Zwei Keys je Ausfuehrungs-Identitaet mit demselben Hash-Tag (ein Slot im
Cluster): der Running-Marker (String, Owner-Token, kurze TTL) und der
Zustand (Hash mit ``done``, ``sup``, ``orphans``, ``lease``; lange TTL).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.backend.valkey.script_pipeline import ScriptPipeline
from swarmq.contracts import GuardClaim
from swarmq.contracts.coordination import GuardStatus
from swarmq.exceptions import BackendConnectionError
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

# Nur ohne Namensraum (Review MR 15, P2-3): ``run_key``/``state_key`` und der
# Store-Default nennen die Keys ohne Praefix; das Backend reicht seinen
# KeySpace (``keys=``) durch.
_DEFAULT_KEYS = KeySpace()


def run_key(identity: str) -> str:
    return _DEFAULT_KEYS.guard_run(identity)


def state_key(identity: str) -> str:
    return _DEFAULT_KEYS.guard_state(identity)


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class ValkeyExecutionGuardStore:
    def __init__(self, client: valkey_asyncio.Valkey, *, keys: KeySpace | None = None) -> None:
        self._client = client
        self._keys = keys if keys is not None else _DEFAULT_KEYS
        self._claim_script = client.register_script(load_script("guard_claim"))
        self._finish_script = client.register_script(load_script("guard_finish"))
        # Compare-and-PEXPIRE auf dem Running-Marker: dieselbe Logik wie die
        # Verlaengerung eines Mutex-Locks (GET == Token -> PEXPIRE).
        self._renew_script = client.register_script(load_script("lock_renew"))
        # Claim und Abschluss laufen je Zustellung: nebenlaeufige Aufrufe
        # teilen sich Roundtrips (Durchsatz, Messung im Plan U8).
        self._pipeline = ScriptPipeline(client)

    def _guard_keys(self, identity: str) -> list[str]:
        return [self._keys.guard_run(identity), self._keys.guard_state(identity)]

    async def _run(self, what: str, call: Any) -> Any:
        try:
            return await call
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise as_backend_error(f"execution guard {what} failed: {exc}", exc) from exc

    async def claim(
        self,
        identity: str,
        *,
        owner: str,
        retry_count: int,
        ttl_ms: int,
        state_ttl_ms: int,
        max_orphans: int,
        backlog_ttl_ms: int | None = None,
    ) -> GuardClaim:
        args: list[str | int] = [owner, ttl_ms, retry_count, state_ttl_ms, max_orphans, backlog_ttl_ms or 0]
        keys = self._guard_keys(identity)
        status, orphans = await self._run("claim", self._pipeline.call(self._claim_script, keys, args))
        return GuardClaim(cast("GuardStatus", _decode(status)), int(orphans))

    async def renew(self, identity: str, *, owner: str, ttl_ms: int) -> bool:
        result = await self._run(
            "renew",
            self._pipeline.call(self._renew_script, [self._keys.guard_run(identity)], [owner, ttl_ms]),
        )
        return int(result) == 1

    async def mark_done(self, identity: str, *, owner: str, state_ttl_ms: int) -> None:
        await self._finish(identity, [owner, "done", state_ttl_ms])

    async def mark_superseded(self, identity: str, *, owner: str, retry_count: int, state_ttl_ms: int) -> None:
        await self._finish(identity, [owner, "superseded", state_ttl_ms, retry_count])

    async def release(self, identity: str, *, owner: str) -> None:
        await self._finish(identity, [owner, "release", 0])

    async def _finish(self, identity: str, args: list[str | int]) -> None:
        await self._run("finish", self._pipeline.call(self._finish_script, self._guard_keys(identity), args))


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ExecutionGuardStore

    _conforms_guard: type[ExecutionGuardStore] = ValkeyExecutionGuardStore
