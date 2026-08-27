"""Valkey-Rate-Limit (Plan U18, KTD4): ``sliding_window_counter.lua``.

``RateLimiter`` ist die heutige Klasse (verbatim aus ``swarmq.limits``,
Importpfad bleibt als Alias gültig bis U15). ``ValkeyRateLimitStore`` erfüllt
den Vertrag ``RateLimitStore`` (``RateLimitDecision``) über demselben Skript.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import valkey.asyncio as valkey_asyncio

from swarmq.contracts import RateLimitDecision
from swarmq.limits import RateLimitConfig
from swarmq.lua import load_script


class RateLimiter:
    """Async wrapper around ``sliding_window_counter.lua``.

    One instance per worker. Holds a registered ``AsyncScript`` so the
    NOSCRIPT-recovery loop inside ``valkey.asyncio.AsyncScript`` is the
    path — we do not reimplement it.
    """

    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._script = client.register_script(load_script("sliding_window_counter"))

    async def check(self, task_name: str, config: RateLimitConfig) -> tuple[bool, int, int]:
        """Return ``(allowed, remaining, retry_after_ms)`` atomically.

        Pulls server-side ``TIME`` once so all derived key windows and
        the script see the same ``now_ms``. Two round-trips
        (``TIME`` + ``EVALSHA``) is acceptable; reading TIME inside the
        script would break the pattern documented in
        ``docs/recherche-deepening-lua-scripts.md`` §3.1 and complicates
        future Valkey-replication semantics.
        """
        seconds, microseconds = await self._client.time()
        now_ms = int(seconds) * 1000 + int(microseconds) // 1000
        window_ts = now_ms - (now_ms % config.window_size_ms)
        prev_ts = window_ts - config.window_size_ms
        # The {task_name} hash tag keeps current +
        # previous window in one Valkey-Cluster hash slot —
        # sliding_window_counter.lua reads both in one EVAL and would
        # otherwise fail with CROSSSLOT on a sharded cluster.
        keys = [
            f"sq:rl:{{{task_name}}}:{window_ts}",
            f"sq:rl:{{{task_name}}}:{prev_ts}",
        ]
        result = await self._script(keys=keys, args=[config.max_requests, config.window_size_ms, now_ms])
        allowed_raw, remaining_raw, retry_raw = result
        return bool(int(allowed_raw)), int(remaining_raw), int(retry_raw)


class ValkeyRateLimitStore:
    """``RateLimitStore``-Vertrag über ``RateLimiter``."""

    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._limiter = RateLimiter(client)

    @property
    def limiter(self) -> RateLimiter:
        """Die heutige API (bis U7 vom Worker genutzt)."""
        return self._limiter

    async def check(self, name: str, *, max_requests: int, window_ms: int) -> RateLimitDecision:
        allowed, remaining, retry_after_ms = await self._limiter.check(
            name, RateLimitConfig(max_requests=max_requests, window_size_ms=window_ms)
        )
        return RateLimitDecision(allowed=allowed, remaining=remaining, retry_after_ms=retry_after_ms)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import RateLimitStore

    _conforms_rate_limit: type[RateLimitStore] = ValkeyRateLimitStore
