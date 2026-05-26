"""Rate limiting primitives for SwarmQ.

Globally-enforced sliding window rate limiter backed by a Lua script in
Valkey. The script itself lives in ``src/swarmq/lua/sliding_window_counter.lua``
and is atomic; this module provides the Python surface (string parser,
config NamedTuple, and async ``check``).

Phase 3b.2 (lock / semaphore / unique) and 3b.3 (cancel) will add
sibling classes using the same ``load_script`` + ``register_script`` path.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import valkey.asyncio as valkey_asyncio

from swarmq.lua import load_script


class RateLimitConfig(NamedTuple):
    """Parsed form of a ``rate_limit`` string."""

    max_requests: int
    window_size_ms: int


_UNIT_TO_MS = {
    "s": 1_000,
    "m": 60_000,
    "min": 60_000,
    "h": 3_600_000,
    "hour": 3_600_000,
}

_RATE_LIMIT_PATTERN = re.compile(r"^\s*(-?\d+)\s*/\s*([a-zA-Z]+)\s*$")


def parse_rate_limit(raw: str) -> RateLimitConfig:
    """Parse a Celery-style rate-limit string into a config.

    Accepts ``"100/m"``, ``"100 / m"``, ``"10/s"``, ``"1000/h"``,
    ``"100/min"``, ``"100/hour"``. Units match ``s``, ``m``, ``min``,
    ``h``, ``hour``.

    Raises ``ValueError`` with an explanatory message for:
      - missing unit separator (``"100"``)
      - unsupported unit (``"100/day"``)
      - non-numeric count (``"abc/m"``)
      - count < 1 (``"0/m"``, ``"-5/m"``)
    """
    if "/" not in raw:
        raise ValueError(
            f"missing unit separator in rate limit {raw!r} — "
            "use <count>/<unit>, e.g. '100/m'"
        )
    match = _RATE_LIMIT_PATTERN.match(raw)
    if match is None:
        raise ValueError(
            f"invalid rate limit format {raw!r} — "
            "expected <int>/<unit>, e.g. '100/m'"
        )
    count_str, unit_str = match.groups()
    try:
        count = int(count_str)
    except ValueError as exc:
        raise ValueError(f"invalid count in rate limit {raw!r}") from exc
    if count < 1:
        raise ValueError(
            f"count must be >= 1 in rate limit {raw!r} (got {count})"
        )
    unit = unit_str.lower()
    if unit not in _UNIT_TO_MS:
        supported = ", ".join(sorted(set(_UNIT_TO_MS)))
        raise ValueError(
            f"unsupported unit {unit_str!r} in rate limit {raw!r} — "
            f"supported: {supported}"
        )
    return RateLimitConfig(max_requests=count, window_size_ms=_UNIT_TO_MS[unit])


class RateLimiter:
    """Async wrapper around ``sliding_window_counter.lua``.

    One instance per worker. Holds a registered ``AsyncScript`` so the
    NOSCRIPT-recovery loop inside ``valkey.asyncio.AsyncScript`` is the
    path — we do not reimplement it.
    """

    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._script = client.register_script(load_script("sliding_window_counter"))

    async def check(
        self,
        task_name: str,
        config: RateLimitConfig,
    ) -> tuple[bool, int, int]:
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
        keys = [
            f"sq:rl:{task_name}:{window_ts}",
            f"sq:rl:{task_name}:{prev_ts}",
        ]
        result = await self._script(
            keys=keys,
            args=[config.max_requests, config.window_size_ms, now_ms],
        )
        allowed_raw, remaining_raw, retry_raw = result
        return bool(int(allowed_raw)), int(remaining_raw), int(retry_raw)
