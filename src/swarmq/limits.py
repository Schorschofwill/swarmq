"""Rate limiting primitives for SwarmQ.

Globally-enforced sliding window rate limiter backed by a Lua script in
Valkey. The script itself lives in ``src/swarmq/lua/sliding_window_counter.lua``
and is atomic; this module provides the Python surface (string parser,
config NamedTuple, and async ``check``).

Seit Plan U18 lebt ``RateLimiter`` in ``swarmq.backend.valkey.rate_limit``;
dieses Modul hält Parser und Konfiguration (rein) und den Import-Alias
``swarmq.limits.RateLimiter`` (KTD10, entfällt in U15).
"""

from __future__ import annotations

import re
from typing import Any, NamedTuple


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


def __getattr__(name: str) -> Any:
    """Import-Alias (KTD10, bis U15): ``swarmq.limits.RateLimiter`` ist
    ``swarmq.backend.valkey.rate_limit.RateLimiter``."""
    if name == "RateLimiter":
        from swarmq.backend.valkey.rate_limit import RateLimiter

        return RateLimiter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
