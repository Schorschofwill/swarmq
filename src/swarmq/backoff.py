"""SwarmQ backoff calculation."""

import random


def calculate_backoff(
    attempt: int,
    strategy: str,
    base_delay: float,
    max_delay: float,
) -> float:
    """Calculate retry backoff delay.

    Args:
        attempt: Current retry attempt (0-based).
        strategy: One of "fixed", "exponential", "linear".
        base_delay: Base delay in seconds.
        max_delay: Maximum delay cap in seconds.

    Returns:
        Delay in seconds.
    """
    if strategy == "fixed":
        return base_delay

    if strategy == "exponential":
        jitter = random.uniform(1.0, 2.0)
        return float(min(base_delay * (2 ** attempt) * jitter, max_delay))

    if strategy == "linear":
        jitter = random.uniform(1.0, 2.0)
        return float(min(base_delay * (attempt + 1) * jitter, max_delay))

    raise ValueError(f"Unknown backoff strategy: {strategy!r}")


# Ab hier liegt ``base * 2**n`` fuer jede sinnvolle Basis ueber jedem Deckel;
# die Grenze haelt ``2**n`` aus dem Float-Ueberlauf.
_MAX_DEFER_EXPONENT = 64


def backend_defer_delay(attempt: int, base_delay: float, max_delay: float) -> float:
    """Wartezeit der ``attempt``-ten Zurueckstellung bei Backend-Ausfall.

    Exponentiell ohne Jitter (``base * 2**(attempt-1)``), gedeckelt auf
    ``max_delay``; ``attempt`` ist 1-basiert, kleinere Werte (kaputter Header)
    zaehlen als erster Versuch. Kein Retry: das Retry-Budget des Tasks bleibt
    unberuehrt.
    """
    exponent = min(max(attempt, 1) - 1, _MAX_DEFER_EXPONENT)
    return float(min(base_delay * 2**exponent, max_delay))
