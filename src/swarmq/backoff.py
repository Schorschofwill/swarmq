"""SwarmQ backoff calculation."""

import random
from collections.abc import Callable


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


# Multiplikativer Jitter der Zurueckstellung: +-20 %.
_DEFER_JITTER = (0.8, 1.2)


def backend_defer_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    *,
    uniform: Callable[[float, float], float] = random.uniform,
) -> float:
    """Wartezeit der ``attempt``-ten Zurueckstellung bei Backend-Ausfall.

    Exponentiell (``base * 2**(attempt-1)``), gedeckelt auf ``max_delay``,
    danach mit multiplikativem Jitter 0,8 ... 1,2 -- auch am Deckel, sonst
    kaemen gleichzeitig zurueckgestellte Nachrichten gleichzeitig wieder und
    trafen das sich erholende Backend in Wellen. ``attempt`` ist 1-basiert,
    kleinere Werte (kaputter Header) zaehlen als erster Versuch. ``uniform``
    ist die Zufallsquelle (Tests). Kein Retry: das Retry-Budget des Tasks
    bleibt unberuehrt.
    """
    exponent = min(max(attempt, 1) - 1, _MAX_DEFER_EXPONENT)
    return float(min(base_delay * 2**exponent, max_delay) * uniform(*_DEFER_JITTER))
