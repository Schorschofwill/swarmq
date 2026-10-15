"""SwarmQ backoff calculation."""

import random
from collections.abc import Callable

# Ab hier liegt ``base * 2**n`` fuer jede sinnvolle Basis ueber jedem Deckel;
# die Grenze haelt ``2**n`` aus dem Float-Ueberlauf.
_MAX_DEFER_EXPONENT = 64


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
        max_delay: Maximum delay cap in seconds. ``fixed`` waits exactly
            ``base_delay``, at most ``max_delay``, without jitter (E-023);
            ``exponential`` and ``linear`` above the cap wait
            0.6 to 0.8 x ``max_delay``.

    Returns:
        Delay in seconds.
    """
    if strategy == "fixed":
        return float(min(base_delay, max_delay))

    if strategy == "exponential":
        jitter = random.uniform(1.0, 2.0)  # noqa: S311 - Jitter, keine Kryptografie
        return _jittered_cap(base_delay * (2 ** min(attempt, _MAX_DEFER_EXPONENT)) * jitter, jitter, max_delay)

    if strategy == "linear":
        jitter = random.uniform(1.0, 2.0)  # noqa: S311 - Jitter, keine Kryptografie
        return _jittered_cap(base_delay * (attempt + 1) * jitter, jitter, max_delay)

    raise ValueError(f"Unknown backoff strategy: {strategy!r}")


def _jittered_cap(delay: float, jitter: float, max_delay: float) -> float:
    """Deckel mit Jitter: oberhalb von ``max_delay`` gilt
    ``max_delay * (0.6 + 0.2 u)`` mit ``u = jitter - 1`` aus derselben
    Ziehung -- sonst kaemen alle gedeckelten Retries gleichzeitig wieder."""
    if delay <= max_delay:
        return float(delay)
    return float(max_delay * (0.6 + 0.2 * (jitter - 1.0)))


# Multiplikativer Jitter der Zurueckstellung: +-20 %. Die Untergrenze ist
# oeffentlich: wer die tatsaechlich verstrichene Wartezeit nach unten
# abschaetzt (Schedfire-Deckel), multipliziert die nominelle Summe damit.
MIN_DEFER_JITTER = 0.8
_DEFER_JITTER = (MIN_DEFER_JITTER, 1.2)


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


def nominal_defer_total(attempts: int, base: float, cap: float) -> float:
    """Summe der nominellen Wartezeiten (ohne Jitter) der ersten
    ``attempts`` Zurueckstellungen, wie ``backend_defer_delay``."""
    total, delay = 0.0, base
    for done in range(attempts):
        if delay >= cap:
            # Ab dem Deckel waechst die Summe linear (kein Ueberlauf bei
            # tausenden Zurueckstellungen).
            return total + cap * (attempts - done)
        total += delay
        delay *= 2
    return total
