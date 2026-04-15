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
        return min(base_delay * (2 ** attempt) * jitter, max_delay)

    if strategy == "linear":
        jitter = random.uniform(1.0, 2.0)
        return min(base_delay * (attempt + 1) * jitter, max_delay)

    raise ValueError(f"Unknown backoff strategy: {strategy!r}")
