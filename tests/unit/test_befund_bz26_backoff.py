"""Nachweis BZ-26: Retry-Jitter verschwindet am Deckel, ``OverflowError`` bei
sehr vielen Versuchen.

``fixed`` bleibt bewusst ohne Jitter (Entscheidung 2026-10-09, s. Register).

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-26).

Testliste:
# [x] exponentiell am Deckel: unterschiedliche Zufallswerte ergeben
#     unterschiedliche Wartezeiten (sonst kommen alle Retries gleichzeitig)
# [x] linear am Deckel: dasselbe
# [x] sehr hoher Versuch (2000) wirft nicht, sondern bleibt beim Deckel
"""

from typing import TypedDict, Unpack

import pytest

from swarmq.backoff import calculate_backoff


class _BackoffArgs(TypedDict):
    attempt: int
    strategy: str
    base_delay: float
    max_delay: float


_REASON_CAP = (
    "BZ-26: Jitter wird vor dem Deckel angewendet, am Deckel liefern alle "
    "Retries exakt max_delay (synchroner Retry-Sturm)"
)


def _backoff_with_uniform(
    monkeypatch: pytest.MonkeyPatch,
    value: float,
    **kwargs: Unpack[_BackoffArgs],
) -> float:
    """Wartezeit mit fester Zufallsquelle (Grenze ``random.uniform`` gemockt)."""
    monkeypatch.setattr("swarmq.backoff.random.uniform", lambda _lo, _hi: value)
    return calculate_backoff(**kwargs)


@pytest.mark.befund("BZ-26")
def test_exponential_backoff_keeps_jitter_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    args: _BackoffArgs = {"attempt": 20, "strategy": "exponential", "base_delay": 1.0, "max_delay": 3600.0}

    low = _backoff_with_uniform(monkeypatch, 1.0, **args)
    high = _backoff_with_uniform(monkeypatch, 2.0, **args)

    assert low != high, f"am Deckel kein Jitter: beide Wartezeiten {low}"


@pytest.mark.befund("BZ-26")
def test_linear_backoff_keeps_jitter_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    args: _BackoffArgs = {"attempt": 5000, "strategy": "linear", "base_delay": 1.0, "max_delay": 3600.0}

    low = _backoff_with_uniform(monkeypatch, 1.0, **args)
    high = _backoff_with_uniform(monkeypatch, 2.0, **args)

    assert low != high, f"am Deckel kein Jitter: beide Wartezeiten {low}"


@pytest.mark.befund("BZ-26")
def test_exponential_backoff_with_huge_attempt_stays_at_cap() -> None:
    delay = calculate_backoff(attempt=2000, strategy="exponential", base_delay=1.0, max_delay=3600.0)

    assert 0 < delay <= 2 * 3600.0
