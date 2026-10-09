"""Nachweis BZ-26: Retry-Jitter verschwindet am Deckel, ``fixed`` ohne Jitter,
``OverflowError`` bei sehr vielen Versuchen.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-26).

Testliste:
# [x] exponentiell am Deckel: unterschiedliche Zufallswerte ergeben
#     unterschiedliche Wartezeiten (sonst kommen alle Retries gleichzeitig)
# [x] linear am Deckel: dasselbe
# [x] fixed: Zufallswerte streuen die Wartezeit (Produktfrage, s. Bericht)
# [x] sehr hoher Versuch (2000) wirft nicht, sondern bleibt beim Deckel
"""

import pytest

from swarmq import backoff
from swarmq.backoff import calculate_backoff

_REASON_CAP = (
    "BZ-26: Jitter wird vor dem Deckel angewendet, am Deckel liefern alle "
    "Retries exakt max_delay (synchroner Retry-Sturm)"
)


def _backoff_with_uniform(monkeypatch: pytest.MonkeyPatch, value: float, **kwargs: float | int | str) -> float:
    """Wartezeit mit fester Zufallsquelle (Grenze ``random.uniform`` gemockt)."""
    monkeypatch.setattr(backoff.random, "uniform", lambda _lo, _hi: value)
    return calculate_backoff(**kwargs)  # type: ignore[arg-type]


@pytest.mark.befund("BZ-26")
@pytest.mark.xfail(strict=True, reason=_REASON_CAP)
def test_exponential_backoff_keeps_jitter_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    args = {"attempt": 20, "strategy": "exponential", "base_delay": 1.0, "max_delay": 3600.0}

    low = _backoff_with_uniform(monkeypatch, 1.0, **args)
    high = _backoff_with_uniform(monkeypatch, 2.0, **args)

    assert low != high, f"am Deckel kein Jitter: beide Wartezeiten {low}"


@pytest.mark.befund("BZ-26")
@pytest.mark.xfail(strict=True, reason=_REASON_CAP)
def test_linear_backoff_keeps_jitter_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    args = {"attempt": 5000, "strategy": "linear", "base_delay": 1.0, "max_delay": 3600.0}

    low = _backoff_with_uniform(monkeypatch, 1.0, **args)
    high = _backoff_with_uniform(monkeypatch, 2.0, **args)

    assert low != high, f"am Deckel kein Jitter: beide Wartezeiten {low}"


@pytest.mark.befund("BZ-26")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-26: Strategie 'fixed' hat keinen Jitter, gleichzeitig gescheiterte Tasks kommen gleichzeitig zurueck",
)
def test_fixed_backoff_is_jittered(monkeypatch: pytest.MonkeyPatch) -> None:
    args = {"attempt": 3, "strategy": "fixed", "base_delay": 30.0, "max_delay": 3600.0}

    low = _backoff_with_uniform(monkeypatch, 0.8, **args)
    high = _backoff_with_uniform(monkeypatch, 1.2, **args)

    assert low != high, f"fixed ohne Jitter: beide Wartezeiten {low}"


@pytest.mark.befund("BZ-26")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-26: base*2**attempt ohne Exponent-Deckel wirft ab ~1024 Versuchen OverflowError",
)
def test_exponential_backoff_with_huge_attempt_stays_at_cap() -> None:
    delay = calculate_backoff(attempt=2000, strategy="exponential", base_delay=1.0, max_delay=3600.0)

    assert 0 < delay <= 2 * 3600.0
