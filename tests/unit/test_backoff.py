"""Tests for swarmq.backoff — backoff calculation."""

import pytest

from swarmq.backoff import calculate_backoff

# Testliste backend_defer_delay (Zurueckstellung bei Backend-Ausfall):
# [x] erster Versuch -> Basiswartezeit
# [x] Verdopplung je Versuch
# [x] Obergrenze greift
# [x] Versuch < 1 wird wie der erste behandelt (kaputter Header)
# [x] sehr hoher Versuch laeuft nicht ueber (bleibt beim Deckel)
# [x] multiplikativer Jitter 0.8..1.2 (Zufallsquelle injizierbar), auch am
#     Deckel -- sonst kaemen gleichzeitig zurueckgestellte Nachrichten
#     gleichzeitig wieder (Wellen nach der Erholung des Backends)
# [x] ohne injizierte Quelle streut die Wartezeit im Bereich
#
# Testliste calculate_backoff am Deckel (BZ-26):
# [x] oberhalb des Deckels liegt die Wartezeit im Band 0.6..0.8 * max_delay,
#     gespeist aus der einen bestehenden Ziehung U(1, 2) (kein zweiter Zufall)


def _no_jitter(_lo: float, _hi: float) -> float:
    return 1.0


@pytest.mark.entscheidung("E-023")
def test_fixed_strategy_returns_base_delay():
    """Fixed strategy always returns base_delay, no jitter, no scaling."""
    result = calculate_backoff(attempt=0, strategy="fixed", base_delay=5.0, max_delay=3600.0)
    assert result == 5.0

    result = calculate_backoff(attempt=5, strategy="fixed", base_delay=5.0, max_delay=3600.0)
    assert result == 5.0

    result = calculate_backoff(attempt=100, strategy="fixed", base_delay=5.0, max_delay=3600.0)
    assert result == 5.0


def test_exponential_strategy_grows_with_attempt():
    """Exponential strategy: base * 2^attempt * jitter(1.0, 2.0)."""
    base = 1.0
    max_d = 3600.0

    # attempt=0: base * 2^0 * jitter = 1.0 * 1 * [1.0, 2.0] = [1.0, 2.0]
    result_0 = calculate_backoff(attempt=0, strategy="exponential", base_delay=base, max_delay=max_d)
    assert 1.0 <= result_0 <= 2.0

    # attempt=1: base * 2^1 * jitter = 1.0 * 2 * [1.0, 2.0] = [2.0, 4.0]
    result_1 = calculate_backoff(attempt=1, strategy="exponential", base_delay=base, max_delay=max_d)
    assert 2.0 <= result_1 <= 4.0

    # attempt=3: base * 2^3 * jitter = 1.0 * 8 * [1.0, 2.0] = [8.0, 16.0]
    result_3 = calculate_backoff(attempt=3, strategy="exponential", base_delay=base, max_delay=max_d)
    assert 8.0 <= result_3 <= 16.0


def test_linear_strategy_grows_linearly():
    """Linear strategy: base * (attempt + 1) * jitter(1.0, 2.0)."""
    base = 2.0
    max_d = 3600.0

    # attempt=0: 2 * 1 * [1.0, 2.0] = [2.0, 4.0]
    result_0 = calculate_backoff(attempt=0, strategy="linear", base_delay=base, max_delay=max_d)
    assert 2.0 <= result_0 <= 4.0

    # attempt=1: 2 * 2 * [1.0, 2.0] = [4.0, 8.0]
    result_1 = calculate_backoff(attempt=1, strategy="linear", base_delay=base, max_delay=max_d)
    assert 4.0 <= result_1 <= 8.0

    # attempt=4: 2 * 5 * [1.0, 2.0] = [10.0, 20.0]
    result_4 = calculate_backoff(attempt=4, strategy="linear", base_delay=base, max_delay=max_d)
    assert 10.0 <= result_4 <= 20.0


def test_max_delay_cap_is_respected():
    """All strategies must not exceed max_delay."""
    max_d = 10.0

    # Exponential with high attempt would exceed max_delay without cap
    # base * 2^20 * jitter = 1.0 * 1048576 * [1,2] => way over 10
    result = calculate_backoff(attempt=20, strategy="exponential", base_delay=1.0, max_delay=max_d)
    assert result <= max_d

    # Linear with high attempt
    result = calculate_backoff(attempt=100, strategy="linear", base_delay=1.0, max_delay=max_d)
    assert result <= max_d

    # Fixed is always base_delay, even if base > max_delay the formula is just base
    result = calculate_backoff(attempt=0, strategy="fixed", base_delay=5.0, max_delay=max_d)
    assert result == 5.0


@pytest.mark.parametrize("strategy", ["exponential", "linear"])
def test_backoff_above_the_cap_uses_the_jitter_band_from_a_single_draw(
    monkeypatch: pytest.MonkeyPatch, strategy: str
) -> None:

    drawn: list[tuple[float, float]] = []

    def _uniform(lo: float, hi: float) -> float:
        drawn.append((lo, hi))
        return values.pop(0)

    values = [1.0, 1.5, 2.0]
    monkeypatch.setattr("swarmq.backoff.random.uniform", _uniform)

    delays = [calculate_backoff(attempt=500, strategy=strategy, base_delay=1.0, max_delay=100.0) for _ in range(3)]

    assert delays == [pytest.approx(60.0), pytest.approx(70.0), pytest.approx(80.0)]
    assert drawn == [(1.0, 2.0)] * 3


def test_backend_defer_delay_first_attempt_is_base_delay():
    from swarmq.backoff import backend_defer_delay

    assert backend_defer_delay(1, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 1.0


def test_backend_defer_delay_doubles_per_attempt():
    from swarmq.backoff import backend_defer_delay

    assert backend_defer_delay(3, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 4.0


def test_backend_defer_delay_is_capped():
    from swarmq.backoff import backend_defer_delay

    assert backend_defer_delay(8, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 60.0


def test_backend_defer_delay_attempt_below_one_is_base_delay():
    from swarmq.backoff import backend_defer_delay

    assert backend_defer_delay(0, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 1.0
    assert backend_defer_delay(-5, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 1.0


def test_backend_defer_delay_huge_attempt_stays_at_cap():
    """Der Zaehler-Header waechst ueber eine lange Stoerung ohne Grenze;
    ``2 ** n`` darf dabei nicht in einen Float-Ueberlauf laufen."""
    from swarmq.backoff import backend_defer_delay

    assert backend_defer_delay(100_000, base_delay=1.0, max_delay=60.0, uniform=_no_jitter) == 60.0


def test_backend_defer_delay_applies_multiplicative_jitter_even_at_the_cap():
    from swarmq.backoff import backend_defer_delay

    drawn: list[tuple[float, float]] = []

    def _uniform(lo: float, hi: float) -> float:
        drawn.append((lo, hi))
        return hi

    assert backend_defer_delay(3, base_delay=1.0, max_delay=60.0, uniform=_uniform) == pytest.approx(4.8)
    assert backend_defer_delay(8, base_delay=1.0, max_delay=60.0, uniform=_uniform) == pytest.approx(72.0)
    assert drawn == [(0.8, 1.2), (0.8, 1.2)]
