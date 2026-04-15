"""Tests for swarmq.backoff — backoff calculation."""

from swarmq.backoff import calculate_backoff


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
