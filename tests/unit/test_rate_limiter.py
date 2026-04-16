"""Unit tests for ``parse_rate_limit``.

Pure-Python: no Valkey, no Docker. Covers every branch in the parser.
The ``RateLimiter.check`` side is tested separately against a real
Valkey in ``tests/integration/test_rate_limiter_integration.py``.
"""

from __future__ import annotations

import pytest

from swarmq.limits import RateLimitConfig, parse_rate_limit


class TestHappyPath:
    def test_per_minute(self):
        assert parse_rate_limit("100/m") == RateLimitConfig(100, 60_000)

    def test_per_second(self):
        assert parse_rate_limit("10/s") == RateLimitConfig(10, 1_000)

    def test_per_hour(self):
        assert parse_rate_limit("1000/h") == RateLimitConfig(1_000, 3_600_000)

    def test_one_per_hour(self):
        assert parse_rate_limit("1/h") == RateLimitConfig(1, 3_600_000)


class TestTolerance:
    def test_leading_and_trailing_whitespace(self):
        assert parse_rate_limit(" 100 / m ") == RateLimitConfig(100, 60_000)

    def test_unit_alias_min(self):
        assert parse_rate_limit("100/min") == RateLimitConfig(100, 60_000)

    def test_unit_alias_hour(self):
        assert parse_rate_limit("100/hour") == RateLimitConfig(100, 3_600_000)

    def test_unit_is_case_insensitive(self):
        assert parse_rate_limit("100/M") == RateLimitConfig(100, 60_000)
        assert parse_rate_limit("100/HOUR") == RateLimitConfig(100, 3_600_000)


class TestErrors:
    def test_unsupported_unit(self):
        with pytest.raises(ValueError, match="unsupported unit"):
            parse_rate_limit("100/day")

    def test_non_integer_count(self):
        with pytest.raises(ValueError, match="invalid rate limit format"):
            parse_rate_limit("abc/m")

    def test_zero_count(self):
        with pytest.raises(ValueError, match="count must be >= 1"):
            parse_rate_limit("0/m")

    def test_negative_count(self):
        with pytest.raises(ValueError, match="count must be >= 1"):
            parse_rate_limit("-5/m")

    def test_missing_unit_separator(self):
        with pytest.raises(ValueError, match="missing unit separator"):
            parse_rate_limit("100")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="missing unit separator"):
            parse_rate_limit("")

    def test_only_slash(self):
        with pytest.raises(ValueError, match="invalid rate limit format"):
            parse_rate_limit("/")
