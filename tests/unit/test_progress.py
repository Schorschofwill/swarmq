"""Unit tests for swarmq.progress (R3.7 Unit 1)."""

from __future__ import annotations

import pytest

from swarmq.progress import ProgressInfo

pytestmark = [pytest.mark.timeout(5)]


class TestProgressInfoFromHash:
    def test_empty_hash_returns_none(self):
        assert ProgressInfo.from_hash({}) is None

    def test_decodes_bytes_hash(self):
        raw = {
            b"current": b"42",
            b"total": b"100",
            b"percent": b"42.0",
            b"message": b"halfway",
            b"updated_at": b"2026-04-17T10:30:00Z",
            b"started_at": b"2026-04-17T10:29:00Z",
            b"eta_seconds": b"58",
        }
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.current == 42
        assert p.total == 100
        assert p.percent == 42.0
        assert p.message == "halfway"
        assert p.updated_at == "2026-04-17T10:30:00Z"
        assert p.started_at == "2026-04-17T10:29:00Z"
        assert p.eta_seconds == 58

    def test_decodes_str_hash(self):
        """Fixture-style dict with str keys/values also works."""
        raw = {
            "current": "1",
            "total": "10",
            "percent": "10.0",
            "message": "one of ten",
            "updated_at": "2026-04-17T00:00:00Z",
        }
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.current == 1
        assert p.total == 10
        assert p.percent == 10.0

    def test_missing_eta_is_none(self):
        raw = {b"current": b"5", b"total": b"10", b"percent": b"50.0"}
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.eta_seconds is None

    def test_empty_eta_string_is_none(self):
        """update_progress stores eta='' when current==0; decode as None."""
        raw = {
            b"current": b"0",
            b"total": b"10",
            b"percent": b"0.0",
            b"eta_seconds": b"",
        }
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.eta_seconds is None

    def test_missing_started_at_is_none(self):
        raw = {b"current": b"1", b"total": b"2", b"percent": b"50.0"}
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.started_at is None

    def test_missing_message_defaults_to_empty_string(self):
        raw = {b"current": b"1", b"total": b"2", b"percent": b"50.0"}
        p = ProgressInfo.from_hash(raw)
        assert p is not None
        assert p.message == ""

    def test_missing_core_fields_raise_valueerror(self):
        with pytest.raises(ValueError, match="missing required"):
            ProgressInfo.from_hash({b"message": b"just a note"})

    def test_non_integer_current_raises(self):
        raw = {b"current": b"not-int", b"total": b"10", b"percent": b"0.0"}
        with pytest.raises(ValueError, match="invalid literal for int"):
            ProgressInfo.from_hash(raw)

    def test_frozen_dataclass(self):
        p = ProgressInfo(
            current=1,
            total=2,
            percent=50.0,
            message="",
            updated_at="2026-04-17T00:00:00Z",
        )
        with pytest.raises((AttributeError, TypeError)):
            p.current = 99  # type: ignore[misc]


class TestFromHashNonMapping:
    """Fuzz-Fund (Härtung R2, erster CI-Lauf der external-publish-Fuzz-
    Suite): ein Pub/Sub-Payload, der als JSON-Skalar dekodiert, erreichte
    ``raw.items()`` → AttributeError statt des ValueError, den
    watch_progress' Drop-Contract fängt."""

    @pytest.mark.parametrize("payload", [5, 0, "x", ["a"], True, 3.14])
    def test_non_mapping_raises_value_error(self, payload):
        with pytest.raises(ValueError, match="must be a mapping"):
            ProgressInfo.from_hash(payload)

    def test_none_returns_none(self):
        assert ProgressInfo.from_hash(None) is None

    def test_empty_mapping_returns_none(self):
        assert ProgressInfo.from_hash({}) is None
