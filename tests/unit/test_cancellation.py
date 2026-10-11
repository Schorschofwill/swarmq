"""Tests for swarmq.cancellation — pure-helper module.

Covers the result-dict builders and the reason-discriminator constants
extracted from ``swarmq.worker`` in the Phase 3b.3 review-follow-up
Unit 6.
"""

from __future__ import annotations

import pytest

from swarmq.cancellation import (
    CANCEL_REASON_CANCEL,
    CANCEL_REASON_TIMEOUT,
    build_cancel_result_dict,
    build_error_result_dict,
    build_result_dict,
)

pytestmark = [pytest.mark.timeout(5)]


class TestReasonConstants:
    def test_cancel_constant_value(self):
        assert CANCEL_REASON_CANCEL == "cancel"

    def test_timeout_constant_value(self):
        assert CANCEL_REASON_TIMEOUT == "timeout"

    def test_literal_alias_exports_both(self):
        # CancelReason is a Literal — asserting by membership is the
        # idiomatic runtime check.
        for value in ("cancel", "timeout"):
            # Re-typing a Literal at runtime requires eval-of-annotation,
            # which is intentionally out of scope. We just verify the
            # exported constants match the Literal's string values.
            assert value in (CANCEL_REASON_CANCEL, CANCEL_REASON_TIMEOUT)


class TestBuildResultDict:
    def test_success_shape(self):
        d = build_result_dict(
            task_id="t1",
            queue="q",
            task_name="add",
            is_error=False,
            is_cancelled=False,
            result=42,
        )
        assert d == {
            "task": {"id": "t1", "queue": "q", "name": "add"},
            "is_error": False,
            "is_cancelled": False,
            "error_message": "",
            "result": 42,
        }

    def test_error_shape(self):
        d = build_result_dict(
            task_id="t1",
            queue="q",
            task_name="add",
            is_error=True,
            is_cancelled=False,
            error_message="boom",
        )
        assert d["is_error"] is True
        assert d["error_message"] == "boom"
        assert d["result"] is None

    def test_cancelled_shape(self):
        d = build_result_dict(
            task_id="t1",
            queue="q",
            task_name="add",
            is_error=False,
            is_cancelled=True,
        )
        assert d["is_cancelled"] is True
        assert d["is_error"] is False

    def test_all_keys_always_present(self):
        """Client._unpack relies on .get() for pre-R3.6 forward-compat,
        but the five keys must still always be present in worker output."""
        d = build_result_dict(
            task_id="t",
            queue="q",
            task_name="n",
            is_error=False,
            is_cancelled=False,
        )
        required = {"task", "is_error", "is_cancelled", "error_message", "result"}
        assert set(d) == required


class TestBuildCancelResultDict:
    def test_canonical_cancel_shape(self):
        d = build_cancel_result_dict(
            task_id="abc",
            queue="default",
            task_name="send",
        )
        assert d == {
            "task": {"id": "abc", "queue": "default", "name": "send"},
            "is_error": False,
            "is_cancelled": True,
            "error_message": "",
            "result": None,
        }


class TestBuildErrorResultDict:
    def test_canonical_error_shape(self):
        d = build_error_result_dict(
            task_id="abc",
            queue="default",
            task_name="send",
            error_message="RuntimeError: boom",
        )
        assert d == {
            "task": {"id": "abc", "queue": "default", "name": "send"},
            "is_error": True,
            "is_cancelled": False,
            "error_message": "RuntimeError: boom",
            "result": None,
        }

    def test_empty_error_message_allowed(self):
        d = build_error_result_dict(
            task_id="t",
            queue="q",
            task_name="n",
            error_message="",
        )
        assert d["is_error"] is True
        assert d["error_message"] == ""
