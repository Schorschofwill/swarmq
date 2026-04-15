"""Tests for swarmq.logging — structlog setup and URL sanitization."""

from __future__ import annotations

# Test list:
# 1. setup_logging() returns a QueueListener
# 2. sanitize_url with password -> password replaced with ***
# 3. sanitize_url without password -> unchanged
# 4. Log output is JSON when log_json=True
# 5. Log level filtering works (DEBUG not output at INFO level)
# 6. contextvars binding: bind task_id, verify it appears in log output

import io
import json
import logging
import logging.handlers

import structlog

from swarmq.logging import sanitize_url, setup_logging


def test_setup_logging_returns_queue_listener():
    listener = setup_logging()
    try:
        assert isinstance(listener, logging.handlers.QueueListener)
    finally:
        listener.stop()


def test_sanitize_url_replaces_password():
    result = sanitize_url("amqp://user:pass@host:5672/")
    assert result == "amqp://user:***@host:5672/"


def test_sanitize_url_without_password_unchanged():
    url = "amqp://host:5672/"
    assert sanitize_url(url) == url


def test_log_output_is_json_when_log_json_true():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        # Replace the listener's handler stream with a StringIO to capture output
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        logger = structlog.get_logger()
        logger.info("test_event", key="value")

        # Stop flushes the queue
        listener.stop()

        output = stream.getvalue().strip()
        line = output.split("\n")[-1]
        data = json.loads(line)
        assert data["event"] == "test_event"
        assert data["key"] == "value"
    finally:
        if listener._thread is not None:
            listener.stop()


def test_log_level_filtering_debug_not_output_at_info():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        logger = structlog.get_logger()
        logger.debug("should_be_filtered")
        logger.info("should_appear")

        listener.stop()

        output = stream.getvalue().strip()
        assert "should_be_filtered" not in output
        assert "should_appear" in output
    finally:
        if listener._thread is not None:
            listener.stop()


def test_contextvars_binding_appears_in_log_output():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(task_id="abc-123")

        logger = structlog.get_logger()
        logger.info("task_started")

        listener.stop()

        output = stream.getvalue().strip()
        data = json.loads(output.split("\n")[-1])
        assert data["task_id"] == "abc-123"
        assert data["event"] == "task_started"
    finally:
        structlog.contextvars.clear_contextvars()
        if listener._thread is not None:
            listener.stop()
