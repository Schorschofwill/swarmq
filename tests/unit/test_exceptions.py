"""Tests for swarmq.exceptions — exception hierarchy."""

import pytest

from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ConfigurationError,
    NoRetry,
    ResultExpiredError,
    ResultNotReadyError,
    ResultSerializationError,
    Retry,
    SwarmQError,
    TaskError,
    TaskNotFoundError,
)


def test_swarmq_error_is_exception_subclass():
    err = SwarmQError("boom")
    assert isinstance(err, Exception)
    assert str(err) == "boom"


@pytest.mark.parametrize(
    "exc_class",
    [
        ConfigurationError,
        BrokerConnectionError,
        BackendConnectionError,
        TaskNotFoundError,
        TaskError,
        ResultNotReadyError,
        ResultExpiredError,
        ResultSerializationError,
        Retry,
        NoRetry,
    ],
)
def test_each_exception_inherits_from_swarmq_error(exc_class):
    err = exc_class()
    assert isinstance(err, SwarmQError)


def test_task_error_stores_message():
    err = TaskError("remote error")
    assert str(err) == "remote error"


def test_retry_stores_delay():
    err = Retry(delay=60)
    assert err.delay == 60


def test_retry_delay_defaults_to_none():
    err = Retry()
    assert err.delay is None


def test_no_retry_stores_reason():
    err = NoRetry(reason="bad input")
    assert err.reason == "bad input"


@pytest.mark.parametrize(
    "exc_class",
    [
        ConfigurationError,
        BrokerConnectionError,
        BackendConnectionError,
        TaskNotFoundError,
        TaskError,
        ResultNotReadyError,
        ResultExpiredError,
        ResultSerializationError,
        Retry,
        NoRetry,
    ],
)
def test_all_exceptions_catchable_with_except_swarmq_error(exc_class):
    with pytest.raises(SwarmQError):
        raise exc_class()
