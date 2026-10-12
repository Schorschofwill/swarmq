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
    TaskCancelledError,
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


def test_task_cancelled_error_is_task_error_subclass():
    err = TaskCancelledError()
    assert isinstance(err, TaskError)


def test_task_cancelled_error_stores_task_id():
    err = TaskCancelledError("task-123")
    assert err.task_id == "task-123"
    assert "task-123" in str(err)


def test_task_cancelled_error_without_task_id():
    err = TaskCancelledError()
    assert err.task_id is None
    assert str(err)  # some default message, non-empty


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


def test_remaining_exceptions_are_exported_from_package_root():
    """LoggingConfigError, SoftTimeoutError und PublishIndeterminate sind
    öffentlich: Import über ``swarmq`` liefert dieselben Klassenobjekte."""
    import swarmq
    from swarmq import LoggingConfigError, PublishIndeterminate, SoftTimeoutError, exceptions

    assert LoggingConfigError is exceptions.LoggingConfigError
    assert SoftTimeoutError is exceptions.SoftTimeoutError
    assert PublishIndeterminate is exceptions.PublishIndeterminate
    assert {"LoggingConfigError", "SoftTimeoutError", "PublishIndeterminate"} <= set(swarmq.__all__)


def test_backend_response_error_is_a_backend_connection_error_exported_from_the_root():
    """0.9.0 (drittes Review): Fehlerantwort des Backends -- eine Unterklasse,
    damit bestehende ``except BackendConnectionError`` sie weiter fangen."""
    import swarmq
    from swarmq import BackendResponseError, exceptions

    assert BackendResponseError is exceptions.BackendResponseError
    assert issubclass(BackendResponseError, exceptions.BackendConnectionError)
    assert "BackendResponseError" in swarmq.__all__


def test_message_headers_too_large_is_a_broker_connection_error_exported_from_the_root():
    """BZ-17 (Plan 0.12.0, MR 2, Q14): Header, die auch gekuerzt nicht in
    einen Frame passen -- Unterklasse von ``BrokerConnectionError``, damit
    bestehende Handler sie weiter fangen."""
    import swarmq
    from swarmq import MessageHeadersTooLarge, exceptions

    assert MessageHeadersTooLarge is exceptions.MessageHeadersTooLarge
    assert issubclass(MessageHeadersTooLarge, exceptions.BrokerConnectionError)
    assert "MessageHeadersTooLarge" in swarmq.__all__


def test_redelivery_limit_error_names_the_dead_letter_reason_of_the_delivery_count_gate():
    """BZ-29 (Plan 0.12.0, MR 3, Q11): Fehlertyp der DLQ-Kopie, wenn nur
    ``x-acquired-count`` das Zustelllimit erreicht -- eine eigene Klasse,
    damit der Fehlerkatalog ihn fuehrt."""
    import swarmq
    from swarmq import RedeliveryLimitError, exceptions
    from swarmq.worker.gates.delivery_count import REDELIVERY_LIMIT_ERROR_TYPE

    assert RedeliveryLimitError is exceptions.RedeliveryLimitError
    assert issubclass(RedeliveryLimitError, exceptions.SwarmQError)
    assert RedeliveryLimitError.__name__ == REDELIVERY_LIMIT_ERROR_TYPE
    assert "RedeliveryLimitError" in swarmq.__all__
