"""Opt-in-Queue fuer eingebettete Apps (Plan 2026-10-06-0831, U8; R5, R6, KTD10).

``SwarmQ(log_queue=True)`` leitet alle ``swarmq.*``-Logs ueber eine
swarmq-eigene Hintergrund-Queue an die Handler der App. Gesteuert wird ueber
die oeffentliche API (``SwarmQ``, ``configure_logging``); geleert wird per
``shutdown_logging()``.
"""

from __future__ import annotations

# Test-Liste:
# 1. Nach ``SwarmQ(log_queue=True)`` laeuft kein Listener-Thread, nach dem
#    ersten Eintrag genau einer (AE6).
# 2. Jede Zeile erscheint genau einmal im Root-Handler der App (keine
#    Duplikate ueber Propagation) -- in Test 1.
# 3. Ein langsamer App-Handler (blockiert auf einem Event) haelt den
#    aufrufenden Coroutine-Logaufruf nicht auf (R6).
# 4. ``log_level="INFO"`` in einer App mit Root-Level WARNING: INFO kommt an.
# 5. Ein Root-Handler der App mit Level ERROR erhaelt kein INFO-Event.
# 6. Ein Handler, den die App nach dem ersten Eintrag an Root haengt,
#    erhaelt spaetere swarmq-Events (kein Schnappschuss).
# 7. Ohne jeden Handler in der App erreicht ein WARNING-Event ``lastResort``.
# 8. Ein Handler-Filter, der eine Kontextvariable liest, sieht im Listener
#    den Wert des Aufrufers.
# 9. Ein App-Formatter mit ``ExtraAdder`` zeigt kein internes
#    Kontext-Attribut.
# 10. ``async with SwarmQ(..., log_queue=True)``: danach sind Handler,
#     ``propagate`` und das vorherige ``swarmq``-Level wiederhergestellt.
# 11. Zwei Instanzen: genau ein Handler; nach dem ersten ``__aexit__`` noch
#     aktiv, nach dem zweiten entfernt.
# 12. Hat ein Einstiegspunkt eingebaut konfiguriert, ist der Opt-in
#     wirkungslos: kein zusaetzlicher Handler an ``swarmq``.
# 13. Unter "nicht konfigurieren" (``--no-configure-logging``) wirkt der
#     Opt-in.
# 14. ``configure_logging`` (eingebaut) deaktiviert einen bereits aktiven
#     Opt-in und stellt Handler, ``propagate`` und Level wieder her.
# 15. Nach einem simulierten Fork laeuft der Opt-in weiter an die Handler der
#     App, ohne geerbte Records.
# 16. ``shutdown_logging()`` leert die Opt-in-Queue; der naechste Eintrag
#     startet den Listener wieder -- in Test 6.
# 18. ``log_queue_size`` begrenzt die Opt-in-Queue: Staut ein blockierender
#     App-Handler, wird verworfen statt zu warten.
# 17. Ersatz durch die App: swarmq-Logs laufen ueber die Queue an die Handler
#     der App -- im Worker ``test_logging_app_wins.py`` (R6), im Compute-Kind
#     ``worker/compute/test_child_logging.py`` (Test 12).
# AE6 mit echtem Forkserver: ``tests/integration/test_compute_child_logging.py``.
import contextvars
import io
import json
import logging
import threading
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.logging import (
    LISTENER_THREAD_NAME,
    configure_logging,
    get_logger,
    shutdown_logging,
)
from swarmq.logging_settings import LoggingSettings

_LOG = get_logger("swarmq.tests.optin")


class _Collect(logging.Handler):
    """Root-Handler der App: sammelt Records."""

    def __init__(self, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def events(self) -> list[str]:
        return [r.getMessage() for r in self.records]


@pytest.fixture()
def broker() -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    b._url = "amqp://guest:guest@localhost/"
    b.connect = AsyncMock()
    b.close = AsyncMock()
    return b


@pytest.fixture()
def backend() -> MagicMock:
    b = MagicMock(spec=ValkeyBackend)
    b._url = "valkey://localhost:6379/0"
    b.connect = AsyncMock()
    b.close = AsyncMock()
    return b


@pytest.fixture()
def app_handler() -> Iterator[_Collect]:
    """Die App besitzt Root: genau ein Handler, Level INFO."""
    root = logging.getLogger()
    handler = _Collect()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    yield handler


def _listener_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


def test_listener_starts_on_the_first_record_not_at_construction(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    SwarmQ(broker, backend, log_queue=True)
    assert _listener_threads() == []

    _LOG.warning("optin_first")

    assert len(_listener_threads()) == 1
    shutdown_logging()
    assert app_handler.events() == ["optin_first"]


class _Blocking(_Collect):
    """Ein langsamer Handler der App: blockiert, bis ``release`` gesetzt ist."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.entered.set()
        self.release.wait(5)
        super().emit(record)


@pytest.mark.timeout(5)
async def test_a_blocking_app_handler_does_not_stall_the_calling_coroutine(
    broker: MagicMock, backend: MagicMock,
) -> None:
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_slow_1")
    assert blocking.entered.wait(5)
    _LOG.warning("optin_slow_2")  # Handler blockiert noch: der Aufruf nicht

    blocking.release.set()
    shutdown_logging()
    assert blocking.events() == ["optin_slow_1", "optin_slow_2"]


def test_swarmq_level_follows_log_level_despite_a_warning_root(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    logging.getLogger().setLevel(logging.WARNING)
    SwarmQ(broker, backend, log_queue=True, log_level="INFO")

    _LOG.info("optin_info")

    shutdown_logging()
    assert app_handler.events() == ["optin_info"]


def test_app_handler_levels_still_apply(broker: MagicMock, backend: MagicMock) -> None:
    errors_only = _Collect(logging.ERROR)
    logging.getLogger().handlers[:] = [errors_only]
    SwarmQ(broker, backend, log_queue=True, log_level="INFO")

    _LOG.info("optin_info")
    _LOG.error("optin_error")

    shutdown_logging()
    assert errors_only.events() == ["optin_error"]


def test_a_handler_added_after_the_first_record_receives_later_events(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    SwarmQ(broker, backend, log_queue=True)
    _LOG.warning("optin_early")
    shutdown_logging()
    late = _Collect()
    logging.getLogger().addHandler(late)

    _LOG.warning("optin_late")

    shutdown_logging()
    assert late.events() == ["optin_late"]


def test_without_any_app_handler_a_warning_reaches_last_resort(
    broker: MagicMock, backend: MagicMock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    last_resort = _Collect(logging.WARNING)
    monkeypatch.setattr(logging, "lastResort", last_resort)
    logging.getLogger().handlers[:] = []
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_last_resort")

    shutdown_logging()
    assert last_resort.events() == ["optin_last_resort"]


_CORRELATION_ID: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="-")


class _CorrelationFilter(logging.Filter):
    """Ein Filter der App, der eine Kontextvariable liest."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _CORRELATION_ID.get()
        return True


@pytest.mark.timeout(5)
async def test_a_handler_filter_sees_the_callers_context_variables(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    app_handler.addFilter(_CorrelationFilter())
    SwarmQ(broker, backend, log_queue=True)

    token = _CORRELATION_ID.set("req-42")
    try:
        _LOG.warning("optin_correlated")
    finally:
        _CORRELATION_ID.reset(token)

    shutdown_logging()
    (record,) = app_handler.records
    assert record.correlation_id == "req-42"  # type: ignore[attr-defined]


def test_an_extra_adder_formatter_shows_no_internal_context_attribute(
    broker: MagicMock, backend: MagicMock,
) -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=[structlog.stdlib.ExtraAdder()],
        )
    )
    logging.getLogger().handlers[:] = [handler]
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_extra", task_id="t-1")

    shutdown_logging()
    line = json.loads(stream.getvalue())
    assert line["event"] == "optin_extra"
    assert line["task_id"] == "t-1"
    assert not [key for key in line if "context" in key]


def _swarmq_state() -> tuple[list[logging.Handler], int, bool]:
    logger = logging.getLogger("swarmq")
    return list(logger.handlers), logger.level, logger.propagate


@pytest.mark.timeout(5)
async def test_leaving_the_app_restores_handler_propagate_and_level(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    logging.getLogger("swarmq").setLevel(logging.ERROR)
    before = _swarmq_state()

    async with SwarmQ(broker, backend, log_queue=True, log_level="DEBUG"):
        assert _swarmq_state() != before

    assert _swarmq_state() == before


@pytest.mark.timeout(5)
async def test_two_instances_share_one_handler_until_the_last_one_leaves(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    before = _swarmq_state()
    first = SwarmQ(broker, backend, log_queue=True)
    second = SwarmQ(broker, backend, log_queue=True)
    assert len(logging.getLogger("swarmq").handlers) == 1

    await first.__aexit__(None, None, None)
    _LOG.warning("optin_still_queued")
    assert len(_listener_threads()) == 1

    await second.__aexit__(None, None, None)
    assert _swarmq_state() == before
    assert app_handler.events() == ["optin_still_queued"]


def test_opt_in_is_ineffective_after_a_builtin_entry_point_setup(
    broker: MagicMock, backend: MagicMock,
) -> None:
    configure_logging(LoggingSettings())
    before = _swarmq_state()

    SwarmQ(broker, backend, log_queue=True, log_level="DEBUG")

    assert _swarmq_state() == before


def test_opt_in_works_when_the_entry_point_does_not_configure(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    configure_logging(LoggingSettings(configure=False))

    SwarmQ(broker, backend, log_queue=True)
    _LOG.warning("optin_unconfigured")

    assert len(_listener_threads()) == 1
    shutdown_logging()
    assert app_handler.events() == ["optin_unconfigured"]


def test_a_builtin_setup_deactivates_an_active_opt_in(
    broker: MagicMock, backend: MagicMock,
) -> None:
    logging.getLogger("swarmq").setLevel(logging.ERROR)
    before = _swarmq_state()
    SwarmQ(broker, backend, log_queue=True, log_level="DEBUG")

    configure_logging(LoggingSettings())

    assert _swarmq_state() == before


def test_after_a_fork_the_opt_in_still_reaches_the_app_without_inherited_records(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
) -> None:
    from swarmq.logging import _after_fork_in_child

    SwarmQ(broker, backend, log_queue=True)
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    try:
        _LOG.warning("optin_parent_busy")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_inherited")  # liegt beim Fork noch in der Queue

        _after_fork_in_child()
        logging.getLogger().handlers[:] = [app_handler]
        _LOG.warning("optin_child")
        (handler,) = logging.getLogger("swarmq").handlers
        handler.listener.stop()  # type: ignore[attr-defined]

        assert app_handler.events() == ["optin_child"]
    finally:
        blocking.release.set()


def test_log_queue_size_bounds_the_opt_in_queue(broker: MagicMock, backend: MagicMock) -> None:
    from swarmq.logging import dropped_log_records

    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    SwarmQ(broker, backend, log_queue=True, log_queue_size=2)
    try:
        _LOG.warning("optin_busy")
        assert blocking.entered.wait(5)
        before = dropped_log_records()

        for _ in range(5):
            _LOG.warning("optin_burst")

        assert dropped_log_records() - before == 3
    finally:
        blocking.release.set()
