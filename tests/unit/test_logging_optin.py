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
# 15. Nach einem echten Fork (``os.fork``) laeuft der Opt-in weiter an die Handler der
#     App, ohne geerbte Records.
# 16. ``shutdown_logging()`` leert die Opt-in-Queue; der naechste Eintrag
#     startet den Listener wieder -- in Test 6.
# 18. ``log_queue_size`` begrenzt die Opt-in-Queue: Staut ein blockierender
#     App-Handler, wird verworfen statt zu warten.
# 17. Ersatz durch die App: swarmq-Logs laufen ueber die Queue an die Handler
#     der App -- im Worker ``test_logging_app_wins.py`` (R6), im Compute-Kind
#     ``worker/compute/test_child_logging.py`` (Test 12).
# 19. Ein Integrations-Hook auf ``Logger.callHandlers`` (Muster von Sentrys
#     ``LoggingIntegration``) sieht jeden swarmq-Record genau einmal: Der
#     Listener gibt an die Root-Handler weiter, ohne ``callHandlers``
#     erneut zu durchlaufen.
# AE6 mit echtem Forkserver: ``tests/integration/test_compute_child_logging.py``.
# Plan 2026-10-06-1811, U2 (R2, KTD1) -- der Listener stirbt nie:
# 20. Ein Root-Handler der App, dessen ``emit`` wirft: Nach dem Gift-Record
#     lebt der Listener, der naechste Record erreicht den sammelnden Handler.
#     Pro Handler gefangen: Der werfende Handler steht vor dem sammelnden.
# 21. Ein Filter der App, der wirft (fehlende Kontextvariable): dasselbe.
# 22. Wirft jeder Handler der App, erscheint ``log_handler_errors`` best
#     effort auf stderr -- aus dem Listener-Thread, nie im Aufrufer.
# Echter Prozess: ``tests/integration/test_logging_robustness.py``.
# Plan 2026-10-06-1811, U5 (R4, KTD4) -- ``__aexit__`` leert asynchron:
# 23. Ein App-Handler blockiert, ``__aexit__`` laeuft als Task: Eine zweite
#     Coroutine (``call_soon``) laeuft, solange der Handler blockiert; nach
#     dem Freigeben endet ``__aexit__``, alle Records sind angekommen.
# 24. Handler nie freigegeben, ``log_drain_timeout=0.05``: ``__aexit__``
#     kehrt zurueck, der Rest ist als verworfen gezaehlt.
# 25. Abhaengen in O(1): Gleich nach dem synchronen Teil sind Handler,
#     ``propagate`` und Level von ``swarmq`` wiederhergestellt.
# 26. Eine neue ``SwarmQ(log_queue=True)`` waehrend des Leerens bekommt
#     frischen Handler und frische Queue; jede Zeile genau einmal.
# 27. Default-Executor ausgelastet: ``__aexit__`` kehrt trotzdem spaetestens
#     nach ``log_drain_timeout`` zurueck.
# 28. Eingebettet gilt die Frist der App auch fuer ``shutdown_logging``
#     (atexit).
# 29. ``configure_logging`` hebt einen Opt-in mit haengendem App-Handler
#     innerhalb der Frist auf.
# Echter Prozess (Loop-Lag): ``tests/integration/test_logging_robustness.py``.
import asyncio
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
from tests.unit._fork_child import run_in_forked_child

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


@pytest.fixture()
def call_handlers_hook(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Haengt sich wie Sentrys ``LoggingIntegration`` an
    ``Logger.callHandlers`` und merkt sich die Nachricht je Aufruf."""
    seen: list[str] = []
    original = logging.Logger.callHandlers

    def hooked(self: logging.Logger, record: logging.LogRecord) -> None:
        seen.append(record.getMessage())
        original(self, record)

    monkeypatch.setattr(logging.Logger, "callHandlers", hooked)
    return seen


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
    SwarmQ(broker, backend, log_queue=True)
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    try:
        _LOG.warning("optin_parent_busy")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_inherited")  # liegt beim Fork noch in der Queue

        def child() -> None:
            logging.getLogger().handlers[:] = [app_handler]
            _LOG.warning("optin_child")
            (handler,) = logging.getLogger("swarmq").handlers
            handler.listener.stop()  # type: ignore[attr-defined]
            assert app_handler.events() == ["optin_child"]

        assert run_in_forked_child(child) == 0
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


def test_a_call_handlers_hook_sees_each_opt_in_record_once(
    broker: MagicMock, backend: MagicMock, app_handler: _Collect,
    call_handlers_hook: list[str],
) -> None:
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_hooked")

    shutdown_logging()
    assert app_handler.events() == ["optin_hooked"]
    assert call_handlers_hook.count("optin_hooked") == 1


class _Raising(logging.Handler):
    """Root-Handler der App, dessen ``emit`` wirft, ohne ``handleError``
    -- etwa ein Log-Shipper ohne Verbindung (Analyse S2)."""

    def emit(self, record: logging.LogRecord) -> None:
        raise ConnectionError("log shipper down")


class _Arrival(_Collect):
    """Sammelt und setzt ``arrived``, sobald ``event`` ankommt."""

    def __init__(self, event: str) -> None:
        super().__init__()
        self.event = event
        self.arrived = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        if record.getMessage() == self.event:
            self.arrived.set()


@pytest.mark.timeout(5)
def test_a_raising_app_handler_does_not_kill_the_listener(
    broker: MagicMock, backend: MagicMock,
) -> None:
    arrival = _Arrival("optin_after_poison")
    logging.getLogger().handlers[:] = [_Raising(), arrival]
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_poison")
    _LOG.warning("optin_after_poison")

    assert arrival.arrived.wait(2), arrival.events()


_CORRELATION: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id")


class _NeedsCorrelation(logging.Filter):
    """Filter der App, der eine Kontextvariable ohne Standardwert liest."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _CORRELATION.get()
        return True


@pytest.mark.timeout(5)
def test_a_raising_app_filter_does_not_kill_the_listener(
    broker: MagicMock, backend: MagicMock,
) -> None:
    arrival = _Arrival("optin_after_poison")
    strict = _Collect()
    strict.addFilter(_NeedsCorrelation())
    logging.getLogger().handlers[:] = [strict, arrival]
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_poison")
    _LOG.warning("optin_after_poison")

    assert arrival.arrived.wait(2), arrival.events()
    # Die Meldung entsteht beim Fehler im ersten Handler, also vor dem
    # Gift-Record im zweiten.
    assert arrival.events() == ["log_handler_errors", "optin_poison", "optin_after_poison"]


@pytest.mark.timeout(5)
def test_when_every_app_handler_raises_the_report_reaches_stderr(
    broker: MagicMock, backend: MagicMock, capsys: pytest.CaptureFixture[str],
) -> None:
    logging.getLogger().handlers[:] = [_Raising()]
    SwarmQ(broker, backend, log_queue=True)

    _LOG.warning("optin_poison")
    shutdown_logging()

    err = capsys.readouterr().err
    assert "log_handler_errors" in err
    assert "Traceback" not in err


@pytest.mark.timeout(5)
async def test_leaving_the_app_drains_without_stalling_the_event_loop(
    broker: MagicMock, backend: MagicMock,
) -> None:
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    app = SwarmQ(broker, backend, log_queue=True)
    _LOG.warning("optin_drain_1")
    assert blocking.entered.wait(5)
    _LOG.warning("optin_drain_2")

    leaving = asyncio.create_task(app.__aexit__(None, None, None))
    other_ran = asyncio.Event()
    asyncio.get_running_loop().call_soon(other_ran.set)
    await other_ran.wait()

    assert not leaving.done()
    blocking.release.set()
    await leaving
    assert blocking.events() == ["optin_drain_1", "optin_drain_2"]


@pytest.mark.timeout(5)
async def test_leaving_the_app_gives_up_at_the_drain_timeout_and_counts_the_rest(
    broker: MagicMock, backend: MagicMock,
) -> None:
    from swarmq.logging import dropped_log_records

    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    app = SwarmQ(broker, backend, log_queue=True, log_drain_timeout=0.05)
    try:
        _LOG.warning("optin_stuck")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_never_written")
        before = dropped_log_records()

        await app.__aexit__(None, None, None)

        assert dropped_log_records() - before == 1
    finally:
        blocking.release.set()


@pytest.mark.timeout(5)
async def test_leaving_the_app_restores_swarmq_before_the_queue_is_drained(
    broker: MagicMock, backend: MagicMock,
) -> None:
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    logging.getLogger("swarmq").setLevel(logging.ERROR)
    before = _swarmq_state()
    app = SwarmQ(broker, backend, log_queue=True, log_level="DEBUG")
    _LOG.warning("optin_detach")
    assert blocking.entered.wait(5)

    leaving = asyncio.create_task(app.__aexit__(None, None, None))
    detached = asyncio.Event()
    asyncio.get_running_loop().call_soon(detached.set)
    await detached.wait()

    assert not leaving.done()
    assert _swarmq_state() == before
    blocking.release.set()
    await leaving


@pytest.mark.timeout(5)
async def test_a_new_instance_while_draining_gets_a_fresh_handler_and_queue(
    broker: MagicMock, backend: MagicMock,
) -> None:
    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    first = SwarmQ(broker, backend, log_queue=True)
    _LOG.warning("optin_old")
    assert blocking.entered.wait(5)
    (old_handler,) = logging.getLogger("swarmq").handlers
    leaving = asyncio.create_task(first.__aexit__(None, None, None))
    detached = asyncio.Event()
    asyncio.get_running_loop().call_soon(detached.set)
    await detached.wait()

    second = SwarmQ(broker, backend, log_queue=True)
    _LOG.warning("optin_new")

    (new_handler,) = logging.getLogger("swarmq").handlers
    assert new_handler is not old_handler
    blocking.release.set()
    await leaving
    await second.__aexit__(None, None, None)
    assert sorted(blocking.events()) == ["optin_new", "optin_old"]


@pytest.mark.timeout(5)
async def test_leaving_the_app_returns_at_the_drain_timeout_with_a_busy_executor(
    broker: MagicMock, backend: MagicMock,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from swarmq.logging import dropped_log_records

    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    app_work_started = threading.Event()
    app_work_done = threading.Event()

    def blocking_app_work() -> None:
        app_work_started.set()
        app_work_done.wait(5)

    app_work = loop.run_in_executor(None, blocking_app_work)
    assert app_work_started.wait(5)
    app = SwarmQ(broker, backend, log_queue=True, log_drain_timeout=0.05)
    try:
        _LOG.warning("optin_stuck")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_never_written")
        before = dropped_log_records()

        await app.__aexit__(None, None, None)

        assert not app_work.done()
        assert dropped_log_records() - before == 1
    finally:
        app_work_done.set()
        blocking.release.set()
        await app_work


@pytest.mark.timeout(5)
def test_an_embedded_app_sets_the_drain_timeout_of_shutdown_logging(
    broker: MagicMock, backend: MagicMock,
) -> None:
    from swarmq.logging import dropped_log_records

    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    SwarmQ(broker, backend, log_queue=True, log_drain_timeout=0.05)
    try:
        _LOG.warning("optin_stuck")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_never_written")
        before = dropped_log_records()

        shutdown_logging()  # wie aus ``atexit``

        assert dropped_log_records() - before == 1
    finally:
        blocking.release.set()


@pytest.mark.timeout(5)
def test_a_builtin_setup_lifts_an_opt_in_with_a_stuck_app_handler_in_time(
    broker: MagicMock, backend: MagicMock,
) -> None:
    from swarmq.logging import dropped_log_records

    blocking = _Blocking()
    logging.getLogger().handlers[:] = [blocking]
    SwarmQ(broker, backend, log_queue=True)
    try:
        _LOG.warning("optin_stuck")
        assert blocking.entered.wait(5)
        _LOG.warning("optin_never_written")
        before = dropped_log_records()

        configure_logging(LoggingSettings(drain_timeout=0.05))

        assert dropped_log_records() - before == 1
    finally:
        blocking.release.set()
