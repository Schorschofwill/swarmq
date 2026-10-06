"""Logging im Rechenkind (Plan 2026-10-06-0831, U7 Teil b: R15, KTD5, KTD6, KTD14).

Ohne echten Fork: ``child_once``/``_serve_jobs`` laufen im Testprozess, der
Rueckkanal ist eine Attrappe, ``bind_child_process`` (setpgid, PDEATHSIG)
und ``logging.shutdown`` sind ersetzt.

Test-Liste:
1. [x] Mit ``log_config_json`` richtet sich das Kind nach der dictConfig aus
       dem Job; die Datei selbst liest es nie.
2. [x] ``child_once``: die Queue ist geleert, bevor das Ergebnis gesendet wird
       -- die Logzeile des Tasks steht vor der Ergebnisnachricht.
3. [x] ``child_once``: am Ende ist ``logging.shutdown`` aufgerufen (atexit
       laeuft im Forkserver-Kind nicht: ``atexit._clear()`` nach dem Fork).
4. [x] ``child_loop`` mit zwei Jobs: die Logzeile jedes Jobs steht vor dessen
       Ergebnis; am Ende ist ``logging.shutdown`` aufgerufen.
5. [x] Eingebaut, aus Job-Feldern: Level und JSON-Format aus dem Job.
6. [x] Vom Forkserver geerbte Root-Handler der App bleiben, die Task-Logs
       landen dort, und das Kind meldet die Uebernahme ueber den Rueckkanal
       (das Event schreibt der Pool, einmal je Pool).
7. [x] Ohne geerbte App-Handler meldet das Kind nichts (Test 2: keine
       Meldung zwischen ``ready`` und der Logzeile).
8. [x] Eine structlog-Konfiguration der App (aus dem Vorladen) bleibt im Kind
       erhalten.
9. [x] Hat die dictConfig der App ``swarmq.*``-Logger stummgeschaltet,
       schaltet das Kind sie wieder ein und nennt die Zahl in der Meldung.
10. [x] Mit geerbten App-Handlern (eingebauter Modus) erreicht
        ``structlog.get_logger()`` im Task die Handler der App -- wie im
        Worker, der structlog vor dem App-Import einrichtet.
10b. [x] Im Datei-Modus bleibt structlog dabei unkonfiguriert (KTD6).
11. [x] Mit geerbten App-Handlern gelten die Logger-Level aus dem Job.
12. [x] Mit geerbten App-Handlern laufen swarmq-Logs ueber die Opt-in-Queue
        im Listener-Thread an die Handler der App (R6, KTD14, U8); die
        Leerung pro Job schreibt sie vor dem Ergebnis.
13. [x] U10: Ein Event im Kind traegt ``compute_child``, ``workflow_id`` und
        ``attempt`` (Plan 2026-10-06-0831, KTD13).
14. [x] U10: Ein wiederverwendetes Kind traegt beim zweiten Job keinen
        Kontext des ersten (``workflow_id`` nur am ersten Job).
15. [x] Plan 2026-10-06-1811, U4: stderr blockiert; das Ergebnis
        kommt trotzdem nach der Frist ``log_drain_timeout`` aus dem Job.
16. [x] ... und am Ende endet das Kind hart, weil der Log-Thread haengt.
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
import weakref
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from swarmq.task import Task, TaskInfo
from swarmq.worker.compute import child
from swarmq.worker.compute.envelope import ComputeJob

RECORDS: list[logging.LogRecord] = []
"""Ziel des ``CollectingHandler`` aus den dictConfig-Tests."""


class CollectingHandler(logging.Handler):
    """Per dictConfig (``"()"``) referenzierbarer Handler."""

    def emit(self, record: logging.LogRecord) -> None:
        RECORDS.append(record)


_COLLECTING = f"{__name__}.CollectingHandler"


class _Channel:
    """Rueckkanal-Attrappe: haelt alle Nachrichten in ``events`` fest."""

    def __init__(self, events: list[Any]) -> None:
        self.events = events

    def send(self, msg: tuple[Any, ...]) -> None:
        self.events.append(msg)


class _Stderr(io.StringIO):
    """stderr-Attrappe, die jede Zeile in dieselbe Ereignisliste schreibt."""

    def __init__(self, events: list[Any]) -> None:
        super().__init__()
        self.events = events

    def write(self, text: str) -> int:
        if text.strip():
            self.events.append(("stderr", text.strip()))
        return super().write(text)


@pytest.fixture
def events() -> list[Any]:
    return []


@pytest.fixture(autouse=True)
def child_process(monkeypatch: pytest.MonkeyPatch, events: list[Any]) -> Iterator[None]:
    """Kindumgebung im Testprozess: keine Prozessgruppe, kein PDEATHSIG, kein
    echtes ``logging.shutdown``; structlog und Task-Registry danach wie vorher."""
    monkeypatch.setattr(child, "bind_child_process", lambda: True)
    real_shutdown = logging.shutdown

    def shutdown_spy(*handler_list: Any) -> None:
        if handler_list:  # dictConfig schliesst damit die alten Handler
            real_shutdown(*handler_list)
        else:  # Prozessende
            events.append("logging.shutdown")

    monkeypatch.setattr(logging, "shutdown", shutdown_spy)
    # dictConfig schliesst alle bekannten Handler -- auch die von pytest.
    monkeypatch.setattr(logging, "_handlerList", [])
    monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
    was_configured = structlog.is_configured()
    old_config = structlog.get_config()
    saved_tasks = list(Task._registered_tasks)
    RECORDS.clear()
    yield
    Task._registered_tasks[:] = saved_tasks
    structlog.contextvars.clear_contextvars()
    structlog.reset_defaults()
    if was_configured:
        structlog.configure(**old_config)


def _job(task_id: str = "t1", **fields: Any) -> ComputeJob:
    return ComputeJob(
        task_id=task_id, task_name="logging_task", queue="q", args=(task_id,), kwargs={}, **fields,
    )


def _forkserver_root() -> None:
    """Root ohne Handler, wie im Forkserver vor dem Vorladen der App -- im
    Testkoerper, denn pytest haengt seine Handler erst zur Call-Phase an."""
    logging.getLogger().handlers[:] = []


def _register_logging_task() -> None:
    class LoggingTask(Task):
        task_info = TaskInfo(name="logging_task", queues=["q"])

        async def run(self, tag: str) -> str:
            logging.getLogger("app.task").warning("task_line %s", tag)
            return "ok"


_FILE_CONFIG = json.dumps({
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"collect": {"()": _COLLECTING}},
    "root": {"level": "INFO", "handlers": ["collect"]},
})


@pytest.mark.timeout(5)
def test_child_configures_from_the_dictconfig_in_the_job(events: list[Any]) -> None:
    _forkserver_root()
    _register_logging_task()
    job = _job(log_config="/does/not/exist.json", log_config_json=_FILE_CONFIG)

    child.child_once(job, _Channel(events), 0)

    assert [r.getMessage() for r in RECORDS] == ["task_line t1"]


def _capture_stderr(monkeypatch: pytest.MonkeyPatch, events: list[Any]) -> None:
    """stderr als Attrappe -- im Testkoerper, denn pytests Capture setzt
    ``sys.stderr`` zu Beginn der Call-Phase neu."""
    monkeypatch.setattr("sys.stderr", _Stderr(events))


@pytest.fixture
def flush_spy(monkeypatch: pytest.MonkeyPatch, events: list[Any]) -> None:
    """``shutdown_logging`` mit Eintrag in der Ereignisliste: Ob der
    Listener-Thread die Zeile ohne Leeren zufaellig schon geschrieben haette,
    ist ein Wettlauf -- das Leeren vor dem Ergebnis ist es nicht."""
    import swarmq.logging as swarmq_logging

    real = swarmq_logging.shutdown_logging

    def spy() -> None:
        real()
        events.append("flush")

    monkeypatch.setattr(swarmq_logging, "shutdown_logging", spy)


def _kinds(events: list[Any]) -> list[str]:
    """Ereignisse als Kurzform: Nachrichtenart, ``stderr:<event>`` oder Marke."""
    out: list[str] = []
    for item in events:
        if isinstance(item, str):
            out.append(item)
        elif item[0] == "stderr":
            out.append("stderr:" + json.loads(item[1])["event"])
        else:
            out.append(f"{item[0]}:{item[1].task_id}" if item[0] == "result" else item[0])
    return out


@pytest.mark.timeout(5)
@pytest.mark.usefixtures("flush_spy")
def test_child_once_flushes_the_task_log_before_sending_the_result(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    _register_logging_task()
    _capture_stderr(monkeypatch, events)

    child.child_once(_job(log_json=True), _Channel(events), 0)

    kinds = _kinds(events)
    assert kinds[:4] == ["ready", "stderr:task_line t1", "flush", "result:t1"]


@pytest.mark.timeout(5)
@pytest.mark.usefixtures("flush_spy")
def test_child_once_ends_with_logging_shutdown(events: list[Any]) -> None:
    _forkserver_root()
    _register_logging_task()

    child.child_once(_job(), _Channel(events), 0)

    assert _kinds(events)[-2:] == ["flush", "logging.shutdown"]


@pytest.mark.timeout(5)
@pytest.mark.usefixtures("flush_spy")
def test_child_loop_flushes_each_jobs_log_before_its_result(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    import multiprocessing as mp

    _register_logging_task()
    _capture_stderr(monkeypatch, events)
    job_r, job_w = mp.Pipe(duplex=False)
    for item in (_job("j1"), _job("j2"), None):
        job_w.send(item)

    child.child_loop(0, job_r, _Channel(events))

    assert _kinds(events) == [
        "ready",
        "stderr:task_line j1", "flush", "result:j1",
        "stderr:task_line j2", "flush", "result:j2",
        "flush", "logging.shutdown",
    ]
    job_r.close()
    job_w.close()


@pytest.mark.timeout(5)
def test_builtin_child_takes_level_and_format_from_the_job(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    class Levels(Task):
        task_info = TaskInfo(name="logging_task", queues=["q"])

        async def run(self, tag: str) -> str:
            logging.getLogger("app.task").warning("filtered_%s", tag)
            logging.getLogger("app.task").error("kept_%s", tag)
            return "ok"

    _capture_stderr(monkeypatch, events)

    child.child_once(_job(log_level="ERROR", log_json=False), _Channel(events), 0)

    lines = [item[1] for item in events if isinstance(item, tuple) and item[0] == "stderr"]
    assert len(lines) == 1 and "kept_t1" in lines[0]
    assert not lines[0].startswith("{")  # human, nicht JSON



@pytest.mark.timeout(5)
def test_inherited_app_root_handlers_win_and_the_child_reports_it(events: list[Any]) -> None:
    _forkserver_root()
    _register_logging_task()
    app_handler = CollectingHandler()
    logging.getLogger().addHandler(app_handler)  # vom Forkserver-Vorladen geerbt

    child.child_once(_job(log_json=True), _Channel(events), 0)

    assert logging.getLogger().handlers == [app_handler]
    assert [r.getMessage() for r in RECORDS] == ["task_line t1"]
    reports = [e for e in events if isinstance(e, tuple) and e[0] == "logging_reconfigured_by_app"]
    assert reports == [("logging_reconfigured_by_app", {"reenabled_loggers": 0})]



@pytest.mark.timeout(5)
def test_app_structlog_configuration_from_the_preload_survives_the_child_setup(
    events: list[Any],
) -> None:
    _forkserver_root()
    _register_logging_task()

    def app_processor(_: Any, __: str, event_dict: Any) -> Any:
        return event_dict

    structlog.configure(processors=[app_processor])  # beim Vorladen der App

    child.child_once(_job(), _Channel(events), 0)

    assert structlog.get_config()["processors"] == [app_processor]



@pytest.mark.timeout(5)
def test_child_reenables_swarmq_loggers_the_app_disabled_and_counts_them(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    _register_logging_task()
    logging.getLogger().addHandler(CollectingHandler())
    muted = logging.getLogger("swarmq.tests.child_muted")
    monkeypatch.setattr(muted, "disabled", True)  # dictConfig der App beim Vorladen
    disabled = sum(
        1 for name, lg in logging.Logger.manager.loggerDict.items()
        if isinstance(lg, logging.Logger) and lg.disabled and name.startswith("swarmq")
    )

    child.child_once(_job(), _Channel(events), 0)

    assert not muted.disabled
    reports = [e[1] for e in events if isinstance(e, tuple) and e[0] == "logging_reconfigured_by_app"]
    assert reports == [{"reenabled_loggers": disabled}]



@pytest.mark.timeout(5)
def test_user_structlog_reaches_the_inherited_app_handlers(events: list[Any]) -> None:
    _forkserver_root()

    class UsesStructlog(Task):
        task_info = TaskInfo(name="logging_task", queues=["q"])

        async def run(self, tag: str) -> str:
            structlog.get_logger("app.user").warning("user_evt", tag=tag)
            return "ok"

    logging.getLogger().addHandler(CollectingHandler())

    child.child_once(_job(), _Channel(events), 0)

    assert [(r.getMessage(), r.__dict__.get("tag")) for r in RECORDS] == [("user_evt", "t1")]



@pytest.mark.timeout(5)
def test_inherited_app_handlers_in_file_mode_leave_structlog_unconfigured(events: list[Any]) -> None:
    _forkserver_root()
    _register_logging_task()
    structlog.reset_defaults()
    logging.getLogger().addHandler(CollectingHandler())

    child.child_once(_job(log_config="/x.json", log_config_json=_FILE_CONFIG), _Channel(events), 0)

    assert not structlog.is_configured()



@pytest.mark.timeout(5)
def test_inherited_app_handlers_still_get_the_logger_levels_of_the_job(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    _register_logging_task()
    noisy = logging.getLogger("app.child_noisy")
    monkeypatch.setattr(noisy, "level", logging.NOTSET)
    logging.getLogger().addHandler(CollectingHandler())

    child.child_once(_job(logger_levels=(("app.child_noisy", "ERROR"),)), _Channel(events), 0)

    assert noisy.level == logging.ERROR



class _ThreadRecordingHandler(logging.Handler):
    """Geerbter App-Handler: haelt Nachricht und Thread jedes Records fest."""

    def __init__(self, events: list[Any]) -> None:
        super().__init__()
        self.events = events

    def emit(self, record: logging.LogRecord) -> None:
        import threading

        self.events.append(f"app:{record.getMessage()}@{threading.current_thread().name}")


@pytest.mark.timeout(5)
@pytest.mark.usefixtures("flush_spy")
def test_swarmq_logs_reach_inherited_app_handlers_via_the_queue_before_the_result(
    events: list[Any],
) -> None:
    from swarmq.logging import LISTENER_THREAD_NAME, get_logger

    _forkserver_root()

    class SwarmqLogging(Task):
        task_info = TaskInfo(name="logging_task", queues=["q"])

        async def run(self, tag: str) -> str:
            get_logger("swarmq.tests.child_queue").warning("swarmq_in_task")
            return "ok"

    logging.getLogger().addHandler(_ThreadRecordingHandler(events))

    child.child_once(_job(), _Channel(events), 0)

    assert _kinds(events)[-5:] == [
        f"app:swarmq_in_task@{LISTENER_THREAD_NAME}", "flush", "result:t1",
        "flush", "logging.shutdown",
    ]


_WF_ID = "0190a3c4-5d6e-7f80-9a1b-2c3d4e5f6a7b"


def _register_context_task() -> None:
    from swarmq.logging import get_logger

    class ContextTask(Task):
        task_info = TaskInfo(name="logging_task", queues=["q"])

        async def run(self, tag: str) -> str:
            get_logger("swarmq.tests.child_context").warning("ctx_line", tag=tag)
            return "ok"


def _ctx_records() -> list[logging.LogRecord]:
    return [r for r in RECORDS if r.getMessage() == "ctx_line"]


@pytest.mark.timeout(5)
def test_child_event_carries_compute_child_workflow_id_and_attempt(events: list[Any]) -> None:
    _forkserver_root()
    _register_context_task()
    job = _job(log_config="/does/not/exist.json", log_config_json=_FILE_CONFIG, workflow_id=_WF_ID, retry_count=1)

    child.child_once(job, _Channel(events), 0)

    [record] = _ctx_records()
    assert record.__dict__["compute_child"] == os.getpid()
    assert record.__dict__["workflow_id"] == _WF_ID
    assert record.__dict__["attempt"] == 2
    assert record.__dict__["task_id"] == "t1"


@pytest.mark.timeout(5)
def test_reused_child_carries_no_context_of_the_previous_job(events: list[Any]) -> None:
    import multiprocessing as mp

    _forkserver_root()
    _register_context_task()
    file_mode = {"log_config": "/does/not/exist.json", "log_config_json": _FILE_CONFIG}
    job_r, job_w = mp.Pipe(duplex=False)
    for item in (_job("j1", workflow_id=_WF_ID, retry_count=4, **file_mode), _job("j2", **file_mode), None):
        job_w.send(item)

    child.child_loop(0, job_r, _Channel(events))

    first, second = _ctx_records()
    assert (first.__dict__["task_id"], first.__dict__["workflow_id"], first.__dict__["attempt"]) == ("j1", _WF_ID, 5)
    assert (second.__dict__["task_id"], second.__dict__["attempt"]) == ("j2", 1)
    assert "workflow_id" not in second.__dict__
    assert second.__dict__["compute_child"] == os.getpid()
    job_r.close()
    job_w.close()


class _BlockedStderr(io.StringIO):
    """stderr auf einer vollen Pipe: ``write`` haengt bis ``opened``."""

    def __init__(self) -> None:
        super().__init__()
        self.opened = threading.Event()

    def write(self, text: str) -> int:
        self.opened.wait()
        return super().write(text)


@pytest.mark.timeout(5)
def test_a_blocked_stderr_delays_the_result_at_most_by_the_drain_timeout(
    events: list[Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forkserver_root()
    _register_logging_task()
    stderr = _BlockedStderr()
    monkeypatch.setattr("sys.stderr", stderr)
    monkeypatch.setattr(
        child, "exit_if_log_thread_stuck", lambda code: events.append(("exit", code)), raising=False,
    )
    done = threading.Event()

    def run() -> None:
        child.child_once(_job(log_drain_timeout=0.05), _Channel(events), 0)
        done.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        assert done.wait(2)
        assert "result:t1" in _kinds(events)
        # Das Kind gehoert swarmq: Haengt der Log-Thread, endet es hart.
        assert events[-1] == ("exit", 0)
    finally:
        stderr.opened.set()
        worker.join(1)
