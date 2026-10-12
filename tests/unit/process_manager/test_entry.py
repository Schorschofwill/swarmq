"""Subprozess-Entry-Tests (Plan U21) — verbatim."""

from __future__ import annotations

# Test-Liste U7 (Plan 2026-10-06-0831, Worker-Subprozess):
# - [x] ``--no-configure-logging``: Root-Handler bleiben unangetastet (R12)
# - [x] Die Settings kommen aufgeloest an: ``configure_logging`` vor dem
#       App-Import, Env im Kind wirkt nicht; die Namespace traegt das Objekt
# - [x] ``RecycleAckTimeout``: strukturierte Zeile statt Rohtext, dann Exit 75
# - [x] Nach dem Ende ist die Log-Queue geleert (``shutdown_logging``)
# - [x] Review-Rest: scheitert ``configure_logging`` erst im Kind, endet es
#       mit Exit 78 und ``worker_config_error`` statt Exit 1 und Neustartschleife
# - [x] ... und die Zeile erscheint ueber das eingebaute Logging mit Fehlertext
# - [x] Plan 2026-10-06-1811, U4: nach dem begrenzten Leeren endet der Prozess
#       hart mit seinem Exit-Code, falls ein Log-Thread haengt
#       (``exit_if_log_thread_stuck`` als letzter Schritt)
# - [x] ... der Rueckfall nach einer kaputten dictConfig behaelt die Frist
#       ``log_drain_timeout`` aus den Settings
# - [x] Schluss-Review: ``ForkSourceRejected`` (der CPU-Pool hat seine
#       Kindquelle verworfen, der Worker ist geordnet beendet) endet mit
#       Exit 71, ohne Traceback; das ERROR-Event schrieb schon der Worker
# Restpunkte (Release 0.9.0):
# - [x] Endet der Worker an einer Ausnahme, bekommt das harte Ende sie
#       mit (``exc``): Traceback bzw. ``SystemExit``-Meldung auf fd 2 wie in
#       ``console_main``
# Plan 2026-10-12-1105, MR 5 (P1, Review P0-2):
# - [x] ``WorkerConfig.scheduler="off"`` landet in der Kind-Namespace und
#       ergibt dort ``SwarmQConfig.scheduler == "off"``, auch wenn im Kind
#       ``SWARMQ_SCHEDULER=on`` gesetzt ist
import os
from typing import Any, override

import pytest

from swarmq.process_manager import (
    worker_subprocess_entry,
)
from tests.unit.process_manager._helpers import _make_test_cfg


class TestWorkerSubprocessEntry:
    """ce-review T-04: cover the spawn-boundary entry + shutdown watcher.

    The function runs inside a subprocess in production; the unit test
    drives the same code path in-process with the cross-process Event
    swapped for ``threading.Event`` (duck-type compatible).
    """

    def test_shutdown_event_triggers_sigterm_on_self(self, monkeypatch):
        """Worker ignores the event (simulating a stuck worker) — the
        watcher must still deliver SIGTERM so the Worker's own signal
        handler can drive graceful shutdown."""
        import asyncio
        import threading

        shutdown_event = threading.Event()
        kills: list[tuple[int, int]] = []
        worker_exited = threading.Event()

        def fake_kill(pid, sig):
            kills.append((pid, sig))
            # Simulate SIGTERM causing the worker to exit.
            worker_exited.set()

        async def fakerun_worker_async(ns):
            # Model a non-cooperative worker that does NOT consult
            # shutdown_event. Exit only when SIGTERM is "delivered".
            while not worker_exited.is_set():  # noqa: ASYNC110 - pollt ein threading.Event, das der Kill-Pfad aus einem anderen Thread setzt
                await asyncio.sleep(0.02)

        monkeypatch.setattr("swarmq.process_manager.supervisor.os.kill", fake_kill)
        monkeypatch.setattr(
            "swarmq.cli.run_worker_async",
            fakerun_worker_async,
        )

        timer = threading.Timer(0.1, shutdown_event.set)
        timer.start()
        try:
            worker_subprocess_entry(
                worker_id=0,
                shutdown_event=shutdown_event,
                cfg=_make_test_cfg(),
            )
        finally:
            timer.cancel()

        import signal as _signal

        assert kills, "shutdown watcher never called os.kill"
        pid, sig = kills[0]
        assert pid == os.getpid()
        assert sig == _signal.SIGTERM

    def test_keyboard_interrupt_returns_cleanly(self, monkeypatch):
        """KeyboardInterrupt from inside the worker must not propagate
        out of the entry — otherwise multiprocessing logs a traceback."""
        import threading

        async def raise_kbd(ns):
            raise KeyboardInterrupt

        monkeypatch.setattr("swarmq.cli.run_worker_async", raise_kbd)

        # Should return None, not raise.
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            cfg=_make_test_cfg(),
        )

    def test_reload_and_watch_dirs_stripped_from_namespace(self, monkeypatch):
        """R5.3 ce-review #18: reload/watch_dirs are supervisor-only.
        The subprocess must never see them on its Namespace — otherwise
        a future `if ns.reload: ...` inside run_worker_async would
        spawn N watchers, one per worker, each killing its own PID."""
        import threading

        captured: dict[str, Any] = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)

        cfg = _make_test_cfg(reload=True, watch_dirs=("/tmp",))
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            cfg=cfg,
        )

        ns = captured["ns"]
        assert not hasattr(ns, "reload"), (
            "reload leaked to worker Namespace — kwargs.pop('reload') missing in worker_subprocess_entry"
        )
        assert not hasattr(ns, "watch_dirs"), "watch_dirs leaked to worker Namespace"

    def test_slot_reaches_the_worker_namespace(self, monkeypatch):
        """U2 (R3/KTD2): Der Supervisor kennt den Slot bereits und uebergibt
        ihn dem Kind — das Kind verwarf ihn bisher und nannte sich nach seiner
        Speicheradresse."""
        import threading

        captured: dict[str, Any] = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)

        worker_subprocess_entry(
            worker_id=2,
            shutdown_event=threading.Event(),
            cfg=_make_test_cfg(),
        )

        assert captured["ns"].worker_slot == 2

    def test_scheduler_switch_beats_the_child_environment(self, monkeypatch):
        import threading

        from swarmq.cli.worker_options import resolve_worker_options, worker_config_kwargs
        from swarmq.config import SwarmQConfig

        captured: dict[str, Any] = {}

        async def capture_ns(ns):
            captured["ns"] = ns

        monkeypatch.setattr("swarmq.cli.run_worker_async", capture_ns)
        worker_subprocess_entry(
            worker_id=0,
            shutdown_event=threading.Event(),
            cfg=_make_test_cfg(scheduler="off"),
        )

        ns = captured["ns"]
        assert ns.scheduler == "off"
        monkeypatch.setenv("SWARMQ_SCHEDULER", "on")
        resolve_worker_options(ns, os.environ)
        config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **worker_config_kwargs(ns))  # type: ignore[arg-type]  # kwargs aus der Namespace
        assert config.scheduler == "off"


def _capture_entry_logs(monkeypatch) -> list[dict[str, Any]]:
    """Faengt die Logsaetze des Entry ab.

    ``structlog.testing.capture_logs`` hilft hier nicht: das Entry ruft
    ``setup_logging`` und konfiguriert structlog dabei neu, womit die
    Capture-Kette wieder verschwindet. Das Format prueft stattdessen der
    Integrationstest mit echtem Subprozess (AE8/AE9).
    """
    from swarmq.process_manager import entry as entry_module

    events: list[dict[str, Any]] = []

    class _Recorder:
        def error(self, event, **kwargs):
            events.append({"event": event, **kwargs})

        def __getattr__(self, _name):
            return lambda *a, **kw: None

    monkeypatch.setattr(entry_module, "logger", _Recorder())
    return events


class TestConfigErrorExitCode:
    """R29 (Plan 2026-09-03-001): Konfigurationsfehler enden mit Exit 78."""

    def test_configuration_error_exits_with_config_code(self, monkeypatch):
        """Bewusst geaenderter Test (U4/R21): der Fehlertext erscheint als
        strukturierter Logsatz ``worker_config_error`` im konfigurierten
        Format, nicht als rohe stderr-Zeile. Eine rohe Zeile war in einer
        JSON-Logpipeline eine kaputte Zeile -- gerade im Fehlerfall, wo sie
        gelesen werden muss."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        async def boom(ns):
            raise ConfigurationError("FromDishka annotation(s) that never inject: t: run.db")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == CONFIG_ERROR_EXIT_CODE == 78
        entry = next(e for e in events if e["event"] == "worker_config_error")
        assert entry["source"] == "worker"
        assert "never inject" in entry["error"]

    def test_import_error_in_the_child_is_a_configuration_error(self, monkeypatch):
        """R19: Eine fehlende Abhaengigkeit im Task-Modul endete mit Traceback
        und fuenf Neustarts -- der Supervisor sah einen Absturz, nicht einen
        Konfigurationsfehler."""
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        async def boom(ns):
            raise ModuleNotFoundError("No module named 'pandas'", name="pandas")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == CONFIG_ERROR_EXIT_CODE
        entry = next(e for e in events if e["event"] == "worker_config_error")
        assert "pandas" in entry["error"]

    def test_config_error_carries_error_type_like_the_supervisor(self, monkeypatch, swarmq_logs):
        """R17: Fehler erscheinen als ``error_type`` + ``error`` -- die
        Kind-Variante von ``worker_config_error`` muss dieselben Felder
        tragen wie die des Supervisors."""
        from swarmq.exceptions import ConfigurationError

        async def boom(ns):
            raise ConfigurationError("no tasks registered")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit):
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        entry = next(e for e in swarmq_logs if e["event"] == "worker_config_error")
        assert entry["error_type"] == "ConfigurationError"
        assert entry["error"] == "ConfigurationError: no tasks registered"


def _broken_log_config_settings(**kwargs):
    """Settings, deren dictConfig erst beim Anwenden im Kind scheitert."""
    from swarmq.logging_settings import LoggingSettings

    return LoggingSettings(
        log_config="/abs/log.json",
        log_config_json=(
            '{"version": 1, "disable_existing_loggers": false, "handlers": {"h": {"class": "nope.Missing"}}}'
        ),
        **kwargs,
    )


class TestLoggingConfigErrorInTheChild:
    """Review-Rest: ``configure_logging`` lag ausserhalb der Exit-78-Abbildung.
    Ein ``LoggingConfigError``, der erst im Kind auftrat, endete mit Exit 1 --
    der Supervisor startete den Worker dann in einer Schleife neu."""

    @staticmethod
    def _isolate_dict_config(monkeypatch):
        import logging
        import weakref

        # dictConfig schliesst alle bekannten Handler -- auch die von pytest.
        monkeypatch.setattr(logging, "_handlerList", [])
        monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())

    def test_exits_with_config_code_and_reports_the_error(self, monkeypatch):
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        async def must_not_run(ns):
            raise AssertionError("App-Import trotz kaputter Logging-Konfiguration")

        self._isolate_dict_config(monkeypatch)
        monkeypatch.setattr("swarmq.cli.run_worker_async", must_not_run)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(
                0,
                shutdown_event,
                _make_test_cfg(logging_settings=_broken_log_config_settings()),
            )

        assert exc_info.value.code == CONFIG_ERROR_EXIT_CODE
        entry = next(e for e in events if e["event"] == "worker_config_error")
        assert entry["error_type"] == "LoggingConfigError"
        assert "/abs/log.json" in entry["error"]

    def test_error_line_is_rendered_by_the_builtin_fallback(self, monkeypatch, capfd):
        """Ohne Rueckfall gab ``logging.lastResort`` nur ``worker_config_error``
        aus -- ohne Pfad und Ursache, also ohne den Grund des Abbruchs."""
        self._isolate_dict_config(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(SystemExit):
            worker_subprocess_entry(
                0,
                shutdown_event,
                _make_test_cfg(logging_settings=_broken_log_config_settings()),
            )

        line = next(line for line in capfd.readouterr().err.splitlines() if "worker_config_error" in line)
        assert "LoggingConfigError" in line
        assert "/abs/log.json" in line


class TestHeartbeatWriter:
    """R21/KTD10: der Entry schreibt das Lebenszeichen aus dem Event-Loop."""

    def test_channel_receives_beats_and_is_exposed(self, monkeypatch):
        import asyncio
        import multiprocessing

        from swarmq import supervision as sv

        ch = sv.SupervisorChannel.create(multiprocessing.get_context("spawn"))
        seen: list[object] = []

        async def fake_run(ns):
            seen.append(sv.current_channel())
            await asyncio.sleep(0.05)

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        worker_subprocess_entry(0, shutdown_event, _make_test_cfg(heartbeat_interval_s=0.01), ch)
        assert ch.last_beat() > 0.0
        assert seen == [ch]

    def test_without_channel_nothing_is_written(self, monkeypatch):
        from swarmq import supervision as sv

        async def fake_run(ns):
            return None

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert sv.current_channel() is None


class TestRecycleAckTimeoutExitCode:
    def test_missing_ack_exits_with_tempfail_code(self, monkeypatch, capsys):
        from swarmq.supervision import RECYCLE_NOACK_EXIT_CODE, RecycleAckTimeout

        async def boom(ns):
            raise RecycleAckTimeout("no ack")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        events = _capture_entry_logs(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == RECYCLE_NOACK_EXIT_CODE == 75
        # Bewusst geaendert (U7, R15): strukturierte Zeile statt Rohtext.
        assert "swarmq worker:" not in capsys.readouterr().err
        entry = next(e for e in events if e["event"] == "worker_recycle_ack_timeout")
        assert entry["error_type"] == "RecycleAckTimeout"
        assert "no ack" in entry["error"]


class TestForkSourceRejectedExitCode:
    def test_a_rejected_fork_source_exits_with_71(self, monkeypatch, capsys):
        from swarmq.supervision import FORK_SOURCE_REJECTED_EXIT_CODE, ForkSourceRejected

        async def rejected(ns):
            raise ForkSourceRejected("compute fork source rejected: 2 threads")

        monkeypatch.setattr("swarmq.cli.run_worker_async", rejected)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with pytest.raises(SystemExit) as exc_info:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert exc_info.value.code == FORK_SOURCE_REJECTED_EXIT_CODE == 71
        assert "Traceback" not in capsys.readouterr().err


class TestInheritedSignalMask:
    def test_entry_unblocks_sigterm_inherited_from_supervisor(self, monkeypatch):
        """Der Supervisor blockiert SIGTERM waehrend des Spawns; das Kind muss die
        Maske aufheben, sonst kommt sein eigenes SIGTERM nie an."""
        import signal

        async def fake_run(ns):
            return None

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        before = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
        try:
            assert signal.SIGTERM in signal.pthread_sigmask(signal.SIG_BLOCK, set())
            shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
            assert signal.SIGTERM not in signal.pthread_sigmask(signal.SIG_BLOCK, set())
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, before)


class TestEntryLogging:
    """U7 (KTD4): Das Entry richtet das Logging genau einmal ein, vor dem
    App-Import, aus den aufgeloesten Settings des Supervisors."""

    def test_no_configure_logging_leaves_root_untouched(self, monkeypatch):
        import logging

        from swarmq.logging_settings import LoggingSettings

        async def fake_run(ns):
            return None

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        app_handler = logging.NullHandler()
        root = logging.getLogger()
        root.addHandler(app_handler)
        before = (root.handlers[:], root.level)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        worker_subprocess_entry(
            0,
            shutdown_event,
            _make_test_cfg(logging_settings=LoggingSettings(configure=False)),
        )

        assert (root.handlers[:], root.level) == before

    def test_resolved_settings_win_over_the_child_env(self, monkeypatch):
        """KTD7: Das Kind liest kein Env mehr; die Namespace traegt das
        Settings-Objekt (kein Dict aus ``dataclasses.asdict``)."""
        import logging

        from swarmq.logging_settings import LoggingSettings

        seen: dict[str, Any] = {}

        async def fake_run(ns):
            seen["level"] = logging.getLogger().level
            seen["settings"] = ns.logging_settings

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        monkeypatch.setenv("SWARMQ_LOG_LEVEL", "DEBUG")
        settings = LoggingSettings(level="WARNING", level_explicit=True)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        worker_subprocess_entry(0, shutdown_event, _make_test_cfg(logging_settings=settings))

        assert seen == {"level": logging.WARNING, "settings": settings}

    def test_config_error_reaches_the_dictconfig_handlers(self, monkeypatch, swarmq_logs):
        """R11: Mit eigener dictConfig bleibt structlog unkonfiguriert (KTD6);
        ``worker_config_error`` muss trotzdem ueber das Standard-``logging``
        laufen, nicht ueber structlogs Default-Ausgabe auf stdout."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.logging_settings import LoggingSettings

        async def boom(ns):
            raise ConfigurationError("no tasks registered")

        import logging
        import weakref

        # dictConfig schliesst alle bekannten Handler -- auch die von pytest.
        monkeypatch.setattr(logging, "_handlerList", [])
        monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        # Wie ``load_log_config`` sie liefert; ohne den Schluessel schaltete
        # dictConfig alle bestehenden Logger stumm -- auch fuer Folgetests.
        settings = LoggingSettings(
            log_config="/abs/log.json",
            log_config_json='{"version": 1, "disable_existing_loggers": false}',
        )
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(SystemExit):
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg(logging_settings=settings))

        assert [e["source"] for e in swarmq_logs if e["event"] == "worker_config_error"] == ["worker"]

    def test_log_queue_is_written_when_the_entry_returns(self, monkeypatch):
        """R15: Beim regulaeren Ende ist alles geschrieben und kein Listener
        laeuft mehr."""
        import io
        import sys
        import threading

        from swarmq.logging import LISTENER_THREAD_NAME, get_logger

        async def fake_run(ns):
            get_logger("swarmq.tests.entry").error("evt_before_exit")

        monkeypatch.setattr("swarmq.cli.run_worker_async", fake_run)
        stderr = io.StringIO()
        monkeypatch.setattr(sys, "stderr", stderr)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        worker_subprocess_entry(0, shutdown_event, _make_test_cfg())

        assert "evt_before_exit" in stderr.getvalue()
        assert not [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]


class TestHardExitAfterTheDrain:
    """Plan 2026-10-06-1811, U4 (KTD3): Der Worker-Subprozess gehoert swarmq.
    Haengt nach dem Leeren noch ein Log-Thread, endet er per ``os._exit``
    mit dem Code, mit dem er ohnehin endete -- sonst bricht die
    Finalisierung ab (Exit 134) oder haengt."""

    @staticmethod
    def _record_order(monkeypatch) -> list[object]:
        calls: list[object] = []
        monkeypatch.setattr(
            "swarmq.process_manager.entry.shutdown_logging",
            lambda *a, **k: calls.append("shutdown_logging"),
        )
        monkeypatch.setattr(
            "swarmq.process_manager.entry.exit_if_log_thread_stuck",
            lambda code, exc=None: calls.append(("exit_if_log_thread_stuck", code)),
        )
        return calls

    def test_an_unexpected_error_is_handed_to_the_hard_exit(self, monkeypatch):
        async def boom(ns):
            raise RuntimeError("kaputt")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        _capture_entry_logs(monkeypatch)
        monkeypatch.setattr("swarmq.process_manager.entry.shutdown_logging", lambda *a, **k: None)
        seen: list[tuple[int, BaseException | None]] = []
        monkeypatch.setattr(
            "swarmq.process_manager.entry.exit_if_log_thread_stuck",
            lambda code, exc=None: seen.append((code, exc)),
        )
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(RuntimeError, match="kaputt") as raised:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())

        assert seen == [(1, raised.value)]

    def test_a_config_error_hands_its_exit_code_to_the_hard_exit(self, monkeypatch):
        from swarmq.exceptions import ConfigurationError

        async def boom(ns):
            raise ConfigurationError("broken")

        monkeypatch.setattr("swarmq.cli.run_worker_async", boom)
        _capture_entry_logs(monkeypatch)
        calls = self._record_order(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(SystemExit):
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())

        assert calls[-2:] == ["shutdown_logging", ("exit_if_log_thread_stuck", 78)]

    def test_a_broken_logging_config_in_the_child_hands_78_to_the_hard_exit(self, monkeypatch):
        async def must_not_run(ns):
            raise AssertionError("App-Import trotz kaputter Logging-Konfiguration")

        TestLoggingConfigErrorInTheChild._isolate_dict_config(monkeypatch)
        monkeypatch.setattr("swarmq.cli.run_worker_async", must_not_run)
        _capture_entry_logs(monkeypatch)
        calls = self._record_order(monkeypatch)
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()

        with pytest.raises(SystemExit):
            worker_subprocess_entry(
                0,
                shutdown_event,
                _make_test_cfg(logging_settings=_broken_log_config_settings()),
            )

        assert calls[-2:] == ["shutdown_logging", ("exit_if_log_thread_stuck", 78)]

    def test_the_fallback_after_a_broken_logging_config_keeps_the_drain_timeout(
        self,
        monkeypatch,
    ):
        import io
        import sys
        import threading

        class BlockedStderr(io.StringIO):
            opened = threading.Event()

            @override
            def write(self, text: str) -> int:
                self.opened.wait()
                return super().write(text)

        TestLoggingConfigErrorInTheChild._isolate_dict_config(monkeypatch)
        stderr = BlockedStderr()
        monkeypatch.setattr(sys, "stderr", stderr)
        monkeypatch.setattr(
            "swarmq.process_manager.entry.exit_if_log_thread_stuck",
            lambda code, exc=None: None,
        )
        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        settings = _broken_log_config_settings(drain_timeout=0.05)
        ended = threading.Event()

        def run() -> None:
            try:
                worker_subprocess_entry(0, shutdown_event, _make_test_cfg(logging_settings=settings))
            except SystemExit:
                ended.set()

        threading.Thread(target=run, daemon=True).start()
        try:
            assert ended.wait(2)
        finally:
            stderr.opened.set()
