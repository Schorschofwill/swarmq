"""Reload-Zustandsmaschine des Supervisors (Plan 2026-10-06-1811, U8, KTD8).

Alle Tests mit gefaelschter Probe-Fabrik und Uhr, ohne echte Prozesse; die
Tests der echten Probe stehen in
``tests/integration/test_hot_reload_logconfig_e2e.py`` (Review B3).
"""

# Test list (U8):
# - [x] ``request`` setzt nur den ausstehenden Reload, startet keine Probe
# - [x] Ohne ``--log-config``: der naechste Tick beschliesst den Reload
# - [x] Ohne ausstehenden Reload tut ein Tick nichts
# - [x] Mit ``--log-config`` startet ein Tick die Probe mit der geladenen
#       Konfiguration; solange ``poll()`` None liefert, kein Reload, ``busy``
# - [x] Probe gueltig: Reload beschlossen, ``busy`` endet
# - [x] Probe gueltig: ihre Pipe wird geschlossen (sonst ein offener
#       Deskriptor je erfolgreichem Reload im Supervisor)
# - [x] Probe ungueltig: ``log_config_reload_failed`` mit Pfad und Fehler
# - [x] Probe ueberschreitet die Frist: beendet,
#       ``log_config_reload_probe_timeout``, kein ``log_config_reload_failed``
# - [x] Zwei ``request`` waehrend einer Probe: genau eine Probe
# - [x] ``cancel`` storniert einen ausstehenden Reload
# - [x] ``abort`` beendet eine laufende Probe (Shutdown)
# - [x] Datei nicht ladbar: ``log_config_reload_failed`` ohne Probe
# - [x] Probe nicht startbar (OSError): ``log_config_reload_failed``
# - [x] Die echte Probe laeuft in eigener Sitzung (``start_new_session=True``)
# - [x] Die echte Probe meldet einen Handler-Tippfehler als Fehlertext
#       (echter Prozess: in ``tests/integration/test_hot_reload_logconfig_e2e.py``)
# - [x] Die echte Probe wird beim Beenden auch eingesammelt
# - [x] Gueltige Konfiguration: die Pipe der echten Probe wird geschlossen
# - [x] Ohne Fabrik prueft die echte Probe die Datei (ebenda)
# - [x] Requests waehrend einer Probe, die in die Frist laeuft: keine zweite
# Review A6 -- ``reexec``: ein SIGTERM nach der letzten Pruefung geht nicht
# verloren:
# - [x] ``execvpe`` laeuft mit blockiertem SIGTERM und SIGINT (Maske und
#       anhaengiges Signal ueberleben ``execve``)
# - [x] ohne ``execvpe`` (SIGTERM schon da, ``execvpe`` scheitert) ist die
#       Maske danach wieder die alte; ein waehrenddessen eingetroffenes
#       SIGTERM wird dann zugestellt (Rot per Mutation geprueft: die
#       Wiederherstellung kam mit der Obvious Implementation des ersten Tests)
# Plan 0.14, U7 (a) -- der Rueckweg im echten Handler:
# - [x] ein SIGTERM, das ein anderer Thread annimmt, waehrend ``reexec`` die
#       Signale haelt: Der Handler im Hauptthread stellt es dort erneut zu,
#       es ist vor ``execve`` anhaengig (``signal.raise_signal``)

from __future__ import annotations

import json
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from swarmq.process_manager import ProcessManager
from swarmq.process_manager.reload import ReloadController, reexec
from tests.unit.process_manager._helpers import FakeClock as _Clock
from tests.unit.process_manager._helpers import FakeProbe as _Probe
from tests.unit.process_manager._helpers import FakeProbeFactory as _Factory
from tests.unit.process_manager._helpers import _entry_stub

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def log_config(tmp_path):
    path = tmp_path / "log.json"
    path.write_text('{"version": 1}')
    return str(path)


def test_request_only_sets_pending():
    factory = _Factory()
    ctl = ReloadController("/nope.json", probe_factory=factory)
    ctl.request()
    assert ctl.pending is True
    assert factory.raws == []


def test_without_log_config_the_next_tick_accepts_the_reload():
    ctl = ReloadController(None)
    ctl.request()
    assert ctl.step() is True
    assert ctl.pending is False


def test_a_tick_without_pending_reload_does_nothing():
    assert ReloadController(None).step() is False


def test_a_tick_starts_the_probe_and_waits_while_it_runs(log_config):
    factory = _Factory(_Probe(rc=None))
    ctl = ReloadController(log_config, probe_factory=factory, clock=_Clock())
    ctl.request()
    assert ctl.step() is False
    assert [json.loads(raw)["version"] for raw in factory.raws] == [1]
    assert ctl.busy is True
    assert ctl.step() is False
    assert len(factory.raws) == 1


def test_a_valid_probe_accepts_the_reload(log_config):
    probe = _Probe(rc=None)
    ctl = ReloadController(log_config, probe_factory=_Factory(probe), clock=_Clock())
    ctl.request()
    assert ctl.step() is False
    probe.rc = 0
    assert ctl.step() is True
    assert ctl.busy is False


def test_an_accepted_reload_closes_the_probe(log_config):
    probe = _Probe(rc=0)
    ctl = ReloadController(log_config, probe_factory=_Factory(probe), clock=_Clock())
    ctl.request()

    assert ctl.step() is True
    assert probe.closed is True


def test_an_invalid_probe_logs_reload_failed(log_config, swarmq_logs):
    probe = _Probe(rc=1, error="Unable to configure handler 'err'")
    ctl = ReloadController(log_config, probe_factory=_Factory(probe), clock=_Clock())
    ctl.request()
    assert ctl.step() is False
    assert ctl.busy is False
    (event,) = [e for e in swarmq_logs if e["event"] == "log_config_reload_failed"]
    assert event["log_level"] == "error"
    assert event["path"] == log_config
    assert event["error_type"] == "LoggingConfigError"
    assert "Unable to configure handler 'err'" in event["error"]
    assert not event["error"].startswith("LoggingConfigError")
    assert "workers keep running" in event["hint"]


def test_a_probe_past_its_deadline_is_killed_and_reported_as_timeout(log_config, swarmq_logs):
    probe = _Probe(rc=None)
    clock = _Clock()
    ctl = ReloadController(log_config, probe_factory=_Factory(probe), clock=clock, timeout_s=30.0)
    ctl.request()
    assert ctl.step() is False
    clock.now += 29.9
    assert ctl.step() is False
    killed_before_timeout = probe.killed
    assert killed_before_timeout is False
    clock.now += 0.2
    assert ctl.step() is False
    assert probe.killed is True
    assert ctl.busy is False
    events = [e["event"] for e in swarmq_logs]
    assert "log_config_reload_failed" not in events
    (event,) = [e for e in swarmq_logs if e["event"] == "log_config_reload_probe_timeout"]
    assert event["log_level"] == "error"
    assert event["path"] == log_config
    assert event["timeout_s"] == 30.0


def test_requests_during_a_probe_are_coalesced_into_it(log_config):
    probe = _Probe(rc=None)
    factory = _Factory(probe, _Probe(rc=1))
    ctl = ReloadController(log_config, probe_factory=factory, clock=_Clock())
    ctl.request()
    ctl.step()
    ctl.request()
    ctl.request()
    assert ctl.step() is False
    probe.rc = 1
    ctl.step()
    ctl.step()
    assert len(factory.raws) == 1
    assert ctl.busy is False


def test_requests_during_a_timed_out_probe_start_no_second_probe(log_config):
    clock = _Clock()
    factory = _Factory(_Probe(rc=None), _Probe(rc=None))
    ctl = ReloadController(log_config, probe_factory=factory, clock=clock, timeout_s=1.0)
    ctl.request()
    ctl.step()
    ctl.request()
    clock.now += 2.0
    ctl.step()
    ctl.step()
    assert len(factory.raws) == 1


def test_a_pending_reload_counts_as_busy():
    ctl = ReloadController(None)
    assert ctl.busy is False
    ctl.request()
    assert ctl.busy is True


def test_cancel_drops_a_pending_reload():
    ctl = ReloadController(None)
    ctl.request()
    ctl.cancel()
    assert ctl.step() is False
    assert ctl.busy is False


def test_abort_kills_a_running_probe(log_config):
    probe = _Probe(rc=None)
    ctl = ReloadController(log_config, probe_factory=_Factory(probe), clock=_Clock())
    ctl.request()
    ctl.step()
    ctl.abort()
    assert probe.killed is True
    assert ctl.busy is False
    ctl.abort()  # ohne Probe: nichts zu tun


def test_an_unloadable_file_logs_reload_failed_without_a_probe(log_config, swarmq_logs):
    factory = _Factory()
    with Path(log_config).open("w") as fh:
        fh.write('{"version": 1,')
    ctl = ReloadController(log_config, probe_factory=factory, clock=_Clock())
    ctl.request()
    assert ctl.step() is False
    assert factory.raws == []
    assert ctl.busy is False
    (event,) = [e for e in swarmq_logs if e["event"] == "log_config_reload_failed"]
    assert event["path"] == log_config
    assert event["error_type"] == "LoggingConfigError"


def test_a_probe_that_cannot_start_logs_reload_failed(log_config, swarmq_logs):
    def factory(raw: str) -> _Probe:
        raise OSError(24, "Too many open files")

    ctl = ReloadController(log_config, probe_factory=factory, clock=_Clock())
    ctl.request()
    assert ctl.step() is False
    assert ctl.busy is False
    (event,) = [e for e in swarmq_logs if e["event"] == "log_config_reload_failed"]
    assert "Too many open files" in event["error"]


def test_the_real_probe_runs_in_its_own_session():
    """Ctrl-C an die Vordergrund-Prozessgruppe darf die Probe nicht treffen:
    abgebrochen sah sie aus wie eine ungueltige Konfiguration (S6)."""
    from swarmq.logging_file import DictConfigProbe

    with patch("swarmq.logging_file.subprocess.Popen") as popen:
        DictConfigProbe('{"version": 1}')
    assert popen.call_args.kwargs["start_new_session"] is True
    popen.return_value.stdin.write.assert_called_once_with('{"version": 1}')
    popen.return_value.stdin.close.assert_called_once()


def test_a_valid_config_closes_the_real_probes_pipe():
    """``worker --check`` mit gueltiger Konfiguration: die stderr-Pipe der
    Probe wird geschlossen, nicht dem Garbage Collector ueberlassen."""
    from swarmq.logging_file import probe_dict_config

    with patch("swarmq.logging_file.subprocess.Popen") as popen:
        proc = popen.return_value
        proc.wait.return_value = 0
        probe_dict_config("log.json", '{"version": 1}')
    proc.stderr.close.assert_called_once()


def test_killing_the_real_probe_also_reaps_it():
    from swarmq.logging_file import DictConfigProbe

    with patch("swarmq.logging_file.subprocess.Popen") as popen:
        proc = popen.return_value
        DictConfigProbe("{}").kill()
    proc.kill.assert_called_once()
    proc.wait.assert_called_once()


def _blocked() -> set[int | signal.Signals]:
    return set(signal.pthread_sigmask(signal.SIG_BLOCK, []))


@pytest.fixture
def signal_mask():
    """Stellt Maske und SIGTERM-Handler wieder her; ein anhaengiges SIGTERM
    landet beim Freigeben im Rekorder statt den Testprozess zu beenden."""
    received: list[int] = []
    old_handler = signal.signal(signal.SIGTERM, lambda signum, _frame: received.append(signum))
    old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
    try:
        yield received
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
        signal.signal(signal.SIGTERM, old_handler)


def _pm(**kwargs):
    defaults = {
        "workers": {},
        "_reload_requested": True,
        "_shutdown_signal": None,
        "_startup_argv": ["-m", "swarmq.cli"],
        "_startup_env": {},
        "_log_shutdown_signal": lambda: None,
    }
    return SimpleNamespace(**{**defaults, **kwargs})


def test_execvpe_runs_with_sigterm_and_sigint_blocked(signal_mask):
    seen: list[set[int | signal.Signals]] = []
    with (
        patch("swarmq.process_manager.reload.shutdown_logging"),
        patch("swarmq.process_manager.reload.os.execvpe", side_effect=lambda *a: seen.append(_blocked())),
    ):
        reexec(_pm(), 0)

    [mask] = seen
    assert {signal.SIGTERM, signal.SIGINT} <= mask


def test_without_execvpe_the_mask_is_restored_and_a_held_sigterm_delivered(signal_mask):
    before = _blocked()
    pm = _pm(
        _shutdown_signal="SIGTERM",
        # SIGTERM im blockierten Fenster: bleibt anhaengig bis zur Rueckgabe.
        # An diesen Thread, nicht an den Prozess: ein prozessweites Signal
        # kann ein anderer Thread (xdist/execnet) annehmen, dann laeuft der
        # Rekorder erst nach der Pruefung (CI-Pipeline 2963323353). Den
        # Rueckweg im echten Handler (``_reexec_holds_signals``) deckt
        # ``test_a_sigterm_taken_by_another_thread_is_pending_again_before_execve``.
        _log_shutdown_signal=lambda: signal.pthread_kill(threading.get_ident(), signal.SIGTERM),
    )
    with patch("swarmq.process_manager.reload.shutdown_logging"):
        assert reexec(pm, 3) == 3

    assert _blocked() == before
    assert signal_mask == [signal.SIGTERM]


def test_a_failed_execvpe_restores_the_mask(signal_mask):
    before = _blocked()
    with (
        patch("swarmq.process_manager.reload.shutdown_logging"),
        patch("swarmq.process_manager.reload.os.execvpe", side_effect=OSError("gone")),
    ):
        assert reexec(_pm(), 0) == 1

    assert _blocked() == before


@pytest.mark.usefixtures("_restore_default_signals")
def test_a_sigterm_taken_by_another_thread_is_pending_again_before_execve(signal_mask):
    """Das SIGTERM nimmt ein Thread an, fuer den es nicht blockiert ist; der
    Python-Handler laeuft im Hauptthread, waehrend ``reexec`` die Signale
    haelt, und stellt es dort erneut zu: Es ist vor ``execve`` anhaengig.

    Zugestellt wird an Threads, nicht an den Prozess -- unter xdist nimmt
    sonst womoeglich ein fremder Thread das Signal an."""
    pm = ProcessManager(num_workers=0, worker_entry=_entry_stub)
    pm._setup_signals()
    pm._reload_requested = True
    fire = threading.Event()
    sent = threading.Event()

    def other_thread() -> None:
        # Vor dem gesperrten Fenster gestartet: SIGTERM ist hier nicht blockiert.
        fire.wait()
        signal.pthread_kill(threading.get_ident(), signal.SIGTERM)
        sent.set()

    helper = threading.Thread(target=other_thread)
    helper.start()
    at_execve: list[tuple[str | None, bool]] = []

    def execvpe(*_args: object) -> None:
        fire.set()
        assert sent.wait(5)
        deadline = time.monotonic() + 5
        while pm._shutdown_signal is None and time.monotonic() < deadline:
            pass  # der Handler laeuft zwischen zwei Bytecodes des Hauptthreads
        at_execve.append((pm._shutdown_signal, signal.SIGTERM in signal.sigpending()))

    try:
        with (
            patch("swarmq.process_manager.reload.shutdown_logging"),
            patch("swarmq.process_manager.reload.os.execvpe", side_effect=execvpe),
        ):
            reexec(pm, 0)
    finally:
        fire.set()
        helper.join(5)

    assert at_execve == [("SIGTERM", True)]
    # Ohne ``execve`` stellt die alte Maske es zu, der Handler laeuft harmlos.
    assert signal.SIGTERM not in signal.sigpending()
    assert pm._shutdown_signal == "SIGTERM"
