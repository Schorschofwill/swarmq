"""Reload-Zustandsmaschine des Supervisors (Plan 2026-10-06-1811, U8, KTD8).

Alle Tests mit gefaelschter Probe-Fabrik und Uhr, ohne echte Signale.
"""

# Test list (U8):
# - [x] ``request`` setzt nur den ausstehenden Reload, startet keine Probe
# - [x] Ohne ``--log-config``: der naechste Tick beschliesst den Reload
# - [x] Ohne ausstehenden Reload tut ein Tick nichts
# - [x] Mit ``--log-config`` startet ein Tick die Probe mit der geladenen
#       Konfiguration; solange ``poll()`` None liefert, kein Reload, ``busy``
# - [x] Probe gueltig: Reload beschlossen, ``busy`` endet
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
# - [x] Die echte Probe wird beim Beenden auch eingesammelt
# - [x] Ohne Fabrik prueft die echte Probe die Datei
# - [x] Requests waehrend einer Probe, die in die Frist laeuft: keine zweite

from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest

from swarmq.process_manager.reload import ReloadController
from tests.unit.process_manager._helpers import FakeClock as _Clock
from tests.unit.process_manager._helpers import FakeProbe as _Probe
from tests.unit.process_manager._helpers import FakeProbeFactory as _Factory

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
    assert probe.killed is False
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
    with open(log_config, "w") as fh:
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


def test_the_real_probe_reports_a_handler_typo():
    """AE4 beim Reload: Der Tippfehler faellt erst in ``dictConfig`` auf."""
    from swarmq.logging_file import DictConfigProbe

    probe = DictConfigProbe(json.dumps({
        "version": 1,
        "handlers": {"err": {"class": "logging.StreamHandlr"}},
        "root": {"handlers": ["err"]},
    }))
    assert probe.wait(timeout=5) == 1
    assert probe.poll() == 1
    assert "StreamHandlr" in probe.error()


def test_killing_the_real_probe_also_reaps_it():
    from swarmq.logging_file import DictConfigProbe

    with patch("swarmq.logging_file.subprocess.Popen") as popen:
        proc = popen.return_value
        DictConfigProbe("{}").kill()
    proc.kill.assert_called_once()
    proc.wait.assert_called_once()


def test_by_default_the_real_probe_checks_the_file(log_config):
    ctl = ReloadController(log_config)
    ctl.request()
    deadline = time.monotonic() + 5
    while not ctl.step():
        assert ctl.busy and time.monotonic() < deadline
        time.sleep(0.01)
