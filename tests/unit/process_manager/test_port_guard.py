"""Beobachtungsports im Mehrprozess-Betrieb.

Plan 2026-08-28-001 U4 (R5-R7) fuehrte hier einen Startguard ein: ein fester
Port bei N Prozessen wurde abgelehnt, weil das erste Kind band und die
uebrigen scheiterten, bis das Restart-Budget aufgebraucht war.

Plan 2026-09-08-001 U3 (R5, R6) loest das Problem statt es zu verbieten: der
Port ist ein Basisport, Slot s bindet ``basis + s``. Der Guard wird damit zur
Bereichspruefung — er lehnt nur noch ab, was auch mit Versatz nicht geht:
ein Bereich ueber 65535 hinaus, oder zwei Bereiche, die sich ueberschneiden.
"""

import os

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.process_manager.check import check_worker_config

_MODULE = "swarmq"


def _check(**kwargs):
    defaults = {"module": _MODULE, "reload": False, "watch_dir": None}
    return check_worker_config(**{**defaults, **kwargs})


class TestFixedPortWithManyProcessesIsAllowedNow:
    """Bewusst geaenderte Tests (U3): was der Startguard ablehnte, ist jetzt
    der Normalfall — jeder Slot bekommt seinen eigenen Port."""

    def test_metrics_port_with_four_processes_passes(self):
        result = _check(processes=4, metrics_port=9000)
        assert result["metrics_ports"] == [9000, 9003]

    def test_health_port_with_four_processes_passes(self):
        result = _check(processes=4, health_port=8080)
        assert result["health_ports"] == [8080, 8083]

    def test_autoscale_reserves_the_upper_bound(self):
        """KTD7 bleibt gueltig: der Bereich muss fuer das Hochskalieren
        reichen, sonst kollidiert es mitten im Betrieb."""
        result = _check(autoscale=(1, 8), metrics_port=9000)
        assert result["metrics_ports"] == [9000, 9007]

    def test_single_process_reports_a_range_of_one(self):
        result = _check(processes=1, metrics_port=9000)
        assert result["metrics_ports"] == [9000, 9000]

    def test_without_ports_the_ranges_are_absent(self):
        result = _check(processes=4)
        assert result["metrics_ports"] is None
        assert result["health_ports"] is None


class TestRejectsRangesThatCannotWork:
    def test_overlapping_ranges_are_rejected(self):
        """R6: metrics 9000-9003 und health 9002-9005 ueberschneiden sich —
        zwei Slots wollten denselben Port binden (Covers AE2)."""
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=9000, health_port=9002)

        message = str(excinfo.value)
        assert "9002" in message
        assert "--health-port" in message or "health" in message

    def test_adjacent_ranges_are_fine(self):
        result = _check(processes=4, metrics_port=9000, health_port=9004)
        assert result["metrics_ports"] == [9000, 9003]
        assert result["health_ports"] == [9004, 9007]

    def test_range_beyond_the_upper_port_bound_is_rejected(self):
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=65534)

        assert "65535" in str(excinfo.value)

    def test_environment_ports_count_too(self, monkeypatch):
        """Die Konfiguration uebernimmt den Umgebungswert bedingungslos — ein
        Guard auf den Kommandozeilenargumenten allein liesse diesen Start
        durch."""
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")

        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, health_port=9001)

        assert "SWARMQ_METRICS_PORT" in str(excinfo.value)

    def test_cli_port_wins_over_the_environment(self, monkeypatch):
        """CLI > Env: die Option ist der Basisport, auch im Worker -- der Guard
        prueft also den Bereich der Option, nicht den der Umgebung."""
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")

        result = _check(processes=2, metrics_port=8000)

        assert result["metrics_ports"] == [8000, 8001]


@pytest.mark.entscheidung("E-062")
class TestPreflightCommandAgrees:
    """``swarmq worker --check`` sagt zu, dass ein erfolgreiches Ergebnis den
    Pre-Flight des Supervisors impliziert."""

    def _run_check(self, capsys, **overrides):
        import argparse
        import json

        from swarmq.cli.check_cmd import _run_worker_check

        fields = {
            "module": _MODULE,
            "reload": False,
            "watch_dir": None,
            "burst": False,
            "processes": 1,
            "autoscale": None,
            "metrics_port": None,
            "health_port": None,
        }
        fields.update(overrides)
        args = argparse.Namespace(**fields)
        code = _run_worker_check(args)
        return code, json.loads(capsys.readouterr().out.strip())

    def test_payload_names_both_ranges(self, capsys):
        """R7: der Betreiber liest die Prometheus-Targets direkt aus dem
        Payload ab, statt sie im Kopf auszurechnen."""
        code, payload = self._run_check(capsys, processes=4, metrics_port=9000, health_port=9100)

        assert code == 0
        assert payload["metrics_ports"] == [9000, 9003]
        assert payload["health_ports"] == [9100, 9103]

    def test_checks_the_effective_values_cli_over_environment(self, capsys, monkeypatch):
        """--check prueft, was der Worker bekommt: ohne Flag das Env, mit
        Flag die Option (CLI > Env > Default)."""
        monkeypatch.setenv("SWARMQ_CONCURRENCY", "5")
        monkeypatch.setenv("SWARMQ_HEALTH_PORT", "9500")

        _, from_env = self._run_check(capsys, processes=2, concurrency=None, pool=None)
        _, from_flag = self._run_check(
            capsys,
            processes=2,
            concurrency=20,
            pool=None,
            health_port=8000,
        )

        assert (from_env["slots"], from_env["health_ports"]) == (5, [9500, 9501])
        assert (from_flag["slots"], from_flag["health_ports"]) == (20, [8000, 8001])

    def test_environment_port_is_named_in_the_error(self, capsys, monkeypatch):
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")

        _, payload = self._run_check(capsys, processes=4, health_port=9001)

        assert "SWARMQ_METRICS_PORT" in payload["error"]

    def test_reports_the_overlap_instead_of_ok(self, capsys):
        code, payload = self._run_check(capsys, processes=4, metrics_port=9000, health_port=9002)

        assert code != 0
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"


class TestRealStartPathEnforcesIt:
    """Die Pruefung sitzt in ``check_worker_config``, aber der echte Startpfad
    muss sie auch aufrufen — ein vergessenes Argument in der Weitergabe bliebe
    sonst unbemerkt."""

    def _args(self, **over):
        import argparse

        ns = argparse.Namespace(
            module="json",
            queues=["default"],
            concurrency=1,
            log_level="INFO",
            log_format="human",
            burst=False,
            burst_idle_timeout=5.0,
            priorities=False,
            metrics_port=None,
            health_port=None,
            processes=1,
            autoscale=None,
            reload=False,
            watch_dir=None,
        )
        for k, v in over.items():
            setattr(ns, k, v)
        return ns

    def test_start_path_rejects_an_overlapping_range(self):
        from swarmq.process_manager.run import run_multi_process_worker

        with pytest.raises(ConfigurationError):
            run_multi_process_worker(self._args(processes=4, metrics_port=9000, health_port=9002))

    def test_start_path_names_the_environment_port(self, monkeypatch):
        """Nach der CLI-Aufloesung steht der Env-Port in ``args``; die Meldung
        nennt trotzdem die Variable, aus der er kam."""
        from swarmq.cli.worker_options import resolve_worker_options
        from swarmq.process_manager.run import run_multi_process_worker

        monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://x")
        monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://x")
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")
        args = self._args(processes=4, health_port=9001)
        resolve_worker_options(args, os.environ)

        with pytest.raises(ConfigurationError, match="SWARMQ_METRICS_PORT"):
            run_multi_process_worker(args)


class TestHeadroomWidensTheRanges:
    """Kontrollkanal U4 (R4): der Spielraum stellt die Obergrenze der Portbereiche."""

    def test_base_ports_too_close_for_the_headroom_are_rejected(self):
        _check(processes=2, metrics_port=9000, health_port=9008)  # ohne Spielraum reicht der Abstand
        with pytest.raises(ConfigurationError, match="16 worker"):
            _check(processes=2, max_processes=16, metrics_port=9000, health_port=9008)

    def test_ranges_report_the_headroom(self):
        result = _check(processes=2, max_processes=16, metrics_port=9000, health_port=9100)
        assert result["metrics_ports"] == [9000, 9015]
        assert result["health_ports"] == [9100, 9115]


class TestRangeBoundariesAreExact:
    """Testlücke T-F-19 (Runde 4): die Grenzwerte des Bereichs-Guards genau
    getroffen; bisher lagen alle Fälle mindestens einen Port daneben.

    Test-Liste:
    - [x] ein Bereich, der genau bei 65535 endet, ist erlaubt
    - [x] läuft nur der zweite Bereich über 65535, wird er trotzdem abgelehnt
    - [x] zwei Bereiche, die sich in genau einem Port berühren, überschneiden sich
    """

    def test_range_ending_exactly_at_the_upper_port_bound_passes(self):
        """T-F-19, Mutant ``check.py:214`` ``rng[1] <= _MAX_TCP_PORT`` → ``<``."""
        result = _check(processes=4, metrics_port=65532)

        assert result["metrics_ports"] == [65532, 65535]

    def test_health_range_is_checked_after_a_valid_metrics_range(self):
        """T-F-19, Mutant ``check.py:215`` ``continue`` → ``break``: nach dem
        gültigen Metrics-Bereich bliebe der Health-Bereich ungeprüft."""
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=9000, health_port=65534)

        assert "65534-65537" in str(excinfo.value)
        assert "--health-port=65534" in str(excinfo.value)

    def test_ranges_sharing_their_last_and_first_port_overlap(self):
        """T-F-19, Mutant ``check.py:224`` ``health_range[0] <= metrics_range[1]``
        → ``<``: metrics 9000-9003 und health 9003-9006 teilen Port 9003."""
        with pytest.raises(ConfigurationError, match="overlap"):
            _check(processes=4, metrics_port=9000, health_port=9003)
