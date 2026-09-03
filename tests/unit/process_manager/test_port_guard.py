"""Startguard gegen Mehrprozess-Betrieb mit festem Beobachtungs-Port.

Plan 2026-08-28-001, U4 (R5, R6, R7).

Bisher band das erste Kind den Port, die uebrigen scheiterten beim Start
mit einem Traceback, und der Supervisor startete sie nach — bis das
Restart-Budget aufgebraucht war. Danach lief der Worker dauerhaft mit
weniger Prozessen als angefordert weiter: Die angeforderte Topologie wurde
nie erreicht, ohne dass der Start je scheiterte, und keine Meldung nannte
die Portkollision als Ursache.
"""

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.process_manager.check import check_worker_config

_MODULE = "swarmq"


def _check(**kwargs):
    defaults = {"module": _MODULE, "reload": False, "watch_dir": None}
    return check_worker_config(**{**defaults, **kwargs})


class TestRejectsMultiProcessWithFixedPort:
    def test_processes_with_metrics_port_from_cli(self):
        """Covers AE3."""
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=9000)

        message = str(excinfo.value)
        assert "--metrics-port" in message
        assert "9000" in message

    def test_processes_with_health_port_from_cli(self):
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, health_port=8080)

        assert "--health-port" in str(excinfo.value)

    def test_both_ports_are_named(self):
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=2, metrics_port=9000, health_port=8080)

        message = str(excinfo.value)
        assert "--metrics-port" in message
        assert "--health-port" in message

    def test_metrics_port_from_environment(self, monkeypatch):
        """Covers AE3. Die Konfiguration uebernimmt den Umgebungswert
        bedingungslos — ein Guard auf den Kommandozeilenargumenten allein
        liesse genau diesen Start durch."""
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9000")

        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4)

        message = str(excinfo.value)
        assert "SWARMQ_METRICS_PORT" in message
        assert "9000" in message

    def test_health_port_from_environment(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_HEALTH_PORT", "8080")

        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4)

        assert "SWARMQ_HEALTH_PORT" in str(excinfo.value)

    def test_environment_wins_over_the_flag_in_the_message(self, monkeypatch):
        """Dieselbe Vorrangregel wie in der Konfiguration: die Umgebung
        schlaegt die Option, und die Meldung nennt die gewinnende Quelle."""
        monkeypatch.setenv("SWARMQ_METRICS_PORT", "9100")

        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=9000)

        message = str(excinfo.value)
        assert "SWARMQ_METRICS_PORT" in message
        assert "9100" in message

    def test_message_names_the_way_out(self):
        """R7: nicht nur was falsch ist, sondern was stattdessen geht."""
        with pytest.raises(ConfigurationError) as excinfo:
            _check(processes=4, metrics_port=9000)

        message = str(excinfo.value)
        assert "--processes 1" in message or "einen Prozess" in message.lower()


class TestAutoscaleUsesTheUpperBound:
    def test_range_above_one_is_rejected_even_when_starting_at_one(self):
        """KTD7: Bei Untergrenze 1 startet der Lauf sauber und kollidiert
        erst beim ersten Hochskalieren — mitten im Betrieb, wo die Ursache
        noch schwerer zuzuordnen ist."""
        with pytest.raises(ConfigurationError):
            _check(autoscale=(1, 4), metrics_port=9000)

    def test_degenerate_range_of_one_is_allowed(self):
        result = _check(autoscale=(1, 1), metrics_port=9000)
        assert result["module"] == _MODULE


class TestPreflightCommandAgrees:
    """``swarmq worker --check`` sagt zu, dass ein erfolgreiches Ergebnis
    den Pre-Flight des Supervisors impliziert. Meldete es "in Ordnung" fuer
    eine Kombination, die der Start danach ablehnt, waere diese Zusage
    gebrochen — und der Operator praeft die Konfiguration vergeblich vor.
    """

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

    def test_reports_the_conflict_instead_of_ok(self, capsys):
        code, payload = self._run_check(capsys, processes=4, metrics_port=9000)

        assert code != 0
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"
        assert "--metrics-port" in payload["error"]

    def test_still_reports_ok_for_a_valid_combination(self, capsys):
        code, payload = self._run_check(capsys, processes=4)

        assert code == 0
        assert payload["ok"] is True


class TestRealStartPathEnforcesIt:
    """Der Guard sitzt in `check_worker_config`, aber der echte Startpfad muss
    ihn auch aufrufen. Ein Tippfehler in der Weitergabe — ein vergessenes
    Argument, ein vertauschtes `metrics_host` — bliebe sonst unbemerkt, weil
    alle uebrigen Tests entweder direkt gegen den Validator laufen oder ueber
    den `--check`-Zweig.
    """

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

    def test_multi_process_start_is_refused(self, monkeypatch):
        from swarmq.process_manager.run import run_multi_process_worker

        monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@localhost/")
        monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://localhost:6379/0")

        with pytest.raises(ConfigurationError) as excinfo:
            run_multi_process_worker(self._args(processes=4, metrics_port=9000))

        assert "--metrics-port" in str(excinfo.value)

    def test_autoscale_start_is_refused(self, monkeypatch):
        from swarmq.process_manager.run import run_multi_process_worker

        monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://guest:guest@localhost/")
        monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://localhost:6379/0")

        with pytest.raises(ConfigurationError) as excinfo:
            run_multi_process_worker(
                self._args(autoscale=(1, 4), health_port=8080), admin_factory=object
            )

        assert "--health-port" in str(excinfo.value)


class TestAllowedCombinations:
    def test_single_process_with_metrics_port(self):
        """Covers AE3."""
        result = _check(processes=1, metrics_port=9000)
        assert result["module"] == _MODULE

    def test_single_process_with_both_ports(self):
        result = _check(processes=1, metrics_port=9000, health_port=8080)
        assert result["module"] == _MODULE

    def test_many_processes_without_any_fixed_port(self):
        result = _check(processes=8)
        assert result["module"] == _MODULE

    def test_defaults_are_unchanged_for_existing_callers(self):
        """Bestehende Aufrufer uebergeben die neuen Argumente nicht."""
        result = _check()
        assert result["module"] == _MODULE
        assert result["warnings"] == []


class TestPoolModePreflightAgrees:
    """``--check`` und der echte Start lehnen ``--pool cpu --autoscale``
    mit derselben Meldung ab (R4); ``--check --pool cpu`` meldet die
    aufgeloeste Slotzahl (R3)."""

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
            "pool": "io",
            "concurrency": None,
        }
        fields.update(overrides)
        code = _run_worker_check(argparse.Namespace(**fields))
        return code, json.loads(capsys.readouterr().out.strip())

    def test_cpu_pool_with_autoscale_is_rejected(self, capsys):
        code, payload = self._run_check(capsys, pool="cpu", autoscale=(2, 8))
        assert code == 1
        assert payload["ok"] is False
        assert "--autoscale" in payload["error"]
        assert payload["error_type"] == "ConfigurationError"

    def test_real_start_rejects_the_same_combination(self, capsys):
        import argparse

        from swarmq.cli import _run_worker_command

        args = argparse.Namespace(
            module=_MODULE, pool="cpu", autoscale=(2, 8), processes=1,
            concurrency=None, check=False, reload=False, log_level="INFO",
        )
        assert _run_worker_command(args) == 1
        assert "--autoscale" in capsys.readouterr().err

    def test_check_reports_resolved_slots(self, capsys, monkeypatch):
        from swarmq import slots

        monkeypatch.setattr(slots, "resolve_default_slots", lambda *_a, **_k: slots.SlotDecision(4, "cgroup"))
        code, payload = self._run_check(capsys, pool="cpu")
        assert code == 0
        assert payload["pool"] == "cpu"
        assert payload["slots"] == 4

    def test_io_pool_reports_default_ten(self, capsys):
        code, payload = self._run_check(capsys)
        assert code == 0
        assert payload["slots"] == 10


class TestRecyclingPortWarning:
    """KTD15: Recycling mit festem Port ist eine Warnung (Ersatz bindet mit Wiederholung)."""

    def test_check_warns_when_recycling_and_fixed_port(self, capsys, monkeypatch):
        monkeypatch.setenv("SWARMQ_RECYCLE_MAX_TASKS", "500")
        import argparse
        import json

        from swarmq.cli.check_cmd import _run_worker_check

        args = argparse.Namespace(
            module=_MODULE, reload=False, watch_dir=None, burst=False, processes=1, autoscale=None,
            metrics_port=9100, health_port=None, pool="io", concurrency=None,
        )
        code = _run_worker_check(args)
        payload = json.loads(capsys.readouterr().out.strip())
        assert code == 0 and payload["ok"] is True
        assert any(w.startswith("recycling_with_fixed_port") for w in payload["warnings"])
