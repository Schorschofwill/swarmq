"""CLI integration tests for R4.6 / R4.7 (--processes, --autoscale)."""

from __future__ import annotations

import argparse
import os
from unittest.mock import patch

import pytest

from swarmq.cli import _autoscale_range, _build_parser, _watch_dir_path, main


class TestAutoscaleRangeParser:
    def test_valid_range(self):
        assert _autoscale_range("2,5") == (2, 5)
        assert _autoscale_range(" 1 , 10 ") == (1, 10)

    def test_rejects_single_value(self):
        with pytest.raises(argparse.ArgumentTypeError, match="MIN,MAX"):
            _autoscale_range("5")

    def test_rejects_non_integer(self):
        with pytest.raises(argparse.ArgumentTypeError, match="integers"):
            _autoscale_range("a,b")

    def test_rejects_min_below_one(self):
        with pytest.raises(
            argparse.ArgumentTypeError, match="1 <= MIN <= MAX",
        ):
            _autoscale_range("0,3")

    def test_rejects_max_below_min(self):
        with pytest.raises(
            argparse.ArgumentTypeError, match="1 <= MIN <= MAX",
        ):
            _autoscale_range("5,2")


class TestWorkerParserFlags:
    def test_processes_default_is_one(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.processes == 1

    def test_processes_accepts_value(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x", "--processes", "4"])
        assert args.processes == 4

    def test_autoscale_parses_to_tuple(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["worker", "--module=x", "--autoscale", "2,10"],
        )
        assert args.autoscale == (2, 10)

    def test_autoscale_default_is_none(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.autoscale is None

    # --- Kontrollkanal (U3, R1/R5): --max-processes und --group ------------

    def test_max_processes_and_group_parse_and_default_to_unset(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x", "--max-processes", "16", "--group", "orders-eu"])
        assert (args.max_processes, args.group) == (16, "orders-eu")
        plain = parser.parse_args(["worker", "--module=x"])
        assert (plain.max_processes, plain.group) == (None, None)

    @pytest.mark.parametrize("raw", ["0", "-1", "zwei"])
    def test_max_processes_rejects_values_below_one(self, raw):
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["worker", "--module=x", "--max-processes", raw])
        assert exc_info.value.code == 2


class TestWatchDirPathType:
    """R5.3 Unit 1: --watch-dir validator."""

    def test_existing_directory_accepted(self, tmp_path):
        result = _watch_dir_path(str(tmp_path))
        # Resolved to absolute path.
        assert result == str(tmp_path.resolve())

    def test_nonexistent_path_rejected(self, tmp_path):
        missing = tmp_path / "does-not-exist"
        with pytest.raises(argparse.ArgumentTypeError, match="not an existing directory"):
            _watch_dir_path(str(missing))

    def test_file_rejected(self, tmp_path):
        f = tmp_path / "not_a_dir.txt"
        f.write_text("x")
        with pytest.raises(argparse.ArgumentTypeError, match="not an existing directory"):
            _watch_dir_path(str(f))


class TestReloadFlag:
    """R5.3 Unit 1: --reload + --watch-dir argparse layer."""

    def test_reload_default_is_false(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.reload is False

    def test_watch_dir_default_is_none(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x"])
        assert args.watch_dir is None

    def test_reload_flag_sets_true(self):
        parser = _build_parser()
        args = parser.parse_args(["worker", "--module=x", "--reload"])
        assert args.reload is True

    def test_watch_dir_repeatable(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        parser = _build_parser()
        args = parser.parse_args([
            "worker", "--module=x",
            "--reload",
            "--watch-dir", str(d1),
            "--watch-dir", str(d2),
        ])
        assert args.watch_dir == [str(d1.resolve()), str(d2.resolve())]

    def test_watch_dir_invalid_path_exits(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "worker", "--module=x",
                "--watch-dir", "/definitely/not/existing/path/xyz",
            ])


class TestMainRouting:
    """Verify that --processes>1 or --autoscale routes to the supervisor."""

    def _env(self):
        return {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }

    def test_default_routes_to_supervisor(self):
        """R20 (Plan 2026-09-03-001): auch --processes 1 laeuft unter dem Supervisor."""
        with patch.dict(os.environ, self._env(), clear=False), \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0) as rmp:
            rc = main(["worker", "--module=ignore.me"])
            rmp.assert_called_once()
            ns = rmp.call_args[0][0]
            assert ns.processes == 1
            assert ns.concurrency == 10  # aufgeloest, nicht None
            assert rc == 0

    def test_processes_gt_one_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            rc = main(["worker", "--module=ignore.me", "--processes", "3"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.processes == 3
            assert rc == 0

    def test_check_reload_validates_without_supervisor(self, capsys):
        """R5.3 ce-review #23: --check --reload validates watchfiles +
        watch-dirs and exits 0, without starting the supervisor."""
        import json

        with patch(
            "swarmq.process_manager.run.run_multi_process_worker",
        ) as rmp:
            rc = main([
                "worker", "--module=json",
                "--reload", "--check",
            ])
        assert rc == 0
        rmp.assert_not_called()  # supervisor NOT started
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is True
        assert payload["reload"] is True
        assert len(payload["watch_dirs"]) == 1

    def test_check_reports_missing_module(self, capsys):
        """--check returns 1 with JSON error when --module unresolvable."""
        import json

        rc = main([
            "worker", "--module=nope.xyz.unresolvable",
            "--check",
        ])
        assert rc == 1
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is False
        assert "nope.xyz.unresolvable" in payload["error"]

    def test_check_reports_missing_watchfiles(self, capsys):
        """R5.3 ce-review pass 2 TG-05: --check --reload without the
        [reload] extra emits JSON with error + exit 1, not a raw
        traceback."""
        import json

        with patch.dict("sys.modules", {"watchfiles": None}):
            rc = main(["worker", "--module=json", "--reload", "--check"])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"
        assert "swarmq[reload]" in payload["error"]
        # Stable field-set even on error path.
        assert payload["watch_dirs"] is None
        assert payload["module_origin"] is None

    def test_check_reports_unexpected_exception(self, capsys):
        """R5.3 ce-review pass 2 #3 / P1: non-ConfigurationError
        exceptions must render as JSON on stdout, not escape as a
        Python traceback."""
        import json

        with patch(
            "swarmq.process_manager.check._validate_module_importable",
            side_effect=PermissionError("simulated .pth lockout"),
        ):
            rc = main(["worker", "--module=json", "--check"])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "PermissionError"
        assert "simulated .pth lockout" in payload["error"]

    def test_check_argparse_error_emits_json(self, capsys):
        """R5.3 ce-review pass 2 #2 / AC-06 / CLI-02: when --check is
        requested but argparse itself rejects the invocation, the JSON
        contract must still hold — agents cannot branch on a missing
        stdout payload."""
        import json

        rc = main([
            "worker", "--module=json",
            "--watch-dir", "/definitely/not/existing/path/xyz",
            "--check",
        ])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ArgparseError"
        assert "watch-dir" in payload["error"].lower() or \
               "not an existing directory" in payload["error"]

    def test_check_warns_on_reload_burst_combo(self, capsys):
        """R5.3 ce-review pass 2 #25: --reload + --burst emits a
        structured warning in the JSON payload."""
        import json

        rc = main([
            "worker", "--module=json", "--reload", "--burst", "--check",
        ])
        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is True
        assert any(
            "reload_burst_conflict" in w for w in payload["warnings"]
        )

    def test_check_rejects_proc_watch_dir(self, capsys):
        """R5.3 ce-review pass 2 #22 / ADV-01: pseudo-filesystem paths
        are rejected at argparse time, surfacing through the --check
        JSON error contract."""
        import json

        rc = main([
            "worker", "--module=json", "--reload",
            "--watch-dir", "/proc",
            "--check",
        ])
        assert rc == 1
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False
        assert payload["error_type"] == "ArgparseError"
        assert "/proc" in payload["error"]

    def test_check_without_reload_validates_module_only(self, capsys):
        """--check alone (no --reload) still validates --module.

        R5.3 ce-review P2-P3 pass #1: payload has stable field-set —
        every key is always present; ``watch_dirs`` is ``None`` when
        ``--reload`` is not set.
        """
        import json

        rc = main(["worker", "--module=json", "--check"])
        assert rc == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["ok"] is True
        assert payload["reload"] is False
        # Stable field-set: watch_dirs is always present, None when unused.
        assert "watch_dirs" in payload
        assert payload["watch_dirs"] is None
        assert payload["error"] is None
        assert payload["warnings"] == []

    def test_reload_alone_routes_to_supervisor(self):
        """R5.3 ce-review #1: --reload (without --processes or --autoscale)
        must activate the multi-process supervisor — otherwise the flag is
        a silent no-op and R-1 is not met."""
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            rc = main(["worker", "--module=ignore.me", "--reload"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.reload is True
            assert ns.processes == 1  # default stays 1; supervisor wraps it
            assert rc == 0

    def test_autoscale_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            main(["worker", "--module=ignore.me", "--autoscale", "2,5"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.autoscale == (2, 5)

    def test_supervisor_surfaces_config_error(self):
        """Bewusst geaenderter Test (U4/R19): der Exit-Code ist jetzt 78."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 side_effect=ConfigurationError("boom"),
             ):
            rc = main(["worker", "--module=x", "--processes", "2"])
            assert rc == CONFIG_ERROR_EXIT_CODE






class TestSupervisorConfigErrorExitCode:
    """U4/KTD8 (R19): Ein Konfigurationsfehler im Elternprozess endet mit 78
    und einem strukturierten Logsatz.

    Vorher gab jeder Startfehler 1 zurueck -- eine Orchestrierung konnte
    "nicht wieder anfassen" nicht von "gleich nochmal" unterscheiden und
    startete einen kaputt konfigurierten Worker endlos neu.
    """

    def _args(self, **over):
        import argparse

        ns = argparse.Namespace(
            module="json", queues=["default"], concurrency=None,
            log_level="INFO", log_format="human", burst=False,
            burst_idle_timeout=5.0, priorities=False, metrics_port=None,
            health_port=None, processes=1, autoscale=None, reload=False,
            watch_dir=None, check=False, pool="io",
        )
        for k, v in over.items():
            setattr(ns, k, v)
        return ns

    def _run(self, monkeypatch, exc):
        """Exit-Code des Worker-Zweigs bei *exc*.

        Der Logsatz selbst laeuft ueber den QueueListener nach stderr und ist
        hier nicht zuverlaessig abzugreifen; sein Format prueft der
        Integrationstest mit echtem Subprozess (AE8).
        """
        from swarmq import cli

        def boom(args, **kwargs):
            raise exc

        monkeypatch.setattr("swarmq.process_manager.run.run_multi_process_worker", boom)
        return cli._run_worker_command(self._args())

    def test_configuration_error_returns_seventy_eight(self, monkeypatch):
        from swarmq.exceptions import ConfigurationError
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        code = self._run(monkeypatch, ConfigurationError("queue_type must be one of"))
        assert code == CONFIG_ERROR_EXIT_CODE

    def test_module_not_found_returns_seventy_eight(self, monkeypatch):
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        code = self._run(monkeypatch, ModuleNotFoundError("No module named 'nope'"))
        assert code == CONFIG_ERROR_EXIT_CODE

    def test_connection_errors_still_return_one(self, monkeypatch):
        """R19: ein nicht erreichbarer Broker ist kein Konfigurationsfehler --
        dort ist "gleich nochmal" die richtige Antwort."""
        from swarmq.exceptions import BrokerConnectionError

        code = self._run(monkeypatch, BrokerConnectionError("connection refused"))
        assert code == 1


class TestHeadroomExitCode:
    """Kontrollkanal U3: ein Spielraum unter den Startwerten endet mit Exit 78 (R1/R3)."""

    def _env(self):
        return {"SWARMQ_BROKER_URL": "amqp://x", "SWARMQ_BACKEND_URL": "valkey://x"}

    def test_max_processes_below_processes_exits_78(self):
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        with patch.dict(os.environ, self._env(), clear=False), \
             patch("swarmq.process_manager.run.ProcessManager") as PM:
            rc = main(["worker", "--module=json", "--processes", "8", "--max-processes", "4"])
        assert rc == CONFIG_ERROR_EXIT_CODE
        PM.assert_not_called()

    def test_max_processes_below_autoscale_upper_bound_exits_78(self):
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        with patch.dict(os.environ, self._env(), clear=False), \
             patch("swarmq.process_manager.run.ProcessManager") as PM:
            rc = main(["worker", "--module=json", "--autoscale", "2,8", "--max-processes", "4"])
        assert rc == CONFIG_ERROR_EXIT_CODE
        PM.assert_not_called()

    def test_check_reports_the_headroom_error_as_json(self, capsys):
        import json

        rc = main(["worker", "--module=json", "--check", "--processes", "8", "--max-processes", "4"])
        payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert rc == 1
        assert payload["error_type"] == "ConfigurationError"
        assert "--max-processes 4" in payload["error"]


class TestCheckValidatesLogging:
    """R14: ``swarmq worker --check`` meldet ungueltige Logging-Einstellungen
    wie jeden anderen Konfigurationsfehler -- sonst sagte der Dry-Run "in
    Ordnung" fuer eine Kombination, an der der Start danach scheitert.

    Test-Liste:
    - [x] fehlende ``--log-config``-Datei
    - [x] ``--log-format`` zusammen mit ``--log-config``
    - [x] nicht positive ``--log-queue-size``
    """

    @staticmethod
    def _check(capsys, *extra: str) -> tuple[int, dict]:
        import json

        rc = main(["worker", "--module=json", "--check", *extra])
        return rc, json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    def test_missing_log_config_file_is_reported(self, capsys, tmp_path):
        missing = tmp_path / "nonexistent.json"
        rc, payload = self._check(capsys, "--log-config", str(missing))
        assert rc == 1
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"
        assert str(missing) in payload["error"]

    def test_log_format_with_log_config_is_reported(self, capsys, tmp_path):
        config = tmp_path / "log.json"
        config.write_text('{"version": 1}')
        rc, payload = self._check(capsys, "--log-config", str(config), "--log-format", "json")
        assert rc == 1
        assert payload["error_type"] == "ConfigurationError"
        assert "--log-format" in payload["error"]

    @pytest.mark.parametrize("size", ["0", "-5"])
    def test_non_positive_log_queue_size_is_reported(self, capsys, size):
        rc, payload = self._check(capsys, f"--log-queue-size={size}")
        assert rc == 1
        assert payload["error_type"] == "ConfigurationError"
        assert "--log-queue-size" in payload["error"]
