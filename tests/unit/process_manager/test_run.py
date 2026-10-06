"""Run-Routing-Tests (Plan U21) — verbatim: Reload-Routing durch die CLI
(``test_process_manager.py``) und ``run_multi_process_worker``-Routing
(``test_cli_multiprocess.py``)."""

# Test list (Plan 2026-10-06-0831, U9):
# 1. ``watch_dir_ignored_without_reload`` ueber ``swarmq_logs`` statt
#    gepatchtem Logger.
# 2. ``log_config_reload_failed`` traegt ``error`` ohne Klassen-Praefix (R17).

from __future__ import annotations

import argparse
import os
from unittest.mock import MagicMock, patch

import pytest


class TestReloadRoutingThroughCLI:
    """R5.3 Unit 6: run_multi_process_worker fails fast on missing dep
    and resolves watch-dirs before handing off to ProcessManager."""

    def _args(self, **over):
        ns = argparse.Namespace(
            module="json",  # stdlib, always resolvable
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

    def _env(self):
        return {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }

    def test_reload_false_skips_watcher_wiring(self):
        from swarmq.process_manager import run_multi_process_worker

        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.ProcessManager",
                 return_value=pm_mock,
             ) as PM, \
             patch(
                 "swarmq.process_manager.check._validate_module_importable"
             ):
            rc = run_multi_process_worker(self._args())
        assert rc == 0
        kwargs = PM.call_args.kwargs
        assert kwargs["reload"] is False
        assert kwargs["watch_dirs"] == ()

    def test_reload_true_resolves_dirs_and_passes_through(self):
        """R5.3 ce-review #7: _validate_module_importable returns the
        spec, which is handed to resolve_watch_dirs — find_spec is NOT
        called a second time."""
        import importlib.util

        from swarmq.process_manager import run_multi_process_worker

        json_spec = importlib.util.find_spec("json")
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.ProcessManager",
                 return_value=pm_mock,
             ) as PM, \
             patch(
                 "swarmq.process_manager.check._validate_module_importable",
                 return_value=json_spec,
             ), \
             patch(
                 "swarmq.hot_reload.importlib.util.find_spec"
             ) as hot_reload_find_spec:
            rc = run_multi_process_worker(self._args(reload=True))
        assert rc == 0
        kwargs = PM.call_args.kwargs
        assert kwargs["reload"] is True
        # json is a real stdlib package — resolved parent must end in 'json'.
        assert len(kwargs["watch_dirs"]) == 1
        assert kwargs["watch_dirs"][0].endswith("json")
        # Single find_spec call — the one inside _validate_module_importable.
        # resolve_watch_dirs must NOT call find_spec again because spec is passed.
        hot_reload_find_spec.assert_not_called()

    def test_reload_true_without_watchfiles_raises(self):
        import importlib.util

        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import run_multi_process_worker

        json_spec = importlib.util.find_spec("json")
        with (
            patch.dict(os.environ, self._env(), clear=False),
            patch(
                "swarmq.process_manager.check._validate_module_importable",
                return_value=json_spec,
            ),
            patch.dict("sys.modules", {"watchfiles": None}),
            pytest.raises(ConfigurationError, match=r"swarmq\[reload\]"),
        ):
            run_multi_process_worker(self._args(reload=True))

    def test_watch_dir_without_reload_logs_warning(self, swarmq_logs):
        """--watch-dir without --reload must warn (not error)."""
        from swarmq.process_manager import run_multi_process_worker

        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run.ProcessManager",
                 return_value=pm_mock,
             ), \
             patch(
                 "swarmq.process_manager.check._validate_module_importable"
             ):
            run_multi_process_worker(
                self._args(watch_dir=["/tmp"]),
            )
        # At least one warning carries the ignore event name.
        [entry] = [e for e in swarmq_logs if e["event"] == "watch_dir_ignored_without_reload"]
        assert entry["log_level"] == "warning" and entry["watch_dirs"] == ["/tmp"]


class TestRunMultiProcessWorker:
    def test_rejects_missing_env_vars(self):
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import run_multi_process_worker

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="human",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        with patch.dict(os.environ, {}, clear=True), \
             pytest.raises(ConfigurationError):
            run_multi_process_worker(ns)

    def test_rejects_unresolvable_module(self):
        """ce-review Unit 2: find_spec pre-flight catches typo/missing
        module in the parent so the supervisor does not waste the
        5-restart-per-hour budget on a deterministic ImportError."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import run_multi_process_worker

        ns = argparse.Namespace(
            processes=2, autoscale=None,
            module="swarmq_definitely_not_a_real_module_xyz",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="human",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        env = {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }
        # setup_logging gemockt: das echte würde structlog global
        # rekonfigurieren und nachfolgende capture_logs-Tests verschmutzen.
        with patch.dict(os.environ, env, clear=False), \
             pytest.raises(ConfigurationError, match="cannot be imported|no loadable"):
            run_multi_process_worker(ns)

    def test_worker_command_configures_structured_logging(self):
        """Bewusst geaenderte Tests (U4/KTD8): Das Logging konfiguriert jetzt
        der Worker-Zweig der CLI, nicht ``run_multi_process_worker``.

        Haertung R2 P1-8 hatte die Konfiguration hier eingefuehrt, weil der
        Supervisor-Parent sie gar nicht hatte. Der Ort war aber zu spaet: die
        Funktion stoppte ihren Listener im ``finally``, und ein
        Konfigurationsfehler, den der Aufrufer danach loggte, blieb in der
        Queue eines gestoppten Listeners liegen (R19/R21).
        """
        from swarmq import cli

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="WARNING", log_format="json",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None, check=False, pool="io",
            reload=False, watch_dir=None,
        )
        env = {"SWARMQ_BROKER_URL": "amqp://x", "SWARMQ_BACKEND_URL": "valkey://x"}
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.logging.configure_logging") as configure, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            rc = cli._run_worker_command(ns)
        assert rc == 0
        # Bewusst geaendert (U7): ``configure_logging`` mit den aufgeloesten
        # Settings statt des deprecated ``setup_logging``.
        (settings,), _ = configure.call_args
        assert (settings.level, settings.format) == ("WARNING", "json")

    def test_worker_command_logging_human_format(self):
        from swarmq import cli

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="human",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None, check=False, pool="io",
            reload=False, watch_dir=None,
        )
        env = {"SWARMQ_BROKER_URL": "amqp://x", "SWARMQ_BACKEND_URL": "valkey://x"}
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.logging.configure_logging") as configure, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            cli._run_worker_command(ns)
        (settings,), _ = configure.call_args
        assert (settings.level, settings.format) == ("INFO", "human")

    def test_worker_command_flushes_the_listener_when_run_raises(self):
        from swarmq import cli

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="json",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None, check=False, pool="io",
            reload=False, watch_dir=None,
        )
        env = {"SWARMQ_BROKER_URL": "amqp://x", "SWARMQ_BACKEND_URL": "valkey://x"}
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.logging.shutdown_logging") as flush, \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 side_effect=RuntimeError("boom"),
             ), \
             pytest.raises(RuntimeError):
            cli._run_worker_command(ns)
        flush.assert_called()

    def test_constructs_process_manager_and_runs(self):
        from swarmq.process_manager import run_multi_process_worker

        # Unit 2 pre-flight requires a resolvable module — use an
        # always-present internal module so the unit test focuses on
        # ProcessManager construction, not module validation.
        ns = argparse.Namespace(
            processes=3, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="human",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        env = {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.process_manager.run.ProcessManager") as PM:
            instance = PM.return_value
            instance.run.return_value = 0
            rc = run_multi_process_worker(ns)
            assert rc == 0
            PM.assert_called_once()
            kwargs = PM.call_args.kwargs
            assert kwargs["autoscale"] is None
            assert kwargs["num_workers"] == 3
            # ce-review F-4: assert WorkerConfig shape to prevent silent
            # drift when worker_args becomes a tuple of unrelated values.
            from swarmq.process_manager import WorkerConfig
            worker_args = kwargs["worker_args"]
            assert len(worker_args) == 1
            assert isinstance(worker_args[0], WorkerConfig)
            assert worker_args[0].module == "swarmq.cli"
            assert worker_args[0].queues == ("default",)


# Test-Liste U7 (Plan 2026-10-06-0831, Supervisor ``swarmq worker``):
# - [x] Einstellungen einmal aufloesen (CLI > Env > Default) und damit
#       ``configure_logging`` aufrufen (AE5)
# - [x] Ungueltige Einstellung: genau eine ``worker_config_error``-Zeile im
#       gewaehlten Format (Fallback), Exit 78, kein Supervisor
# - [x] ``--log-config`` relativ: absolut aufgeloest, geprueft, die JSON-
#       Konfiguration steht in den Settings; Kinder lesen die Datei nicht
# - [x] Ungueltige ``--log-config``: Exit 78, Zeile mit ``path`` und ``error_type``
# - [x] Die aufgeloesten Settings reisen in ``WorkerConfig.logging_settings``
# - [x] ``BrokerConnectionError``: strukturierte Zeile statt Rohtext, Exit 1
# - [x] Hot-Reload: ungueltig gewordene ``--log-config`` bricht den Reload ab
#       (``log_config_reload_failed``), gueltige laesst ihn zu


def _worker_ns(**over):
    ns = argparse.Namespace(
        processes=2, autoscale=None, module="swarmq.cli",
        queues=["default"], concurrency=10,
        log_level=None, log_format=None,
        burst=False, burst_idle_timeout=5.0, priorities=False,
        metrics_port=None, health_port=None, check=False, pool="io",
        reload=False, watch_dir=None,
    )
    for key, value in over.items():
        setattr(ns, key, value)
    return ns


_URLS = {"SWARMQ_BROKER_URL": "amqp://x", "SWARMQ_BACKEND_URL": "valkey://x"}


class TestSupervisorLoggingSettings:
    """U7 (KTD7, AE5): Der Supervisor loest die Logging-Einstellungen einmal
    auf und richtet sich danach ein."""

    def test_settings_follow_cli_over_env(self, monkeypatch):
        from swarmq import cli

        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "json")
        monkeypatch.setenv("SWARMQ_LOG_LEVEL", "WARNING")
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.logging.configure_logging") as configure, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            rc = cli._run_worker_command(_worker_ns(log_format="human"))

        assert rc == 0
        (settings,), _ = configure.call_args
        assert (settings.format, settings.level) == ("human", "WARNING")


    def test_cli_hands_the_resolved_settings_to_the_supervisor(self):
        from swarmq import cli

        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.logging.configure_logging") as configure, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0) as run:
            cli._run_worker_command(_worker_ns(log_level="ERROR"))

        (settings,), _ = configure.call_args
        assert run.call_args.kwargs["logging_settings"] is settings

    def test_invalid_setting_is_one_structured_line_in_the_chosen_format(
        self, monkeypatch, capsys,
    ):
        """R14: Der Fallback-Renderer schreibt die Zeile im gewaehlten Format;
        der Supervisor startet nicht."""
        import json

        from swarmq import cli
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        monkeypatch.setenv("SWARMQ_LOG_LEVEL", "LOUD")
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.process_manager.run.run_multi_process_worker") as run:
            rc = cli._run_worker_command(_worker_ns(log_format="json"))

        assert rc == CONFIG_ERROR_EXIT_CODE
        run.assert_not_called()
        (line,) = capsys.readouterr().err.splitlines()
        data = json.loads(line)
        assert data["event"] == "worker_config_error"
        assert data["error_type"] == "ConfigurationError"
        assert "SWARMQ_LOG_LEVEL" in data["error"]

    def test_relative_log_config_is_resolved_and_validated_once(
        self, monkeypatch, tmp_path,
    ):
        """KTD9: absolut aufgeloest und geparst im Supervisor; die Kinder
        bekommen die gepruefte Konfiguration und lesen die Datei nie."""
        import json

        from swarmq import cli

        (tmp_path / "log.json").write_text(json.dumps({"version": 1}))
        monkeypatch.chdir(tmp_path)
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.logging.configure_logging") as configure, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            cli._run_worker_command(_worker_ns(log_config="log.json"))

        (settings,), _ = configure.call_args
        assert settings.log_config == str(tmp_path / "log.json")
        assert json.loads(settings.log_config_json) == {
            "version": 1, "disable_existing_loggers": False,
        }

    def test_invalid_log_config_is_one_line_with_path_and_error_type(
        self, monkeypatch, tmp_path, capsys,
    ):
        """AE4: Tippfehler in einem Handler-Schluessel -- Exit 78, eine Zeile
        mit ``path``, ``error_type`` und der Ursache."""
        import json

        from swarmq import cli
        from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({
            "version": 1,
            "handlers": {"err": {"class": "logging.StreamHandlr"}},
            "root": {"handlers": ["err"]},
        }))
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.process_manager.run.run_multi_process_worker") as run:
            rc = cli._run_worker_command(_worker_ns(log_config=str(bad)))

        assert rc == CONFIG_ERROR_EXIT_CODE
        run.assert_not_called()
        (line,) = capsys.readouterr().err.splitlines()
        assert "worker_config_error" in line
        assert f"path={bad}" in line
        assert "error_type=LoggingConfigError" in line
        assert "Unable to configure handler" in line  # Ursache aus __cause__

    def test_resolved_settings_reach_the_worker_config(self):
        """KTD7: Die Kinder bekommen die Settings aufgeloest und lesen kein Env."""
        from swarmq.logging_settings import LoggingSettings
        from swarmq.process_manager import run_multi_process_worker

        settings = LoggingSettings(level="WARNING", level_explicit=True, format="json")
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock) as PM, \
             patch("swarmq.process_manager.check._validate_module_importable"):
            run_multi_process_worker(
                TestReloadRoutingThroughCLI()._args(log_level=None, log_format=None),
                logging_settings=settings,
            )

        cfg = PM.call_args.kwargs["worker_args"][0]
        assert cfg.logging_settings is settings
        assert (cfg.log_level, cfg.log_format) == ("WARNING", "json")

    def test_invalid_logging_env_is_ignored_with_resolved_settings(self, monkeypatch):
        """Die CLI hat ``SWARMQ_LOG_FORMAT`` schon zugunsten von ``--log-format``
        verworfen; die Supervisor-``SwarmQConfig`` darf das ungueltige Env
        nicht erneut parsen und mit 78 abbrechen."""
        from swarmq.logging_settings import LoggingSettings
        from swarmq.process_manager import run_multi_process_worker

        monkeypatch.setenv("SWARMQ_LOG_FORMAT", "jsonl")
        monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", "7")
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock) as PM, \
             patch("swarmq.process_manager.check._validate_module_importable"):
            rc = run_multi_process_worker(
                TestReloadRoutingThroughCLI()._args(), logging_settings=LoggingSettings(format="json"),
            )

        assert rc == 0
        assert PM.call_args.kwargs["shutdown_grace_s"] == 7.0

    def test_broker_connection_error_is_a_structured_line(self, capsys):
        """R15: kein Rohtext mehr auf stderr; die Zeile ist vor dem Exit
        geschrieben."""
        import json

        from swarmq import cli
        from swarmq.exceptions import BrokerConnectionError

        with patch.dict(os.environ, _URLS, clear=False), \
             patch(
                 "swarmq.process_manager.run.run_multi_process_worker",
                 side_effect=BrokerConnectionError("amqp unreachable"),
             ):
            rc = cli._run_worker_command(_worker_ns(log_format="json"))

        assert rc == 1
        (line,) = capsys.readouterr().err.splitlines()
        data = json.loads(line)
        assert data["event"] == "worker_connection_error"
        assert data["error_type"] == "BrokerConnectionError"
        assert "amqp unreachable" in data["error"]

    def _supervisor_with(self, settings):
        from swarmq.process_manager import run_multi_process_worker

        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        pm_mock.reload_precheck = None
        with patch.dict(os.environ, _URLS, clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock), \
             patch("swarmq.process_manager.check._validate_module_importable"):
            run_multi_process_worker(
                TestReloadRoutingThroughCLI()._args(), logging_settings=settings,
            )
        return pm_mock

    def test_hot_reload_revalidates_the_log_config_first(self, tmp_path, swarmq_logs):
        """KTD9: Vor dem Stoppen eines Workers prueft der Supervisor die Datei
        neu; ist sie inzwischen ungueltig, laeuft alles weiter -- kein
        ``execvpe``, kein Exit 78."""
        from swarmq.logging_settings import LoggingSettings

        path = tmp_path / "log.json"
        path.write_text('{"version": 1}')
        pm = self._supervisor_with(LoggingSettings(log_config=str(path)))

        assert pm.reload_precheck() is True
        path.write_text('{"version": 1,')
        assert pm.reload_precheck() is False
        (event,) = [e for e in swarmq_logs if e["event"] == "log_config_reload_failed"]
        assert event["path"] == str(path)
        assert event["error_type"] == "LoggingConfigError"
        assert event["error"] and not event["error"].startswith("LoggingConfigError")
        assert event["log_level"] == "error"

    def test_hot_reload_also_catches_a_handler_typo(self, tmp_path, swarmq_logs):
        """AE4 beim Reload: Ein Tippfehler im Handler faellt erst in
        ``dictConfig`` auf -- der neu gestartete Supervisor endete sonst mit
        78, nachdem die Worker schon gestoppt sind."""
        import json

        from swarmq.logging_settings import LoggingSettings

        path = tmp_path / "log.json"
        path.write_text('{"version": 1}')
        pm = self._supervisor_with(LoggingSettings(log_config=str(path)))
        path.write_text(json.dumps({
            "version": 1,
            "handlers": {"err": {"class": "logging.StreamHandlr"}},
            "root": {"handlers": ["err"]},
        }))

        assert pm.reload_precheck() is False
        assert [e["event"] for e in swarmq_logs] == ["log_config_reload_failed"]

class TestHeartbeatIntervalPassThrough:
    """Review #8: SWARMQ_HEARTBEAT_INTERVAL_S wurde geparst, kam aber nie in der WorkerConfig an —
    der Worker schlug immer alle 5 s, ein Timeout unter 5 s ersetzte jeden gesunden Worker."""

    def test_worker_config_carries_the_configured_interval(self):
        from swarmq.process_manager import run_multi_process_worker

        args = TestReloadRoutingThroughCLI()._args()
        env = {
            **TestReloadRoutingThroughCLI()._env(),
            "SWARMQ_HEARTBEAT_INTERVAL_S": "2",
            "SWARMQ_HEARTBEAT_TIMEOUT_S": "9",
        }
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock) as PM, \
             patch("swarmq.process_manager.check._validate_module_importable"):
            assert run_multi_process_worker(args) == 0
        cfg = PM.call_args.kwargs["worker_args"][0]
        assert cfg.heartbeat_interval_s == 2.0
        assert PM.call_args.kwargs["heartbeat_timeout_s"] == 9.0


class TestControlChannelWiring:
    """Kontrollkanal U3 (R1, R3, R5): Gruppenschluessel und Spielraum erreichen Kinder und Supervisor."""

    def _run(self, setpoint_store_factory=None, **over):
        args = TestReloadRoutingThroughCLI()._args(**over)
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, TestReloadRoutingThroughCLI()._env(), clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock) as PM, \
             patch("swarmq.process_manager.check._validate_module_importable"):
            from swarmq.process_manager import run_multi_process_worker

            assert run_multi_process_worker(args, setpoint_store_factory=setpoint_store_factory) == 0
        return PM.call_args.kwargs

    def test_group_key_defaults_to_the_sorted_queue_set(self):
        """Covers AE4 (ohne Label)."""
        kwargs = self._run(queues=["orders", "billing", "orders"], group=None)
        assert kwargs["group"] == "billing,orders"
        assert kwargs["worker_args"][0].group == "billing,orders"

    def test_group_label_wins_over_the_queues(self):
        """Covers AE4 (mit Label)."""
        kwargs = self._run(queues=["orders"], group="eu-west")
        assert kwargs["group"] == "eu-west"
        assert kwargs["worker_args"][0].group == "eu-west"

    def test_headroom_reaches_config_and_supervisor(self):
        kwargs = self._run(processes=2, max_processes=16)
        assert kwargs["max_processes"] == 16
        assert kwargs["worker_args"][0].max_processes == 16

    def test_without_the_new_arguments_nothing_is_enabled(self):
        """R2: keine Namespace-Attribute noetig, Spielraum bleibt ungesetzt."""
        kwargs = self._run()
        assert kwargs["max_processes"] is None
        assert kwargs["worker_args"][0].max_processes is None

    def test_store_factory_reaches_the_supervisor_only_with_headroom(self):
        """Covers AE1 (Verdrahtung): ohne --max-processes keine Fabrik, mit Spielraum die gereichte."""
        sentinel = object()
        factory = lambda: sentinel  # noqa: E731
        without = self._run(setpoint_store_factory=factory)
        assert without["setpoint_store_factory"] is None
        with_headroom = self._run(setpoint_store_factory=factory, max_processes=8)
        assert with_headroom["setpoint_store_factory"] is factory
        assert with_headroom["supervisor_id"].endswith(f":p{os.getpid()}")
