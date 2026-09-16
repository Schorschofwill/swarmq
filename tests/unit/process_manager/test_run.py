"""Run-Routing-Tests (Plan U21) — verbatim: Reload-Routing durch die CLI
(``test_process_manager.py``) und ``run_multi_process_worker``-Routing
(``test_cli_multiprocess.py``)."""

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

    def test_watch_dir_without_reload_logs_warning(self):
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
             ), \
             patch(
                 "swarmq.process_manager.run.logger"
             ) as mock_logger:
            run_multi_process_worker(
                self._args(watch_dir=["/tmp"]),
            )
        # At least one warning call carries the ignore event name.
        warn_events = [
            call.args[0] for call in mock_logger.warning.call_args_list
            if call.args
        ]
        assert "watch_dir_ignored_without_reload" in warn_events


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
             patch("swarmq.logging.setup_logging") as sl, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            rc = cli._run_worker_command(ns)
        assert rc == 0
        sl.assert_called_once_with("WARNING", True)

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
             patch("swarmq.logging.setup_logging") as sl, \
             patch("swarmq.process_manager.run.run_multi_process_worker", return_value=0):
            cli._run_worker_command(ns)
        sl.assert_called_once_with("INFO", False)

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
             patch("swarmq.logging.flush_logging") as flush, \
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

    def _run(self, **over):
        args = TestReloadRoutingThroughCLI()._args(**over)
        pm_mock = MagicMock()
        pm_mock.run.return_value = 0
        with patch.dict(os.environ, TestReloadRoutingThroughCLI()._env(), clear=False), \
             patch("swarmq.process_manager.run.ProcessManager", return_value=pm_mock) as PM, \
             patch("swarmq.process_manager.check._validate_module_importable"):
            from swarmq.process_manager import run_multi_process_worker

            assert run_multi_process_worker(args) == 0
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
