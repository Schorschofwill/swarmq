"""Run-Routing-Tests (Plan U21) — verbatim: Reload-Routing durch die CLI
(``test_process_manager.py``) und ``run_multi_process_worker``-Routing
(``test_cli_multiprocess.py``)."""

from __future__ import annotations

import argparse
import os
from unittest.mock import MagicMock, patch

import pytest

from swarmq.process_manager import ProcessManager


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
             patch("swarmq.process_manager.run.setup_logging"), \
             pytest.raises(ConfigurationError, match="cannot be imported|no loadable"):
            run_multi_process_worker(ns)

    def test_supervisor_configures_structured_logging(self):
        """Härtung R2 P1-8: der Supervisor-Parent rief nie
        ``setup_logging()`` — worker_died/restart_limit_exceeded/
        Autoscale-Events landeten trotz ``--log-format json`` im
        structlog-Default-Console-Format. Der Parent muss dieselbe
        Renderer-Konfiguration nutzen wie die Worker und den
        Listener beim Exit stoppen (flush-on-exit)."""
        from swarmq.process_manager import (
            run_multi_process_worker,
        )

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="WARNING", log_format="json",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        env = {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.process_manager.run.setup_logging") as sl, \
             patch.object(ProcessManager, "run", return_value=0):
            rc = run_multi_process_worker(ns)
        assert rc == 0
        sl.assert_called_once_with("WARNING", log_json=True)
        sl.return_value.stop.assert_called_once()

    def test_supervisor_logging_human_format(self):
        from swarmq.process_manager import (
            run_multi_process_worker,
        )

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
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
             patch("swarmq.process_manager.run.setup_logging") as sl, \
             patch.object(ProcessManager, "run", return_value=0):
            run_multi_process_worker(ns)
        sl.assert_called_once_with("INFO", log_json=False)

    def test_supervisor_stops_listener_when_run_raises(self):
        from swarmq.process_manager import (
            run_multi_process_worker,
        )

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="swarmq.cli",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="json",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        env = {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }
        with patch.dict(os.environ, env, clear=False), \
             patch("swarmq.process_manager.run.setup_logging") as sl, \
             patch.object(
                 ProcessManager, "run", side_effect=RuntimeError("boom"),
             ), \
             pytest.raises(RuntimeError):
            run_multi_process_worker(ns)
        sl.return_value.stop.assert_called_once()

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
             patch("swarmq.process_manager.run.setup_logging"), \
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
