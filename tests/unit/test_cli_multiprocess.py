"""CLI integration tests for R4.6 / R4.7 (--processes, --autoscale)."""

from __future__ import annotations

import argparse
import os
from unittest.mock import MagicMock, patch

import pytest

from swarmq.cli import _autoscale_range, _build_parser, main


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


class TestMainRouting:
    """Verify that --processes>1 or --autoscale routes to the supervisor."""

    def _env(self):
        return {
            "SWARMQ_BROKER_URL": "amqp://x",
            "SWARMQ_BACKEND_URL": "valkey://x",
        }

    def test_single_process_path_unchanged(self):
        """--processes=1 must not reach run_multi_process_worker."""
        def _consume_coro(coro):
            # Consume the coroutine object so it's not leaked as
            # "never awaited". We don't actually run it.
            coro.close()
            return None

        with patch.dict(os.environ, self._env(), clear=False), \
             patch("swarmq.process_manager.run_multi_process_worker") as rmp, \
             patch("swarmq.cli.asyncio.run", side_effect=_consume_coro):
            rc = main(["worker", "--module=ignore.me"])
            rmp.assert_not_called()
            assert rc == 0

    def test_processes_gt_one_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            rc = main(["worker", "--module=ignore.me", "--processes", "3"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.processes == 3
            assert rc == 0

    def test_autoscale_routes_to_supervisor(self):
        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run_multi_process_worker",
                 return_value=0,
             ) as rmp:
            main(["worker", "--module=ignore.me", "--autoscale", "2,5"])
            rmp.assert_called_once()
            ns = rmp.call_args.args[0]
            assert ns.autoscale == (2, 5)

    def test_supervisor_surfaces_config_error(self):
        from swarmq.exceptions import ConfigurationError

        with patch.dict(os.environ, self._env(), clear=False), \
             patch(
                 "swarmq.process_manager.run_multi_process_worker",
                 side_effect=ConfigurationError("boom"),
             ):
            rc = main(["worker", "--module=x", "--processes", "2"])
            assert rc == 1


class TestRunMultiProcessWorker:
    def test_rejects_missing_env_vars(self):
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import run_multi_process_worker

        ns = argparse.Namespace(
            processes=2, autoscale=None, module="x",
            queues=["default"], concurrency=10,
            log_level="INFO", log_format="human",
            burst=False, burst_idle_timeout=5.0, priorities=False,
            metrics_port=None, health_port=None,
        )
        with patch.dict(os.environ, {}, clear=True), \
             pytest.raises(ConfigurationError):
            run_multi_process_worker(ns)

    def test_constructs_process_manager_and_runs(self):
        from swarmq.process_manager import run_multi_process_worker

        ns = argparse.Namespace(
            processes=3, autoscale=None, module="x",
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
             patch("swarmq.process_manager.ProcessManager") as PM:
            instance = PM.return_value
            instance.run.return_value = 0
            rc = run_multi_process_worker(ns)
            assert rc == 0
            PM.assert_called_once()
            kwargs = PM.call_args.kwargs
            assert kwargs["autoscale"] is None
            assert PM.call_args.kwargs["num_workers"] == 3
