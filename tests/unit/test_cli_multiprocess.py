"""CLI integration tests for R4.6 / R4.7 (--processes, --autoscale)."""

from __future__ import annotations

import argparse
import os
from unittest.mock import MagicMock, patch

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
        with patch.dict(os.environ, env, clear=False), \
             pytest.raises(ConfigurationError, match="cannot be imported|no loadable"):
            run_multi_process_worker(ns)

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
             patch("swarmq.process_manager.ProcessManager") as PM:
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


class TestValidateModuleImportable:
    """ce-review Unit 2: find_spec pre-flight check direct tests."""

    def test_existing_module_accepted(self):
        from swarmq.process_manager import _validate_module_importable
        _validate_module_importable("swarmq.cli")  # no raise

    def test_nonexistent_module_rejected(self):
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import _validate_module_importable
        with pytest.raises(ConfigurationError, match="cannot be imported|no loadable"):
            _validate_module_importable("swarmq_does_not_exist_xyz")

    def test_malformed_dotted_raises_value_error_wrapped(self):
        """find_spec raises ValueError on some malformed inputs. The
        pre-flight wraps these as ConfigurationError (not bare ValueError)."""
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import _validate_module_importable
        # Leading-dot style triggers ValueError on most Python versions.
        with pytest.raises(ConfigurationError):
            _validate_module_importable(".leading.dot")

    def test_namespace_package_without_init_rejected(self, tmp_path, monkeypatch):
        """ce-review F-05: namespace package (no __init__.py) has
        spec.origin=None and will fail in import_module — we reject it
        upfront."""
        import sys as _sys
        from swarmq.exceptions import ConfigurationError
        from swarmq.process_manager import _validate_module_importable

        # Create a namespace package on PYTHONPATH.
        ns_dir = tmp_path / "my_namespace_pkg"
        ns_dir.mkdir()
        # NO __init__.py → namespace package → spec.origin is None
        monkeypatch.syspath_prepend(str(tmp_path))
        # Sanity: find_spec returns a spec but origin is None.
        import importlib.util as _iu
        spec = _iu.find_spec("my_namespace_pkg")
        assert spec is not None
        assert spec.origin is None

        with pytest.raises(ConfigurationError, match="no loadable"):
            _validate_module_importable("my_namespace_pkg")
        # Cleanup sys.modules so other tests aren't polluted.
        _sys.modules.pop("my_namespace_pkg", None)
