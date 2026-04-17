"""Unit tests for the ``swarmq`` CLI.

Argparse layer + error-rendering boundaries. The actual worker
lifecycle is mocked so these tests do not touch a broker.
"""

from __future__ import annotations

import argparse
import sys
from unittest.mock import AsyncMock, patch

import pytest

from swarmq import cli
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ConfigurationError,
)
from swarmq.task import Task


@pytest.fixture(autouse=True)
def _isolate_task_registry():
    """Each CLI test starts with an empty Task registry."""
    saved = list(Task._registered_tasks)
    Task._clear_registered_tasks()
    yield
    Task._registered_tasks[:] = saved


# ---------------------------------------------------------------------------
# argparse layer
# ---------------------------------------------------------------------------


class TestQueueListType:
    def test_single_queue(self):
        assert cli._queue_list("default") == ["default"]

    def test_multiple_queues(self):
        assert cli._queue_list("a,b,c") == ["a", "b", "c"]

    def test_strips_whitespace_and_drops_empties(self):
        assert cli._queue_list("a, b , c ,") == ["a", "b", "c"]

    def test_empty_string_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="at least one"):
            cli._queue_list("")

    def test_only_commas_and_whitespace_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="at least one"):
            cli._queue_list(" , , ")


class TestPositiveIntType:
    def test_one_is_valid(self):
        assert cli._positive_int("1") == 1

    def test_large_value(self):
        assert cli._positive_int("100") == 100

    def test_zero_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match=">= 1"):
            cli._positive_int("0")

    def test_negative_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match=">= 1"):
            cli._positive_int("-3")

    def test_non_integer_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="not an integer"):
            cli._positive_int("abc")


class TestBuildParser:
    def test_worker_subcommand_with_minimum_args(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["worker", "--module", "myapp.tasks"])
        assert ns.command == "worker"
        assert ns.module == "myapp.tasks"
        assert ns.queues == ["default"]
        assert ns.concurrency == 10
        assert ns.log_level == "INFO"
        assert ns.log_format == "human"

    def test_worker_subcommand_with_all_args(self):
        parser = cli._build_parser()
        ns = parser.parse_args(
            [
                "worker",
                "--module", "myapp.tasks",
                "--queues", "a,b",
                "--concurrency", "5",
                "--log-level", "DEBUG",
                "--log-format", "json",
            ]
        )
        assert ns.queues == ["a", "b"]
        assert ns.concurrency == 5
        assert ns.log_level == "DEBUG"
        assert ns.log_format == "json"

    def test_module_is_required(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["worker"])

    def test_concurrency_zero_is_rejected(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["worker", "--module", "m", "--concurrency", "0"])

    def test_log_level_invalid_choice_rejected(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["worker", "--module", "m", "--log-level", "VERBOSE"]
            )

    def test_log_format_invalid_choice_rejected(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["worker", "--module", "m", "--log-format", "jsonl"]
            )

    def test_unknown_subcommand_rejected(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["dlq", "list"])


class TestVersion:
    def test_version_string_is_non_empty(self):
        # Either a real version, or the documented "unknown" fallback.
        v = cli._swarmq_version()
        assert isinstance(v, str)
        assert v  # non-empty

    def test_version_flag_exits_zero(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            cli._build_parser().parse_args(["--version"])
        assert exc_info.value.code == 0
        out = capsys.readouterr().out
        assert "swarmq " in out


# ---------------------------------------------------------------------------
# main() error-rendering boundary
# ---------------------------------------------------------------------------


class TestMainErrorRendering:
    """`main()` should render every documented exception class as a single
    stderr line and exit 1 — never let a Python traceback reach the user
    at default log level."""

    def _run_main_with_mocked_run(
        self,
        log_level: str,
        side_effect: BaseException,
        capsys,
    ) -> tuple[int, str]:
        with patch.object(cli, "_run_worker_async", new_callable=AsyncMock) as mock:
            mock.side_effect = side_effect
            rc = cli.main(
                [
                    "worker",
                    "--module", "myapp",
                    "--log-level", log_level,
                ]
            )
        captured = capsys.readouterr()
        return rc, captured.err

    def test_module_not_found_renders_one_line(self, capsys):
        rc, err = self._run_main_with_mocked_run(
            "INFO", ModuleNotFoundError("No module named 'myapp'"), capsys
        )
        assert rc == 1
        assert "swarmq worker:" in err
        assert "myapp" in err
        # Default log level: no traceback.
        assert "Traceback" not in err

    def test_configuration_error_renders_one_line(self, capsys):
        rc, err = self._run_main_with_mocked_run(
            "INFO", ConfigurationError("no tasks registered"), capsys
        )
        assert rc == 1
        assert "no tasks registered" in err
        assert "Traceback" not in err

    def test_broker_connection_error_renders_one_line(self, capsys):
        rc, err = self._run_main_with_mocked_run(
            "INFO", BrokerConnectionError("amqp unreachable"), capsys
        )
        assert rc == 1
        assert "amqp unreachable" in err
        assert "Traceback" not in err

    def test_backend_connection_error_renders_one_line(self, capsys):
        rc, err = self._run_main_with_mocked_run(
            "INFO", BackendConnectionError("valkey unreachable"), capsys
        )
        assert rc == 1
        assert "valkey unreachable" in err
        assert "Traceback" not in err

    def test_debug_log_level_includes_traceback(self, capsys):
        rc, err = self._run_main_with_mocked_run(
            "DEBUG", ConfigurationError("boom"), capsys
        )
        assert rc == 1
        assert "boom" in err
        assert "Traceback" in err

    def test_keyboard_interrupt_exits_zero(self, capsys):
        with patch.object(cli, "_run_worker_async", new_callable=AsyncMock) as mock:
            mock.side_effect = KeyboardInterrupt()
            rc = cli.main(["worker", "--module", "myapp"])
        assert rc == 0


# ---------------------------------------------------------------------------
# _run_worker_async error paths (without spawning a real worker)
# ---------------------------------------------------------------------------


class TestRunWorkerAsync:
    """Verify the pre-flight checks without ever calling start_worker()."""

    @pytest.mark.asyncio
    async def test_module_not_found_propagates(self):
        ns = argparse.Namespace(
            module="definitely_not_a_real_module_xyz",
            queues=["default"],
            concurrency=10,
            log_level="INFO",
            log_format="human",
        )
        with pytest.raises(ModuleNotFoundError):
            await cli._run_worker_async(ns)

    @pytest.mark.asyncio
    async def test_no_tasks_registered_raises_configuration_error(
        self, monkeypatch
    ):
        # Import a known-empty stdlib module that registers nothing.
        ns = argparse.Namespace(
            module="json",  # stdlib, no Task subclasses
            queues=["default"],
            concurrency=10,
            log_level="INFO",
            log_format="human",
        )
        # Even with broker URL set, the no-tasks check fires first.
        monkeypatch.setenv("SWARMQ_BROKER_URL", "amqp://localhost:5672/")
        monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://localhost:6379/0")
        with pytest.raises(ConfigurationError, match="no tasks registered"):
            await cli._run_worker_async(ns)

    @pytest.mark.asyncio
    async def test_missing_broker_url_raises_configuration_error(
        self, monkeypatch
    ):
        # Register a task so the no-tasks branch is not the one that fires.
        class _Probe(Task):
            from swarmq.task import TaskInfo
            task_info = TaskInfo(name="cli_probe", queues=["default"])

            async def run(self) -> None:
                pass

        ns = argparse.Namespace(
            module="json",  # any importable module — task above is already registered
            queues=["default"],
            concurrency=10,
            log_level="INFO",
            log_format="human",
        )
        monkeypatch.delenv("SWARMQ_BROKER_URL", raising=False)
        monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://localhost:6379/0")
        with pytest.raises(ConfigurationError, match="SWARMQ_BROKER_URL"):
            await cli._run_worker_async(ns)


# ---------------------------------------------------------------------------
# schedule subcommand parser (Unit 12)
# ---------------------------------------------------------------------------


class TestScheduleParser:
    def test_list_default_format_is_table(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "list"])
        assert ns.command == "schedule"
        assert ns.schedule_command == "list"
        assert ns.format == "table"

    def test_list_json_format(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "list", "--format", "json"])
        assert ns.format == "json"

    def test_add_with_cron(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "mycron",
            "--cron", "*/5 * * * *",
            "--task", "mytask",
            "--args", '[1, 2]',
            "--kwargs", '{"k": 1}',
            "--queue", "high",
        ])
        assert ns.name == "mycron"
        assert ns.cron == "*/5 * * * *"
        assert ns.at is None
        assert ns.task == "mytask"
        assert ns.args == "[1, 2]"
        assert ns.queue == "high"

    def test_add_with_at(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "one",
            "--at", "2026-05-01T03:00:00Z",
            "--task", "mytask",
        ])
        assert ns.at == "2026-05-01T03:00:00Z"
        assert ns.cron is None

    def test_add_rejects_both_cron_and_at(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "schedule", "add", "x", "--task", "t",
                "--cron", "* * * * *",
                "--at", "2026-01-01T00:00:00Z",
            ])

    def test_add_requires_task(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "schedule", "add", "x", "--cron", "* * * * *",
            ])

    def test_remove_without_permanent(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "remove", "abc"])
        assert ns.name == "abc"
        assert ns.permanent is False

    def test_remove_permanent_flag(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "remove", "abc", "--permanent"])
        assert ns.permanent is True

    def test_add_format_json(self):
        """Unit 4 / M-2: agent-parseable output."""
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "x", "--cron", "* * * * *",
            "--task", "t", "--format", "json",
        ])
        assert ns.format == "json"

    def test_add_format_defaults_to_table(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "x", "--cron", "* * * * *", "--task", "t",
        ])
        assert ns.format == "table"

    def test_remove_format_json(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "remove", "x", "--format", "json"])
        assert ns.format == "json"

    def test_status_default_limit(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "status"])
        assert ns.limit == 10
        assert ns.format == "table"

    def test_status_with_limit_and_json(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "status", "--limit", "50", "--format", "json"
        ])
        assert ns.limit == 50
        assert ns.format == "json"


class TestScheduleHandlerValidation:
    async def test_backend_from_env_requires_url(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_BACKEND_URL", raising=False)
        with pytest.raises(ConfigurationError, match="SWARMQ_BACKEND_URL"):
            await cli._backend_from_env()

    async def test_schedule_add_rejects_invalid_cron(self, monkeypatch):
        monkeypatch.setenv(
            "SWARMQ_BACKEND_URL", "valkey://localhost:6379/0"
        )
        ns = argparse.Namespace(
            name="bad", cron="not-a-cron", at=None, task="t",
            args="[]", kwargs="{}", queue="default",
        )
        with pytest.raises(ConfigurationError, match="invalid cron"):
            await cli._run_schedule_add(ns)

    async def test_schedule_add_rejects_invalid_args_json(self, monkeypatch):
        monkeypatch.setenv(
            "SWARMQ_BACKEND_URL", "valkey://localhost:6379/0"
        )
        ns = argparse.Namespace(
            name="x", cron="* * * * *", at=None, task="t",
            args="not json", kwargs="{}", queue="default",
        )
        with pytest.raises(ConfigurationError, match="--args"):
            await cli._run_schedule_add(ns)

    async def test_schedule_add_rejects_non_array_args(self, monkeypatch):
        monkeypatch.setenv(
            "SWARMQ_BACKEND_URL", "valkey://localhost:6379/0"
        )
        ns = argparse.Namespace(
            name="x", cron="* * * * *", at=None, task="t",
            args='{"oops": 1}', kwargs="{}", queue="default",
        )
        with pytest.raises(ConfigurationError, match="JSON array"):
            await cli._run_schedule_add(ns)

    async def test_schedule_add_rejects_invalid_at(self, monkeypatch):
        monkeypatch.setenv(
            "SWARMQ_BACKEND_URL", "valkey://localhost:6379/0"
        )
        ns = argparse.Namespace(
            name="x", cron=None, at="not-a-datetime", task="t",
            args="[]", kwargs="{}", queue="default",
        )
        with pytest.raises(ConfigurationError, match="--at"):
            await cli._run_schedule_add(ns)
