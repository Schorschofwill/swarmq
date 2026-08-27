"""Unit tests for the ``swarmq`` CLI.

Argparse layer + error-rendering boundaries. The actual worker
lifecycle is mocked so these tests do not touch a broker.
"""

from __future__ import annotations

import argparse
import json
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


class TestDottedModuleName:
    """ce-review Unit 3: --module format validation via _dotted_module_name."""

    def test_plain_dotted_accepted(self):
        assert cli._dotted_module_name("myapp.tasks") == "myapp.tasks"

    def test_single_segment_accepted(self):
        assert cli._dotted_module_name("tasks") == "tasks"

    def test_underscore_prefix_accepted(self):
        assert cli._dotted_module_name("_private.worker._impl") == "_private.worker._impl"

    def test_unicode_identifier_accepted(self):
        """PEP 3131 — valid Python 3 identifier must pass the regex."""
        assert cli._dotted_module_name("café.tasks") == "café.tasks"
        assert cli._dotted_module_name("日本.tasks") == "日本.tasks"

    def test_hyphen_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name("my-app.tasks")

    def test_path_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name("/etc/passwd")

    def test_empty_segment_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name("myapp..tasks")

    def test_leading_digit_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name("123.main")

    def test_leading_dot_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name(".hidden")

    def test_empty_string_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="dotted"):
            cli._dotted_module_name("")

    def test_worker_parser_rejects_invalid_module(self):
        """Regression guard: validator wired into worker subparser."""
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["worker", "--module", "my-app.tasks"])

    def test_inspect_parser_rejects_invalid_module(self):
        """ce-review F-05: inspect subparser has its own --module —
        must carry the same validator, else it's a lower-friction
        bypass path."""
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["inspect", "--module", "/etc/passwd"])


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
            # Use a name we definitely haven't registered. Previously this
            # test picked "dlq" for that purpose; dlq is now a real
            # subcommand (R3.10), so switch to something obviously bogus.
            parser.parse_args(["not-a-real-subcommand", "anything"])


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
        with patch.object(cli, "run_worker_async", new_callable=AsyncMock) as mock:
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
        with patch.object(cli, "run_worker_async", new_callable=AsyncMock) as mock:
            mock.side_effect = KeyboardInterrupt()
            rc = cli.main(["worker", "--module", "myapp"])
        assert rc == 0


# ---------------------------------------------------------------------------
# run_worker_async error paths (without spawning a real worker)
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
            await cli.run_worker_async(ns)

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
            await cli.run_worker_async(ns)

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
            await cli.run_worker_async(ns)


# ---------------------------------------------------------------------------
# schedule subcommand parser (Unit 12)
# ---------------------------------------------------------------------------


class TestScheduleParser:
    def test_list_default_format_is_table(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "list"])
        assert ns.command == "schedule"
        assert ns.schedule_command == "list"
        # Plan 2026-04-23-001 Unit 5: parser default is None;
        # resolve_format in main() maps that to 'table' on TTY / 'json' on pipe.
        assert ns.format is None

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
        # Plan 2026-04-23-001 Unit 5: parser default is None;
        # resolve_format in main() maps that to 'table' on TTY / 'json' on pipe.
        assert ns.format is None

    def test_remove_format_json(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "remove", "x", "--format", "json"])
        assert ns.format == "json"

    def test_status_default_limit(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["schedule", "status"])
        assert ns.limit == 10
        # Plan 2026-04-23-001 Unit 5: parser default is None;
        # resolve_format in main() maps that to 'table' on TTY / 'json' on pipe.
        assert ns.format is None

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


# ---- R3.5 ce-review P3 #23: CLI surface for dedup ------------------

class TestScheduleAddUniqueFor:
    """Argparse coverage for --unique-for / --unique-until on schedule add."""

    def test_unique_for_defaults_to_none(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "test",
            "--cron", "* * * * *", "--task", "t",
        ])
        assert ns.unique_for is None
        assert ns.unique_until == "completion"

    def test_unique_for_accepts_positive_int(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "schedule", "add", "test",
            "--cron", "* * * * *", "--task", "t",
            "--unique-for", "60", "--unique-until", "start",
        ])
        assert ns.unique_for == 60
        assert ns.unique_until == "start"

    def test_unique_for_rejects_zero(self, capsys):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "schedule", "add", "test",
                "--cron", "* * * * *", "--task", "t",
                "--unique-for", "0",
            ])


class TestInspectCommand:
    """Argparse + import-side behaviour for the new `swarmq inspect` cmd."""

    def test_inspect_requires_module(self, capsys):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["inspect"])

    def test_inspect_table_default(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "inspect", "--module", "some.module",
        ])
        assert ns.module == "some.module"
        # Plan 2026-04-23-001 Unit 5: parser default is None;
        # resolve_format in main() maps that to 'table' on TTY / 'json' on pipe.
        assert ns.format is None

    def test_inspect_json_format(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "inspect", "--module", "some.module", "--format", "json",
        ])
        assert ns.format == "json"

    async def test_inspect_module_without_tasks_raises(self, monkeypatch):
        """Empty module → ConfigurationError (same discipline as worker)."""
        import importlib

        from swarmq.task import Task

        Task._clear_registered_tasks()
        monkeypatch.setattr(importlib, "import_module", lambda m: None)
        ns = argparse.Namespace(module="empty", format="table")
        with pytest.raises(ConfigurationError, match="no tasks registered"):
            await cli._run_inspect_async(ns)


# ---- R3.10 DLQ CLI -----------------------------------------------------


class TestDlqParser:
    """Argparse shape for the dlq subcommand group."""

    def test_dlq_list_defaults(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["dlq", "list"])
        assert ns.command == "dlq"
        assert ns.dlq_command == "list"
        assert ns.queue == "default"
        assert ns.limit == 100
        # Plan 2026-04-23-001 Unit 5: parser default is None;
        # resolve_format in main() maps that to 'table' on TTY / 'json' on pipe.
        assert ns.format is None

    def test_dlq_list_overrides(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "dlq", "list",
            "--queue", "work", "--limit", "50", "--format", "json",
        ])
        assert ns.queue == "work"
        assert ns.limit == 50
        assert ns.format == "json"

    def test_dlq_inspect_requires_task_id(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["dlq", "inspect"])

    def test_dlq_inspect_accepts_task_id(self):
        parser = cli._build_parser()
        ns = parser.parse_args([
            "dlq", "inspect", "abc-123", "--queue", "q",
            "--scan-limit", "500",
        ])
        assert ns.task_id == "abc-123"
        assert ns.queue == "q"
        assert ns.scan_limit == 500

    def test_dlq_retry_positional(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["dlq", "retry", "abc-123"])
        assert ns.task_id == "abc-123"
        assert ns.all is False

    def test_dlq_retry_all_flag(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["dlq", "retry", "--all"])
        assert ns.task_id is None
        assert ns.all is True

    def test_dlq_retry_rejects_zero_limit(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["dlq", "retry", "--all", "--limit", "0"])

    def test_dlq_purge_defaults(self):
        parser = cli._build_parser()
        ns = parser.parse_args(["dlq", "purge"])
        assert ns.dlq_command == "purge"
        assert ns.queue == "default"


class TestDlqHandlerValidation:
    @pytest.mark.timeout(5)
    async def test_broker_from_env_requires_url(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_BROKER_URL", raising=False)
        with pytest.raises(ConfigurationError, match="SWARMQ_BROKER_URL"):
            await cli._broker_from_env()

    @pytest.mark.timeout(5)
    async def test_retry_without_task_or_all_exits_2(self, monkeypatch):
        """Review P3 #12: bad argument shape exits 2 (argparse convention),
        not 1 (runtime error)."""
        monkeypatch.setenv(
            "SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5672/"
        )
        ns = argparse.Namespace(
            task_id=None, all=False, queue="default",
            limit=10_000, scan_limit=1000, format="table",
        )
        with pytest.raises(SystemExit) as excinfo:
            await cli._run_dlq_retry(ns)
        assert excinfo.value.code == 2

    @pytest.mark.timeout(5)
    async def test_retry_with_both_task_and_all_exits_2(self, monkeypatch):
        monkeypatch.setenv(
            "SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5672/"
        )
        ns = argparse.Namespace(
            task_id="t-1", all=True, queue="default",
            limit=10_000, scan_limit=1000, format="table",
        )
        with pytest.raises(SystemExit) as excinfo:
            await cli._run_dlq_retry(ns)
        assert excinfo.value.code == 2

class TestDlqFormatter:
    """Pure-function table formatter — no broker / no argparse."""

    def test_empty_renders_placeholder(self):
        assert cli._format_dlq_list_table([]) == "(DLQ empty)"

    def test_full_row_with_retry_and_traceback(self):
        rendered = cli._format_dlq_list_table([{
            "task_id": "abc-123",
            "task_name": "add",
            "queue": "work",
            "headers": {
                "x-swarmq-retry-count": 3,
                "x-swarmq-traceback": "RuntimeError: boom\nmore lines",
            },
            "body_size": 128,
        }])
        lines = rendered.splitlines()
        # Header row + data row.
        assert len(lines) == 2
        assert "abc-123" in lines[1]
        assert "add" in lines[1]
        assert "3" in lines[1]
        assert "RuntimeError: boom" in lines[1]
        assert "128" in lines[1]

    def test_malformed_task_id_renders_placeholder(self):
        rendered = cli._format_dlq_list_table([{
            "task_id": None,
            "task_name": None,
            "queue": "work",
            "headers": {},
            "body_size": 10,
        }])
        assert "(malformed)" in rendered

    def test_traceback_bytes_decoded(self):
        rendered = cli._format_dlq_list_table([{
            "task_id": "t",
            "task_name": "n",
            "queue": "q",
            "headers": {
                "x-swarmq-traceback": b"ValueError: bad input",
            },
            "body_size": 8,
        }])
        assert "ValueError: bad input" in rendered


class TestDlqNormaliseHeaders:
    def test_bytes_values_decoded(self):
        result = cli._normalise_headers_for_json({
            "x-swarmq-traceback": b"err",
            "x-swarmq-retry-count": 3,
        })
        assert result["x-swarmq-traceback"] == "err"
        assert result["x-swarmq-retry-count"] == 3

    def test_empty_dict_passes_through(self):
        assert cli._normalise_headers_for_json({}) == {}

    def test_none_treated_as_empty(self):
        assert cli._normalise_headers_for_json(None) == {}


class TestDlqHandlerOrchestration:
    """Unit coverage for _run_dlq_* handler orchestration (review P2 #8 /
    T-01 / T-02). Each test mocks the broker + env var, then exercises
    one handler's output / dispatch branches without needing RabbitMQ.
    """

    @pytest.fixture
    def mock_broker(self, monkeypatch):
        """Patch _broker_from_env to return a MagicMock-backed broker."""
        from unittest.mock import AsyncMock, MagicMock

        broker = MagicMock()
        broker.close = AsyncMock()
        broker.list_dead_letter_queue = AsyncMock()
        broker.inspect_dead_letter_message = AsyncMock()
        broker.retry_dead_letter_message = AsyncMock()
        broker.retry_all_dead_letter_messages = AsyncMock()
        broker.purge_dead_letter_queue = AsyncMock()

        async def fake_from_env():
            return broker

        monkeypatch.setenv(
            "SWARMQ_BROKER_URL", "amqp://guest:guest@localhost:5672/"
        )
        monkeypatch.setattr(cli, "_broker_from_env", fake_from_env)
        return broker

    @pytest.mark.timeout(5)
    async def test_list_json_output_wraps_envelope(self, mock_broker, capsys):
        mock_broker.list_dead_letter_queue.return_value = (
            [{"task_id": "t-1", "task_name": "add", "queue": "q",
              "headers": {"x-swarmq-retry-count": 3}, "body_size": 128}],
            1,
        )
        ns = argparse.Namespace(queue="q", limit=100, format="json")
        await cli._run_dlq_list(ns)
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["scanned"] == 1
        assert payload["limit"] == 100
        assert payload["truncated"] is False
        assert payload["queue"] == "q"
        assert len(payload["entries"]) == 1
        mock_broker.close.assert_awaited_once()

    @pytest.mark.timeout(5)
    async def test_list_table_truncated_hint_when_at_limit(
        self, mock_broker, capsys,
    ):
        mock_broker.list_dead_letter_queue.return_value = (
            [{"task_id": f"t-{i}", "task_name": "x", "queue": "q",
              "headers": {}, "body_size": 10} for i in range(2)],
            2,
        )
        ns = argparse.Namespace(queue="q", limit=2, format="table")
        await cli._run_dlq_list(ns)
        out = capsys.readouterr().out
        assert "truncated at --limit 2" in out

    @pytest.mark.timeout(5)
    async def test_inspect_found_json_adds_found_true(self, mock_broker, capsys):
        mock_broker.inspect_dead_letter_message.return_value = (
            {"task_id": "t-1", "task_name": "add", "queue": "q",
             "headers": {}, "body": {"task": {"id": "t-1"}},
             "body_raw": '{"task":{"id":"t-1"}}', "body_size": 20},
            1,
        )
        ns = argparse.Namespace(
            task_id="t-1", queue="q", scan_limit=1000, format="json",
        )
        await cli._run_dlq_inspect(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload["found"] is True
        assert payload["scanned"] == 1

    @pytest.mark.timeout(5)
    async def test_inspect_not_found_raises_dlq_not_found(
        self, mock_broker, capsys,
    ):
        mock_broker.inspect_dead_letter_message.return_value = (None, 5)
        ns = argparse.Namespace(
            task_id="missing", queue="q", scan_limit=1000, format="json",
        )
        with pytest.raises(cli._DLQNotFound):
            await cli._run_dlq_inspect(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload["found"] is False
        assert payload["scanned"] == 5
        assert payload["scan_exhausted"] is False

    @pytest.mark.timeout(5)
    async def test_inspect_table_body_raw_fallback_for_malformed(
        self, mock_broker, capsys,
    ):
        mock_broker.inspect_dead_letter_message.return_value = (
            {"task_id": None, "task_name": None, "queue": "q",
             "headers": {}, "body": None, "body_raw": "not-json",
             "body_size": 8},
            1,
        )
        ns = argparse.Namespace(
            task_id=None, queue="q", scan_limit=1000, format="table",
        )
        # Passing task_id=None happens to be what you'd hit if you tried
        # to inspect a malformed DLQ message; this test pins the
        # body_raw fallback branch.
        mock_broker.inspect_dead_letter_message.return_value = (
            mock_broker.inspect_dead_letter_message.return_value[0], 1,
        )
        await cli._run_dlq_inspect(ns)
        out = capsys.readouterr().out
        assert "not-json" in out

    @pytest.mark.timeout(5)
    async def test_retry_single_success_json_shape(self, mock_broker, capsys):
        mock_broker.retry_dead_letter_message.return_value = (True, 3)
        ns = argparse.Namespace(
            task_id="t-1", all=False, queue="q",
            limit=10_000, scan_limit=1000, format="json",
        )
        await cli._run_dlq_retry(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload["found"] is True
        assert payload["moved"] == 1
        assert payload["scanned"] == 3

    @pytest.mark.timeout(5)
    async def test_retry_single_not_found_exits_via_sentinel(
        self, mock_broker, capsys,
    ):
        mock_broker.retry_dead_letter_message.return_value = (False, 1000)
        ns = argparse.Namespace(
            task_id="missing", all=False, queue="q",
            limit=10_000, scan_limit=1000, format="json",
        )
        with pytest.raises(cli._DLQNotFound):
            await cli._run_dlq_retry(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload["found"] is False
        assert payload["scan_exhausted"] is True

    @pytest.mark.timeout(5)
    async def test_retry_all_json_reports_skipped_malformed(
        self, mock_broker, capsys,
    ):
        mock_broker.retry_all_dead_letter_messages.return_value = (4, 5)
        ns = argparse.Namespace(
            task_id=None, all=True, queue="q",
            limit=10_000, scan_limit=1000, format="json",
        )
        await cli._run_dlq_retry(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload["moved"] == 4
        assert payload["scanned"] == 5
        assert payload["skipped_malformed"] == 1
        assert payload["truncated"] is False

    @pytest.mark.timeout(5)
    async def test_retry_all_table_warns_about_skipped_malformed(
        self, mock_broker, capsys,
    ):
        mock_broker.retry_all_dead_letter_messages.return_value = (3, 4)
        ns = argparse.Namespace(
            task_id=None, all=True, queue="q",
            limit=10_000, scan_limit=1000, format="table",
        )
        await cli._run_dlq_retry(ns)
        out = capsys.readouterr().out
        assert "republished 3" in out
        assert "1 malformed" in out

    @pytest.mark.timeout(5)
    async def test_purge_json_shape(self, mock_broker, capsys):
        mock_broker.purge_dead_letter_queue.return_value = 7
        ns = argparse.Namespace(queue="q", format="json")
        await cli._run_dlq_purge(ns)
        payload = json.loads(capsys.readouterr().out)
        assert payload == {"purged": 7, "queue": "q"}

    @pytest.mark.timeout(5)
    async def test_dispatch_all_four_commands(self, mock_broker):
        """Dispatcher _run_dlq_async routes the four subcommand strings
        to the right handler (P2 #8 / T-02)."""
        mock_broker.list_dead_letter_queue.return_value = ([], 0)
        mock_broker.inspect_dead_letter_message.return_value = (None, 0)
        mock_broker.retry_all_dead_letter_messages.return_value = (0, 0)
        mock_broker.purge_dead_letter_queue.return_value = 0

        # list
        ns = argparse.Namespace(
            dlq_command="list", queue="q", limit=10, format="json",
        )
        await cli._run_dlq_async(ns)
        mock_broker.list_dead_letter_queue.assert_awaited_once()

        # inspect not-found raises sentinel; swallow for this smoke.
        ns = argparse.Namespace(
            dlq_command="inspect", task_id="x", queue="q",
            scan_limit=10, format="json",
        )
        with pytest.raises(cli._DLQNotFound):
            await cli._run_dlq_async(ns)
        mock_broker.inspect_dead_letter_message.assert_awaited_once()

        # retry --all
        ns = argparse.Namespace(
            dlq_command="retry", task_id=None, all=True, queue="q",
            limit=10, scan_limit=10, format="json",
        )
        await cli._run_dlq_async(ns)
        mock_broker.retry_all_dead_letter_messages.assert_awaited_once()

        # purge
        ns = argparse.Namespace(
            dlq_command="purge", queue="q", format="json",
        )
        await cli._run_dlq_async(ns)
        mock_broker.purge_dead_letter_queue.assert_awaited_once()


class TestRenderTable:
    """Shared table renderer used by three CLI formatters (P3 #14)."""

    def test_empty_returns_label(self):
        assert cli._render_table([("H",)], "(empty)") == "(empty)"

    def test_header_plus_data_aligned(self):
        out = cli._render_table(
            [("A", "BB"), ("x", "yy"), ("zzz", "w")],
            empty_label="(none)",
        )
        lines = out.splitlines()
        assert len(lines) == 3
        # All data cells should start at the same column (header alignment).
        assert lines[0].startswith("A  ") or lines[0].startswith("A   ")


class TestDlqNotFoundExitCode:
    """main() maps _DLQNotFound to exit code 3 (review P3 #19)."""

    def test_main_returns_3_on_dlq_not_found(self, monkeypatch):
        async def raiser(_args):
            raise cli._DLQNotFound

        monkeypatch.setattr(cli, "_run_dlq_async", raiser)
        rc = cli.main(["dlq", "inspect", "nope", "--queue", "x"])
        assert rc == 3
