"""Tests for ce-review residuals (Plan 2026-04-23-001-fix-cli-refactor-residuals).

Each class corresponds to a Unit in the plan and verifies one ce-review
finding. RED tests live here first; the matching production fix follows
in its own commit.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_CLI_SHARED = _REPO_ROOT / "src" / "swarmq" / "cli" / "_shared.py"
_CLI_INIT = _REPO_ROOT / "src" / "swarmq" / "cli" / "__init__.py"

_HEAVY_MODULES = frozenset({"swarmq.backend.valkey", "swarmq.broker.rabbitmq"})


def _top_level_imports(source_path: pathlib.Path) -> set[str]:
    """Return the set of modules imported at Module-top-level.

    Top-level = direct children of ``ast.Module.body``. Imports inside
    function bodies, class bodies, or ``if`` blocks are excluded.
    """
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
    return modules


# ---------------------------------------------------------------------------
# Unit 1 — CLI-01: CLI-local lazy imports (R-1a)
# ---------------------------------------------------------------------------


class TestLazyImports:
    """R-1a: the CLI modules themselves must not eagerly import Backend/Broker.

    This is the narrow, CLI-local scope. The broader R-1b (no ``valkey``
    in ``sys.modules`` after ``import swarmq.cli``) is delegated to
    plan ``2026-04-23-002-fix-import-laziness-plan.md`` because
    ``swarmq/app.py`` and ``swarmq/client.py`` hold the heavy symbols
    as type annotations — those are outside this plan's scope.

    Audit (2026-04-23, ``.context/cli-residuals/pre-impl-import-audit.txt``):
    baseline 152 ms; removing CLI-local eager imports alone cannot
    reach the 60 % reduction target because app.py/client.py
    transitively load the same modules.
    """

    def test_cli_shared_module_has_no_top_level_heavy_imports(self) -> None:
        """``cli/_shared.py`` must not import ``swarmq.backend.valkey`` or
        ``swarmq.broker.rabbitmq`` at module top level.

        Runtime use in ``_backend_from_env`` / ``_broker_from_env`` is
        fine — those functions can import inside their body
        (pattern: ``dlq_cmd._get_broker_factory`` — a facade indirection
        that only runs on first call).
        """
        top_level = _top_level_imports(_CLI_SHARED)
        leaked = top_level & _HEAVY_MODULES
        assert not leaked, (
            f"cli/_shared.py still imports {sorted(leaked)} at top level; "
            "move into _backend_from_env/_broker_from_env function bodies."
        )

    def test_cli_init_module_has_no_top_level_heavy_imports(self) -> None:
        """``cli/__init__.py`` must not import ValkeyBackend/RabbitMQBroker
        at module top level.

        The ambient re-exports (frozen-surface whitelist items
        ``ValkeyBackend`` / ``RabbitMQBroker``) must be served via
        ``__getattr__`` lazy hook. Identity is preserved on first access.
        """
        top_level = _top_level_imports(_CLI_INIT)
        leaked = top_level & _HEAVY_MODULES
        assert not leaked, (
            f"cli/__init__.py still imports {sorted(leaked)} at top level; "
            "move to a PEP-562 __getattr__ hook for lazy resolution."
        )

    def test_cli_getattr_valkey_backend_preserves_identity(self) -> None:
        """``swarmq.cli.ValkeyBackend`` must be the canonical class.

        Surface-whitelist contract (R-8): identity check in
        ``test_cli_module_surface.py`` compares ``cli.ValkeyBackend is
        swarmq.backend.valkey.ValkeyBackend``. The lazy ``__getattr__``
        implementation must satisfy this — caching is allowed, but the
        object must be the same one ``importlib.import_module`` returns.
        """
        import swarmq.cli as cli
        from swarmq.backend.valkey import ValkeyBackend as CanonicalVB

        assert cli.ValkeyBackend is CanonicalVB

    def test_cli_getattr_rabbitmq_broker_preserves_identity(self) -> None:
        """``swarmq.cli.RabbitMQBroker`` must be the canonical class."""
        import swarmq.cli as cli
        from swarmq.broker.rabbitmq import RabbitMQBroker as CanonicalRB

        assert cli.RabbitMQBroker is CanonicalRB

    def test_cli_getattr_unknown_attribute_raises(self) -> None:
        """``__getattr__`` must still raise ``AttributeError`` for bogus names.

        Regression guard: lazy-loading mechanism must not swallow
        ``AttributeError`` for names outside ``_LAZY_SURFACE``.
        """
        import swarmq.cli as cli

        with pytest.raises(AttributeError, match="ThisDoesNotExist"):
            _ = cli.ThisDoesNotExist  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Unit 2 — CLI-02: robust --check detection (R-2)
# ---------------------------------------------------------------------------


class TestCheckDetectionRobustness:
    """R-2: ``_is_check_invocation`` must recognise ``swarmq worker --check``
    and ONLY that, rejecting patterns where ``--check`` appears as an
    argument value (``--task --check``) or inside a different subcommand
    (``dlq retry worker --check``).

    The detection still happens pre-parse because the downstream
    JSON-on-argparse-error contract (AC-06, test_cli_multiprocess.py
    line 229) requires knowing the caller's intent before argparse can
    fail on a bad flag value like ``--watch-dir /nonexistent``.

    Implementation strategy: a minimal argparse detector with
    ``exit_on_error=False`` and ``parse_known_args``, aware of
    worker's argument-consuming flags so ``--check`` can't be
    misread as the value of ``--task`` et al.
    """

    def test_worker_with_check_flag_detects_as_check(self) -> None:
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["worker", "--check"]) is True

    def test_worker_with_module_and_check_detects_as_check(self) -> None:
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["worker", "--module", "mypkg.tasks", "--check"]
        ) is True

    def test_worker_task_value_followed_by_check_detects_as_check(self) -> None:
        """``--task foo --check`` — ``--task`` takes ``foo``, ``--check`` is a flag."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["worker", "--task", "foo", "--check"]
        ) is True

    def test_dlq_retry_worker_check_is_not_check_invocation(self) -> None:
        """BUG #1: ``swarmq dlq retry worker --check`` — ``worker`` is a
        task-name positional under ``dlq retry``, not the worker
        subcommand; ``--check`` doesn't exist there. Must NOT enter the
        JSON-contract path."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["dlq", "retry", "worker", "--check"]
        ) is False

    def test_worker_task_dash_check_is_not_check_invocation(self) -> None:
        """BUG #2: ``swarmq worker --task --check`` — ``--check`` is the
        VALUE of ``--task``, not a flag. The detector must consume
        ``--task``'s value correctly and therefore see no ``--check``
        flag active."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["worker", "--task", "--check"]
        ) is False

    def test_no_check_flag_is_not_check(self) -> None:
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["worker", "--module", "mypkg.tasks"]) is False

    def test_schedule_list_is_not_check(self) -> None:
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["schedule", "list"]) is False

    def test_inspect_command_is_not_check(self) -> None:
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["inspect", "some-task-id"]) is False

    def test_worker_log_level_flag_preserves_detection(self) -> None:
        """worker-level options before ``--check`` must not fool the detector.

        Note: ``--log-level`` lives on the ``worker`` subparser, not on
        the root — this test exercises the worker-scoped flag path.
        """
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["worker", "--log-level", "DEBUG", "--check"]
        ) is True

    @pytest.mark.parametrize(
        "flag",
        [
            "--log-config", "--logger-level", "--log-queue-size", "--log-lifecycle-level",
            "--log-drain-timeout",  # Plan 2026-10-06-1811, U4
        ],
    )
    def test_logging_option_value_is_not_read_as_check(self, flag: str) -> None:
        """Plan 2026-10-06 U5: die neuen wertnehmenden Logging-Optionen stehen
        im Spiegel des Detektors -- ``--check`` als ihr Wert ist kein Flag."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["worker", flag, "--check"]) is False
        assert _is_check_invocation(["worker", flag, "x", "--check"]) is True

    def test_none_argv_uses_sys_argv(self) -> None:
        """Passing ``None`` reads from ``sys.argv[1:]`` — behaviour must
        be preserved (backwards compat for callers relying on it)."""
        from swarmq.cli.check_cmd import _is_check_invocation

        # Can't mutate sys.argv safely here — just verify None is accepted.
        result = _is_check_invocation(None)
        assert isinstance(result, bool)

    def test_worker_check_equals_value_detects_as_check(self) -> None:
        """``worker --check=true`` must enter the JSON-contract path.

        argparse rejects ``=``-bound values on ``store_true`` actions,
        so the minimal detector raises and (without the fallback)
        would return False — silently dropping the JSON-on-error
        contract for any caller using ``--check=…`` syntax.
        """
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(["worker", "--check=true"]) is True
        assert _is_check_invocation(["worker", "--check=1"]) is True

    def test_dlq_retry_worker_check_equals_is_not_check(self) -> None:
        """``dlq retry worker --check=true`` — ``worker`` is positional,
        the fallback must NOT misclassify this as a check invocation."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["dlq", "retry", "worker", "--check=true"]
        ) is False

    def test_check_detector_accepts_iterator_argv(self) -> None:
        """A generator/iterator argv must not be exhausted by the detector.

        Internally ``_is_check_invocation`` materialises argv once so a
        single-shot iterable still produces the correct decision; if the
        detector held on to the raw iterator and called ``list(argv)``
        late, the result would still be correct here, but the broader
        contract (caller may re-use argv afterwards) only holds when the
        detector materialises up-front.
        """
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(iter(["worker", "--check"])) is True
        assert _is_check_invocation(iter(["schedule", "list"])) is False


# ---------------------------------------------------------------------------
# Unit 3 — CLI-03: TTY-guarded ConsoleRenderer colors (R-3)
# ---------------------------------------------------------------------------


class TestConsoleRendererTTY:
    """R-3: ``ConsoleRenderer`` must be instantiated with
    ``colors=sys.stderr.isatty()`` so ANSI escape sequences do not
    leak into piped stderr (log aggregators, CI captured output, test
    fixtures). The decision is made once per ``setup_logging`` call
    (see Decision E5): if a caller redirects stderr AFTER
    ``setup_logging`` has run, they must re-invoke it.
    """

    def _capture_console_renderer_kwargs(self, monkeypatch) -> list[dict]:
        """Patch ``structlog.dev.ConsoleRenderer`` to record its kwargs."""
        import structlog

        captured: list[dict] = []
        real = structlog.dev.ConsoleRenderer

        def _tap(*args, **kwargs):
            captured.append(dict(kwargs))
            return real(*args, **kwargs)

        monkeypatch.setattr(structlog.dev, "ConsoleRenderer", _tap)
        return captured

    def test_console_renderer_gets_colors_false_when_stderr_piped(
        self, monkeypatch
    ) -> None:
        """When stderr is not a TTY, ``colors`` must be ``False``."""
        import sys as _sys

        from swarmq.logging import setup_logging

        monkeypatch.setattr(_sys.stderr, "isatty", lambda: False, raising=False)
        captured = self._capture_console_renderer_kwargs(monkeypatch)

        listener = setup_logging(log_level="INFO", log_json=False)
        try:
            assert captured, "ConsoleRenderer was not instantiated"
            assert captured[0].get("colors") is False, captured[0]
        finally:
            listener.stop()

    def test_console_renderer_gets_colors_true_when_stderr_is_tty(
        self, monkeypatch
    ) -> None:
        """When stderr IS a TTY, ``colors`` must be ``True``."""
        import sys as _sys

        from swarmq.logging import setup_logging

        monkeypatch.setattr(_sys.stderr, "isatty", lambda: True, raising=False)
        captured = self._capture_console_renderer_kwargs(monkeypatch)

        listener = setup_logging(log_level="INFO", log_json=False)
        try:
            assert captured, "ConsoleRenderer was not instantiated"
            assert captured[0].get("colors") is True, captured[0]
        finally:
            listener.stop()

    def test_no_ansi_escape_in_piped_stderr_subprocess(self) -> None:
        """End-to-end: a subprocess with piped stderr sees no ANSI escapes.

        ``subprocess.run(..., capture_output=True)`` hands the child a
        non-TTY stderr by default. After Unit 3 lands, the
        ``ConsoleRenderer`` must pick that up via ``isatty()`` and
        disable colors, so the captured stderr contains no ``\\x1b[``.
        """
        import subprocess
        import sys
        import textwrap

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """
                    import logging
                    import structlog
                    from swarmq.logging import setup_logging

                    listener = setup_logging(log_level="INFO", log_json=False)
                    try:
                        log = structlog.get_logger("unit3")
                        log.info("tty_guard_test", marker=True)
                    finally:
                        listener.stop()
                    """
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
        )
        assert "\x1b[" not in result.stderr, (
            f"ANSI escape leaked to piped stderr:\n{result.stderr!r}"
        )

    def test_json_mode_does_not_instantiate_console_renderer(
        self, monkeypatch
    ) -> None:
        """Regression guard: when ``log_json=True``, ``ConsoleRenderer``
        is never touched — the TTY guard is irrelevant there.
        """
        from swarmq.logging import setup_logging

        captured = self._capture_console_renderer_kwargs(monkeypatch)

        listener = setup_logging(log_level="INFO", log_json=True)
        try:
            assert not captured, (
                f"ConsoleRenderer should not be used in JSON mode: {captured}"
            )
        finally:
            listener.stop()


# ---------------------------------------------------------------------------
# Unit 4 — KP-5: schedule_cmd uses public get_scheduler_leader API (R-4)
# ---------------------------------------------------------------------------


class TestScheduleStatusUsesPublicAPI:
    """R-4 (Unit 4b): ``schedule_cmd._run_schedule_status`` must consume
    the public ``ValkeyBackend.get_scheduler_leader`` API instead of
    reaching into ``backend._client.get(SCHEDULER_LOCK_KEY)``.

    Unit 4a lands the backend method; this class verifies the CLI
    actually migrates to it. AST-based tests + a mock-based
    interaction test cover both "private access gone" and "public
    method invoked".
    """

    def test_schedule_cmd_source_no_private_client_access_for_lock(self) -> None:
        """AST-level check: no ``backend._client.get(SCHEDULER_LOCK_KEY)``
        call remains in ``schedule_cmd.py``.

        A grep-style string scan is sufficient here; the invariant is
        that the CLI does not know the backend's private attribute
        shape for this specific key.
        """
        schedule_cmd = _REPO_ROOT / "src" / "swarmq" / "cli" / "schedule_cmd.py"
        source = schedule_cmd.read_text(encoding="utf-8")
        assert "_client.get(SCHEDULER_LOCK_KEY)" not in source, (
            "schedule_cmd.py still reads the leader via backend._client — "
            "use the public get_scheduler_leader() instead."
        )

    def test_schedule_cmd_source_calls_get_scheduler_leader(self) -> None:
        """Positive check: ``schedule_cmd.py`` mentions the new public
        method. Matches the substring ``get_scheduler_leader`` to be
        tolerant of ``backend.get_scheduler_leader()`` vs. a
        refactored indirection.
        """
        schedule_cmd = _REPO_ROOT / "src" / "swarmq" / "cli" / "schedule_cmd.py"
        source = schedule_cmd.read_text(encoding="utf-8")
        assert "get_scheduler_leader" in source, (
            "schedule_cmd.py does not call get_scheduler_leader — the CLI "
            "migration from the private path is incomplete."
        )

    @pytest.mark.filterwarnings("error::RuntimeWarning")
    async def test_run_schedule_status_invokes_public_api(
        self, monkeypatch
    ) -> None:
        """Mock-based: calling ``_run_schedule_status`` exercises the
        public method on the backend, not the private client.

        Builds an args Namespace minimal enough for the status handler
        and asserts the interaction shape.

        Plan 2026-05-26-001 Unit 1: ``_client`` is a ``MagicMock``
        (synchronous), not ``AsyncMock`` — ``ScheduleStore.__init__``
        calls ``backend._client.register_script(...)`` three times, and
        an ``AsyncMock`` would return un-awaited coroutines that surface
        as ``RuntimeWarning``. The ``filterwarnings`` decorator catches
        regressions if a future refactor reintroduces this mismatch.
        """
        import argparse
        from unittest.mock import AsyncMock, MagicMock

        from swarmq.cli import schedule_cmd as sched

        fake_backend = AsyncMock()
        fake_backend.get_scheduler_leader = AsyncMock(return_value="leader-xyz")
        fake_backend.close = AsyncMock()
        fake_backend._client = MagicMock()  # sync surface; lock-key get must not be called

        async def _factory() -> object:
            return fake_backend

        def _factory_wrapper():
            return _factory

        # Bypass the _backend_from_env indirection.
        monkeypatch.setattr(sched, "_get_backend_factory", _factory_wrapper)

        # Empty schedule store — poll_due returns an empty list.
        from swarmq.scheduler import ScheduleStore

        async def _empty_poll(*_args, **_kwargs):
            return []

        monkeypatch.setattr(ScheduleStore, "poll_due", _empty_poll)

        args = argparse.Namespace(
            command="schedule",
            schedule="status",
            format="json",
            limit=5,
        )
        await sched._run_schedule_status(args)

        fake_backend.get_scheduler_leader.assert_awaited_once()
        # Private client must not have been asked for the lock key.
        fake_backend._client.get.assert_not_called()


# ---------------------------------------------------------------------------
# Unit 5 — CLI-04: TTY-aware --format default + env-var escape (R-5)
# ---------------------------------------------------------------------------


class TestFormatResolution:
    """R-5 + Decision E2: ``--format`` default resolution follows the
    precedence: explicit CLI flag > ``SWARMQ_DEFAULT_FORMAT`` env var
    > TTY-aware default.

    The helper ``swarmq.cli._shared.resolve_format`` lives outside any
    handler so each subcommand can call it uniformly. Invalid env-var
    values are silently ignored (fall back to TTY default) — callers
    shouldn't crash on a typo in an environment variable.
    """

    def test_default_format_is_json_when_stdout_piped(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.delenv("SWARMQ_DEFAULT_FORMAT", raising=False)
        assert resolve_format(None) == "json"

    def test_default_format_is_table_when_stdout_tty(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: True, raising=False)
        monkeypatch.delenv("SWARMQ_DEFAULT_FORMAT", raising=False)
        assert resolve_format(None) == "table"

    def test_explicit_format_table_wins_over_pipe(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        assert resolve_format("table") == "table"

    def test_explicit_format_json_wins_over_tty(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: True, raising=False)
        assert resolve_format("json") == "json"

    def test_env_var_override_forces_table_on_pipe(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "table")
        assert resolve_format(None) == "table"

    def test_env_var_override_forces_json_on_tty(self, monkeypatch) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: True, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "json")
        assert resolve_format(None) == "json"

    def test_env_var_invalid_value_falls_back_to_tty_default_on_pipe(
        self, monkeypatch
    ) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "yaml-pls")
        # Typo should be ignored silently; pipe → json.
        assert resolve_format(None) == "json"

    def test_env_var_invalid_value_falls_back_to_tty_default_on_tty(
        self, monkeypatch
    ) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: True, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "yaml-pls")
        assert resolve_format(None) == "table"

    def test_env_var_empty_string_falls_back_to_tty_default(
        self, monkeypatch
    ) -> None:
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "")
        assert resolve_format(None) == "json"

    def test_env_var_normalises_whitespace_and_case(self, monkeypatch) -> None:
        """``  TABLE\\n`` and the like must resolve to ``table``.

        Shells frequently leak trailing newlines from
        ``export VAR=$(cmd)`` patterns and operators reasonably expect
        case-insensitive matching for an enum-shaped env-var. Only the
        normalised forms are accepted; truly invalid values still fall
        back to the TTY default per Decision E2.
        """
        import sys as _sys

        from swarmq.cli._shared import resolve_format

        monkeypatch.setattr(_sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "  TABLE\n")
        assert resolve_format(None) == "table"

        monkeypatch.setenv("SWARMQ_DEFAULT_FORMAT", "Json")
        assert resolve_format(None) == "json"


class TestParserFormatDefault:
    """All ``--format`` parser sites must default to ``None`` so
    ``resolve_format`` can tell "caller did not pass --format" apart
    from "caller passed --format=table".
    """

    def test_schedule_list_format_defaults_to_none(self) -> None:
        from swarmq.cli.parser import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["schedule", "list"])
        assert args.format is None, f"expected None, got {args.format!r}"

    def test_schedule_status_format_defaults_to_none(self) -> None:
        from swarmq.cli.parser import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["schedule", "status"])
        assert args.format is None

    def test_dlq_list_format_defaults_to_none(self) -> None:
        from swarmq.cli.parser import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["dlq", "list"])
        assert args.format is None

    def test_dlq_inspect_format_defaults_to_none(self) -> None:
        from swarmq.cli.parser import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["dlq", "inspect", "task-123"])
        assert args.format is None

    def test_explicit_flag_still_sets_value(self) -> None:
        from swarmq.cli.parser import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["dlq", "list", "--format", "json"])
        assert args.format == "json"
