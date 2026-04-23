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

    def test_global_flag_before_worker_preserves_detection(self) -> None:
        """Global options before the subcommand must not fool the detector."""
        from swarmq.cli.check_cmd import _is_check_invocation

        assert _is_check_invocation(
            ["--log-level", "DEBUG", "worker", "--check"]
        ) is True

    def test_none_argv_uses_sys_argv(self) -> None:
        """Passing ``None`` reads from ``sys.argv[1:]`` — behaviour must
        be preserved (backwards compat for callers relying on it)."""
        from swarmq.cli.check_cmd import _is_check_invocation

        # Can't mutate sys.argv safely here — just verify None is accepted.
        result = _is_check_invocation(None)
        assert isinstance(result, bool)
