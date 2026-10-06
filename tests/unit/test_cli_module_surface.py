"""Surface audit for ``swarmq.cli``.

Frozen public attribute set of the ``swarmq.cli`` package/module.
Exists because the refactor from ``src/swarmq/cli.py`` to
``src/swarmq/cli/`` (plan 2026-04-22-001) must keep every pre-split
dotted path alive:

- ``swarmq.process_manager`` does ``from swarmq.cli import
  run_worker_async`` (process_manager.py:754).
- ``pyproject.toml`` console script is ``swarmq = "swarmq.cli:main"``.
- Tests patch via string paths:
  ``patch("swarmq.cli.run_worker_async", ...)``,
  ``patch("swarmq.cli.asyncio.run", ...)``,
  ``monkeypatch.setattr("swarmq.cli.run_worker_async", ...)``.
- Tests import directly: ``_autoscale_range``, ``_build_parser``,
  ``_watch_dir_path``, ``main``.

WHITELIST-WARTUNG — wann diese Liste legitim geändert werden darf:

- Neuer öffentlicher Export MIT konkretem Test-Konsumenten oder
  dokumentiertem externen Aufruf — nicht "weil ruff ``F401`` meldet",
  nicht "weil ein Submodul es zufällig importiert".
- Entfernen nur, wenn **alle** Konsumenten umgezogen sind
  (grep-verifiziert über tests/, src/, docs/).

Whitelist regenerieren:
    python -c "import swarmq.cli; \
        print(sorted(n for n in dir(swarmq.cli) if not n.startswith('__')))"

Tagging-Konvention in der Whitelist unten:
    # public       — dokumentiertes oder verdrahtetes Einstiegssymbol
    # internal     — CLI-interner Helfer, nur via dotted-path-Patch
                    adressiert; kann bei künftigem Split wandern
    # ambient      — stdlib-/third-party-Namespace-Verschmutzung aus
                    ``from … import …`` in cli.py, eingefroren für
                    R-1-Kompatibilität (unbekannte Konsumenten)
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

import pytest

import swarmq.cli

# --- frozen public surface ---------------------------------------------------

_CLI_PUBLIC_SURFACE: frozenset[str] = frozenset(
    {
        # --- public CLI entry points -----------------------------------------
        "main",                         # public — swarmq = "swarmq.cli:main"
        "run_worker_async",             # public — process_manager.py:754
        # --- internal helpers (patch targets / direct imports in tests) ------
        "_DLQNotFound",                 # internal
        "_DLQ_NOT_FOUND_EXIT_CODE",     # internal
        "_DOTTED_IDENTIFIER_RE",        # internal
        "_FORBIDDEN_WATCH_PREFIXES",    # internal
        "_LAZY_SURFACE",                # internal — PEP-562 lazy-surface registry (R-1a, plan 2026-04-23-001)
        "_VALID_LOG_FORMATS",           # internal
        "_VALID_LOG_LEVELS",            # internal
        "_WORKER_CHECK_PAYLOAD_KEYS",   # internal
        # U4 (Plan 2026-09-08-001, R19/KTD8): Exit-Code-Vertrag im
        # Worker-Zweig. Die Konstante kommt aus ``swarmq.supervision`` und
        # wird hier gelesen, damit ``cli`` sie ohne Import von
        # ``process_manager`` kennt.
        "CONFIG_ERROR_EXIT_CODE",       # ambient — aus swarmq.supervision
        "_log_worker_config_error",     # internal — worker_config_error (R19)
        "_setup_worker_logging",        # internal — setup_logging als erster Schritt (KTD8)
        "_flush_worker_logging",        # internal — flush-on-exit (KTD8)
        "_autoscale_range",             # internal — test_cli_multiprocess.py
        "prepare_pool_args",            # internal — Pool-Modus + --concurrency Aufloesung (Plan 2026-09-03-001)
        "_backend_from_env",            # internal
        "_parse_args_for_mode",         # internal — main()-Zerlegung (Follow-Up 0.2.x)
        "_run_command",                 # internal — dito
        "_run_worker_command",          # internal — dito
        "_startup_error_classes",       # internal — dito
        "_broker_from_env",             # internal
        "_build_parser",                # internal — test_cli_multiprocess.py
        "_dotted_module_name",          # internal
        "_emit_argparse_check_error",   # internal
        "_format_dlq_list_table",       # internal
        "_format_inspect_table",        # internal
        "_format_schedule_table",       # internal
        "_is_check_invocation",         # internal
        "_normalise_headers_for_json",  # internal
        "_positive_float",              # internal
        "_positive_int",                # internal
        "_queue_list",                  # internal
        "_render_table",                # internal
        "_run_dlq_async",               # internal
        "_run_dlq_inspect",             # internal
        "_run_dlq_list",                # internal
        "_run_dlq_purge",               # internal
        "_run_dlq_retry",               # internal
        "_run_inspect_async",           # internal
        "_run_schedule_add",            # internal
        "_run_schedule_async",          # internal
        "_run_schedule_list",           # internal
        "_run_schedule_remove",         # internal
        "_run_schedule_status",         # internal
        "_run_worker_check",            # internal
        "_seed_check_payload",          # internal
        "_swarmq_version",              # internal
        "_watch_dir_path",              # internal — test_cli_multiprocess.py
        "resolve_format",               # internal — TTY+env --format resolver (R-5, CLI-04)
        # --- ambient imports (R-1 compat: frozen to avoid silent breakage) --
        "Any",                          # ambient
        "TYPE_CHECKING",                # ambient — U12: Typ-Import für die Sampler-Factory
        "BackendConnectionError",       # ambient
        "BrokerConnectionError",        # ambient
        "ConfigurationError",           # ambient
        "Path",                         # ambient
        "RabbitMQBroker",               # ambient
        "SCHEDULER_LOCK_KEY",           # ambient
        "ScheduleDefinition",           # ambient
        "ScheduleStore",                # ambient
        "Sequence",                     # ambient
        "SwarmQ",                       # ambient
        "Task",                         # ambient
        "UTC",                          # ambient
        "ValkeyBackend",                # ambient
        "annotations",                  # ambient — __future__
        "argparse",                     # ambient
        "asyncio",                      # ambient — patch("swarmq.cli.asyncio.run")
        "compute_dedup_hash",           # ambient
        "datetime",                     # ambient
        "importlib",                    # ambient
        "json",                         # ambient
        "msgspec",                      # ambient
        "os",                           # ambient
        "re",                           # ambient
        "sys",                          # ambient
        "time",                         # ambient
        "traceback",                    # ambient
        # --- submodules exposed by the cli/ package (new, post-split) -----
        # Package-attribute bindings created automatically when a submodule
        # is imported from __init__.py. Kept in the whitelist so
        # `from swarmq.cli import dlq_cmd` remains valid (useful for
        # advanced testing / introspection) and so that accidental
        # removal of a submodule is caught early.
        "_shared",                      # package — shared helpers
        "check_cmd",                    # package — --check handler
        "dlq_cmd",                      # package — dlq subcommand
        "inspect_cmd",                  # package — inspect subcommand
        "parser",                       # package — argparse tree
        "pool",                         # package — --pool / --concurrency Aufloesung (Plan 2026-09-03-001)
        "schedule_cmd",                 # package — schedule subcommand
        "worker_cmd",                   # package — worker command
    }
)


def _current_surface() -> frozenset[str]:
    return frozenset(n for n in dir(swarmq.cli) if not n.startswith("__"))


# --- tests -------------------------------------------------------------------


class TestPublicSurfaceEquality:
    """R-1: dotted-path stability is an equality, not a superset."""

    def test_no_symbols_removed(self) -> None:
        missing = _CLI_PUBLIC_SURFACE - _current_surface()
        assert not missing, (
            f"Symbols removed from swarmq.cli: {sorted(missing)}. "
            "A silent removal breaks patch('swarmq.cli.X') in downstream "
            "tests and process_manager.py:754. See WHITELIST-WARTUNG above."
        )

    def test_no_symbols_added(self) -> None:
        extra = _current_surface() - _CLI_PUBLIC_SURFACE
        assert not extra, (
            f"Symbols added to swarmq.cli: {sorted(extra)}. "
            "New public exports must be whitelisted explicitly with a "
            "tag (# public | # internal | # ambient) and a rationale. "
            "See WHITELIST-WARTUNG above."
        )


class TestSymbolIdentity:
    """Each whitelisted name must resolve to a usable object.

    ``dir()`` reports presence but can include unbound or partially
    initialised references during circular import recovery. This
    catches re-export wiring that silently breaks ``getattr`` (e.g.
    a ``None`` placeholder or a stale module object).
    """

    @pytest.mark.parametrize("name", sorted(_CLI_PUBLIC_SURFACE))
    def test_getattr_succeeds(self, name: str) -> None:
        obj = getattr(swarmq.cli, name)
        assert obj is not None, f"swarmq.cli.{name} resolves to None"


class TestLivePatchThrough:
    """Verify facade-level patches intercept real call sites.

    The refactor keeps ``main()`` inside ``swarmq.cli`` itself (not in
    a submodule) so that ``patch('swarmq.cli.run_worker_async')``
    replaces the exact binding ``main()`` resolves. This test fails if
    a future split moves ``main()`` into a submodule that imports
    ``run_worker_async`` into its own namespace — the classic
    "patch where it is used" trap.
    """

    def test_patch_swarmq_cli_run_worker_async_is_called_by_worker_entry(self) -> None:
        """Seit R20 (Plan 2026-09-03-001) ruft nicht mehr main(), sondern der
        Worker-Subprozess ``run_worker_async`` — weiterhin ueber den
        ``swarmq.cli``-Namensraum, damit ``patch('swarmq.cli.run_worker_async')``
        den Aufruf abfaengt (Target-Structure-Note im Refactor-Plan
        docs/plans/2026-04-22-001-refactor-cli-split-plan.md)."""
        from swarmq.process_manager.entry import worker_subprocess_entry
        from tests.unit.process_manager._helpers import _make_test_cfg

        async def _noop(_args):  # AsyncMock-compatible stand-in
            return None

        shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
        with patch("swarmq.cli.run_worker_async", side_effect=_noop) as mock:
            worker_subprocess_entry(0, shutdown_event, _make_test_cfg())
        assert mock.call_count == 1, (
            "patch('swarmq.cli.run_worker_async') did not intercept the call "
            "from worker_subprocess_entry(); the entry must resolve "
            "run_worker_async via the swarmq.cli namespace."
        )

    # ------------------------------------------------------------------
    # Plan 2026-04-23-001 Unit 6 / RR-2: the same live-patch guarantee
    # for the other three dispatched async runners. A future refactor
    # that rebinds these names via a submodule-local ``from ... import``
    # inside ``main()`` would silently defeat ``patch('swarmq.cli.X')``
    # — these tests break first.
    # ------------------------------------------------------------------

    def test_patch_swarmq_cli_run_schedule_async_is_called_by_main(self) -> None:
        async def _noop(_args):
            return None

        with patch(
            "swarmq.cli._run_schedule_async", side_effect=_noop
        ) as mock:
            rc = swarmq.cli.main(["schedule", "list"])
        assert mock.call_count == 1, (
            "patch('swarmq.cli._run_schedule_async') did not intercept the "
            "call from main(). See plan 2026-04-23-001 Unit 6 — main()'s "
            "schedule dispatch must resolve _run_schedule_async via the "
            "swarmq.cli namespace."
        )
        assert rc == 0

    def test_patch_swarmq_cli_run_dlq_async_is_called_by_main(self) -> None:
        async def _noop(_args):
            return None

        with patch("swarmq.cli._run_dlq_async", side_effect=_noop) as mock:
            rc = swarmq.cli.main(["dlq", "list"])
        assert mock.call_count == 1, (
            "patch('swarmq.cli._run_dlq_async') did not intercept the "
            "call from main(). Plan 2026-04-23-001 Unit 6 — dlq dispatch "
            "must resolve via the facade namespace."
        )
        assert rc == 0

    def test_patch_swarmq_cli_run_inspect_async_is_called_by_main(self) -> None:
        async def _noop(_args):
            return None

        with patch(
            "swarmq.cli._run_inspect_async", side_effect=_noop
        ) as mock:
            rc = swarmq.cli.main(["inspect", "--module", "swarmq"])
        assert mock.call_count == 1, (
            "patch('swarmq.cli._run_inspect_async') did not intercept the "
            "call from main(). Plan 2026-04-23-001 Unit 6 — inspect dispatch "
            "must resolve via the facade namespace."
        )
        assert rc == 0

    def test_patch_swarmq_cli__DLQNotFound_is_caught_by_main(self) -> None:
        """The ``_DLQNotFound`` class on the facade IS the class raised
        deep in ``dlq_cmd``.

        ``main()``'s ``except _DLQNotFound`` clause resolves the name
        against the ``swarmq.cli`` facade namespace, while the raise
        sites in ``dlq_cmd._run_dlq_inspect`` / ``_run_dlq_retry`` use
        their own module-local binding. The whole exit-code-3 contract
        (R3.10 P3 #19) only holds when those two bindings are the same
        class object. If a future refactor created a separate
        ``_DLQNotFound`` in the facade (e.g. a re-aliased subclass),
        the ``except`` would silently miss the raise.
        """
        import swarmq.cli
        from swarmq.cli import dlq_cmd

        # Identity check — the facade name and the raise-site name
        # must point at the same class. This is what makes
        # ``except _DLQNotFound`` in main() catch the raise.
        assert swarmq.cli._DLQNotFound is dlq_cmd._DLQNotFound

        # Functional check — patch the dispatcher to raise the facade's
        # _DLQNotFound directly. main()'s except clause must convert
        # that into the documented exit code.
        async def _raise_facade_dlq_not_found(_args):
            raise swarmq.cli._DLQNotFound

        with patch(
            "swarmq.cli._run_dlq_async",
            side_effect=_raise_facade_dlq_not_found,
        ):
            rc = swarmq.cli.main(["dlq", "list"])
        assert rc == swarmq.cli._DLQ_NOT_FOUND_EXIT_CODE, (
            "main() did not convert _DLQNotFound (raised through the "
            "facade) into the documented exit code. Plan 2026-04-23-001 "
            "Unit 6 / R3.10 P3 #19."
        )

    @pytest.mark.skip(
        reason=(
            "Demonstration-only, plan 2026-04-23-001 Unit 6: shows what "
            "refactor anti-pattern would break the live-patch guarantees "
            "above. Unskip locally to verify the assertion flips RED — "
            "replace, say, the schedule dispatch line in cli/__init__.py::"
            "main() with a submodule-local ``from swarmq.cli.schedule_cmd "
            "import _run_schedule_async`` (inside the elif body) and "
            "re-run. The live-patch test for schedule will turn RED while "
            "the other live-patch tests stay GREEN — exactly the "
            "regression signature this skipped-demo documents. Do NOT "
            "unskip in CI."
        )
    )
    def test_demonstration_submodule_import_defeats_facade_patch(self) -> None:
        # Intentionally empty — the docstring above IS the test.
        pass


class TestSubprocessImport:
    """Fresh-interpreter import cannot hide circular-init bugs.

    In-process pytest has ``swarmq.cli`` already loaded from an earlier
    test, so partial-init hazards are invisible. A subprocess starts
    from scratch and fails loudly if any whitelisted symbol is only
    reachable after a warm-cache ``reload``.
    """

    @pytest.mark.timeout(45)
    def test_fresh_interpreter_can_import_all_surface(self) -> None:
        names = sorted(_CLI_PUBLIC_SURFACE)
        script = (
            "import swarmq.cli; "
            f"names = {names!r}; "
            "missing = [n for n in names if not hasattr(swarmq.cli, n)]; "
            "assert not missing, missing"
        )
        # Pass the current env through so the subprocess uses the same
        # editable install resolution as the test interpreter. Without
        # this, PYTHONPATH / VIRTUAL_ENV inheritance is fine today, but
        # an adversarial reviewer flagged the risk of a CI environment
        # where a globally-installed release shadows the editable
        # checkout — keeping env= explicit makes the contract visible.
        import os
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env=os.environ.copy(),
        )
        assert result.returncode == 0, (
            f"Fresh-interpreter import failed.\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    @pytest.mark.timeout(45)
    def test_python_m_swarmq_cli_version_works(self) -> None:
        """`python -m swarmq.cli --version` is the entry point tests
        and chaos harnesses rely on; verify __main__.py wiring survives
        the package split. Without this, regressing to a missing or
        broken __main__.py would only surface in integration/e2e.
        """
        import os
        result = subprocess.run(
            [sys.executable, "-m", "swarmq.cli", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env=os.environ.copy(),
        )
        assert result.returncode == 0, (
            f"`python -m swarmq.cli --version` failed with "
            f"rc={result.returncode}.\nstdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
        assert "swarmq" in result.stdout.lower()


class TestMainModuleGuard:
    """``swarmq/cli/__main__.py`` braucht den ``__name__``-Guard: Spawn-Kinder
    importieren das Hauptmodul erneut als ``__mp_main__``. Ohne Guard ruft der
    Import ``main()`` und das Kind scheitert mit "An attempt has been made to
    start a new process before the current process has finished its
    bootstrapping phase" (Hot-Reload unter ``python -m swarmq.cli``)."""

    def test_import_as_non_main_module_does_not_call_main(self, monkeypatch) -> None:
        import runpy
        from unittest.mock import MagicMock

        fake_main = MagicMock(return_value=0)
        monkeypatch.setattr("swarmq.cli.main", fake_main)
        runpy.run_module("swarmq.cli.__main__", run_name="__mp_main__")
        fake_main.assert_not_called()

    def test_run_as_main_calls_main_and_exits_with_its_code(self, monkeypatch) -> None:
        import runpy
        from unittest.mock import MagicMock

        fake_main = MagicMock(return_value=3)
        monkeypatch.setattr("swarmq.cli.main", fake_main)
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("swarmq.cli.__main__", run_name="__main__")
        fake_main.assert_called_once_with()
        assert exc_info.value.code == 3
