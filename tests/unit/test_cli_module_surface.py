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
        "_VALID_LOG_FORMATS",           # internal
        "_VALID_LOG_LEVELS",            # internal
        "_WORKER_CHECK_PAYLOAD_KEYS",   # internal
        "_autoscale_range",             # internal — test_cli_multiprocess.py
        "_backend_from_env",            # internal
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
        # --- ambient imports (R-1 compat: frozen to avoid silent breakage) --
        "Any",                          # ambient
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

    def test_patch_swarmq_cli_run_worker_async_is_called_by_main(self) -> None:
        async def _noop(_args):  # AsyncMock-compatible stand-in
            return None

        with patch("swarmq.cli.run_worker_async", side_effect=_noop) as mock:
            rc = swarmq.cli.main(
                [
                    "worker",
                    "--module",
                    "swarmq",  # any importable module; run is patched
                ]
            )
        assert mock.call_count == 1, (
            "patch('swarmq.cli.run_worker_async') did not intercept the "
            "call from main(). main() must resolve run_worker_async via "
            "the swarmq.cli namespace, not via a private import in a "
            "submodule. See the Target Structure note in the refactor "
            "plan (docs/plans/2026-04-22-001-refactor-cli-split-plan.md)."
        )
        assert rc == 0


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
