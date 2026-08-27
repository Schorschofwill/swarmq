"""``--check``-Pfad (Plan U21) — verbatim: Modul-Importbarkeit,
``WorkerConfigCheckResult`` und ``check_worker_config`` (JSON-Kontrakt)."""

from __future__ import annotations

import importlib.util
import sys
from importlib.machinery import ModuleSpec
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

import structlog

from swarmq.exceptions import ConfigurationError

logger = structlog.get_logger()


def _validate_module_importable(module: str) -> ModuleSpec:
    """Pre-flight check: raise ConfigurationError if ``module`` cannot
    be resolved.

    Runs in the PARENT before the supervisor starts, so a typo or
    a filesystem-level missing module fails once with a clear error
    instead of looping through the 5-per-hour subprocess restart cap.

    Known limitations (find_spec is not a Sandbox — dokumentiert im
    Plan unter Unit 2). Die folgenden Fälle laufen weiterhin in den
    Subprocess-Restart-Loop, weil find_spec das Modul NICHT lädt:

    - fehlende transitive Dependency (tasks.py existiert, importiert
      aber ein nicht installiertes Paket)
    - kaputte C-Extension / ABI-Mismatch
    - env-abhängige Top-Level-Imports mit Parent/Child-Divergenz
    - Zirkuläre Imports, die partiell resolven
    - Namespace-Package-Shadowing über PYTHONPATH-Order

    Für diese Fälle greift das Restart-Rate-Limit (5/h). Operator soll
    ``swarmq inspect --module ...`` nutzen um Import-Fehler vorab zu
    diagnostizieren.

    Note that resolving ``pkg.sub`` via
    find_spec imports and executes ``pkg/__init__.py`` in THIS
    process (the supervisor). For most operators that is harmless;
    for packages with side-effectful top-level code (thread spawn,
    signal-handler installation) the effects are visible in the
    supervisor's process state. The check runs only at startup
    (inside ``run_multi_process_worker``), not on SIGHUP-reload.
    """
    try:
        spec = importlib.util.find_spec(module)
    except (ImportError, ValueError) as exc:
        # ImportError covers ModuleNotFoundError + the "no package
        # specified for '.relative'" case; ValueError handles some
        # malformed dotted paths depending on Python version.
        raise ConfigurationError(
            f"--module {module!r} cannot be imported: {exc}",
        ) from exc
    # ``spec.origin is None`` für Namespace-Packages ohne __init__.py —
    # find_spec liefert einen spec, aber import_module wird scheitern.
    if spec is None or spec.origin is None:
        raise ConfigurationError(
            f"--module {module!r} resolves to no loadable spec — "
            "check spelling, PYTHONPATH, and __init__.py presence",
        )
    # Return the spec so run_multi_process_worker
    # can hand it to resolve_watch_dirs instead of calling find_spec
    # a second time (which would re-execute pkg/__init__.py in the
    # edge case where it invalidates its own sys.modules entry).
    return spec


class WorkerConfigCheckResult(TypedDict):
    """Return shape for :func:`check_worker_config`.

    TypedDict so mypy narrows field accesses
    instead of treating values as ``object``. Mirrors the ``--check``
    JSON payload keys but drops ``ok``/``error``/``error_type`` —
    those are CLI-level framing (success implies ok=True; raise on
    failure).
    """

    module: str
    module_origin: str | None
    reload: bool
    watch_dirs: list[str] | None
    warnings: list[str]


def check_worker_config(
    *,
    module: str,
    reload: bool,
    watch_dir: list[str] | tuple[str, ...] | None,
    burst: bool = False,
) -> WorkerConfigCheckResult:
    """Run the pre-flight validation for ``swarmq worker`` and return
    a machine-readable summary dict.

    Scope mirrors ``run_multi_process_worker``'s pre-supervisor checks
    so a successful ``check_worker_config`` result implies the
    supervisor would get past pre-flight:

    - ``module`` is importable (``find_spec``-based).
    - When ``reload=True``: ``watchfiles`` is installed and watch-dirs
      resolve to a non-empty tuple (stdlib + ``[reload]`` extra).
    - When ``reload=True`` and ``burst=True``: a non-fatal warning is
      recorded; the combination is ambiguous in intent but not
      rejected.

    Does **not** connect to broker or backend. Raises
    :class:`ConfigurationError` on any validation failure.

    ``stdout`` is captured while importing the module to protect
    machine-readable callers (``--check``) against ``print()`` calls
    inside ``pkg/__init__.py``.

    Returned dict has a fixed shape with every key always present —
    callers can rely on ``dict["watch_dirs"]`` being accessible
    regardless of ``reload`` flag. Unused fields are ``None``.
    """
    import contextlib
    import io

    from swarmq.hot_reload import (
        check_watchfiles_available,
        resolve_watch_dirs,
    )

    warnings: list[str] = []

    # Protect JSON-on-stdout callers against
    # module-init print() contaminating the payload. Side-effects
    # captured here are discarded on success; on failure the captured
    # text is forwarded to stderr so an operator debugging a bad
    # --module sees it.
    capture = io.StringIO()
    try:
        with contextlib.redirect_stdout(capture):
            module_spec = _validate_module_importable(module)
    except Exception:
        # Forward whatever the module printed to stderr before re-raising
        # so humans still get the context.
        sys.stderr.write(capture.getvalue())
        raise

    watch_dirs_result: list[str] | None = None
    if reload:
        check_watchfiles_available()
        resolved = resolve_watch_dirs(
            module, tuple(watch_dir or ()), spec=module_spec,
        )
        watch_dirs_result = list(resolved)

        if burst:
            warnings.append(
                "reload_burst_conflict: --reload and --burst are not "
                "designed to be combined; burst drains-and-exits while "
                "reload re-execs the supervisor. Pick one per invocation."
            )

    return {
        "module": module,
        "module_origin": module_spec.origin,
        "reload": reload,
        "watch_dirs": watch_dirs_result,
        "warnings": warnings,
    }
