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
from swarmq.slots import effective_concurrency, resolve_default_slots, validate_pool_mode

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
    pool: str
    slots: int
    # U5 (R11): der gepruefte ``MODULE:CALLABLE``-String, oder ``None``.
    configure: str | None
    # U3 (R7): die belegten Portbereiche ``[erster, letzter]``, oder ``None``
    # ohne Basisport. Der Betreiber liest daraus seine Prometheus-Targets.
    metrics_ports: list[int] | None
    health_ports: list[int] | None


def _effective_port(
    cli_value: int | None, env_name: str, cli_flag: str
) -> tuple[int | None, str] | None:
    """Aufgeloester Port samt Herkunft, oder ``None`` wenn keiner wirkt.

    Bildet die Vorrangregel der Konfiguration nach: ``_apply_env_overrides``
    schreibt den Umgebungswert bedingungslos ueber den uebergebenen — die
    Umgebung schlaegt also die Option. Ein Guard auf den Kommandozeilen-
    argumenten allein liesse ``SWARMQ_METRICS_PORT=9000 swarmq worker
    --processes 4`` durch, also genau den Fall, den er abfangen soll
    (Plan 2026-08-28-001, R5).
    """
    import os

    from swarmq.config import _parse_port

    raw = os.environ.get(env_name, "")
    if raw:
        return _parse_port(raw, env_name), env_name
    if cli_value is not None:
        return cli_value, cli_flag
    return None


def _validate_observability_ports(
    *,
    processes: int,
    autoscale: tuple[int, int] | None,
    metrics_port: int | None,
    health_port: int | None,
) -> tuple[list[int] | None, list[int] | None]:
    """Prueft die Portbereiche und gibt sie zurueck (R5, R6).

    Seit U3 (Plan 2026-09-08-001) ist ein fester Port kein Fehler mehr,
    sondern der Basisport: Slot s bindet ``basis + s``. Abgelehnt wird nur
    noch, was auch mit Versatz nicht aufgeht — ein Bereich ueber 65535
    hinaus, oder zwei Bereiche, die sich ueberschneiden (dann wollten zwei
    Slots denselben Port binden).

    Der Autoskalierungsbereich zaehlt mit seiner OBERGRENZE: sonst startet
    der Lauf sauber und kollidiert erst beim ersten Hochskalieren mitten im
    Betrieb (KTD7).
    """
    from swarmq.config import observability_port_ranges

    upper = max(processes, autoscale[1] if autoscale is not None else 1)
    found: dict[str, tuple[int | None, str] | None] = {
        "--metrics-port": _effective_port(metrics_port, "SWARMQ_METRICS_PORT", "--metrics-port"),
        "--health-port": _effective_port(health_port, "SWARMQ_HEALTH_PORT", "--health-port"),
    }

    def _base(flag: str) -> int | None:
        entry = found[flag]
        return entry[0] if entry is not None else None

    def _source(flag: str) -> str:
        entry = found[flag]
        return entry[1] if entry is not None else flag

    base_metrics = _base("--metrics-port")
    base_health = _base("--health-port")
    metrics_range, health_range = observability_port_ranges(base_metrics, base_health, upper)

    for flag, rng in (("--metrics-port", metrics_range), ("--health-port", health_range)):
        if rng is None or rng[1] <= 65535:
            continue
        source = _source(flag)
        raise ConfigurationError(
            f"{upper} worker processes need the port range {rng[0]}-{rng[1]} "
            f"({source}={rng[0]}), which runs past the upper port bound 65535. "
            f"Lower the base port or run fewer processes."
        )

    if metrics_range and health_range:
        overlap = metrics_range[0] <= health_range[1] and health_range[0] <= metrics_range[1]
        if overlap:
            metrics_source = _source("--metrics-port")
            health_source = _source("--health-port")
            raise ConfigurationError(
                f"the observability port ranges overlap for {upper} worker "
                f"processes: {metrics_source} covers "
                f"{metrics_range[0]}-{metrics_range[1]} and {health_source} covers "
                f"{health_range[0]}-{health_range[1]}, so two slots would bind the "
                f"same port. Move one base port at least {upper} ports away."
            )

    return metrics_range, health_range


def _validate_headroom(
    *,
    max_processes: int | None,
    processes: int,
    autoscale: tuple[int, int] | None,
    pool: str,
    concurrency: int | None,
) -> None:
    """``--max-processes`` gegen die Startwerte pruefen (Kontrollkanal U3, R1/R3).

    Der Spielraum dimensioniert Slot-Tabelle und Portbereiche (R4); liegt er
    unter der Startprozesszahl oder der Autoscale-Obergrenze, gaebe es beim
    Start mehr Worker als Slots. Ohne das Argument bleibt alles wie bisher (R2).
    """
    if max_processes is None:
        return
    if max_processes < processes:
        raise ConfigurationError(
            f"--max-processes {max_processes} is below --processes {processes}: "
            "the headroom must cover the start-up worker count"
        )
    if autoscale is not None and max_processes < autoscale[1]:
        raise ConfigurationError(
            f"--max-processes {max_processes} is below the --autoscale upper bound {autoscale[1]}: "
            "the headroom must cover the autoscale range"
        )
    if pool != "cpu":
        return
    available = resolve_default_slots(1).count
    if concurrency is None and max_processes > available:
        raise ConfigurationError(
            f"--max-processes {max_processes} exceeds the {available} CPUs assigned to this worker: "
            "with --pool cpu every worker process needs at least one compute slot, so a setpoint "
            "at the headroom would overbook the CPUs. Lower --max-processes or assign more CPUs."
        )
    if concurrency is not None and concurrency * max_processes > available:
        raise ConfigurationError(
            f"--concurrency {concurrency} x --max-processes {max_processes} = {concurrency * max_processes} "
            f"compute slots exceed the {available} CPUs assigned to this worker: a setpoint at the "
            "headroom would overbook the CPUs. Lower --concurrency or --max-processes."
        )


def _validate_configure_spec(spec: str) -> None:
    """Form ``MODULE:CALLABLE`` und Auffindbarkeit des Moduls (R11)."""
    module_name, sep, attr_name = spec.partition(":")
    if not sep or not module_name or not attr_name.isidentifier():
        raise ConfigurationError(
            f"--configure {spec!r} is not MODULE:CALLABLE "
            "(expected: package.module:callable)"
        )
    _validate_module_importable(module_name)


def check_worker_config(
    *,
    module: str,
    reload: bool,
    watch_dir: list[str] | tuple[str, ...] | None,
    burst: bool = False,
    processes: int = 1,
    autoscale: tuple[int, int] | None = None,
    metrics_port: int | None = None,
    health_port: int | None = None,
    pool: str = "io",
    concurrency: int | None = None,
    recycling: bool = False,
    configure: str | None = None,
    max_processes: int | None = None,
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

    # Die Warnung ``recycling_with_fixed_port`` entfaellt mit U3: das
    # Bind-Budget des Ersatzes leitet sich jetzt aus beiden Freigabefristen
    # ab (KTD4), statt fest 5 s zu betragen.
    warnings: list[str] = []
    validate_pool_mode(pool=pool, autoscale=autoscale)
    _validate_headroom(
        max_processes=max_processes, processes=processes, autoscale=autoscale, pool=pool, concurrency=concurrency,
    )
    slots = effective_concurrency(concurrency, pool, processes)

    # Vor dem teuren Modul-Import: eine Portkollision ist ohne Dateisystem-
    # oder Import-Arbeit entscheidbar.
    metrics_ports, health_ports = _validate_observability_ports(
        processes=processes,
        autoscale=autoscale,
        metrics_port=metrics_port,
        health_port=health_port,
    )

    # U5/R11: Form und Auffindbarkeit des Hook-Moduls, ohne Import. Ein
    # Import hier liesse Anwendungscode im Supervisor laufen (und im
    # Forkserver-Preload); das Attribut loest erst der Worker auf.
    if configure is not None:
        _validate_configure_spec(configure)

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
        "pool": pool,
        "slots": slots.count,
        "configure": configure,
        "metrics_ports": metrics_ports,
        "health_ports": health_ports,
    }
