"""Shared helpers for the ``swarmq`` CLI subcommands.

Holds argparse type-functions, connection factories and the generic
table renderer that more than one subcommand consumes. Command-
specific helpers (e.g. ``_format_schedule_table``) live beside their
command handler, not here.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

from swarmq.config_validation import MAX_DEADLINE_S
from swarmq.exceptions import ConfigurationError
from swarmq.logging_settings import VALID_LOG_FORMATS, VALID_LOG_LEVELS

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

_VALID_OUTPUT_FORMATS = frozenset({"table", "json"})
_AUTOSCALE_BOUND_COUNT = 2  # --autoscale MIN,MAX

#: Canonical ``choices=`` tuple for every ``--format`` add_argument site in
#: :mod:`swarmq.cli.parser`. Keep alignment with ``_VALID_OUTPUT_FORMATS``
#: — argparse needs a sequence with stable iteration order for help text.
_FORMAT_CHOICES: tuple[str, ...] = ("table", "json")


def resolve_format(explicit: str | None) -> str:
    """Decide the effective ``--format`` value for output-shaped commands.

    Precedence (plan 2026-04-23-001 Unit 5 / CLI-04):

    1. Explicit ``--format`` on the command line (``explicit`` non-None).
    2. ``SWARMQ_DEFAULT_FORMAT`` environment variable, if it names a
       valid format (``table`` or ``json``).
    3. TTY-aware fallback: ``"table"`` when ``sys.stdout.isatty()`` is
       true, else ``"json"`` — machine-readable by default for
       pipe/file/log redirects.

    Invalid env-var values (typo, unknown format, empty string) are
    silently ignored so a misconfigured shell environment does not
    crash the CLI. Decision E2.
    """
    if explicit is not None:
        return explicit
    # Tolerate surrounding whitespace and case variation — shells often
    # carry trailing newlines from ``export VAR=$(...)`` patterns and
    # operators reasonably expect ``TABLE`` to mean ``table``. Unknown
    # values still fall through to the TTY default (Decision E2).
    override = os.environ.get("SWARMQ_DEFAULT_FORMAT", "").strip().lower()
    if override in _VALID_OUTPUT_FORMATS:
        return override
    return "table" if sys.stdout.isatty() else "json"


# Eine Quelle fuer CLI, SwarmQConfig und WorkerConfig; CRITICAL gilt
# ueberall (Plan 2026-10-06, KTD8).
_VALID_LOG_LEVELS = VALID_LOG_LEVELS
_VALID_LOG_FORMATS = VALID_LOG_FORMATS


def _positive_int(raw: str) -> int:
    """argparse type-function — rejects values < 1 with a clear message."""
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an integer: {raw!r}") from exc
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1 (got {value})")
    return value


def _deadline_seconds(raw: str) -> int:
    """argparse type-function for a deadline in whole seconds: ``>= 1`` and at
    most ``MAX_DEADLINE_S`` like ``TaskInfo`` (E-063, Nachtrag 2026-10-19)."""
    value = _positive_int(raw)
    if value > MAX_DEADLINE_S:
        raise argparse.ArgumentTypeError(f"must be at most {MAX_DEADLINE_S} seconds (got {value})")
    return value


def _positive_float(raw: str) -> float:
    """argparse type-function — rejects values <= 0 with a clear message."""
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a number: {raw!r}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0 (got {value})")
    if not math.isfinite(value):
        # E-063: Fristen sind endlich; sonst scheitert der Start erst im Kind.
        raise argparse.ArgumentTypeError(f"must be a finite number (got {value})")
    return value


#: Dotted Python-3 identifier grammar (PEP 3131-compliant). ``\w`` under
#: ``re.UNICODE`` matches ASCII alphanumerics + underscore plus every
#: Python-valid Unicode letter/digit (café, 日本 etc.). ``[^\W\d]`` =
#: word-char but not digit = a valid identifier start. We block
#: shell-injection-shaped inputs (/, -, whitespace), **not** valid
#: Python names in non-ASCII languages. Security scope: this is a
#: reliability/UX guard, NOT a code-execution sandbox — importlib still
#: imports any regex-compliant name, so the argv trust boundary (Unit 6
#: startup-snapshot + Unit 8 docs) is the real threat gate.
_DOTTED_IDENTIFIER_RE = re.compile(
    r"^[^\W\d]\w*(\.[^\W\d]\w*)*$",
    re.UNICODE,
)


def _dotted_module_name(raw: str) -> str:
    """argparse type-function: accept dotted Python identifiers.

    Rejects obvious non-imports (paths, hyphens, leading digits, empty
    segments) with a clear error. PEP 3131 Unicode identifiers are
    accepted because Python itself accepts them as module names and
    SwarmQ does not target an ASCII-only operator base.
    """
    if not _DOTTED_IDENTIFIER_RE.match(raw):
        raise argparse.ArgumentTypeError(
            f"--module {raw!r} is not a dotted Python identifier (expected: package.module.submodule)"
        )
    return raw


def _configure_hook(raw: str) -> str:
    """argparse type-function fuer ``--configure MODULE:CALLABLE`` (R9/R11).

    Geprueft wird nur die Form: ein dotted Modulname, ein Doppelpunkt, ein
    Attributname. Der Import passiert im Worker, nicht hier -- der Supervisor
    importiert das Hook-Modul nie (KTD5), sonst liefe Anwendungscode im
    Elternprozess und im Forkserver.
    """
    module, sep, attr = raw.partition(":")
    if not sep or not _DOTTED_IDENTIFIER_RE.match(module) or not attr.isidentifier():
        raise argparse.ArgumentTypeError(
            f"--configure {raw!r} is not MODULE:CALLABLE (expected: package.module:callable)"
        )
    return raw


def _autoscale_range(raw: str) -> tuple[int, int]:
    """argparse type-function for ``--autoscale=MIN,MAX``.

    R4.7: accepts two positive integers separated by a comma. MIN
    must be >= 1 and MAX must be >= MIN. Values outside that grammar
    raise :class:`argparse.ArgumentTypeError` so the parser surfaces
    a single clean error line instead of a Python traceback.
    """
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) != _AUTOSCALE_BOUND_COUNT:
        raise argparse.ArgumentTypeError(f"--autoscale must be MIN,MAX (got {raw!r})")
    try:
        low, high = int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--autoscale values must be integers (got {raw!r})") from exc
    if low < 1 or high < low:
        raise argparse.ArgumentTypeError(f"--autoscale requires 1 <= MIN <= MAX (got MIN={low} MAX={high})")
    return low, high


# Pseudo-filesystems generate inotify
# events on every process / device / attribute read. Watching them
# triggers reload-storms at kernel-event rate. Argparse rejects these
# up-front so --check exit 0 cannot mislead an operator into a
# deployment that would reload-loop the moment it starts.
_FORBIDDEN_WATCH_PREFIXES = ("/proc", "/sys", "/dev")


def _watch_dir_path(raw: str) -> str:
    """argparse type-function for ``--watch-dir PATH``.

    R5.3: accepts only existing directories and returns an absolute
    path so duplicate-dir-with-different-relative-spelling collapses
    cleanly at parse-time instead of watchfiles discovering it later.

    Rejects pseudo-filesystem prefixes (``/proc``, ``/sys``, ``/dev``)
    that would cause reload-storms on Linux: every process spawn
    generates ``/proc/*`` inotify events, every device attribute read
    generates ``/sys/*`` events.
    """
    p = Path(raw)
    if not p.is_dir():
        raise argparse.ArgumentTypeError(f"--watch-dir {raw!r} is not an existing directory")
    resolved = str(p.resolve())
    for forbidden in _FORBIDDEN_WATCH_PREFIXES:
        if resolved == forbidden or resolved.startswith(forbidden + "/"):
            raise argparse.ArgumentTypeError(
                f"--watch-dir {raw!r} points into {forbidden} (pseudo-"
                f"filesystem). Watching pseudo-FS generates kernel-rate "
                f"inotify events and causes reload-storms."
            )
    return resolved


#: Hilfetext von ``--app-dir`` an ``worker`` und ``inspect`` (Plan 2026-10-12-1105, P6).
_APP_DIR_HELP = (
    "Directory put first on sys.path before --module is imported. Default: "
    "the current working directory, as with 'python -m', unless "
    "PYTHONSAFEPATH or -P is set."
)


def _app_dir_path(raw: str) -> str:
    """argparse type-function fuer ``--app-dir DIR``: nur bestehende
    Verzeichnisse, als absoluter Pfad (Plan 2026-10-12-1105, P6)."""
    if not Path(raw).is_dir():
        raise argparse.ArgumentTypeError(f"--app-dir {raw!r} is not an existing directory")
    return os.path.abspath(raw)  # noqa: PTH100 - abspath normalisiert ohne Symlinks aufzulösen


def _queue_list(raw: str) -> list[str]:
    """Comma-separated queue names with whitespace stripped.

    Mirrors :class:`SwarmQConfig`'s ``SWARMQ_QUEUES`` env-var parsing:
    splits on commas, strips whitespace per item, drops empties. An
    all-empty result is rejected with a clear message rather than
    silently giving the worker no queues to consume.
    """
    queues = [q.strip() for q in raw.split(",") if q.strip()]
    if not queues:
        raise argparse.ArgumentTypeError(f"--queues must list at least one non-empty name (got {raw!r})")
    return queues


async def _backend_from_env() -> ValkeyBackend:
    """Construct + connect a ValkeyBackend from SWARMQ_BACKEND_URL.

    Shared helper for the schedule subcommands which only need the
    backend (no broker). Raises :class:`ConfigurationError` when the
    env var is missing so the CLI can surface a helpful message.
    """
    # Lazy import — keeps `import swarmq.cli._shared` (and therefore
    # `swarmq --help`) off the valkey import path. R-1a.
    from swarmq.backend.valkey import ValkeyBackend

    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not backend_url:
        raise ConfigurationError(
            "SWARMQ_BACKEND_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0"
        )
    backend = ValkeyBackend(backend_url)
    await backend.connect()
    return backend


async def _broker_from_env() -> RabbitMQBroker:
    """Construct + connect a RabbitMQBroker from SWARMQ_BROKER_URL.

    Shared helper for the dlq subcommands which only need the broker
    (no backend). Mirrors :func:`_backend_from_env` in shape and error
    semantics — raises :class:`ConfigurationError` with a concrete
    example URL when the env var is missing.
    """
    # Lazy import — keeps `import swarmq.cli._shared` off the aio_pika
    # path for help/version/parse-only invocations. R-1a.
    from swarmq.broker.rabbitmq import RabbitMQBroker

    broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
    if not broker_url:
        raise ConfigurationError(
            "SWARMQ_BROKER_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BROKER_URL=amqp://guest:guest@localhost:5672/"
        )
    broker = RabbitMQBroker(broker_url)
    # Die DLQ-Befehle legen gelesene Nachrichten mit der Rest-TTL zurueck
    # (Plan 2026-10-08-1541, KTD6); die konfigurierte Frist gilt nur fuer
    # Altbestand ohne x-swarmq-dead-letter-ttl-ms.
    ttl_raw = os.environ.get("SWARMQ_DEAD_LETTER_TTL_S", "")
    if ttl_raw:
        from swarmq.config import parse_env_value

        ttl = cast("float | None", parse_env_value("optional_float", ttl_raw, "SWARMQ_DEAD_LETTER_TTL_S"))
        if ttl is not None and ttl <= 0:
            msg = f"dead_letter_ttl_s must be > 0, got {ttl}"
            raise ConfigurationError(msg)
        if ttl is not None and not math.isfinite(ttl):
            # E-063: unbegrenzt heisst ``none``, nicht ``inf``.
            msg = f"dead_letter_ttl_s must be a finite number, got {ttl}"
            raise ConfigurationError(msg)
        broker.dead_letter_ttl_s = ttl
    await broker.connect()
    return broker


def _render_table(rows: Sequence[tuple[str, ...]], empty_label: str) -> str:
    """Render a list of tuples as a two-space-separated, left-aligned table.

    The first row is the header; every subsequent row is data. Column
    widths are computed across the full set so header + data align.
    Returns *empty_label* when only the header row would be rendered.

    Shared helper for all CLI table formatters (schedule, inspect,
    dlq list) — R3.10 review P3 #14 / M-3 / KP-5 deduplicated three
    identical copies.
    """
    if len(rows) <= 1:
        return empty_label
    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)) for row in rows]
    return "\n".join(lines)


def apply_app_dir(args: argparse.Namespace, *, safe_path: bool = sys.flags.safe_path) -> None:
    """App-Verzeichnis an ``sys.path[0]`` stellen (Plan 2026-10-12-1105, P6, E1 C).

    Das Console-Script hat dort das ``bin`` des venv, nicht das
    Arbeitsverzeichnis. Ohne ``--app-dir`` gilt das Arbeitsverzeichnis wie bei
    ``python -m``, also nicht unter ``-P``/``PYTHONSAFEPATH`` (``safe_path``
    ist injizierbar, weil ``sys.flags`` schreibgeschuetzt ist) und nicht, wenn
    es geloescht wurde. Ein ausdrueckliches ``--app-dir`` gilt immer. Steht der
    Pfad schon an Stelle 0, passiert nichts. Spawn-Kinder und der Forkserver
    erben ``sys.path`` (Spike S2). Befehle ohne die Option (``schedule``,
    ``dlq``, ``check``) importieren kein App-Modul und bleiben unberuehrt.
    """
    if not hasattr(args, "app_dir"):
        return
    if args.app_dir is not None:
        raw = args.app_dir
    elif safe_path:
        return
    else:
        try:
            raw = os.getcwd()  # noqa: PTH109 - os.getcwd wie python -m
        except FileNotFoundError:
            return
    app_dir = os.path.abspath(raw)  # noqa: PTH100 - abspath normalisiert ohne Symlinks aufzulösen
    if sys.path[:1] != [app_dir]:
        sys.path.insert(0, app_dir)
