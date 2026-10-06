"""Machine-readable agent spec generator (R5.5).

Emits structured JSON a program can consume WITHOUT parsing the prose
``website/reference/agent/*.md`` pages or reading the source:

* ``errors.json``      — every ``SwarmQError`` subclass as data.
* ``config.json``      — every ``SwarmQConfig`` option (type/default/env-var).
* ``cli.json``         — every CLI command/flag/default/choice/exit-code.
* ``messages/*.schema.json`` — JSON Schema (Draft 2020-12) per wire format.
* ``index.json``       — aggregate + version.

Everything here is *generated from the code* and pinned by sync tests in
``tests/unit/test_agent_spec.py`` (the spec cannot drift from reality without
a test going red — the reason R5.5's machine-readable half was deferred until
it could be kept honest).

This module is intentionally NOT part of the lazy ``swarmq`` public surface
(``swarmq/__init__.py``); importing it is explicit and carries no
``import swarmq`` cost. Regenerate the committed files with::

    python -m swarmq.agent_spec [OUT_DIR]
"""

from __future__ import annotations

import argparse
import dataclasses
import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from swarmq.cli import _DLQ_NOT_FOUND_EXIT_CODE
from swarmq.cli.parser import _build_parser
from swarmq.config import SwarmQConfig
from swarmq.exceptions import SwarmQError
from swarmq.progress import ProgressInfo
from swarmq.scheduler import ScheduleDefinition

# Default output root when run from a source checkout. Only meaningful in the
# repo; an installed consumer calls ``build_spec()`` and writes wherever it
# likes. src/swarmq/agent_spec.py -> parents[2] == repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SPEC_DIR = _REPO_ROOT / "website" / "reference" / "agent" / "spec"
DEFAULT_LLMS_TXT = _REPO_ROOT / "website" / "llms.txt"


def _spec_version() -> str:
    """Installed swarmq version, or ``0+unknown`` outside an install."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("swarmq")
    except PackageNotFoundError:  # pragma: no cover - source-only checkout
        return "0+unknown"


# ---------------------------------------------------------------------------
# U1 — error catalog
# ---------------------------------------------------------------------------

# Curated per-exception semantics, using the canonical vocabulary of
# ``website/reference/agent/error-catalog-machine.md`` (the prose catalog this
# JSON is the strict form of). ``parent`` and ``init_params`` are NOT here —
# they are derived from the class so they cannot drift. Every SwarmQError
# subclass MUST appear; ``build_error_catalog`` raises otherwise, so adding an
# exception without classifying it fails generation (and the sync test) loudly.
#
# Fields: origin ("library"|"task_code"), when (lifecycle stages),
# retriable ("yes"|"no"|"controlled"|"n/a"), dlq_on_uncaught ("yes"|"no"|"n/a"),
# exit_code_on_cli (int|None), transport_field (str|None).
_ERROR_SEMANTICS: dict[str, dict[str, Any]] = {
    "SwarmQError": dict(
        origin="library", when=["any"], retriable="controlled",
        dlq_on_uncaught="yes", exit_code_on_cli=1, transport_field="error_class",
    ),
    "ConfigurationError": dict(
        origin="library", when=["startup", "cli"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=1, transport_field="stderr line",
    ),
    "LoggingConfigError": dict(
        # Ungueltige Logging-Einstellungen enden im Worker mit Exit 78
        # (Plan 2026-10-06-0831, R14).
        origin="library", when=["startup", "cli"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=78, transport_field="stderr line",
    ),
    "BrokerConnectionError": dict(
        origin="library", when=["startup", "schedule", "run"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=1, transport_field="stderr line",
    ),
    "PublishIndeterminate": dict(
        # Absichtlich NICHT retriable: Die Frist riss beim Warten auf das
        # Publisher-Confirm, der Broker kann die Nachricht also bereits
        # haben. Ein blindes Wiederholen fuehrt den Task moeglicherweise
        # ein zweites Mal aus (Plan 2026-08-28-001, R11).
        origin="library", when=["schedule", "run"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=1, transport_field="stderr line",
    ),
    "BackendConnectionError": dict(
        origin="library", when=["startup", "schedule", "run"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=1, transport_field="stderr line",
    ),
    "TaskNotFoundError": dict(
        origin="library", when=["schedule"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "TaskError": dict(
        origin="library", when=["result"], retriable="n/a",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="error_message, is_error=true",
    ),
    "TaskCancelledError": dict(
        origin="library", when=["run", "result"], retriable="no",
        dlq_on_uncaught="no", exit_code_on_cli=None,
        transport_field="is_cancelled=true",
    ),
    "ResultNotReadyError": dict(
        origin="library", when=["result"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "ResultExpiredError": dict(
        origin="library", when=["result"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "ResultSerializationError": dict(
        origin="library", when=["run"], retriable="no",
        dlq_on_uncaught="yes", exit_code_on_cli=None,
        transport_field="error_message, is_error=true",
    ),
    "Retry": dict(
        origin="task_code", when=["run"], retriable="yes",
        dlq_on_uncaught="no", exit_code_on_cli=None, transport_field="control flow",
    ),
    "NoRetry": dict(
        origin="task_code", when=["run"], retriable="no",
        dlq_on_uncaught="yes", exit_code_on_cli=None, transport_field="control flow",
    ),
    "SoftTimeoutError": dict(
        origin="library", when=["run"], retriable="controlled",
        dlq_on_uncaught="yes", exit_code_on_cli=None,
        transport_field="error_message, is_error=true",
    ),
    # Workflow primitives (R4.1) — absent from the original markdown table;
    # surfaced + classified by the R5.5 sync test.
    "WorkflowValidationError": dict(
        origin="library", when=["schedule", "run"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "WorkflowNotFound": dict(
        origin="library", when=["result"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "WorkflowResultExpired": dict(
        origin="library", when=["result"], retriable="no",
        dlq_on_uncaught="n/a", exit_code_on_cli=None,
        transport_field="raised to caller",
    ),
    "WorkflowStateConflict": dict(
        origin="library", when=["run"], retriable="yes",
        dlq_on_uncaught="no", exit_code_on_cli=None,
        transport_field="redelivery of the step message",
    ),
}


def _iter_exception_classes() -> list[type]:
    """The root SwarmQError plus every subclass, depth-first, deduped."""
    seen: dict[str, type] = {SwarmQError.__name__: SwarmQError}

    def walk(cls: type) -> None:
        for sub in cls.__subclasses__():
            seen.setdefault(sub.__name__, sub)
            walk(sub)

    walk(SwarmQError)
    return [seen[name] for name in sorted(seen)]


def _init_params(cls: type) -> list[str]:
    """Named __init__ params (excludes self and *args/**kwargs).

    A subclass that inherits Exception.__init__ reports ``[]`` — its only
    params are var-positional/var-keyword.
    """
    try:
        sig = inspect.signature(cls)
    except (TypeError, ValueError):  # pragma: no cover - builtins without sig
        return []
    return [
        name
        for name, p in sig.parameters.items()
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    ]


def build_error_catalog() -> dict[str, Any]:
    """Every SwarmQError subclass as data — code-derived shape + curated
    semantics. Raises if a discovered class has no semantics entry."""
    errors: list[dict[str, Any]] = []
    for cls in _iter_exception_classes():
        name = cls.__name__
        semantics = _ERROR_SEMANTICS.get(name)
        if semantics is None:
            raise RuntimeError(
                f"{name} descends from SwarmQError but has no entry in "
                f"_ERROR_SEMANTICS — classify it (retriable / dlq_on_uncaught / "
                f"origin / when / exit_code_on_cli / transport_field) so the "
                f"agent error catalog stays complete."
            )
        own_doc = cls.__dict__.get("__doc__")
        errors.append({
            "exception_class": f"{cls.__module__}.{name}",
            "name": name,
            "parent": cls.__bases__[0].__name__,
            "module": cls.__module__,
            "doc": inspect.cleandoc(own_doc) if own_doc else None,
            "init_params": _init_params(cls),
            **semantics,
        })
    return {
        "$comment": (
            "Generated by `python -m swarmq.agent_spec`. Do not edit by hand; "
            "see tests/unit/test_agent_spec.py. Strict form of "
            "website/reference/agent/error-catalog-machine.md."
        ),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# U2 — config spec
# ---------------------------------------------------------------------------

# Enum constraints validated in SwarmQConfig.__post_init__ (local sets there,
# not module-level, so they can't be introspected). The sync test asserts these
# are exactly what the config accepts/rejects, so a change to the validation
# without updating here goes red.
_CONFIG_CHOICES: dict[str, list[str]] = {
    "log_level": ["CRITICAL", "DEBUG", "ERROR", "INFO", "WARNING"],
    "log_lifecycle_level": ["CRITICAL", "DEBUG", "ERROR", "INFO", "WARNING"],
    "queue_type": ["classic", "quorum"],
}


def build_config_spec() -> dict[str, Any]:
    """Every SwarmQConfig option: name, type, default, env var, choices.

    Fully code-derived. The env var follows the uniform ``SWARMQ_<UPPER>``
    rule SwarmQConfig.__post_init__ applies; defaults come straight off the
    dataclass (factories are evaluated)."""
    # Die Env-Namen kommen aus der Override-Tabelle der Konfiguration, nicht
    # aus einer Namensregel: seit U2 gibt es Felder, die bewusst kein Env
    # haben (``worker_slot`` vergibt allein der Supervisor). Eine erfundene
    # Env-Var stuende sonst in der Spezifikation, ohne dass sie jemand liest.
    # Mehrere Env-Namen je Feld (``SWARMQ_LOG_FORMAT`` fuer ``log_json``,
    # KTD8): der erste in der Tabelle ist ``env_var``, weitere sind Aliase.
    env_by_field: dict[str, str] = {}
    aliases_by_field: dict[str, list[str]] = {}
    for env, field, _kind in SwarmQConfig._ENV_OVERRIDES:
        if field in env_by_field:
            aliases_by_field.setdefault(field, []).append(env)
        else:
            env_by_field[field] = env
    options: list[dict[str, Any]] = []
    for f in dataclasses.fields(SwarmQConfig):
        default = f.default_factory() if f.default_factory is not dataclasses.MISSING else f.default
        entry: dict[str, Any] = {
            "name": f.name,
            "type": str(f.type),
            "default": default,
            "env_var": env_by_field.get(f.name),
        }
        if f.name in aliases_by_field:
            entry["env_var_aliases"] = aliases_by_field[f.name]
        if f.name in _CONFIG_CHOICES:
            entry["choices"] = _CONFIG_CHOICES[f.name]
        options.append(entry)
    return {
        "$comment": (
            "Generated by `python -m swarmq.agent_spec`. Do not edit by hand; "
            "see tests/unit/test_agent_spec.py."
        ),
        "options": options,
    }


# ---------------------------------------------------------------------------
# U3 — CLI spec
# ---------------------------------------------------------------------------

# The `swarmq` binary's exit-code contract. Code 3 is anchored to the real
# module constant (not a copied literal) so a change there flows through here.
def _cli_exit_codes() -> list[dict[str, Any]]:
    return [
        {"code": 0, "meaning": "Success or graceful shutdown"},
        {"code": 1, "meaning": (
            "Runtime error (ConfigurationError, Broker/BackendConnectionError, "
            "ModuleNotFoundError)"
        )},
        {"code": 2, "meaning": "Bad arguments (argparse usage error)"},
        {"code": _DLQ_NOT_FOUND_EXIT_CODE, "meaning": (
            "`dlq inspect` / `dlq retry <task_id>` target not found in the DLQ"
        )},
    ]


def _arg_type(action: argparse.Action) -> str:
    if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
        return "bool"
    if isinstance(action, argparse._VersionAction):
        return "version"
    if action.type is None:
        return "str"
    # Custom validators surface their intent (e.g. _positive_int → positive_int).
    return getattr(action.type, "__name__", str(action.type)).lstrip("_")


def _arg_entry(action: argparse.Action) -> dict[str, Any]:
    positional = not action.option_strings
    name = action.option_strings[0] if action.option_strings else action.dest
    required = (
        action.nargs not in ("?", "*") if positional else bool(action.required)
    )
    entry: dict[str, Any] = {
        "name": name,
        "flags": list(action.option_strings),
        "positional": positional,
        "type": _arg_type(action),
        "default": action.default,
        "required": required,
        "help": action.help,
    }
    if action.choices is not None:
        entry["choices"] = list(action.choices)
    if action.nargs is not None:
        entry["nargs"] = action.nargs
    return entry


def _walk_commands(
    parser: argparse.ArgumentParser, prefix: str, out: list[dict[str, Any]]
) -> None:
    subactions = [
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
    ]
    if not subactions:
        args = [
            _arg_entry(a)
            for a in parser._actions
            if not isinstance(a, (argparse._HelpAction, argparse._SubParsersAction))
        ]
        out.append({
            "path": prefix.strip(),
            "help": parser.description,
            "arguments": sorted(args, key=lambda a: a["name"]),
        })
        return
    for sub in subactions:
        for name, subparser in sub.choices.items():
            _walk_commands(subparser, f"{prefix} {name}".strip(), out)


def build_cli_spec() -> dict[str, Any]:
    """Every `swarmq` leaf command + its arguments, walked from the argparse
    parser, plus the exit-code contract."""
    parser = _build_parser()
    commands: list[dict[str, Any]] = []
    _walk_commands(parser, "", commands)
    return {
        "$comment": (
            "Generated by `python -m swarmq.agent_spec` from the argparse "
            "parser. Do not edit by hand; see tests/unit/test_agent_spec.py."
        ),
        "program": parser.prog,
        "version_flag": "--version",
        "commands": sorted(commands, key=lambda c: c["path"]),
        "exit_codes": _cli_exit_codes(),
    }


# ---------------------------------------------------------------------------
# U4 — message JSON schemas
# ---------------------------------------------------------------------------

# Mirror structs are the schema source of record for the inline-dict wire
# formats (the real producers build plain dicts, not typed objects). They use
# ``forbid_unknown_fields`` so the sync test, which decodes *real* emitted
# payloads into them, fails if a producer adds/renames/drops a field.


class _TaskRef(msgspec.Struct, forbid_unknown_fields=True):
    id: str
    queue: str
    name: str


class _TaskMessage(msgspec.Struct, forbid_unknown_fields=True):
    """RabbitMQ task message body (``Client.schedule`` producer contract)."""

    task: _TaskRef
    args: list[Any]
    kwargs: dict[str, Any]


class _TaskResult(msgspec.Struct, forbid_unknown_fields=True):
    """Valkey task-result envelope (``cancellation.build_result_dict``)."""

    task: _TaskRef
    is_error: bool
    is_cancelled: bool
    error_message: str
    result: Any


_MESSAGE_SCHEMAS: dict[str, Callable[[], dict[str, Any]]] = {
    "messages/task-message.schema.json": lambda: msgspec.json.schema(_TaskMessage),
    "messages/result.schema.json": lambda: msgspec.json.schema(_TaskResult),
    # ProgressInfo (frozen dataclass) and ScheduleDefinition (msgspec.Struct)
    # are real typed sources — schema generated straight off them.
    "messages/progress.schema.json": lambda: msgspec.json.schema(ProgressInfo),
    "messages/schedule-definition.schema.json": (
        lambda: msgspec.json.schema(ScheduleDefinition)
    ),
}


def build_message_schemas() -> dict[str, dict[str, Any]]:
    """JSON Schema (Draft 2020-12, via msgspec) per wire format."""
    return {path: build() for path, build in _MESSAGE_SCHEMAS.items()}


# ---------------------------------------------------------------------------
# U5 — aggregate index + llms.txt
# ---------------------------------------------------------------------------

# The content parts (everything except the meta index). Each maps a
# spec-relative path to a zero-arg builder returning a JSON-serialisable dict.
_DATA_PARTS: dict[str, Callable[[], dict[str, Any]]] = {
    "errors.json": build_error_catalog,
    "config.json": build_config_spec,
    "cli.json": build_cli_spec,
    **_MESSAGE_SCHEMAS,
}


def build_index() -> dict[str, Any]:
    """Manifest of the spec: version + the content parts it ships."""
    return {
        "$comment": (
            "Generated by `python -m swarmq.agent_spec`. Do not edit by hand; "
            "see tests/unit/test_agent_spec.py."
        ),
        "generated_by": "python -m swarmq.agent_spec",
        "swarmq_version": _spec_version(),
        "parts": sorted(_DATA_PARTS),
    }


# Human/agent-facing doc pages worth surfacing in the llms.txt index. These are
# the curated entry points; the machine-readable spec links below are derived
# from _DATA_PARTS so a new spec part cannot be omitted.
_LLMS_DOC_LINKS: list[tuple[str, str, str]] = [
    ("Agent reference", "reference/agent/index.md",
     "rules + entry point for automation"),
    ("Message formats", "reference/agent/message-formats.md",
     "wire schemas (prose form of messages/*.schema.json)"),
    ("Decision trees", "reference/agent/decision-trees.md",
     "which feature to use when"),
    ("Error catalog", "reference/agent/error-catalog-machine.md",
     "exceptions, retriable, dlq_on_uncaught (prose form of errors.json)"),
    ("CLI reference", "reference/cli.md", "commands and flags"),
    ("Configuration", "reference/configuration.md",
     "options, defaults, env vars"),
    ("Logging", "reference/logging.md",
     "log events, fields, formats, levels"),
    ("API reference", "reference/api.md", "public classes and methods"),
]


def build_llms_txt() -> str:
    """The llmstxt.org index: H1, one-line blockquote, then link sections.

    The machine-readable section is generated from _DATA_PARTS (+ the index),
    so every spec file is always linked."""
    lines = [
        "# SwarmQ",
        "",
        "> Python task queue on RabbitMQ + Valkey — at-least-once delivery, "
        "retries, scheduling, workflows, and a machine-readable agent spec.",
        "",
        "The spec files under `reference/agent/spec/` are generated from the "
        "code and kept in sync by tests, so an agent can consume them without "
        "reading the source.",
        "",
        "## Documentation",
        "",
    ]
    lines += [f"- [{t}]({u}): {d}" for t, u, d in _LLMS_DOC_LINKS]
    lines += ["", "## Machine-readable spec (generated)", ""]
    lines.append(
        "- [Spec index](reference/agent/spec/index.json): version + manifest"
    )
    lines += [
        f"- [{path}](reference/agent/spec/{path})"
        for path in sorted(_DATA_PARTS)
    ]
    return "\n".join(lines) + "\n"


# Every part written under the spec dir (content parts + the meta index).
_PARTS: dict[str, Callable[[], dict[str, Any]]] = {
    **_DATA_PARTS,
    "index.json": build_index,
}


def build_spec() -> dict[str, dict[str, Any]]:
    """Build every spec part. Returns ``{relative_path: content}``."""
    return {path: builder() for path, builder in _PARTS.items()}


def _dump(obj: Any) -> str:
    """Deterministic JSON: sorted keys, 2-space indent, UTF-8, trailing NL."""
    return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def write_spec(spec_dir: Path = DEFAULT_SPEC_DIR) -> list[Path]:
    """Write all spec parts under ``spec_dir``. Returns written paths."""
    written: list[Path] = []
    for rel, content in build_spec().items():
        target = spec_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_dump(content), encoding="utf-8")
        written.append(target)
    return written


def write_llms_txt(path: Path = DEFAULT_LLMS_TXT) -> Path:
    """Write the llms.txt site index. Returns the written path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_llms_txt(), encoding="utf-8")
    return path


def _main(argv: list[str] | None = None) -> int:
    import sys

    args = sys.argv[1:] if argv is None else argv
    spec_dir = Path(args[0]) if args else DEFAULT_SPEC_DIR
    # llms.txt lives at the site root (spec dir's website/ ancestor) by default;
    # when an explicit OUT_DIR is given, keep llms.txt beside it.
    llms_path = DEFAULT_LLMS_TXT if not args else spec_dir / "llms.txt"
    written = write_spec(spec_dir)
    written.append(write_llms_txt(llms_path))
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(_main())
