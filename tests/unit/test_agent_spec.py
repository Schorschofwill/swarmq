"""Sync tests for the machine-readable agent spec (R5.5).

These pin the generated spec to the code: the generator introspects swarmq,
and every assertion here fails the moment the spec could drift from reality —
the guarantee that lets the machine-readable docs be trusted without a human
re-checking them each release.

Test list
=== U1: error catalog ===
- build_error_catalog returns every SwarmQError subclass (+ the root) and no
  stragglers.
- parent + init_params are code-derived (cannot drift from the class).
- curated semantics use the canonical vocabulary from error-catalog-machine.md
  and are present for every class.
- ConfigurationError ends `swarmq worker` with the EX_CONFIG constant (78),
  the main case for agents; other commands' exit 1 is in the CLI table.
- the CLI exit codes list every code of the `swarmq` binary: 78 and 130 are
  anchored to the real constants.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path
from typing import Any

import pytest

import swarmq
from swarmq.agent_spec import (
    build_cli_spec,
    build_config_spec,
    build_error_catalog,
)
from swarmq.cli import _DLQ_NOT_FOUND_EXIT_CODE
from swarmq.cli._shared import _VALID_LOG_FORMATS
from swarmq.cli.parser import _build_parser
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError, SwarmQError

_RETRIABLE = {"yes", "no", "controlled", "n/a"}
_DLQ = {"yes", "no", "n/a"}
_ORIGIN = {"library", "task_code"}
_WHEN = {"startup", "schedule", "run", "result", "cli", "any"}


def _all_swarmq_errors() -> set[str]:
    """Independently discover the error class names (root + all subclasses)."""
    found = {SwarmQError.__name__}

    def walk(cls: type[BaseException]) -> None:
        for sub in cls.__subclasses__():
            found.add(sub.__name__)
            walk(sub)

    walk(SwarmQError)
    return found


def test_error_catalog_covers_every_swarmq_error_no_more_no_less() -> None:
    catalog = build_error_catalog()
    names = {e["name"] for e in catalog["errors"]}
    assert names == _all_swarmq_errors()


def test_error_catalog_parent_is_code_derived() -> None:
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    assert by_name["SwarmQError"]["parent"] == "Exception"
    assert by_name["ConfigurationError"]["parent"] == "SwarmQError"
    assert by_name["TaskError"]["parent"] == "SwarmQError"
    # Grandchild keeps its immediate base, not the root.
    assert by_name["TaskCancelledError"]["parent"] == "TaskError"


def test_error_catalog_init_params_are_code_derived() -> None:
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    assert by_name["TaskCancelledError"]["init_params"] == ["task_id"]
    assert by_name["Retry"]["init_params"] == ["delay"]
    assert by_name["NoRetry"]["init_params"] == ["reason"]
    assert by_name["ConfigurationError"]["init_params"] == []


def test_error_catalog_entries_are_fully_shaped() -> None:
    for entry in build_error_catalog()["errors"]:
        assert set(entry) >= {
            "exception_class",
            "name",
            "parent",
            "module",
            "doc",
            "init_params",
            "origin",
            "when",
            "retriable",
            "dlq_on_uncaught",
            "exit_code_on_cli",
            "transport_field",
        }
        assert entry["exception_class"] == f"{entry['module']}.{entry['name']}"
        assert entry["module"].startswith("swarmq")
        assert entry["origin"] in _ORIGIN
        assert entry["retriable"] in _RETRIABLE
        assert entry["dlq_on_uncaught"] in _DLQ
        assert isinstance(entry["when"], list)
        assert entry["when"]
        assert set(entry["when"]) <= _WHEN
        assert entry["exit_code_on_cli"] is None or isinstance(entry["exit_code_on_cli"], int)
        assert entry["doc"] is None or isinstance(entry["doc"], str)
        assert entry["transport_field"] is None or isinstance(entry["transport_field"], str)


def test_error_catalog_known_semantics() -> None:
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    # Retry is the explicit retry signal; NoRetry short-circuits to the DLQ.
    assert by_name["Retry"]["retriable"] == "yes"
    assert by_name["NoRetry"]["retriable"] == "no"
    assert by_name["NoRetry"]["dlq_on_uncaught"] == "yes"
    # SoftTimeoutError is retriable only when listed in retry_exceptions.
    assert by_name["SoftTimeoutError"]["retriable"] == "controlled"
    # The workflow exceptions (missing from the original markdown table) are
    # present here — the sync test that surfaced that gap.
    assert by_name["WorkflowValidationError"]["origin"] == "library"
    # P9 (Plan 2026-10-12-1105, MR 3, Review P2-28): das Fehlerergebnis
    # traegt zusaetzlich die Klasse.
    assert by_name["TaskError"]["transport_field"] == "error_message, error_class, is_error=true"
    # P12d (Plan 2026-10-12-1105, MR 4, T15): ResultExpiredError wird derzeit
    # nie geworfen; der Katalog sagt das.
    assert by_name["ResultExpiredError"]["transport_field"] == "not raised (reserved)"
    assert "never raised" in by_name["ResultExpiredError"]["doc"]


# === U2: config spec ===


def _config_env_vars_in_source() -> set[str]:
    """Every SWARMQ_* literal referenced in config.py."""
    text = (Path(swarmq.__file__).parent / "config.py").read_text()
    return set(re.findall(r"SWARMQ_[A-Z_]+", text))


def test_config_spec_covers_every_dataclass_field() -> None:
    spec = build_config_spec()
    names = {o["name"] for o in spec["options"]}
    assert names == {f.name for f in dataclasses.fields(SwarmQConfig)}


def test_config_spec_env_vars_match_source_and_naming_rule() -> None:
    spec = build_config_spec()
    # Seit U2 (Plan 2026-09-08-001) gibt es Felder ohne Env: ``worker_slot``
    # vergibt allein der Supervisor, eine Env-Var waere ein zweiter Weg zur
    # selben Groesse. Solche Optionen tragen ``env_var: null``.
    spec_env = {o["env_var"] for o in spec["options"] if o["env_var"] is not None}
    spec_env |= {a for o in spec["options"] for a in o.get("env_var_aliases", ())}
    # The naming rule (SWARMQ_<UPPER>) is applied uniformly...
    for o in spec["options"]:
        if o["env_var"] is not None:
            assert o["env_var"] == f"SWARMQ_{o['name'].upper()}"
    # ...and matches exactly what config.py actually reads — so a new
    # env-parsed field cannot be added without showing up here.
    assert spec_env == _config_env_vars_in_source()


def test_config_spec_marks_fields_without_env_var() -> None:
    """U2: ``worker_slot`` kommt nur aus dem Supervisor-Entry; die Spec sagt
    das ausdruecklich, statt eine Env-Var zu behaupten, die niemand liest."""
    by_name = {o["name"]: o for o in build_config_spec()["options"]}
    assert by_name["worker_slot"]["env_var"] is None
    assert by_name["worker_id"]["env_var"] == "SWARMQ_WORKER_ID"


def test_config_spec_lists_log_format_as_env_alias_of_log_json() -> None:
    """Plan 2026-10-06 U5 (KTD8): ``SWARMQ_LOG_FORMAT`` setzt ``log_json`` und
    hat Vorrang vor ``SWARMQ_LOG_JSON``. Die Namensregel bleibt fuer
    ``env_var``, der zweite Name steht in ``env_var_aliases``."""
    by_name = {o["name"]: o for o in build_config_spec()["options"]}
    assert by_name["log_json"]["env_var"] == "SWARMQ_LOG_JSON"
    assert by_name["log_json"]["env_var_aliases"] == ["SWARMQ_LOG_FORMAT"]
    assert "env_var_aliases" not in by_name["log_level"]


def test_config_spec_defaults_are_json_native() -> None:
    by_name = {o["name"]: o for o in build_config_spec()["options"]}
    assert by_name["broker_url"]["default"] == ""
    assert by_name["queues"]["default"] == ["default"]
    assert by_name["concurrency"]["default"] == 10
    assert by_name["log_json"]["default"] is True
    assert by_name["metrics_port"]["default"] is None
    assert by_name["scheduler_lock_ttl"]["default"] == 30.0


def test_config_spec_enum_choices_match_validation() -> None:
    by_name = {o["name"]: o for o in build_config_spec()["options"]}
    assert by_name["log_level"]["choices"] == [
        "CRITICAL",
        "DEBUG",
        "ERROR",
        "INFO",
        "WARNING",
    ]
    assert by_name["queue_type"]["choices"] == ["classic", "quorum"]
    assert by_name["log_lifecycle_level"]["choices"] == by_name["log_level"]["choices"]
    # The documented choices are exactly what the config accepts/rejects.
    base: dict[str, Any] = {"broker_url": "amqp://x", "backend_url": "valkey://x"}
    for level in by_name["log_level"]["choices"]:
        SwarmQConfig(**base, log_level=level)  # no raise
        SwarmQConfig(**base, log_lifecycle_level=level)
    with pytest.raises(ConfigurationError):
        SwarmQConfig(**base, log_level="TRACE")
    for qt in by_name["queue_type"]["choices"]:
        SwarmQConfig(**base, queue_type=qt)
    with pytest.raises(ConfigurationError):
        SwarmQConfig(**base, queue_type="streaming")
    # P1 (Plan 2026-10-12-1105, MR 5, Review P2-25): Scheduler-Schalter.
    assert by_name["scheduler"]["env_var"] == "SWARMQ_SCHEDULER"
    assert by_name["scheduler"]["choices"] == ["off", "on"]
    for mode in by_name["scheduler"]["choices"]:
        SwarmQConfig(**base, scheduler=mode)
    with pytest.raises(ConfigurationError):
        SwarmQConfig(**base, scheduler="no")


# === U3: CLI spec ===

import argparse  # noqa: E402


def _leaf_commands() -> dict[str, argparse.ArgumentParser]:
    """Independently walk the argparse tree → {full path: leaf parser}."""
    leaves: dict[str, argparse.ArgumentParser] = {}

    def walk(parser: argparse.ArgumentParser, prefix: str) -> None:
        subs = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
        if not subs:
            leaves[prefix.strip()] = parser
            return
        for sub in subs:
            for name, subparser in sub.choices.items():
                walk(subparser, f"{prefix} {name}".strip())

    walk(_build_parser(), "")
    return leaves


def _option_strings(parser: argparse.ArgumentParser) -> set[str]:
    out: set[str] = set()
    for a in parser._actions:
        if isinstance(a, (argparse._HelpAction, argparse._SubParsersAction)):
            continue
        if a.option_strings:
            out.update(a.option_strings)
        else:
            out.add(a.dest)  # positional
    return out


def test_cli_spec_covers_every_leaf_command() -> None:
    spec = build_cli_spec()
    assert spec["program"] == "swarmq"
    paths = {c["path"] for c in spec["commands"]}
    assert paths == set(_leaf_commands())


def test_cli_spec_arguments_match_parser_per_command() -> None:
    leaves = _leaf_commands()
    by_path = {c["path"]: c for c in build_cli_spec()["commands"]}
    for path, parser in leaves.items():
        spec_flags: set[str] = set()
        for arg in by_path[path]["arguments"]:
            spec_flags.update(arg["flags"] or [arg["name"]])
        assert spec_flags == _option_strings(parser), f"mismatch for {path!r}"


def test_cli_spec_worker_argument_details() -> None:
    by_path = {c["path"]: c for c in build_cli_spec()["commands"]}
    args = {a["name"]: a for a in by_path["worker"]["arguments"]}
    assert args["--module"]["required"] is True
    # Plan 2026-09-03-001 (R3): der Parser laesst --concurrency offen; die
    # Aufloesung (10 fuer io, zugeteilte CPUs fuer cpu) folgt nach dem Parsen.
    assert args["--concurrency"]["default"] is None
    assert args["--pool"]["choices"] == ["io", "cpu"]
    assert args["--log-format"]["choices"] == list(_VALID_LOG_FORMATS)
    assert args["--burst"]["type"] == "bool"  # store_true flag
    # P1 (Plan 2026-10-12-1105, MR 5): setzt ``scheduler`` auf ``off``.
    assert args["--no-scheduler"]["nargs"] == 0


def test_cli_spec_schedule_add_has_positional_and_required_task() -> None:
    by_path = {c["path"]: c for c in build_cli_spec()["commands"]}
    args = {a["name"]: a for a in by_path["schedule add"]["arguments"]}
    assert args["name"]["positional"] is True
    assert args["--task"]["required"] is True


def test_configuration_error_exit_code_is_the_worker_config_exit_code() -> None:
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    assert by_name["ConfigurationError"]["exit_code_on_cli"] == CONFIG_ERROR_EXIT_CODE


def test_cli_spec_exit_codes_anchored_to_code() -> None:
    codes = {e["code"]: e["meaning"] for e in build_cli_spec()["exit_codes"]}
    assert set(codes) >= {0, 1, 2, 3}
    # Code 3 is anchored to the real constant, not a copied literal.
    assert _DLQ_NOT_FOUND_EXIT_CODE == 3


def test_cli_spec_lists_the_config_and_interrupt_exit_codes() -> None:
    from swarmq.logging_drain import exit_code_for
    from swarmq.supervision import CONFIG_ERROR_EXIT_CODE

    codes = {e["code"] for e in build_cli_spec()["exit_codes"]}
    assert CONFIG_ERROR_EXIT_CODE in codes
    assert exit_code_for(KeyboardInterrupt()) in codes


# === U4: message JSON schemas ===

import msgspec  # noqa: E402

from swarmq.agent_spec import (  # noqa: E402
    _TaskMessage,
    _TaskResult,
    build_message_schemas,
)
from swarmq.cancellation import (  # noqa: E402
    build_cancel_result_dict,
    build_error_result_dict,
    build_result_dict,
)
from swarmq.progress import ProgressInfo  # noqa: E402
from swarmq.scheduler import ScheduleDefinition  # noqa: E402
from swarmq.serialization import encode_json  # noqa: E402

_SCHEMA_FILES = {
    "messages/task-message.schema.json",
    "messages/result.schema.json",
    "messages/progress.schema.json",
    "messages/schedule-definition.schema.json",
}


def _schema_props(schema: dict[str, Any]) -> set[str]:
    """Property names of a (possibly $ref/$defs) msgspec-generated schema."""
    if "properties" in schema:
        return set(schema["properties"])
    ref = schema.get("$ref", "")
    name = ref.rsplit("/", 1)[-1]
    return set(schema["$defs"][name]["properties"])


def test_message_schemas_cover_expected_formats() -> None:
    schemas = build_message_schemas()
    assert set(schemas) == _SCHEMA_FILES
    for body in schemas.values():
        assert isinstance(body, dict)
        assert body
        assert "type" in body or "$ref" in body


def test_result_schema_validates_real_builder_output() -> None:
    """The result schema is anchored to the actual cancellation.py builders —
    decoding their output into the schema struct fails if the shape drifts."""
    for real in (
        build_result_dict(
            task_id="t",
            queue="q",
            task_name="n",
            is_error=False,
            is_cancelled=False,
            result="ok",
        ),
        build_error_result_dict(
            task_id="t",
            queue="q",
            task_name="n",
            error_message="boom",
        ),
        # P9 (Plan 2026-10-12-1105, MR 3, Abschnitt 6): optionales Feld.
        build_error_result_dict(
            task_id="t",
            queue="q",
            task_name="n",
            error_message="",
            error_class="builtins.ValueError",
        ),
        build_cancel_result_dict(task_id="t", queue="q", task_name="n"),
    ):
        # Round-trips cleanly into the forbid-unknown-fields schema struct.
        msgspec.json.decode(encode_json(real), type=_TaskResult)


def test_result_schema_rejects_drifted_shape() -> None:
    drifted = {
        "task": {"id": "t", "queue": "q", "name": "n"},
        "is_error": False,
        "is_cancelled": False,
        "error_message": "",
        "result": None,
        "surprise": 1,  # an extra key a drifting builder might add
    }
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(encode_json(drifted), type=_TaskResult)


def test_task_message_schema_matches_canonical_example() -> None:
    example = {
        "task": {"id": "01HV", "queue": "emails", "name": "send_email"},
        "args": ["x", 42],
        "kwargs": {"to": "a@b.c"},
    }
    msgspec.json.decode(encode_json(example), type=_TaskMessage)
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(encode_json({"args": [], "kwargs": {}}), type=_TaskMessage)


def test_progress_schema_properties_match_dataclass() -> None:
    schema = build_message_schemas()["messages/progress.schema.json"]
    assert _schema_props(schema) == set(ProgressInfo.__dataclass_fields__)


def test_schedule_definition_schema_properties_match_struct() -> None:
    schema = build_message_schemas()["messages/schedule-definition.schema.json"]
    assert _schema_props(schema) == set(ScheduleDefinition.__struct_fields__)


# === U5: aggregate index + llms.txt ===

from importlib.metadata import version as _pkg_version  # noqa: E402

from swarmq.agent_spec import (  # noqa: E402
    _DATA_PARTS,
    build_index,
    build_llms_txt,
)


def test_index_lists_every_data_part_and_version() -> None:
    index = build_index()
    assert index["swarmq_version"] == _pkg_version("swarmq")
    assert index["parts"] == sorted(_DATA_PARTS)
    # index.json describes the *content* parts, not itself.
    assert "index.json" not in index["parts"]


def test_llms_txt_is_well_formed_and_links_every_spec_part() -> None:
    text = build_llms_txt()
    assert text.startswith("# SwarmQ")
    # llms.txt convention: a blockquote one-liner right after the H1.
    assert "\n> " in text
    # Every machine-readable spec part is linked (so a new part can't be
    # silently omitted from the index).
    for path in _DATA_PARTS:
        assert f"spec/{path}" in text


def test_llms_txt_links_the_logging_reference() -> None:
    """Die Logging-Referenz (Plan 2026-10-06-0831, U12) beschreibt Events
    und Felder, die eine Automatisierung auswertet -- sie gehoert in den
    Index."""
    assert "(reference/logging.md)" in build_llms_txt()


# === U6: drift guard — committed files must match the generators ===

from swarmq.agent_spec import (  # noqa: E402
    _PARTS,
    DEFAULT_LLMS_TXT,
    DEFAULT_SPEC_DIR,
    _dump,
)


def _strip_version(text: str) -> str:
    """Blank out the version line so a release bump alone doesn't fail drift."""
    return re.sub(r'"swarmq_version": ".*?"', '"swarmq_version": "X"', text)


def test_committed_spec_files_match_generators() -> None:
    """The committed JSON/llms.txt must equal a fresh generation — otherwise
    run `python -m swarmq.agent_spec`. This is the master sync guard tying the
    published artefacts to the code."""
    for rel, builder in _PARTS.items():
        committed = (DEFAULT_SPEC_DIR / rel).read_text(encoding="utf-8")
        expected = _dump(builder())
        if rel == "index.json":
            committed, expected = _strip_version(committed), _strip_version(expected)
        assert committed == expected, f"{rel} is stale — run `python -m swarmq.agent_spec`"
    assert DEFAULT_LLMS_TXT.read_text(encoding="utf-8") == build_llms_txt()
