"""Nachweis DP-07: Die Agent-Spec nennt für ``ConfigurationError`` nur Exit 78.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-07, Verdacht V6).

``errors.json`` trug für ``ConfigurationError`` nur ``exit_code_on_cli: 78``.
``swarmq schedule``, ``dlq`` und ``inspect`` beenden einen
Konfigurationsfehler aber mit 1 (``cli/__init__.py``, ``_run_command``),
``swarmq worker --check`` ebenso (``check_cmd.py``); 78 gilt für ``swarmq
worker``, ``swarmq check`` und einen ungültigen ``SWARMQ_NAMESPACE`` bei
jedem Befehl. So steht es in ``reference/cli.md`` (Exit-Codes) und
``reference/agent/error-catalog-machine.md``. Ein Agent, der nur die Spec
liest, erwartet bei ``swarmq schedule add --args '{'`` sonst 78.

``exit_code_on_cli`` bleibt der Code des Worker-Starts (Abwärtskompatibilität
der Spec); die Zuordnung je Befehl kommt als zusätzliches Feld.

Testliste:
# [x] ConfigurationError trägt exit_code_by_command mit 78 und 1 je Befehl
"""

from __future__ import annotations

import pytest

from swarmq.agent_spec import build_error_catalog


@pytest.mark.befund("DP-07")
def test_configuration_error_lists_the_exit_code_per_command() -> None:
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}

    assert by_name["ConfigurationError"]["exit_code_by_command"] == {
        "swarmq worker": 78,
        "swarmq check": 78,
        "swarmq worker --check": 1,
        "swarmq schedule": 1,
        "swarmq dlq": 1,
        "swarmq inspect": 1,
        "invalid SWARMQ_NAMESPACE (any command)": 78,
    }
