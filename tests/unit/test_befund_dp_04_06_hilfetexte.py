"""Nachweise DP-04 bis DP-06: Hilfe- und Hinweistexte, die dem Verhalten
widersprechen.

Register: docs/befunde/doku-pruefung-2026-10.md (DP-04 V5a, DP-05 V5c,
DP-06 V5e).

- DP-04: Die Hilfe zu ``--concurrency`` nannte als Teiler des cpu-Standards
  nur ``--processes``; ``cli/pool.py`` teilt durch ``--max-processes``, wenn
  gesetzt (wie die Hilfe zu ``--max-processes`` und ``reference/cli.md``).
- DP-05: Die Hilfe zu ``schedule list`` nannte den Key ``sq:sched:defs``;
  er heißt ``sq:{sched}:defs`` (``Keyspace.sched_defs``).
- DP-06: Der Hinweis bei ``reload_aborted_alive_children`` riet „re-send
  SIGHUP“, der Supervisor endet danach aber mit 0 (``reload.py``); es
  bleibt niemand, dem man SIGHUP schicken könnte.

Testliste:
# [x] --concurrency-Hilfe nennt --max-processes als Teiler
# [x] schedule-list-Hilfe nennt den Key aus KeySpace().sched_defs()
# [x] Reload-Abbruch mit lebendem Kind: Exit 0, Hinweis auf Neustart statt SIGHUP
"""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from swarmq.cli.parser import _build_parser
from swarmq.keyspace import KeySpace
from swarmq.process_manager.reload import reexec


def _subparser(parser: argparse.ArgumentParser, *path: str) -> argparse.ArgumentParser:
    for name in path:
        action = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        parser = action.choices[name]
    return parser


def _help_of(parser: argparse.ArgumentParser, option: str) -> str:
    action = next(a for a in parser._actions if option in a.option_strings)
    return " ".join(str(action.help).split())


def _subcommand_help(parser: argparse.ArgumentParser, *path: str) -> str:
    *parents, name = path
    parent = _subparser(parser, *parents)
    action = next(a for a in parent._actions if isinstance(a, argparse._SubParsersAction))
    choice = next(c for c in action._choices_actions if c.dest == name)
    return str(choice.help)


@pytest.mark.befund("DP-04")
def test_concurrency_help_names_max_processes_as_divisor() -> None:
    text = _help_of(_subparser(_build_parser(), "worker"), "--concurrency")

    assert "divided by --max-processes if set, otherwise by --processes" in text


@pytest.mark.befund("DP-05")
def test_schedule_list_help_names_the_real_key() -> None:
    text = _subcommand_help(_build_parser(), "schedule", "list")

    assert KeySpace().sched_defs() in text


class _AliveProcess:
    pid = 4242

    def is_alive(self) -> bool:
        return True

    def join(self, timeout: float | None = None) -> None:
        return None


@pytest.mark.befund("DP-06")
def test_aborted_reload_hints_at_a_restart_not_at_sighup(swarmq_logs: list[dict[str, Any]]) -> None:
    pm = SimpleNamespace(workers={1: SimpleNamespace(process=_AliveProcess())})

    with patch("swarmq.process_manager.reload.REAP_BEFORE_REEXEC_S", 0.0):
        assert reexec(pm, 0) == 0

    (event,) = [e for e in swarmq_logs if e["event"] == "reload_aborted_alive_children"]
    assert "SIGHUP" not in event["hint"]
    assert "restart" in event["hint"]
