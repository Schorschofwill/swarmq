"""Tests fuer swarmq.cli.worker_options: CLI > Env > Default fuer die
Optionen von ``swarmq worker``, einmal im Supervisor aufgeloest.

Test-Liste:
- concurrency: Env gilt ohne Flag; Flag schlaegt Env; ohne beides None
- queues: Env gilt ohne Flag; Flag schlaegt Env; Default ["default"]
- burst/priorities: Env gilt ohne Flag; Flag schlaegt Env
- burst_idle_timeout: Env gilt ohne Flag; Flag schlaegt Env; Default 5.0
- metrics_port/health_port: Env gilt ohne Flag; Flag schlaegt Env
- ungueltiger Env-Wert ohne Flag -> ConfigurationError mit Variablennamen
- ungueltiger Env-Wert mit Flag wird nicht geparst
- cli_fields nennt nur die per Flag gesetzten Felder
- idempotent: zweiter Aufruf aendert nichts
"""

from __future__ import annotations

import argparse

import pytest

from swarmq.cli.worker_options import resolve_worker_options
from swarmq.exceptions import ConfigurationError


def _args(**kw: object) -> argparse.Namespace:
    base: dict[str, object] = {
        "queues": None,
        "concurrency": None,
        "pool": None,
        "burst": None,
        "burst_idle_timeout": None,
        "priorities": None,
        "metrics_port": None,
        "health_port": None,
    }
    base.update(kw)
    return argparse.Namespace(**base)


class TestConcurrency:
    def test_environment_applies_without_flag(self):
        args = _args()
        resolve_worker_options(args, {"SWARMQ_CONCURRENCY": "5"})
        assert args.concurrency == 5

    def test_flag_wins_over_environment(self):
        args = _args(concurrency=20)
        resolve_worker_options(args, {"SWARMQ_CONCURRENCY": "5"})
        assert args.concurrency == 20


class TestQueues:
    def test_environment_applies_without_flag(self):
        args = _args()
        resolve_worker_options(args, {"SWARMQ_QUEUES": "a, b,,c"})
        assert args.queues == ["a", "b", "c"]

    def test_flag_wins_over_environment(self):
        args = _args(queues=["x"])
        resolve_worker_options(args, {"SWARMQ_QUEUES": "a,b"})
        assert args.queues == ["x"]

    def test_default_without_flag_and_environment(self):
        args = _args()
        resolve_worker_options(args, {})
        assert args.queues == ["default"]


class TestBurst:
    def test_environment_applies_without_flag(self):
        args = _args()
        resolve_worker_options(
            args,
            {"SWARMQ_BURST_MODE": "true", "SWARMQ_BURST_IDLE_TIMEOUT": "2.5"},
        )
        assert (args.burst, args.burst_idle_timeout) == (True, 2.5)

    def test_flags_win_over_environment(self):
        args = _args(burst=True, burst_idle_timeout=9.0)
        resolve_worker_options(
            args,
            {"SWARMQ_BURST_MODE": "false", "SWARMQ_BURST_IDLE_TIMEOUT": "2.5"},
        )
        assert (args.burst, args.burst_idle_timeout) == (True, 9.0)

    def test_defaults_without_flag_and_environment(self):
        args = _args()
        resolve_worker_options(args, {})
        assert (args.burst, args.burst_idle_timeout) == (False, 5.0)


class TestPriorities:
    def test_environment_applies_without_flag(self):
        args = _args()
        resolve_worker_options(args, {"SWARMQ_PRIORITIES": "1"})
        assert args.priorities is True


class TestPorts:
    def test_environment_applies_without_flag(self):
        args = _args()
        resolve_worker_options(
            args,
            {"SWARMQ_METRICS_PORT": "9100", "SWARMQ_HEALTH_PORT": "9200"},
        )
        assert (args.metrics_port, args.health_port) == (9100, 9200)

    def test_flags_win_over_environment(self):
        args = _args(metrics_port=8000, health_port=8100)
        resolve_worker_options(
            args,
            {"SWARMQ_METRICS_PORT": "9100", "SWARMQ_HEALTH_PORT": "9200"},
        )
        assert (args.metrics_port, args.health_port) == (8000, 8100)


class TestInvalidEnvironment:
    def test_invalid_value_without_flag_names_the_variable(self):
        with pytest.raises(ConfigurationError, match="SWARMQ_METRICS_PORT"):
            resolve_worker_options(_args(), {"SWARMQ_METRICS_PORT": "70000"})

    def test_overridden_invalid_value_is_not_parsed(self):
        args = _args(concurrency=3)
        resolve_worker_options(args, {"SWARMQ_CONCURRENCY": "abc"})
        assert args.concurrency == 3


class TestCliFields:
    def test_names_only_the_options_given_as_flags(self):
        args = _args(concurrency=3, queues=["x"])
        resolve_worker_options(args, {"SWARMQ_PRIORITIES": "true"})
        assert args.cli_fields == frozenset({"concurrency", "queues"})

    def test_second_call_keeps_values_and_origin(self):
        args = _args(concurrency=3)
        env = {"SWARMQ_QUEUES": "a"}
        resolve_worker_options(args, env)
        resolve_worker_options(args, env)
        assert (args.queues, args.concurrency, args.cli_fields) == (
            ["a"],
            3,
            frozenset({"concurrency"}),
        )
