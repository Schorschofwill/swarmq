"""Tests fuer swarmq.cli.pool: Pool-Aufloesung (Flag > SWARMQ_POOL > io) und Slot-Aufloesung (R1, R3, R4)."""

from __future__ import annotations

import argparse

import pytest

from swarmq.cli.pool import prepare_pool_args, resolve_pool
from swarmq.exceptions import ConfigurationError


def _args(**kw) -> argparse.Namespace:
    base = {"pool": None, "concurrency": None, "processes": 1, "autoscale": None}
    base.update(kw)
    return argparse.Namespace(**base)


class TestResolvePool:
    def test_default_is_io(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        args = _args()
        assert resolve_pool(args) == "io"
        assert args.pool == "io"

    def test_flag_wins_over_environment(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_POOL", "cpu")
        args = _args(pool="io")
        assert resolve_pool(args) == "io"

    def test_environment_applies_without_flag(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_POOL", "cpu")
        args = _args()
        assert resolve_pool(args) == "cpu"
        assert args.pool == "cpu"

    def test_invalid_environment_value_is_a_configuration_error(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_POOL", "gpu")
        with pytest.raises(ConfigurationError, match="SWARMQ_POOL"):
            resolve_pool(_args())


class TestPreparePoolArgs:
    def test_io_default_keeps_ten_slots(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        args = _args()
        prepare_pool_args(args)
        assert (args.pool, args.concurrency, args.concurrency_source) == ("io", 10, "default")

    def test_explicit_concurrency_is_kept_in_both_pools(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        for pool in ("io", "cpu"):
            args = _args(pool=pool, concurrency=3)
            prepare_pool_args(args)
            assert (args.concurrency, args.concurrency_source) == (3, "explicit")

    def test_cpu_resolves_slots_from_affinity_or_cgroup(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        args = _args(pool="cpu")
        prepare_pool_args(args)
        assert args.concurrency >= 1
        assert args.concurrency_source in ("affinity", "cgroup")

    def test_cpu_from_environment_resolves_slots_and_rejects_autoscale(self, monkeypatch):
        """Review #12: SWARMQ_POOL=cpu ohne Flag lief frueher als CPU-Modus mit zehn IO-Slots
        an der Autoscale-Pruefung vorbei."""
        monkeypatch.setenv("SWARMQ_POOL", "cpu")
        args = _args()
        prepare_pool_args(args)
        assert args.pool == "cpu"
        assert args.concurrency_source != "default"
        with pytest.raises(ConfigurationError):
            prepare_pool_args(_args(autoscale=(1, 4)))

    def test_processes_divide_the_cpu_slots(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        one = _args(pool="cpu")
        many = _args(pool="cpu", processes=1000)
        prepare_pool_args(one)
        prepare_pool_args(many)
        assert many.concurrency == 1
        assert one.concurrency >= many.concurrency


class TestCheckWithInvalidPool:
    def test_check_reports_invalid_environment_pool_as_json_error(self, monkeypatch, capsys):
        """Zweit-Review #1: ``--check`` haelt seinen JSON-Vertrag auch bei ungueltigem SWARMQ_POOL."""
        import json

        from swarmq.cli import main

        monkeypatch.setenv("SWARMQ_POOL", "gpu")
        rc = main(["worker", "--check", "--module", "json"])
        out = capsys.readouterr().out.strip().splitlines()
        payload = json.loads(out[-1])
        assert rc == 1
        assert payload["ok"] is False
        assert payload["error_type"] == "ConfigurationError"
        assert "SWARMQ_POOL" in payload["error"]
        assert payload["pool"] == "gpu"


class TestHeadroomDividesTheCpuSlots:
    """Kontrollkanal U4 (R4, R11): die Rechenslots je Prozess richten sich nach dem Spielraum."""

    @pytest.fixture
    def eight_cpus(self, monkeypatch):
        monkeypatch.delenv("SWARMQ_POOL", raising=False)
        monkeypatch.setattr("swarmq.slots._affinity_count", lambda: 8)
        monkeypatch.setattr("swarmq.slots._cgroup_quota", lambda _path: None)

    def test_headroom_eight_on_eight_cpus_gives_one_slot_per_process(self, eight_cpus):
        args = _args(pool="cpu", processes=2, max_processes=8)
        prepare_pool_args(args)
        assert args.concurrency == 1
        assert args.concurrency_source == "affinity/8"

    def test_without_headroom_the_start_count_divides(self, eight_cpus):
        args = _args(pool="cpu", processes=2)
        prepare_pool_args(args)
        assert args.concurrency == 4
