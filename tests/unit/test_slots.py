"""Tests fuer swarmq.slots — Pool-Modus und Slot-Voreinstellung (R3, R4, KTD7)."""

from __future__ import annotations

from pathlib import Path

import pytest

from swarmq import slots
from swarmq.exceptions import ConfigurationError


@pytest.fixture
def affinity(monkeypatch):
    def _set(n: int) -> None:
        monkeypatch.setattr("swarmq.slots.os.sched_getaffinity", lambda _pid: set(range(n)), raising=False)

    return _set


def _cpu_max(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "cpu.max"
    p.write_text(content, encoding="ascii")
    return p


def test_default_is_affinity_without_cgroup_quota(affinity, tmp_path):
    affinity(8)
    decision = slots.resolve_default_slots(cgroup_path=tmp_path / "missing")
    assert decision == slots.SlotDecision(count=8, source="affinity")


def test_cgroup_quota_below_affinity_wins(affinity, tmp_path):
    affinity(8)
    decision = slots.resolve_default_slots(cgroup_path=_cpu_max(tmp_path, "400000 100000\n"))
    assert decision == slots.SlotDecision(count=4, source="cgroup")


def test_cgroup_max_means_no_quota(affinity, tmp_path):
    affinity(8)
    decision = slots.resolve_default_slots(cgroup_path=_cpu_max(tmp_path, "max 100000\n"))
    assert decision.count == 8
    assert decision.source == "affinity"


def test_fractional_quota_rounds_up(affinity, tmp_path):
    affinity(8)
    decision = slots.resolve_default_slots(cgroup_path=_cpu_max(tmp_path, "150000 100000\n"))
    assert decision.count == 2


def test_processes_divide_the_budget_and_never_go_below_one(affinity, tmp_path):
    affinity(8)
    path = _cpu_max(tmp_path, "400000 100000\n")
    assert slots.resolve_default_slots(2, cgroup_path=path) == slots.SlotDecision(2, "cgroup/2")
    assert slots.resolve_default_slots(16, cgroup_path=path).count == 1


@pytest.mark.parametrize("content", ["garbage", "1 2 3", "-5 100000", "0 100000"])
def test_unparseable_cpu_max_is_ignored(affinity, tmp_path, content):
    affinity(3)
    assert slots.resolve_default_slots(cgroup_path=_cpu_max(tmp_path, content)).count == 3


@pytest.mark.entscheidung("E-061")
def test_effective_concurrency_explicit_wins_for_every_pool():
    assert slots.effective_concurrency(6, "cpu", 4) == slots.SlotDecision(6, "explicit")
    assert slots.effective_concurrency(6, "io") == slots.SlotDecision(6, "explicit")


def test_effective_concurrency_io_default_is_ten():
    assert slots.effective_concurrency(None, "io") == slots.SlotDecision(10, "default")


@pytest.mark.entscheidung("E-061")
def test_effective_concurrency_cpu_uses_assigned_cpus(affinity, monkeypatch):
    affinity(4)
    monkeypatch.setattr(slots, "_cgroup_quota", lambda _path: None)
    assert slots.effective_concurrency(None, "cpu", 2).count == 2


def test_validate_pool_mode_accepts_io_with_anything():
    slots.validate_pool_mode(pool="io", autoscale=(2, 8))


def test_validate_pool_mode_rejects_cpu_with_autoscale():
    with pytest.raises(ConfigurationError, match="--autoscale"):
        slots.validate_pool_mode(pool="cpu", autoscale=(2, 8))


def test_validate_pool_mode_rejects_cpu_on_non_posix(monkeypatch):
    monkeypatch.setattr("swarmq.slots.os.name", "nt")
    with pytest.raises(ConfigurationError, match="POSIX"):
        slots.validate_pool_mode(pool="cpu", autoscale=None)
