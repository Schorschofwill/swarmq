"""Slot-Voreinstellung einer eingebetteten Konfiguration (F3-14, E-061).

Den Grundfall (``pool="cpu"`` ohne ``concurrency`` -> zugeteilte CPUs,
``concurrency=10`` bleibt 10) hält der Nachweis
``test_befund_f3_14_embedded_cpu_concurrency.py`` fest. Gemockt sind nur die
Grenzen: CPU-Affinität (``os.sched_getaffinity``) und das Lesen der
cgroup-Quote.

Testliste:
# [x] SWARMQ_POOL=cpu ohne Argumente -> zugeteilte CPUs
# [x] SWARMQ_CONCURRENCY=6 mit pool="cpu" -> 6
# [x] cgroup-Quote 2 bei Affinität 8 -> 2
# [x] pool="io" ohne concurrency -> 10, Typ genau int
# [x] pool="cpu" ohne concurrency -> Typ genau int (kein Marker nach außen)
# [x] concurrency=0 -> weiter ConfigurationError
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from swarmq import slots
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError


def _cfg(**kwargs: Any) -> SwarmQConfig:
    return SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", **kwargs)


@pytest.fixture
def assigned_cpus(monkeypatch: pytest.MonkeyPatch) -> Callable[[int, int | None], None]:
    """Setzt Affinität und cgroup-Quote; ``SWARMQ_POOL``/``SWARMQ_CONCURRENCY`` leer."""
    monkeypatch.delenv("SWARMQ_POOL", raising=False)
    monkeypatch.delenv("SWARMQ_CONCURRENCY", raising=False)

    def _set(affinity: int, quota: int | None) -> None:
        def _affinity(_pid: int) -> set[int]:
            return set(range(affinity))

        def _quota(_path: Path) -> int | None:
            return quota

        monkeypatch.setattr(os, "sched_getaffinity", _affinity, raising=False)
        monkeypatch.setattr(slots, "_cgroup_quota", _quota)

    return _set


def test_cpu_pool_from_env_defaults_to_assigned_cpus(
    assigned_cpus: Callable[[int, int | None], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    assigned_cpus(3, None)
    monkeypatch.setenv("SWARMQ_POOL", "cpu")

    assert _cfg().concurrency == 3


def test_concurrency_from_env_wins_over_assigned_cpus(
    assigned_cpus: Callable[[int, int | None], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    assigned_cpus(3, None)
    monkeypatch.setenv("SWARMQ_CONCURRENCY", "6")

    assert _cfg(pool="cpu").concurrency == 6


def test_cgroup_quota_below_affinity_limits_the_slots(assigned_cpus: Callable[[int, int | None], None]) -> None:
    assigned_cpus(8, 2)

    assert _cfg(pool="cpu").concurrency == 2


def test_io_pool_keeps_ten_slots_as_plain_int(assigned_cpus: Callable[[int, int | None], None]) -> None:
    assigned_cpus(3, None)

    concurrency = _cfg(pool="io").concurrency

    assert concurrency == 10
    assert type(concurrency) is int


def test_cpu_pool_default_is_a_plain_int(assigned_cpus: Callable[[int, int | None], None]) -> None:
    assigned_cpus(3, None)

    assert type(_cfg(pool="cpu").concurrency) is int


def test_zero_concurrency_is_still_rejected_for_the_cpu_pool(
    assigned_cpus: Callable[[int, int | None], None],
) -> None:
    assigned_cpus(3, None)

    with pytest.raises(ConfigurationError, match="concurrency must be >= 1"):
        _cfg(pool="cpu", concurrency=0)
