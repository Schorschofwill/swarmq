"""Nachweis F3-14: Ein eingebetteter CPU-Pool nimmt 10 Slots statt der
zugeteilten CPUs.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-14, R3D-09).

``SwarmQConfig(pool="cpu")`` ohne ``concurrency`` behaelt den IO-Default 10.
Die CLI loest dagegen ueber ``slots.effective_concurrency`` die zugeteilten
CPUs auf (CPU-Plan 2026-09-03-001, R3: "nicht 10", R25 Speicherschutz);
``configuration.md:34`` nennt "assigned CPUs (cpu)" auch fuer den
eingebetteten Betrieb. Auf kleinen Containern droht OOM.

Festlegung (Entscheidung 7): Code-Fix -- der eingebettete CPU-Pool nimmt
dieselbe Slot-Voreinstellung wie die CLI; ein ausdrueckliches
``concurrency=`` gewinnt weiterhin.

Behoben in 0.12.3: ``concurrency`` hat einen ``int``-Marker als Default,
``_apply_pool_defaults`` loest ihn ueber ``slots.effective_concurrency`` auf.

Gemockt sind nur die Grenzen: die CPU-Affinitaet (``os.sched_getaffinity``)
und das Lesen der cgroup-Quote.

Testliste:
# [x] pool="cpu" ohne concurrency -> Slots = zugeteilte CPUs (hier 3);
#     ausdrueckliches concurrency=10 bleibt 10
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from swarmq import slots
from swarmq.config import SwarmQConfig

_ASSIGNED_CPUS = 3


def _three_cpus(_pid: int) -> set[int]:
    return set(range(_ASSIGNED_CPUS))


def _no_cgroup_quota(_path: Path) -> int | None:
    return None


@pytest.fixture
def three_assigned_cpus(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARMQ_CONCURRENCY", raising=False)
    monkeypatch.setattr(os, "sched_getaffinity", _three_cpus, raising=False)
    # Lesen von ``/sys/fs/cgroup/cpu.max`` (Dateisystem-Grenze): keine Quote.
    monkeypatch.setattr(slots, "_cgroup_quota", _no_cgroup_quota)


@pytest.mark.befund("F3-14")
@pytest.mark.entscheidung("E-061")
@pytest.mark.usefixtures("three_assigned_cpus")
def test_embedded_cpu_pool_defaults_to_assigned_cpus() -> None:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", pool="cpu")

    assert config.concurrency == _ASSIGNED_CPUS, (
        f"eingebetteter CPU-Pool: {config.concurrency} Slots statt {_ASSIGNED_CPUS} zugeteilter CPUs"
    )
    explicit = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", pool="cpu", concurrency=10)
    assert explicit.concurrency == 10
