"""ProcessManager-Test-Helfer (Plan U21) — verbatim aus ``test_process_manager.py``."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock

from swarmq.process_manager import (
    WorkerConfig,
)
from swarmq.process_manager.slots import WorkerInfo
from swarmq.supervision import SupervisorChannel


def as_mock(obj: object) -> MagicMock:
    """Gibt ``obj`` als MagicMock zurueck; Tests ersetzen ``_ctx``/Prozesse durch Mocks."""
    assert isinstance(obj, MagicMock)
    return obj


def channel_of(info: WorkerInfo) -> SupervisorChannel:
    """Kontrollkanal eines gestarteten Workers (nach ``_start_worker`` immer gesetzt)."""
    assert info.channel is not None
    return info.channel


def _make_test_cfg(**overrides: Any) -> WorkerConfig:
    """Return a minimal WorkerConfig for unit tests (pickle-safe)."""
    defaults: dict[str, Any] = {
        "module": "dummy.module",
        "queues": ("default",),
        "concurrency": 1,
        "log_level": "ERROR",
        "log_format": "human",
        "burst": False,
        "burst_idle_timeout": 5.0,
        "priorities": False,
        "metrics_port": None,
        "health_port": None,
    }
    defaults.update(overrides)
    return WorkerConfig(**defaults)


def _fake_process(alive: bool = True, exitcode: int | None = None, pid: int = 1234) -> MagicMock:
    p = MagicMock()
    p.is_alive.return_value = alive
    p.exitcode = exitcode
    p.pid = pid
    return p


def _entry_stub(worker_id, shutdown_event, *args):
    """No-op entry — never actually invoked in these unit tests."""
    return


def _instant_crash_entry(worker_id, shutdown_event, *args):
    """Real-subprocess entry that crashes immediately (exitcode 3)."""
    os._exit(3)


def _register(pm, *infos) -> None:
    """Worker wie ``_start_worker`` eintragen: ``pm.workers[pid]`` plus Slot-Zustand (KTD1)."""
    for info in infos:
        pm.workers[info.process.pid] = info
        pm._refresh_slot(info.worker_id)


def _report_pdeathsig_entry(conn):
    """Real-subprocess entry: meldet das gesetzte Parent-Death-Signal (Linux)."""
    import ctypes

    value = ctypes.c_int(0)
    libc = ctypes.CDLL(None, use_errno=True)
    rc = libc.prctl(2, ctypes.byref(value), 0, 0, 0)  # PR_GET_PDEATHSIG
    conn.send(int(value.value) if rc == 0 else -1)
    conn.close()


class FakeProbe:
    """Gefaelschte Probe: ``rc`` None = laeuft noch."""

    def __init__(self, rc: int | None = None, error: str = "") -> None:
        self.rc = rc
        self.err = error
        self.killed = False
        self.closed = False

    def poll(self) -> int | None:
        return self.rc

    def error(self) -> str:
        return self.err

    def kill(self) -> None:
        self.killed = True

    def close(self) -> None:
        self.closed = True


class FakeProbeFactory:
    """Probe-Fabrik (U8): merkt sich die Konfigurationen, liefert die Proben der Reihe nach."""

    def __init__(self, *probes: FakeProbe) -> None:
        self.probes = list(probes)
        self.raws: list[str] = []

    def __call__(self, raw: str) -> FakeProbe:
        self.raws.append(raw)
        return self.probes.pop(0)


class FakeClock:
    """Monotone Uhr zum Vorstellen (``now``)."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now
