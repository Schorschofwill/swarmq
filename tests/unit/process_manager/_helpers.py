"""ProcessManager-Test-Helfer (Plan U21) — verbatim aus ``test_process_manager.py``."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

from swarmq.process_manager import (
    WorkerConfig,
)


def _make_test_cfg(**overrides) -> WorkerConfig:
    """Return a minimal WorkerConfig for unit tests (pickle-safe)."""
    defaults = dict(
        module="dummy.module",
        queues=("default",),
        concurrency=1,
        log_level="ERROR",
        log_format="human",
        burst=False,
        burst_idle_timeout=5.0,
        priorities=False,
        metrics_port=None,
        health_port=None,
    )
    defaults.update(overrides)
    return WorkerConfig(**defaults)


def _fake_process(alive: bool = True, exitcode: int | None = None, pid: int = 1234):
    p = MagicMock()
    p.is_alive.return_value = alive
    p.exitcode = exitcode
    p.pid = pid
    return p


def _entry_stub(worker_id, shutdown_event, *args):
    """No-op entry — never actually invoked in these unit tests."""
    return None


def _instant_crash_entry(worker_id, shutdown_event, *args):
    """Real-subprocess entry that crashes immediately (exitcode 3)."""
    os._exit(3)
