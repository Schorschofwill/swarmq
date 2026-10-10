"""Fixture-Modul der Mischbetrieb-Suite (Q2): läuft unter swarmq 0.11.x und 0.12.

Nur stdlib, ``valkey`` und die öffentliche Task-API, die beide Versionen
kennen; nichts aus ``tests.support`` (gibt es im 0.11-venv nicht in dieser Form).

Echtheitsprüfung beim Import: ``COMPAT_EXPECT_SWARMQ`` ist ``repo`` (der
Arbeitsstand unter ``<repo>/src``) oder eine Version wie ``0.11.1`` (aus
PyPI, nicht unter ``<repo>/src``). Passt die geladene swarmq nicht, bricht
der Prozess ab; sonst würde die Suite vakuum-grün.

Jede Ausführung landet als ``name|task_id|attempt|runtime`` in der Liste
``compat:runs:<queue>``; ``runtime`` ist ``repo`` oder ``pypi-<version>``.
"""

from __future__ import annotations

import importlib.metadata
import os
from pathlib import Path

import valkey.asyncio as aio_valkey

import swarmq
from swarmq.task import Task, TaskInfo

_REPO_SRC = Path(__file__).resolve().parents[2] / "src"
_QUEUE = os.environ.get("CLI_TEST_QUEUE", "compat_default")


def _runtime() -> str:
    expect = os.environ.get("COMPAT_EXPECT_SWARMQ", "")
    version = importlib.metadata.version("swarmq")
    from_repo = Path(swarmq.__file__).resolve().is_relative_to(_REPO_SRC)
    if expect == "repo" and from_repo:
        return "repo"
    if expect and expect != "repo" and version == expect and not from_repo:
        return f"pypi-{version}"
    raise SystemExit(f"compat: expected swarmq {expect or '<unset>'}, loaded {version} from {swarmq.__file__}")


RUNTIME = _runtime()


async def _record(name: str, task_id: str) -> int:
    """Zählt den Versuch je Task-ID und hängt die Ausführung an die Liste."""
    client = aio_valkey.Valkey.from_url(os.environ["SWARMQ_BACKEND_URL"])
    try:
        attempt = int(await client.incr(f"compat:attempts:{task_id}"))
        await client.rpush(f"compat:runs:{_QUEUE}", f"{name}|{task_id}|{attempt}|{RUNTIME}")
        return attempt
    finally:
        await client.aclose()


async def _flag(key: str) -> bool:
    client = aio_valkey.Valkey.from_url(os.environ["SWARMQ_BACKEND_URL"])
    try:
        return await client.get(f"compat:flag:{key}") is not None
    finally:
        await client.aclose()


class CompatFlakyTask(Task):
    """Erster Versuch scheitert, der Retry gelingt -- auf welcher Version auch immer."""

    task_info = TaskInfo(
        name="compat_flaky", queues=[_QUEUE], max_retries=2, retry_backoff="fixed", retry_base_delay=0.1
    )

    async def run(self) -> str:
        if await _record("compat_flaky", self._task_id) == 1:
            raise RuntimeError("first attempt fails")
        return RUNTIME


class CompatPoisonTask(Task):
    """Scheitert ohne Retry (DLQ), bis ``compat:flag:<queue>`` gesetzt ist."""

    task_info = TaskInfo(name="compat_poison", queues=[_QUEUE], max_retries=0)

    async def run(self) -> str:
        await _record("compat_poison", self._task_id)
        if not await _flag(_QUEUE):
            raise RuntimeError("poison until fixed")
        return RUNTIME


class CompatSlowRetryTask(Task):
    """Wie ``compat_flaky``, aber der Retry wartet 10 s (Plan 0.12.0, MR 10):
    unter 0.12 in der Delay-Stufe ``{queue}.DQ.15s``, unter 0.11 in ``{queue}.DQ``."""

    task_info = TaskInfo(
        name="compat_slow_retry", queues=[_QUEUE], max_retries=2, retry_backoff="fixed", retry_base_delay=10.0
    )

    async def run(self) -> str:
        if await _record("compat_slow_retry", self._task_id) == 1:
            raise RuntimeError("first attempt fails")
        return RUNTIME
