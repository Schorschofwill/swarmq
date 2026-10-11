"""E2E-Abnahme des CPU-Modus (Plan 2026-09-03-001, U7: AE2, AE3, AE9-Teil, AE13, R7).

Laeuft in-process mit ``pool="cpu"`` und Burst-Modus gegen echtes RabbitMQ
und Valkey. Die Tasks kommen aus ``tests/e2e/_cpu_fixture_tasks.py``, das
der Forkserver vorlaedt.
"""

from __future__ import annotations

import asyncio
import os
import signal
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.signals import Signal
from swarmq.task import Task

pytestmark = [pytest.mark.timeout(90)]

_QUEUE = f"e2e-cpu-{uuid.uuid4().hex[:8]}"
os.environ["CPU_FIXTURE_QUEUE"] = _QUEUE

from tests.e2e import _cpu_fixture_tasks as fixture  # noqa: E402  (QUEUE braucht die Env-Var)


@pytest.fixture
def _register_fixture_tasks():
    """Die Fixture-Klassen erneut registrieren — andere Tests leeren die Registry."""
    for cls in (
        fixture.CpuEcho,
        fixture.CpuFail,
        fixture.CpuHang,
        fixture.CpuSubprocess,
        fixture.CpuSlowOnce,
        fixture.CpuWeirdError,
        fixture.CpuBig,
    ):
        if cls not in Task._registered_tasks:
            Task._registered_tasks.append(cls)
    for name in ("CpuDi", "CpuDiLock"):
        di = getattr(fixture, name, None)
        if di is not None and di not in Task._registered_tasks:
            Task._registered_tasks.append(di)
    yield
    Task._clear_registered_tasks()


async def _run_burst(app: SwarmQ, timeout: float = 40.0) -> None:
    await asyncio.wait_for(app.start_worker(), timeout=timeout)


async def _results(rabbitmq_url: str, valkey_url: str, tids: list[str]) -> list[object]:
    """Der Worker schliesst beim Ende seine Verbindungen — Ergebnisse mit frischer App lesen."""
    reader = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[_QUEUE])
    async with reader:
        return [await reader.get_result(t, timeout=10.0) for t in tids]


async def _error_message(rabbitmq_url: str, valkey_url: str, tid: str) -> str:
    """Fehlerergebnisse kommen als ``TaskError`` an — den Text zurueckgeben."""
    with pytest.raises(TaskError) as excinfo:
        await _results(rabbitmq_url, valkey_url, [tid])
    return str(excinfo.value)


def _app(rabbitmq_url: str, valkey_url: str, **cfg) -> SwarmQ:
    return SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[_QUEUE],
        pool="cpu",
        burst_mode=True,
        **{"burst_idle_timeout": 2.0, **cfg},
    )


async def test_fresh_child_per_task_and_results_round_trip(rabbitmq_url, valkey_url, _register_fixture_tasks):
    """AE2: Tasks ohne DI laufen in je einem eigenen Kind, das danach endet."""
    app = _app(rabbitmq_url, valkey_url, concurrency=2)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        tids = [await app.schedule("cpu_echo", i) for i in range(6)]
        await _run_burst(app)
    results = await _results(rabbitmq_url, valkey_url, tids)
    values = sorted(r["value"] for r in results)
    assert values == [0, 2, 4, 6, 8, 10]
    pids = {r["pid"] for r in results}
    assert len(pids) == 6 and os.getpid() not in pids
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


async def test_hard_timeout_kills_blocking_child_and_dead_letters(rabbitmq_url, valkey_url, _register_fixture_tasks):
    """AE3 (max_retries=0): ein synchron blockierender Task wird nach hard_timeout getoetet."""
    app = _app(rabbitmq_url, valkey_url, concurrency=1)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        tid = await app.schedule("cpu_hang", 30.0)
        await _run_burst(app)
    message = await _error_message(rabbitmq_url, valkey_url, tid)
    assert "hard timeout" in message


async def test_kill_takes_the_grandchild_with_it(rabbitmq_url, valkey_url, _register_fixture_tasks, tmp_path):
    """AE13: der Kill trifft die Prozessgruppe, ein ImageMagick-artiger Enkel ueberlebt nicht."""
    pid_file = tmp_path / "grandchild.pid"
    app = _app(rabbitmq_url, valkey_url, concurrency=1)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        tid = await app.schedule("cpu_subprocess", str(pid_file))
        await _run_burst(app)
    await _error_message(rabbitmq_url, valkey_url, tid)
    grandchild = int(pid_file.read_text(encoding="ascii"))
    for _ in range(50):
        try:
            os.kill(grandchild, 0)
        except ProcessLookupError:
            break
        await asyncio.sleep(0.1)
    else:
        os.kill(grandchild, 9)
        pytest.fail("grandchild survived the process-group kill")


async def test_exception_in_child_reaches_error_path_typed(rabbitmq_url, valkey_url, _register_fixture_tasks):
    """AE9-Teil / KTD5: NoRetry aus dem Kind landet ohne Retry als Fehlerergebnis."""
    app = _app(rabbitmq_url, valkey_url, concurrency=1)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        tid = await app.schedule("cpu_fail")
        await _run_burst(app)
    message = await _error_message(rabbitmq_url, valkey_url, tid)
    assert "deterministic failure" in message


async def test_di_child_is_reused_and_recycled_with_signal(rabbitmq_url, valkey_url, _register_fixture_tasks):
    """R6/R7/AE1-Analogon: DI-Tasks teilen ein Kind; nach recycle_max_tasks kommt ein neues
    Kind und WORKER_RECYCLING feuert mit Grund ``tasks``."""
    pytest.importorskip("dishka")
    recyclings: list[dict] = []
    app = _app(rabbitmq_url, valkey_url, concurrency=1, recycle_max_tasks=2)

    @app.on_signal(Signal.WORKER_RECYCLING)
    async def _on_recycle(**kw):
        recyclings.append(kw)

    async with app:
        await app.broker.declare_queue(_QUEUE)
        tids = [await app.schedule("cpu_di") for _ in range(5)]
        await _run_burst(app)
    results = await _results(rabbitmq_url, valkey_url, tids)
    for r in results:
        assert r["app_pid"] == r["pid"]  # APP-Singleton lebt im Kind, nicht im Loop
    pids = [r["pid"] for r in results]
    assert len(set(pids)) >= 2 and len(set(pids)) <= 3  # Wiederverwendung + Recycling
    assert recyclings and all(r["kind"] == "compute_child" and r["reason"] == "tasks" for r in recyclings)


async def _wait_for_pid(pid_file, timeout: float = 20.0) -> int:
    deadline = asyncio.get_running_loop().time() + timeout
    while not pid_file.exists():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("compute child never wrote its pid")
        await asyncio.sleep(0.05)
    return int(pid_file.read_text(encoding="ascii"))


async def test_chaos_child_sigkill_is_a_typed_failure_and_the_slot_stays_usable(
    rabbitmq_url, valkey_url, _register_fixture_tasks, tmp_path
):
    """Chaos (R8/KTD2): stirbt das Rechenkind hart, wird der Task als Fehler
    abgeschlossen (kein Haenger, keine Exception in der Pipeline), und der
    Slot nimmt sofort den naechsten Task an."""
    pid_file = tmp_path / "child.pid"
    app = _app(rabbitmq_url, valkey_url, concurrency=1, burst_idle_timeout=3.0)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        slow = await app.schedule("cpu_slow_once", str(pid_file), 60.0)
        echo = await app.schedule("cpu_echo", 5)
        worker = asyncio.create_task(app.start_worker())
        child_pid = await _wait_for_pid(pid_file)
        os.kill(child_pid, signal.SIGKILL)
        await asyncio.wait_for(worker, timeout=40.0)
    message = await _error_message(rabbitmq_url, valkey_url, slow)
    assert "exited without result" in message
    echo_result = (await _results(rabbitmq_url, valkey_url, [echo]))[0]
    assert echo_result == {"value": 10, "pid": echo_result["pid"]}


async def test_large_results_and_small_bursts_never_lose_a_result(rabbitmq_url, valkey_url, _register_fixture_tasks):
    """P0 #3 (Sentinel-Wettlauf): das frische Kind endet direkt nach dem Schreiben —
    kein Ergebnis darf als Kindtod enden, weder bei 20 MB noch bei vielen kleinen."""
    app = _app(rabbitmq_url, valkey_url, concurrency=2, burst_idle_timeout=3.0)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        big = [await app.schedule("cpu_big", 20) for _ in range(6)]
        small = [await app.schedule("cpu_echo", i) for i in range(40)]
        await _run_burst(app, timeout=120.0)
    results = await _results(rabbitmq_url, valkey_url, big + small)
    assert all(len(r) == 20 * 1024 * 1024 for r in results[:6])
    assert [r["value"] for r in results[6:]] == [i * 2 for i in range(40)]


async def test_exception_that_does_not_unpickle_in_the_parent_is_a_typed_failure(
    rabbitmq_url, valkey_url, _register_fixture_tasks
):
    """P0 #1: eine Exception mit eigener __init__-Signatur darf den Pool nicht taub machen."""
    app = _app(rabbitmq_url, valkey_url, concurrency=1, burst_idle_timeout=3.0)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        weird = await app.schedule("cpu_weird_error")
        after = await app.schedule("cpu_echo", 7)
        await _run_burst(app)
    message = await _error_message(rabbitmq_url, valkey_url, weird)
    assert "left/right" in message
    assert (await _results(rabbitmq_url, valkey_url, [after]))[0]["value"] == 14


async def test_di_child_keeps_one_event_loop_for_loop_bound_app_resources(
    rabbitmq_url, valkey_url, _register_fixture_tasks
):
    """P1 #4: ein asyncio.Lock aus dem APP-Scope muss ueber mehrere Jobs desselben Kindes gueltig bleiben."""
    if not hasattr(fixture, "CpuDiLock"):
        pytest.skip("dishka nicht installiert")
    app = _app(rabbitmq_url, valkey_url, concurrency=1, burst_idle_timeout=3.0)
    async with app:
        await app.broker.declare_queue(_QUEUE)
        tids = [await app.schedule("cpu_di_lock") for _ in range(4)]
        await _run_burst(app)
    pids = await _results(rabbitmq_url, valkey_url, tids)
    assert len(set(pids)) == 1, pids  # ein Kind, vier Jobs, ein Loop
