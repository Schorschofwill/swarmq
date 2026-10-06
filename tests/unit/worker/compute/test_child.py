"""Tests fuer swarmq.worker.compute.child — Kind-Laufzeit ohne echten Fork (U4)."""


import asyncio
import queue as _queue
from typing import Any

import pytest

from swarmq.exceptions import NoRetry, SoftTimeoutError
from swarmq.task import Task, TaskInfo
from swarmq.worker.compute import child
from swarmq.worker.compute.envelope import ComputeJob


@pytest.fixture(autouse=True)
def _clean_registry():
    saved = list(Task._registered_tasks)
    Task._clear_registered_tasks()
    yield
    Task._clear_registered_tasks()
    Task._registered_tasks.extend(saved)


def _job(name: str, *args, **kwargs) -> ComputeJob:
    return ComputeJob(task_id="tid-1", task_name=name, queue="q", args=args, kwargs=kwargs)


@pytest.mark.timeout(5)
async def test_run_job_returns_value_and_calls_lifecycle_in_order():
    calls: list[str] = []

    class Plain(Task):
        task_info = TaskInfo(name="plain", queues=["q"])

        async def pre_execution(self) -> None:
            calls.append("pre")

        async def run(self, x: int) -> int:
            calls.append("run")
            return x * 2

        async def post_execution(self) -> None:
            calls.append("post")

    env = await child.run_job(_job("plain", 21), _queue.Queue())
    assert env.ok and env.value == 42
    assert calls == ["pre", "run", "post"]
    assert env.duration_s >= 0


@pytest.mark.timeout(5)
async def test_run_job_wraps_exception_typed():
    class Failing(Task):
        task_info = TaskInfo(name="failing", queues=["q"])

        async def run(self) -> None:
            raise NoRetry("deterministic")

    env = await child.run_job(_job("failing"), _queue.Queue())
    assert not env.ok
    assert isinstance(env.error(), NoRetry)


@pytest.mark.timeout(5)
async def test_run_job_soft_timeout_becomes_soft_timeout_error():
    class Slow(Task):
        task_info = TaskInfo(name="slow", queues=["q"], soft_timeout=0.05)

        async def run(self) -> None:
            await asyncio.sleep(1)

    env = await child.run_job(_job("slow"), _queue.Queue())
    assert isinstance(env.error(), SoftTimeoutError)


@pytest.mark.timeout(5)
async def test_run_job_unknown_task_is_an_error_envelope():
    env = await child.run_job(_job("ghost"), _queue.Queue())
    assert not env.ok
    assert "not registered" in str(env.error())


@pytest.mark.timeout(5)
async def test_progress_goes_through_the_shim_queue():
    class _Channel:
        def __init__(self) -> None:
            self.sent: list[tuple[Any, ...]] = []

        def send(self, msg: tuple[Any, ...]) -> None:
            self.sent.append(msg)

    progress = _Channel()

    class Reporting(Task):
        task_info = TaskInfo(name="reporting", queues=["q"])

        async def run(self) -> str:
            await self.update_progress(1, 2, "half")
            await self.update_progress(2, 2, "done")
            return "ok"

    env = await child.run_job(_job("reporting"), progress)
    assert env.ok
    kinds = [msg[0] for msg in progress.sent]
    assert kinds and set(kinds) <= {"progress", "publish"}


@pytest.mark.timeout(5)
async def test_di_request_scope_commits_on_success_and_rolls_back_on_exception():
    """R26 / AE14: yield-Provider sehen Erfolg (None) bzw. die Exception."""
    pytest.importorskip("dishka")
    from collections.abc import AsyncIterable

    from dishka import FromDishka, Provider, Scope, from_context, make_async_container, provide

    from swarmq import dishka as di

    events: list[object] = []

    class Tx:
        pass

    class Prov(Provider):
        tc = from_context(di.TaskContext, scope=Scope.REQUEST)

        @provide(scope=Scope.REQUEST)
        async def tx(self, ctx: di.TaskContext) -> AsyncIterable[Tx]:
            # dishka *sendet* die Exception als Yield-Wert (asend), wirft sie nicht.
            exc = yield Tx()
            if exc is None:
                events.append(("commit", ctx.task_id))
            else:
                events.append(("rollback", type(exc).__name__, ctx.task_id))

    di.setup_dishka(make_async_container(Prov()))

    class Good(Task):
        task_info = TaskInfo(name="good_di", queues=["q"])

        async def run(self, tx: FromDishka[Tx]) -> str:  # type: ignore[valid-type]
            return "ok"

    class Bad(Task):
        task_info = TaskInfo(name="bad_di", queues=["q"])

        async def run(self, tx: FromDishka[Tx]) -> str:  # type: ignore[valid-type]
            raise ValueError("boom")

    ok = await child.run_job(_job("good_di"), _queue.Queue())
    bad = await child.run_job(_job("bad_di"), _queue.Queue())
    assert ok.ok and not bad.ok
    assert events == [("commit", "tid-1"), ("rollback", "ValueError", "tid-1")]


def test_rss_reads_vmrss(tmp_path):
    (tmp_path / "self").mkdir()
    (tmp_path / "self" / "status").write_text("VmRSS:\t204800 kB\n", encoding="ascii")
    assert child.rss_mb(tmp_path) == 200.0
    assert child.rss_mb(tmp_path / "missing") is None


def test_release_forkserver_alive_fd_closes_the_inherited_write_end(monkeypatch):
    """KTD3: das Kind darf den Forkserver nicht am Leben halten — sonst feuert das
    Parent-Death-Signal nach dem Tod des Loop-Prozesses nie."""
    import os
    from multiprocessing import forkserver as fs

    from swarmq.worker.compute import child as childmod

    r, w = os.pipe()
    monkeypatch.setattr(fs._forkserver, "_forkserver_alive_fd", w, raising=False)
    try:
        childmod.release_forkserver_alive_fd()
        assert fs._forkserver._forkserver_alive_fd is None
        with pytest.raises(OSError):
            os.fstat(w)
        assert os.read(r, 1) == b""  # EOF: kein Schreibende mehr offen
        childmod.release_forkserver_alive_fd()  # idempotent
    finally:
        os.close(r)


# --------------------------------------------------------------------------- Job-Loop (Zweit-Review #8, #9)


@pytest.fixture
def _quiet_child_logging(monkeypatch):
    monkeypatch.setattr(child, "_configure_child_logging", lambda job, channel: None)
    # Am Kind-Ende laeuft ``logging.shutdown`` -- im Testprozess schloesse es
    # alle Handler.
    monkeypatch.setattr(child, "_shutdown_child_logging", lambda: None)
    monkeypatch.setattr(child, "_bind_job_context", lambda job: None)


@pytest.mark.timeout(5)
async def test_recv_job_delivers_jobs_then_none_on_end_signal_and_eof():
    import multiprocessing as mp

    r, w = mp.Pipe(duplex=False)
    stop = asyncio.Event()
    w.send(_job("plain", 1))
    assert (await child._recv_job(r, stop)).task_name == "plain"
    w.send(None)
    assert await child._recv_job(r, stop) is None
    w.close()
    assert await child._recv_job(r, stop) is None  # EOF: der Loop-Prozess ist weg
    r.close()


@pytest.mark.timeout(5)
async def test_recv_job_returns_none_on_stop_and_on_an_unreadable_frame():
    import multiprocessing as mp

    r, w = mp.Pipe(duplex=False)
    stop = asyncio.Event()
    stop.set()
    assert await child._recv_job(r, stop) is None  # SIGTERM im Leerlauf
    stop.clear()
    w.send_bytes(b"\x80\x05definitely not a pickle")
    assert await child._recv_job(r, stop) is None  # Zweit-Review #9: kein ewig offenes Future
    r.close()
    w.close()


@pytest.mark.timeout(5)
async def test_serve_jobs_runs_consecutive_jobs_on_one_loop_and_ends_on_none(_quiet_child_logging):
    import multiprocessing as mp

    seen_loops: list[int] = []

    class LoopWitness(Task):
        task_info = TaskInfo(name="loop_witness", queues=["q"])

        async def run(self, x: int) -> int:
            seen_loops.append(id(asyncio.get_running_loop()))
            return x + 1

    job_r, job_w = mp.Pipe(duplex=False)
    chan_r, chan_w = mp.Pipe(duplex=False)
    server = asyncio.create_task(child._serve_jobs(0, job_r, chan_w))
    job_w.send(ComputeJob(task_id="j1", task_name="loop_witness", queue="q", args=(1,), kwargs={}))
    job_w.send(ComputeJob(task_id="j2", task_name="loop_witness", queue="q", args=(2,), kwargs={}))
    first = await asyncio.to_thread(chan_r.recv)
    second = await asyncio.to_thread(chan_r.recv)
    assert [m[1].value for m in (first, second)] == [2, 3]
    assert [m[1].task_id for m in (first, second)] == ["j1", "j2"]
    assert len(set(seen_loops)) == 1  # ein Loop fuer alle Jobs (Erst-Review #4)
    job_w.send(None)
    await asyncio.wait_for(server, timeout=2.0)
    for c in (job_r, job_w, chan_r, chan_w):
        c.close()
