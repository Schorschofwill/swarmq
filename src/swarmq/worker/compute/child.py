"""Laufzeit eines Rechenkinds (U4: R6, R12, R13, R26, R27, KTD3, KTD5, KTD6).

Das Kind besitzt den Task-Koerper (``pre_execution``, ``run``,
``post_execution``) und den REQUEST-Scope des DI-Containers; Gates, Ack und
Fehlerpfad bleiben im Loop-Prozess. Zwei Betriebsarten: ``child_once`` fuer
ein frisches Kind je Task, ``child_loop`` fuer ein wiederverwendetes
Slot-Kind bis zum Ende-Signal.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
import signal
import time
from pathlib import Path
from typing import Any

from swarmq.worker.compute.envelope import (
    ComputeJob,
    ResultEnvelope,
    error_envelope,
    success_envelope,
)

_PR_SET_PDEATHSIG = 1


class ProgressBackend:
    """Rueckkanal-Shim: die zwei Methoden, die ``Task.update_progress`` braucht."""

    def __init__(self, queue: Any) -> None:
        self._queue = queue

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._queue.put(("progress", task_id, dict(fields), ttl_ms))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        self._queue.put(("publish", task_id, bytes(payload)))


def rss_mb(proc_root: Path = Path("/proc")) -> float | None:
    """Eigener RSS in MB aus ``/proc/self/status``; ``None`` ohne ``/proc``."""
    try:
        text = (proc_root / "self" / "status").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError):
        return None
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1]) / 1024
            except (IndexError, ValueError):
                return None
    return None


def release_forkserver_alive_fd() -> None:
    """Das geerbte Schreibende der Forkserver-Alive-Pipe schliessen (KTD3).

    Der Forkserver reicht jedem Kind ``_forkserver_alive_fd`` weiter, damit es
    selbst ueber ihn spawnen koennte. Der Forkserver beendet sich aber erst,
    wenn ALLE Schreibenden zu sind — ein lebendes Kind hielte ihn also nach dem
    Tod des Loop-Prozesses am Leben, und das Parent-Death-Signal, das das Kind
    toeten soll, koennte nie feuern. Rechenkinder spawnen nicht per
    Forkserver, das fd wird nicht gebraucht.
    """
    try:
        from multiprocessing import forkserver as _fs
    except ImportError:  # pragma: no cover - Nicht-POSIX
        return
    server: Any = _fs._forkserver  # privates Attribut, nicht typisiert
    fd = getattr(server, "_forkserver_alive_fd", None)
    if fd is None:
        return
    with contextlib.suppress(OSError):
        os.close(fd)
    server._forkserver_alive_fd = None


def bind_child_process() -> bool:
    """KTD3: eigene Prozessgruppe plus Parent-Death-Signal (Linux).

    Liefert ``True``, wenn das Parent-Death-Signal gesetzt werden konnte.
    Ohne ``prctl`` (macOS) bleibt das EOF der Job-Pipe die Rueckfallebene.
    """
    release_forkserver_alive_fd()
    try:
        os.setpgid(0, 0)
    except OSError:
        pass
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        return int(libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0)) == 0
    except (AttributeError, OSError):
        return False


def _lookup_task_class(task_name: str) -> type:
    from swarmq.task import Task

    for cls in Task._registered_tasks:
        if cls.task_info.name == task_name:
            return cls
    raise LookupError(
        f"task {task_name!r} is not registered in this compute child; the "
        "forkserver preload must import the module that defines it"
    )


def _uses_di(task_class: type) -> bool:
    return bool(getattr(task_class, "_dishka_injected_keys", None))


async def run_job(job: ComputeJob, progress_queue: Any, *, slot: int | None = None) -> ResultEnvelope:
    """Einen Task-Koerper im Kind ausfuehren und als Envelope verpacken.

    Spiegelt ``TaskExecutor``: REQUEST-Scope nur bei DI-Keys (KTD6),
    ``pre_execution`` → ``run`` (mit Soft-Timeout) → ``post_execution``,
    Scope-Abbau mit der eingefangenen Exception (R26). Ein Cancel des
    umgebenden Tasks (SIGTERM-Handler) erreicht den Scope als
    ``CancelledError``, wie im Loop-Prozess.
    """
    started = time.monotonic()
    meta: dict[str, Any] = {"slot": slot}
    try:
        task_class = _lookup_task_class(job.task_name)
    except LookupError as exc:
        return error_envelope(job.task_id, exc, duration_s=0.0, rss_mb=rss_mb(), **meta)

    task_info = task_class.task_info  # type: ignore[attr-defined]
    instance: Any = task_class()
    instance._task_id = job.task_id
    instance._backend = ProgressBackend(progress_queue)
    instance._task_start_time = time.monotonic()
    instance._result_ttl_ms = job.result_ttl_ms

    scope: Any = None
    token: Any = None
    if _uses_di(task_class):
        from swarmq.dishka import _MISSING, TaskContext, get_registered_container, request_container

        container: Any = get_registered_container()
        if container is not _MISSING:
            scope = container(
                context={
                    TaskContext: TaskContext(
                        task_id=job.task_id,
                        task_name=job.task_name,
                        queue=job.queue,
                        retry_count=job.retry_count,
                    )
                }
            )
            token = request_container.set(await scope.__aenter__())

    caught: BaseException | None = None
    envelope: ResultEnvelope
    try:
        await instance.pre_execution()
        run_coro = instance.run(*job.args, **job.kwargs)
        if task_info.soft_timeout is not None:
            run_coro = asyncio.wait_for(run_coro, timeout=task_info.soft_timeout)
        try:
            value = await run_coro
        except TimeoutError as exc:
            from swarmq.exceptions import SoftTimeoutError

            raise SoftTimeoutError(f"soft timeout after {task_info.soft_timeout}s") from exc
        await instance.post_execution()
        envelope = success_envelope(job.task_id, value, **meta)
    except asyncio.CancelledError as exc:
        caught = exc
        raise
    except Exception as exc:
        caught = exc
        envelope = error_envelope(job.task_id, exc, **meta)
    finally:
        if token is not None:
            request_container.reset(token)
        if scope is not None:
            et = type(caught) if caught is not None else None
            tb = caught.__traceback__ if caught is not None else None
            await scope.__aexit__(et, caught, tb)
    envelope.duration_s = time.monotonic() - started
    envelope.rss_mb = rss_mb()
    return envelope


def _run_job_with_sigterm(job: ComputeJob, progress_queue: Any, slot: int | None) -> ResultEnvelope:
    """``run_job`` unter einem Event-Loop, dessen SIGTERM den Task abbricht.

    So bekommt der REQUEST-Teardown die Gnadenfrist aus KTD3, bevor der
    Loop-Prozess SIGKILL schickt.
    """

    async def _main() -> ResultEnvelope:
        loop = asyncio.get_running_loop()
        task = asyncio.ensure_future(run_job(job, progress_queue, slot=slot))
        try:
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - kein Unix-Loop
            pass
        try:
            return await task
        finally:
            try:
                loop.remove_signal_handler(signal.SIGTERM)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                pass

    return asyncio.run(_main())


def _configure_child_logging(job: ComputeJob) -> Any:
    import structlog

    from swarmq.logging import setup_logging

    listener = setup_logging(job.log_level, job.log_json)
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        task_id=job.task_id, task_name=job.task_name, queue=job.queue,
        worker_id=job.worker_id, compute_child=os.getpid(),
    )
    return listener


def child_once(job: ComputeJob, result_queue: Any, progress_queue: Any, slot: int | None = None) -> None:
    """Frisches Kind je Task (R6): binden, Ready melden, rechnen, Envelope, Ende."""
    pdeath = bind_child_process()
    result_queue.put(("ready", slot, os.getpid(), pdeath))
    listener = _configure_child_logging(job)
    try:
        try:
            envelope = _run_job_with_sigterm(job, progress_queue, slot)
        except asyncio.CancelledError:
            return
        result_queue.put(("result", envelope))
    finally:
        listener.stop()


def child_loop(slot: int, job_queue: Any, result_queue: Any, progress_queue: Any) -> None:
    """Wiederverwendetes Slot-Kind (R6): Jobs bis zum Ende-Signal ``None``."""
    pdeath = bind_child_process()
    result_queue.put(("ready", slot, os.getpid(), pdeath))
    listener: Any = None
    while True:
        try:
            job = job_queue.get()
        except (EOFError, OSError):
            break  # Job-Pipe tot: der Loop-Prozess ist weg (R27, Rueckfallebene)
        if job is None:
            break
        if listener is None:
            listener = _configure_child_logging(job)
        try:
            envelope = _run_job_with_sigterm(job, progress_queue, slot)
        except asyncio.CancelledError:
            break
        result_queue.put(("result", envelope))
    if listener is not None:
        listener.stop()
