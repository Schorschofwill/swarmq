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
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

from swarmq.logging_drain import exit_code_for, exit_if_log_thread_stuck
from swarmq.worker.compute.envelope import (
    ComputeJob,
    ResultEnvelope,
    error_envelope,
    success_envelope,
)

_PR_SET_PDEATHSIG = 1


class ProgressBackend:
    """Rueckkanal-Shim: die zwei Methoden, die ``Task.update_progress`` braucht."""

    def __init__(self, channel: Any) -> None:
        self._channel = channel

    async def set_progress(self, task_id: str, fields: dict[str, str], ttl_ms: int) -> None:
        self._channel.send(("progress", task_id, dict(fields), ttl_ms))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        self._channel.send(("publish", task_id, bytes(payload)))


def rss_mb(proc_root: Path = Path("/proc")) -> float | None:
    """Eigener RSS in MB aus ``/proc/self/status``; ``None`` ohne ``/proc``."""
    try:
        text = (proc_root / "self" / "status").read_text(encoding="ascii")
    except OSError, UnicodeDecodeError:
        return None
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1]) / 1024
            except IndexError, ValueError:
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
    except AttributeError, OSError:
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


async def run_job(job: ComputeJob, channel: Any, *, slot: int | None = None) -> ResultEnvelope:
    """Einen Task-Koerper im Kind ausfuehren und als Envelope verpacken.

    Spiegelt ``TaskExecutor``: REQUEST-Scope nur bei DI-Keys (KTD6),
    ``pre_execution`` → ``run`` (mit Soft-Timeout) → ``post_execution``,
    Scope-Abbau mit der eingefangenen Exception (R26). Ein Cancel des
    umgebenden Tasks (SIGTERM-Handler) erreicht den Scope als
    ``CancelledError``, wie im Loop-Prozess. Der Logkontext gilt nur fuer
    diesen Job (KTD13): ein wiederverwendetes Kind traegt nichts weiter.
    """
    with _job_log_context(job):
        return await _run_job(job, channel, slot)


def _job_log_context(job: ComputeJob) -> Any:
    """Task-Kontext je Job, zusaetzlich mit der PID des Kinds (R4, KTD13)."""
    from swarmq.logging import _task_log_context

    return _task_log_context(
        task_id=job.task_id,
        task_name=job.task_name,
        queue=job.queue,
        worker_id=job.worker_id,
        workflow_id=job.workflow_id,
        attempt=job.retry_count + 1,
        compute_child=os.getpid(),
    )


async def _run_job(job: ComputeJob, channel: Any, slot: int | None) -> ResultEnvelope:
    started = time.monotonic()
    meta: dict[str, Any] = {"slot": slot, "pid": os.getpid()}
    try:
        task_class = _lookup_task_class(job.task_name)
    except LookupError as exc:
        return error_envelope(job.task_id, exc, duration_s=0.0, rss_mb=rss_mb(), **meta)

    task_info = task_class.task_info  # type: ignore[attr-defined]
    instance: Any = task_class()
    instance._task_id = job.task_id
    instance._backend = ProgressBackend(channel)
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
    except Exception as exc:  # noqa: BLE001 - Task-Fehler wird als Fehler-Envelope gemeldet
        caught = exc
        envelope = error_envelope(job.task_id, exc, **meta)
    finally:
        if token is not None:
            token.var.reset(token)  # dieselbe ContextVar wie request_container
        if scope is not None:
            et = type(caught) if caught is not None else None
            tb = caught.__traceback__ if caught is not None else None
            await scope.__aexit__(et, caught, tb)
    envelope.duration_s = time.monotonic() - started
    envelope.rss_mb = rss_mb()
    return envelope


def _run_job_with_sigterm(job: ComputeJob, channel: Any, slot: int | None) -> ResultEnvelope:
    """``run_job`` unter einem Event-Loop, dessen SIGTERM den Task abbricht.

    So bekommt der REQUEST-Teardown die Gnadenfrist aus KTD3, bevor der
    Loop-Prozess SIGKILL schickt. Nur fuer das frische Kind (``child_once``);
    das wiederverwendete Kind haelt EINEN Loop ueber alle Jobs (``_serve_jobs``).
    """

    async def _main() -> ResultEnvelope:
        loop = asyncio.get_running_loop()
        task = asyncio.ensure_future(run_job(job, channel, slot=slot))
        try:
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
        except NotImplementedError, RuntimeError:  # pragma: no cover - kein Unix-Loop
            pass
        try:
            return await task
        finally:
            try:
                loop.remove_signal_handler(signal.SIGTERM)
            except NotImplementedError, RuntimeError:  # pragma: no cover
                pass

    return asyncio.run(_main())


def _child_logging_settings(job: ComputeJob) -> Any:
    """Die im Loop-Prozess aufgeloesten Einstellungen aus dem Job (KTD7)."""
    from swarmq.logging_settings import LoggingSettings

    return LoggingSettings(
        level=job.log_level,
        level_explicit=job.log_level_explicit,
        format="json" if job.log_json else "human",
        log_config=job.log_config,
        log_config_json=job.log_config_json,
        configure=job.configure_logging,
        logger_levels=job.logger_levels,
        queue_size=job.log_queue_size,
        drain_timeout=job.log_drain_timeout,
    )


def _configure_child_logging(job: ComputeJob, channel: Any) -> None:
    """Einstiegspunkt Compute-Kind (KTD4): genau einmal je Kind einrichten.

    Hat die App beim Vorladen das Logging eingerichtet, gewinnt sie
    (KTD14); das Kind meldet das ueber den Rueckkanal, und der Pool schreibt
    ``logging_reconfigured_by_app`` einmal je Pool.
    """
    from swarmq.logging import configure_child_logging

    for report in configure_child_logging(_child_logging_settings(job)):
        channel.send(report)


def _send_result(channel: Any, envelope: ResultEnvelope) -> None:
    """Erst die Log-Queue leeren, dann das Ergebnis senden (KTD5, KTD14):
    die Logzeilen des Tasks stehen vor dem Ergebnis im Strom."""
    from swarmq.logging import shutdown_logging

    shutdown_logging()
    channel.send(("result", envelope))


def _shutdown_child_logging() -> None:
    """Logging am Kind-Ende abschliessen (R15, KTD14).

    Das Forkserver-Kind ruft nach dem Fork ``atexit._clear()`` auf: Die beim
    Vorladen registrierten atexit-Funktionen -- ``shutdown_logging`` und
    ``logging.shutdown`` -- laufen dort nie, also hier ausdruecklich.
    """
    from swarmq.logging import shutdown_logging

    shutdown_logging()
    logging.shutdown()
    # Das Kind gehoert swarmq (Plan 2026-10-06-1811, KTD3): Haengt nach der
    # Frist noch ein Log-Thread, endet es hart mit seinem Exit-Code; die
    # Ausnahme, an der es endet, geht vorher auf fd 2 (wie ``console_main``).
    exc = sys.exception()
    exit_if_log_thread_stuck(exit_code_for(exc), exc)


def child_once(job: ComputeJob, channel: Any, slot: int | None = None) -> None:
    """Frisches Kind je Task (R6): binden, Ready melden, rechnen, Envelope, Ende."""
    pdeath = bind_child_process()
    channel.send(("ready", slot, os.getpid(), pdeath))
    _configure_child_logging(job, channel)
    try:
        try:
            envelope = _run_job_with_sigterm(job, channel, slot)
        except asyncio.CancelledError:
            return
        _send_result(channel, envelope)
    finally:
        _shutdown_child_logging()


def child_loop(slot: int, job_conn: Any, channel: Any) -> None:
    """Wiederverwendetes Slot-Kind (R6): Jobs bis zum Ende-Signal ``None``.

    EIN Event-Loop fuer alle Jobs: APP-Scope-Ressourcen des DI-Containers
    (Sessions, Pools, Locks) sind an den Loop gebunden und muessen ueber
    Jobgrenzen hinweg gueltig bleiben.
    """
    pdeath = bind_child_process()
    channel.send(("ready", slot, os.getpid(), pdeath))
    asyncio.run(_serve_jobs(slot, job_conn, channel))


async def _recv_job(job_conn: Any, stop: asyncio.Event) -> ComputeJob | None:
    """Naechsten Job lesen; ``None`` bei Ende-Signal, EOF oder Stop (SIGTERM im Leerlauf)."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()

    def _readable() -> None:
        loop.remove_reader(job_conn.fileno())
        if fut.done():
            return
        try:
            fut.set_result(job_conn.recv())
        except Exception:  # noqa: BLE001 - offenes Future hinge sonst fuer immer
            # EOF/OSError: der Loop-Prozess ist weg (R27, Rueckfallebene). Alles andere
            # (unlesbarer Rahmen): ebenfalls beenden — ein offenes Future hinge fuer immer.
            fut.set_result(None)

    loop.add_reader(job_conn.fileno(), _readable)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({fut, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopper.cancel()
        with contextlib.suppress(Exception):
            loop.remove_reader(job_conn.fileno())
    if not fut.done():
        fut.cancel()
        return None
    job = fut.result()
    return job if isinstance(job, ComputeJob) else None


async def _serve_jobs(slot: int, job_conn: Any, channel: Any) -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    current: list[asyncio.Task[ResultEnvelope] | None] = [None]

    def _on_sigterm() -> None:
        task = current[0]
        if task is not None and not task.done():
            task.cancel()  # laufender Job: Cancel, REQUEST-Teardown in der Gnadenfrist (KTD3)
        else:
            stop.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    except NotImplementedError, RuntimeError:  # pragma: no cover - kein Unix-Loop
        pass
    configured = False
    try:
        while True:
            job = await _recv_job(job_conn, stop)
            if job is None:
                break
            if not configured:
                _configure_child_logging(job, channel)
                configured = True
            task = asyncio.ensure_future(run_job(job, channel, slot=slot))
            current[0] = task
            try:
                envelope = await task
            except asyncio.CancelledError:
                break
            finally:
                current[0] = None
            _send_result(channel, envelope)
    finally:
        await _close_app_container()
        _shutdown_child_logging()


async def _close_app_container() -> None:
    """APP-Scope des DI-Containers am Kind-Ende schliessen (best effort)."""
    try:
        from swarmq.dishka import _MISSING, get_registered_container
    except ImportError:  # pragma: no cover - ohne dishka
        return
    container: Any = get_registered_container()
    if container is _MISSING:
        return
    with contextlib.suppress(Exception):
        await container.close()
