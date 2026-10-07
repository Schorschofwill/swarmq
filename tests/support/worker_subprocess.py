"""Subprocess-worker helpers for e2e, combinations and chaos tests.

Lifted from ``tests/e2e/test_worker_shutdown_load.py`` (Plan 2026-05-26-002 Unit 2)
and parametrised so multiple test modules can spawn workers against either
the raw broker/backend or the Toxiproxy-proxied variants.

The original helpers in ``test_worker_shutdown_load.py`` are kept as thin
wrappers that delegate here, so the existing tests stay green. Moved from
``tests/e2e/combinations/_subprocess.py`` to ``tests/support`` (0.9.0): the
chaos tests need the crash helpers too.

Process tree since R20: ``swarmq worker`` is a supervisor; the worker loop
(consumer, scheduler, locks) runs in a ``multiprocessing`` child. Two
consequences for the helpers here:

- Teardown (:func:`kill_subprocess`) goes through
  :func:`tests.support.cli_process.terminate_worker_process` and then reaps
  the whole process group. Workers start with ``start_new_session=True`` so
  a stray worker child cannot outlive the test and keep the scheduler leader
  lock alive for the next one (MR !56, note e2e-scheduler-leader-flake).
- A crash (:func:`crash_worker`) must SIGKILL the worker child, not just
  the supervisor: a supervisor-only SIGKILL makes the child shut down
  gracefully via its parent-death signal, releasing its locks — no TTL path
  is exercised at all.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil

from tests.support.cli_process import terminate_worker_process

_REPO_ROOT = Path(__file__).resolve().parents[2]


def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if a TCP connect to ``(host, port)`` succeeds within ``timeout``."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def spawn_worker(
    queue_name: str,
    *,
    fixture_module: str,
    broker_url: str,
    backend_url: str,
    concurrency: int = 4,
    env_extra: dict[str, str] | None = None,
    extra_args: list[str] | None = None,
) -> subprocess.Popen[str]:
    """Launch ``swarmq.cli worker`` as a subprocess.

    Returns the ``Popen`` object with stdout as text so callers can poll for
    log lines. The worker listens on ``queue_name`` and imports
    ``fixture_module`` to register task classes.

    Parameters
    ----------
    queue_name :
        Queue the worker consumes from. Also exposed to the subprocess as
        ``CLI_TEST_QUEUE`` so fixture modules can name the queue
        dynamically.
    fixture_module :
        Dotted module path passed to ``--module``. The module's top-level
        code must register the task classes via SwarmQ's ``Task`` subclass
        machinery.
    broker_url, backend_url :
        Connection URLs for RabbitMQ and Valkey. Pass the Toxiproxy-proxied
        URLs to inject failures.
    concurrency :
        ``--concurrency`` flag value.
    env_extra :
        Additional env vars to set in the subprocess.
    extra_args :
        Additional CLI args appended after the standard set.
    """
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = broker_url
    env["SWARMQ_BACKEND_URL"] = backend_url
    env["CLI_TEST_QUEUE"] = queue_name
    env["PYTHONUNBUFFERED"] = "1"
    if env_extra:
        env.update(env_extra)

    cmd: list[str] = [
        sys.executable,
        "-m", "swarmq.cli",
        "worker",
        "--module", fixture_module,
        "--queues", queue_name,
        "--concurrency", str(concurrency),
        "--log-format", "json",
    ]
    if extra_args:
        cmd.extend(extra_args)

    return subprocess.Popen(
        cmd,
        cwd=str(_REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def worker_children(proc: subprocess.Popen[str]) -> list[psutil.Process]:
    """The worker-loop children of the supervisor ``proc`` (``multiprocessing``
    spawn children; the resource tracker is not one of them)."""
    try:
        children = psutil.Process(proc.pid).children(recursive=True)
    except psutil.NoSuchProcess:
        return []
    result: list[psutil.Process] = []
    for child in children:
        with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            if any("spawn_main" in part for part in child.cmdline()):
                result.append(child)
    return result


def wait_for_worker_children(
    proc: subprocess.Popen[str], *, count: int = 1, timeout: float = 10.0
) -> list[psutil.Process]:
    """Block until the supervisor runs ``count`` worker children."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        children = worker_children(proc)
        if len(children) >= count:
            return children
        if proc.poll() is not None:
            raise RuntimeError(f"supervisor exited (rc={proc.returncode}) before spawning workers")
        time.sleep(0.05)
    raise TimeoutError(f"supervisor did not spawn {count} worker child(ren) in {timeout}s")


def crash_worker(proc: subprocess.Popen[str], *, timeout: float = 10.0) -> list[int]:
    """Hard crash: SIGKILL the worker children AND the supervisor.

    No graceful path runs anywhere — no parent-death drain, no lock release,
    no restart by the supervisor. Locks the child held stay behind until
    their TTL expires; in-flight messages go back to the broker when the
    connection drops. Returns the PIDs of the killed worker children.
    """
    children = wait_for_worker_children(proc, timeout=timeout)
    for child in children:
        with contextlib.suppress(psutil.NoSuchProcess):
            child.kill()
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)
    with contextlib.suppress(ProcessLookupError):
        proc.kill()
    proc.wait(timeout=timeout)
    _, alive = psutil.wait_procs(children, timeout=timeout)
    if alive:
        raise RuntimeError(f"worker children survived SIGKILL: {[p.pid for p in alive]}")
    return [child.pid for child in children]


def kill_worker_child(proc: subprocess.Popen[str], *, timeout: float = 10.0) -> int:
    """SIGKILL only the worker child; the supervisor stays up (and restarts
    the slot). Returns the killed child's PID."""
    (child, *_) = wait_for_worker_children(proc, timeout=timeout)
    child.kill()
    child.wait(timeout=timeout)
    return int(child.pid)


def _process_group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def wait_for_worker_started(
    proc: subprocess.Popen[str],
    *,
    timeout: float = 30.0,
    marker: str = "worker_started",
) -> None:
    """Block until the worker emits the start ``marker`` on stdout.

    Raises ``TimeoutError`` if the marker doesn't appear in ``timeout``
    seconds, or ``RuntimeError`` if the worker exits before emitting it.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"worker exited before starting; rc={proc.returncode}"
                )
            continue
        if marker in line:
            return
    raise TimeoutError(f"worker did not emit {marker!r} in time")


def kill_subprocess(proc: subprocess.Popen[str], *, grace_s: float = 20.0) -> None:
    """Teardown that leaves nothing behind.

    A live supervisor gets SIGTERM and ``grace_s`` to drain
    (:func:`terminate_worker_process`, SIGKILL for the group after that).
    Then the helper waits for the rest of the process group — worker children
    of an already-dead supervisor drain via their parent-death signal and
    release the scheduler lock — and SIGKILLs whatever is still there after
    ``grace_s``. Requires ``start_new_session=True`` (``spawn_worker`` does).
    """
    terminate_worker_process(proc, grace_s=grace_s)
    deadline = time.monotonic() + grace_s
    while _process_group_alive(proc.pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)


# Short scheduler-leader lock for crash tests: the TTL must be at least 2x the
# heartbeat (config validation), and the leader renews every heartbeat.
SHORT_SCHEDULER_LOCK_TTL_S = 3.0
SHORT_SCHEDULER_LOCK_ENV = {
    "SWARMQ_SCHEDULER_LOCK_TTL": str(SHORT_SCHEDULER_LOCK_TTL_S),
    "SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL": "1",
}


def lock_owner_pid(owner: str | None) -> int | None:
    """PID part of a scheduler owner token ``{host}:{pid}:{uuid}``."""
    if not owner:
        return None
    parts = owner.rsplit(":", 2)
    if len(parts) != 3:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


async def scheduler_lock_owner(valkey_url: str) -> str | None:
    """Current value of the scheduler-leader lock (the owner token)."""
    import valkey.asyncio as aiovalkey

    from swarmq.locking import SCHEDULER_LOCK_KEY

    client = aiovalkey.Valkey.from_url(valkey_url)
    try:
        raw = await client.get(SCHEDULER_LOCK_KEY)
    finally:
        await client.aclose()
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else str(raw)


async def wait_for_scheduler_leader(valkey_url: str, pid: int, *, timeout: float = 15.0) -> float:
    """Block until the worker process ``pid`` holds the scheduler-leader lock;
    returns the ``time.monotonic()`` of the observation."""
    import asyncio

    deadline = time.monotonic() + timeout
    owner: str | None = None
    while time.monotonic() < deadline:
        owner = await scheduler_lock_owner(valkey_url)
        if lock_owner_pid(owner) == pid:
            return time.monotonic()
        await asyncio.sleep(0.05)
    raise TimeoutError(f"pid {pid} did not become scheduler leader in {timeout}s (owner={owner!r})")


def ensure_fixture_registered(fixture_module: str, *, queue: str) -> None:
    """Register the fixture task classes in *this* process.

    ``Task._registered_tasks`` is cleared between tests by ``_cleanup_tasks``,
    so re-import via ``importlib.reload`` is needed — a plain ``import``
    hits the module cache and skips ``__init_subclass__``.

    The ``CLI_TEST_QUEUE`` env var lets fixture modules read the queue name
    at import time.
    """
    os.environ["CLI_TEST_QUEUE"] = queue
    if fixture_module in sys.modules:
        importlib.reload(sys.modules[fixture_module])
    else:
        importlib.import_module(fixture_module)
