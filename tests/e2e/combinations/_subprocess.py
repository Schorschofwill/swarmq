"""Subprocess-worker helpers for combinations tests.

Lifted from ``tests/e2e/test_worker_shutdown_load.py`` (Plan 2026-05-26-002 Unit 2)
and parametrised so multiple test modules can spawn workers against either
the raw broker/backend or the Toxiproxy-proxied variants.

The original helpers in ``test_worker_shutdown_load.py`` are kept as thin
wrappers that delegate here, so the existing tests stay green.
"""

from __future__ import annotations

import importlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


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
    )


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


def kill_subprocess(proc: subprocess.Popen[str]) -> None:
    """Defensive teardown — caller already signaled the worker, this
    ensures no orphan if the test bailed out mid-flow."""
    if proc.poll() is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


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
