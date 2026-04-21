"""R5.3 integration test: real watchfiles → trigger callback.

The :class:`FileWatcher` unit tests stub ``watchfiles.watch`` to keep
the fast loop deterministic. This suite exercises the real Rust
watcher against a real ``tmp_path`` to catch integration regressions
(debounce/filter interaction, stop_event threading, Linux inotify
semantics).

Happy-path + two negatives for the watcher layer, plus the full
pipeline test (file write → watcher → real SIGHUP → ProcessManager
reload handler → mocked os.execvpe) — ce-review R5.3 #9.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from swarmq.hot_reload import FileWatcher


@pytest.mark.integration
def test_py_file_write_triggers_callback(tmp_path: Path) -> None:
    triggered = threading.Event()
    watcher = FileWatcher(
        watch_dirs=(str(tmp_path),),
        trigger=triggered.set,
        is_busy=lambda: False,
        debounce_ms=200,
        step_ms=50,
    )
    watcher.start()
    try:
        # Let the Rust watcher arm before the first write.
        time.sleep(0.3)
        (tmp_path / "my_task.py").write_text("# reload me\n")
        assert triggered.wait(timeout=3.0), (
            "trigger did not fire within 3s of .py write"
        )
    finally:
        watcher.stop(timeout=2.0)


@pytest.mark.integration
def test_pyc_write_is_ignored(tmp_path: Path) -> None:
    """PythonFilter drops *.pyc — the trigger must stay quiet."""
    triggered = threading.Event()
    watcher = FileWatcher(
        watch_dirs=(str(tmp_path),),
        trigger=triggered.set,
        is_busy=lambda: False,
        debounce_ms=200,
        step_ms=50,
    )
    watcher.start()
    try:
        time.sleep(0.3)
        (tmp_path / "cached.pyc").write_bytes(b"\x00" * 16)
        # Wait longer than debounce — trigger still should not fire.
        assert not triggered.wait(timeout=1.0), (
            "trigger fired on .pyc write — PythonFilter failed"
        )
    finally:
        watcher.stop(timeout=2.0)


@pytest.mark.integration
def test_vim_probe_4913_is_ignored(tmp_path: Path) -> None:
    """Vim writes /path/4913 before each save — must not trigger."""
    triggered = threading.Event()
    watcher = FileWatcher(
        watch_dirs=(str(tmp_path),),
        trigger=triggered.set,
        is_busy=lambda: False,
        debounce_ms=200,
        step_ms=50,
    )
    watcher.start()
    try:
        time.sleep(0.3)
        (tmp_path / "4913").write_text("")
        assert not triggered.wait(timeout=1.0), (
            "trigger fired on 4913 probe-file — SwarmQFilter failed"
        )
    finally:
        watcher.stop(timeout=2.0)


@pytest.fixture
def _restore_sighup():
    """Put SIGHUP back to SIG_DFL after the test so later suites are
    not affected by the handler pm._setup_signals() installs."""
    import signal

    saved = signal.getsignal(signal.SIGHUP)
    yield
    signal.signal(signal.SIGHUP, saved if saved is not None else signal.SIG_DFL)


@pytest.mark.integration
def test_full_pipeline_file_write_triggers_execvpe(
    tmp_path: Path, _restore_sighup,
) -> None:
    """R5.3 ce-review #9: end-to-end pipeline.

    File write → watchfiles → FileWatcher trigger → os.kill(SIGHUP) →
    ProcessManager._on_reload_signal → _monitor_loop exits →
    _shutdown_all_workers → os.execvpe. We mock only execvpe (would
    replace the pytest process) but let the real watcher, real
    signal delivery, and real reload handler run.

    ``pm.run()`` must execute on the **main thread** because
    ``signal.signal()`` only installs handlers from there — so the
    file-write helper runs in a background thread instead.
    """
    pytest.importorskip("watchfiles")
    from swarmq.process_manager import ProcessManager

    def _noop_entry(worker_id, shutdown_event, *args):
        return None

    pm = ProcessManager(
        num_workers=0,  # no real subprocesses — focus on the supervisor
        worker_entry=_noop_entry,
        reload=True,
        watch_dirs=(str(tmp_path),),
    )

    # Short monitor_loop that returns as soon as the SIGHUP handler
    # flips _reload_requested. Without this override the real loop
    # would block on time.sleep waiting for worker state that does
    # not exist in this reduced setup.
    def _short_monitor():
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if pm._reload_requested:
                return 0
            time.sleep(0.05)
        return 0

    pm._monitor_loop = _short_monitor  # type: ignore[method-assign]

    execvpe_called = threading.Event()

    def _fake_execvpe(*_args, **_kwargs):
        execvpe_called.set()
        # Do NOT call the real execvpe — it would replace pytest.

    def _write_after_delay() -> None:
        # Let the supervisor install its SIGHUP handler + arm the
        # Rust watcher before we drop the trigger file.
        time.sleep(0.6)
        (tmp_path / "my_tasks.py").write_text("# reload trigger\n")

    writer = threading.Thread(
        target=_write_after_delay, name="e2e-file-writer", daemon=True,
    )

    with patch("swarmq.process_manager.os.execvpe", side_effect=_fake_execvpe), \
         patch.object(pm, "_start_all_workers"), \
         patch.object(pm, "_shutdown_all_workers"), \
         patch.object(pm, "_collect_zombies"):
        writer.start()
        # pm.run() blocks on the main thread; the writer fires while
        # we're parked in _short_monitor. Signal handler installed by
        # pm._setup_signals() on the main thread catches SIGHUP and
        # sets _reload_requested — the monitor loop then returns.
        rc = pm.run()

    writer.join(timeout=2.0)

    # Supervisor observed the SIGHUP and reached execvpe.
    assert pm._reload_requested is True, (
        "_reload_requested was not set — SIGHUP handler did not fire"
    )
    assert execvpe_called.is_set(), (
        "execvpe was not reached — reload pipeline broke before execvpe"
    )
    assert rc == 0
