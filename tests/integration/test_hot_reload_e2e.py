"""R5.3 integration test: real watchfiles → trigger callback.

The :class:`FileWatcher` unit tests stub ``watchfiles.watch`` to keep
the fast loop deterministic. This suite exercises the real Rust
watcher against a real ``tmp_path`` to catch integration regressions
(debounce/filter interaction, stop_event threading, Linux inotify
semantics).

Kept lean — one happy-path assertion plus two negatives. The full
R4.6 reload pipeline (SIGHUP → execvp → new supervisor) is covered
by the existing multi-process E2E suite.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

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
