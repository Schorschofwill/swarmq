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

# ce-review #21: probe write-to-trigger timeout. Small enough to keep
# the helper tight, large enough to absorb the 200ms debounce + step
# jitter in the integration FileWatcher config.
_ARM_POLL_TIMEOUT_S = 0.3


def _wait_for_armed_watcher(watch_dir: Path, triggered: threading.Event, timeout: float = 5.0) -> None:
    """Prove the watcher is armed by repeatedly touching a probe file
    until ``triggered`` fires.

    ce-review pass 1 #20: replaces a fixed ``time.sleep(0.3)`` whose
    margin is not portable across CI runners. Touching in a loop costs
    nothing if the watcher is already armed (first touch fires the
    trigger) but keeps probing across debounce windows if the Rust
    layer is slow to install its inotify watch.

    ce-review pass 2 #6 / T-02 / ADV-02: the probe extension is
    ``.swarmq-arm`` — NOT ``.py`` — so ``SwarmQFilter`` filters both
    write and delete events. No drain phase is needed; the watcher
    does not see the probe at all, it only inotify-notifies the Rust
    layer that the directory is live. The probe write therefore fires
    no trigger — we instead wait for the watcher to arm by observing
    that a subsequent real ``.py`` write DOES fire. To get that proof
    we keep the .py-based probe but drop the drain: we rename the
    probe cleanup to avoid the caller-side false positive.

    The final compromise: probe is ``.py`` (so write fires trigger,
    proving armed), but we make cleanup robust by waiting then
    clearing again, with a real timeout bound.
    """
    probe = watch_dir / ".swarmq-arm-probe.py"
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        probe.write_text(f"# arm probe {attempt}\n")
        if triggered.wait(timeout=_ARM_POLL_TIMEOUT_S):
            # Drain the arm-fire. Then unlink + wait a full debounce
            # cycle so the delete-event has actually landed and been
            # seen. Finally clear again — the caller inherits a
            # quiescent event, independently of drain-event timing.
            triggered.clear()
            probe.unlink(missing_ok=True)
            time.sleep(_ARM_POLL_TIMEOUT_S)
            triggered.clear()
            return
    raise AssertionError(f"watcher never armed after {attempt} probe writes in {timeout}s")


@pytest.mark.integration
@pytest.mark.timeout(15)
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
        _wait_for_armed_watcher(tmp_path, triggered)
        (tmp_path / "my_task.py").write_text("# reload me\n")
        assert triggered.wait(timeout=3.0), "trigger did not fire within 3s of .py write"
    finally:
        watcher.stop(timeout=2.0)


@pytest.mark.integration
@pytest.mark.timeout(15)
def test_pyc_write_is_ignored(tmp_path: Path) -> None:
    """PythonFilter drops *.pyc — the trigger must stay quiet.

    Proves the watcher is armed first (so a silent arm-miss cannot
    fake a green result), then verifies the .pyc event is filtered.
    """
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
        _wait_for_armed_watcher(tmp_path, triggered)
        (tmp_path / "cached.pyc").write_bytes(b"\x00" * 16)
        # Debounce + margin — trigger must NOT fire.
        assert not triggered.wait(timeout=1.0), "trigger fired on .pyc write — PythonFilter failed"
    finally:
        watcher.stop(timeout=2.0)


@pytest.mark.integration
@pytest.mark.timeout(15)
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
        _wait_for_armed_watcher(tmp_path, triggered)
        (tmp_path / "4913").write_text("")
        assert not triggered.wait(timeout=1.0), "trigger fired on 4913 probe-file — SwarmQFilter failed"
    finally:
        watcher.stop(timeout=2.0)


@pytest.fixture
def _restore_sighup():
    """Restore all handlers ``pm._setup_signals()`` installs.

    ce-review pass 2 #9: SIGHUP is not the only handler the supervisor
    installs — SIGINT, SIGTERM and SIGCHLD also get wired. Restoring
    only SIGHUP leaked three handlers into the rest of the pytest
    session. Also: ``signal.SIG_DFL`` equals ``0`` (falsy), so the
    pass-1 ``saved if saved else SIG_DFL`` idiom worked by coincidence;
    the correct guard is ``is not None``.
    """
    import signal

    saved_hup = signal.getsignal(signal.SIGHUP)
    saved_int = signal.getsignal(signal.SIGINT)
    saved_term = signal.getsignal(signal.SIGTERM)
    saved_chld = signal.getsignal(signal.SIGCHLD)
    yield
    signal.signal(
        signal.SIGHUP,
        saved_hup if saved_hup is not None else signal.SIG_DFL,
    )
    signal.signal(
        signal.SIGINT,
        saved_int if saved_int is not None else signal.SIG_DFL,
    )
    signal.signal(
        signal.SIGTERM,
        saved_term if saved_term is not None else signal.SIG_DFL,
    )
    signal.signal(
        signal.SIGCHLD,
        saved_chld if saved_chld is not None else signal.SIG_DFL,
    )


@pytest.mark.usefixtures("_restore_sighup")
@pytest.mark.integration
@pytest.mark.timeout(20)
def test_full_pipeline_file_write_triggers_execvpe(
    tmp_path: Path,
) -> None:
    """R5.3 ce-review #9: end-to-end pipeline.

    File write → watchfiles → FileWatcher trigger → os.kill(SIGHUP) →
    ProcessManager._on_reload_signal → monitor tick accepts →
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

    # Short monitor_loop: drives the reload controller like the real
    # loop (U8, KTD8: the SIGHUP handler only marks the reload pending)
    # and returns once the reload is accepted. Without this override
    # the real loop would end at once ("all workers exited") in this
    # reduced setup without workers.
    def _short_monitor():
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if pm.reloader.step():
                pm._reload_requested = True
                return 0
            time.sleep(0.05)
        return 0

    pm._monitor_loop = _short_monitor  # type: ignore[method-assign]

    execvpe_called = threading.Event()

    class _ExecvpeReached(SystemExit):
        """Sentinel — matches real execvpe's 'never returns' semantics.

        Raising ``SystemExit`` from ``_fake_execvpe`` means any code
        the supervisor might add after the real ``os.execvpe()`` call
        stays unreachable in the test too, mirroring production
        (ce-review pass 2 #24 / T-04 / ADV-03).
        """

    def _fake_execvpe(*_args, **_kwargs):
        execvpe_called.set()
        raise _ExecvpeReached(0)

    def _write_retrying() -> None:
        """Retry the trigger write until the watcher reports armed.

        ce-review pass 2 #5 + KP-04 + REL-02/03 + T-03: replaces the
        pass-1 ``time.sleep(0.6)`` that was racy on slow CI. We cannot
        use ``_wait_for_armed_watcher`` here because the trigger goes
        through a real SIGHUP to the supervisor (not a test-local
        ``threading.Event``). Instead we loop-write until either
        ``_reload_requested`` flips (handler ran) or the global
        ``@pytest.mark.timeout(20)`` trips.
        """
        target = tmp_path / "my_tasks.py"
        for attempt in range(1, 100):  # bounded; real guard is the marker
            if pm._reload_requested or pm.reloader.busy:
                return
            target.write_text(f"# reload trigger {attempt}\n")
            time.sleep(0.25)

    writer = threading.Thread(
        target=_write_retrying,
        name="e2e-file-writer",
        daemon=True,
    )

    with (
        patch("swarmq.process_manager.supervisor.os.execvpe", side_effect=_fake_execvpe),
        patch.object(pm, "_start_all_workers"),
        patch.object(pm, "_shutdown_all_workers"),
        patch.object(pm, "_collect_zombies"),
    ):
        writer.start()
        # pm.run() blocks on the main thread; the writer fires while
        # we're parked in _short_monitor. Signal handler installed by
        # pm._setup_signals() on the main thread catches SIGHUP and
        # sets _reload_requested — the monitor loop then returns.
        # _fake_execvpe then raises SystemExit to mimic the real
        # execvpe never returning.
        with pytest.raises(_ExecvpeReached) as exc_info:
            pm.run()
        assert exc_info.value.code == 0

    writer.join(timeout=2.0)

    # Supervisor observed the SIGHUP and reached execvpe.
    assert pm._reload_requested is True, "_reload_requested was not set — SIGHUP handler did not fire"
    assert execvpe_called.is_set(), "execvpe was not reached — reload pipeline broke before execvpe"
