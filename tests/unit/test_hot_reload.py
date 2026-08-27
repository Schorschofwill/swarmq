"""R5.3 unit tests for the hot-reload watcher primitives."""

from __future__ import annotations

import sys
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from swarmq.exceptions import ConfigurationError
from swarmq.hot_reload import (
    FileWatcher,
    SwarmQFilter,
    check_watchfiles_available,
    resolve_watch_dirs,
)

# ---------------------------------------------------------------------------
# Unit 4: SwarmQFilter
# ---------------------------------------------------------------------------


class TestSwarmQFilter:
    """Public filter class — PythonFilter plus the editor-noise we saw in the wild."""

    def _f(self):
        return SwarmQFilter()

    def _change(self):
        from watchfiles import Change
        return Change.modified

    def test_plain_py_file_accepted(self):
        assert self._f()(self._change(), "/x/tasks.py") is True

    def test_pyc_rejected(self):
        # PythonFilter built-in: *.pyc is ignored.
        assert self._f()(self._change(), "/x/tasks.pyc") is False

    def test_pycache_dir_rejected(self):
        assert self._f()(
            self._change(), "/x/__pycache__/tasks.cpython-314.pyc"
        ) is False

    def test_vim_probe_4913_rejected(self):
        # Vim writes a probe file named "4913" before saving — no extension.
        assert self._f()(self._change(), "/x/4913") is False

    def test_tmp_suffix_rejected(self):
        assert self._f()(self._change(), "/x/tasks.py.tmp") is False

    def test_bak_suffix_rejected(self):
        assert self._f()(self._change(), "/x/tasks.py.bak") is False

    def test_tilde_backup_rejected(self):
        # PythonFilter built-in.
        assert self._f()(self._change(), "/x/tasks.py~") is False

    def test_emacs_lock_rejected(self):
        assert self._f()(self._change(), "/x/.#something") is False

    def test_git_head_rejected(self):
        assert self._f()(self._change(), "/x/.git/HEAD") is False

    def test_bak_py_passes_through(self):
        """R5.3 ce-review #21: ``tasks.bak.py`` ends in ``.py`` — the
        SwarmQFilter only rejects ``*.bak`` at path-end, not infix.
        This is a legitimate Python filename and must trigger a reload."""
        assert self._f()(self._change(), "/x/tasks.bak.py") is True


# ---------------------------------------------------------------------------
# Unit 5: resolve_watch_dirs
# ---------------------------------------------------------------------------


class TestResolveWatchDirs:
    def test_explicit_dirs_win(self):
        # Non-empty explicit list is returned verbatim (the operator's choice).
        out = resolve_watch_dirs("json", ("/tmp",))
        assert out == ("/tmp",)

    def test_explicit_multiple_preserves_order(self):
        out = resolve_watch_dirs("json", ("/tmp", "/var"))
        assert out == ("/tmp", "/var")

    def test_auto_resolves_standard_library_module(self):
        import json as _json
        from pathlib import Path

        expected = str(Path(_json.__file__).parent)
        out = resolve_watch_dirs("json", ())
        assert out == (expected,)

    def test_frozen_module_rejected(self):
        # `os` is a frozen module since Python 3.11+ — parent-dir would be ".".
        with pytest.raises(ConfigurationError, match="frozen|built-in"):
            resolve_watch_dirs("os", ())

    def test_nonexistent_module_raises(self):
        with pytest.raises(ConfigurationError, match="cannot auto-detect|cannot resolve"):
            resolve_watch_dirs("definitely_not_a_real_module_xyz", ())

    def test_explicit_spec_skips_find_spec(self):
        """R5.3 ce-review #7: caller-supplied spec must bypass
        importlib.util.find_spec entirely so the module's __init__.py
        is not re-executed in the supervisor."""
        import importlib.util
        import json as _json
        from pathlib import Path

        prebuilt = importlib.util.find_spec("json")
        assert prebuilt is not None

        with patch("swarmq.hot_reload.importlib.util.find_spec") as mock_find:
            out = resolve_watch_dirs("json", (), spec=prebuilt)
            mock_find.assert_not_called()

        assert out == (str(Path(_json.__file__).parent),)

    def test_missing_spec_falls_back_to_find_spec(self):
        """Regression: programmatic callers without spec=kwarg still work."""
        import json as _json
        from pathlib import Path

        out = resolve_watch_dirs("json", ())  # no spec= — legacy path
        assert out == (str(Path(_json.__file__).parent),)

    def test_namespace_package_hint(self, tmp_path, monkeypatch):
        # Create a namespace-package root (no __init__.py) on sys.path.
        ns_root = tmp_path / "ns_root"
        ns_root.mkdir()
        pkg_dir = ns_root / "swarmq_ns_test_pkg"
        pkg_dir.mkdir()
        # No __init__.py — makes it a namespace package.
        monkeypatch.syspath_prepend(str(ns_root))
        try:
            with pytest.raises(ConfigurationError, match="namespace package|missing origin"):
                resolve_watch_dirs("swarmq_ns_test_pkg", ())
        finally:
            sys.modules.pop("swarmq_ns_test_pkg", None)


# ---------------------------------------------------------------------------
# Unit 7: check_watchfiles_available
# ---------------------------------------------------------------------------


class TestCheckWatchfilesAvailable:
    def test_present_is_noop(self):
        # watchfiles is installed via [reload] extra in the dev env.
        check_watchfiles_available()  # no raise

    def test_missing_raises_configuration_error(self):
        # Simulate missing dep by inserting None into sys.modules — the
        # import statement then raises ImportError on lookup.
        with patch.dict(sys.modules, {"watchfiles": None}), pytest.raises(
            ConfigurationError,
            match=r"pip install 'swarmq\[reload\]'",
        ):
            check_watchfiles_available()


# ---------------------------------------------------------------------------
# Unit 7: FileWatcher lifecycle (no real FS)
# ---------------------------------------------------------------------------


class _FakeWatch:
    """Stand-in for watchfiles.watch — yields pre-seeded change sets, then stops."""

    def __init__(self, batches, raise_on_iter=False):
        self._batches = list(batches)
        self._raise_on_iter = raise_on_iter

    def __call__(self, *paths, stop_event, **_kwargs):
        if self._raise_on_iter:
            raise RuntimeError("fake watcher crashed")

        def _gen():
            for b in self._batches:
                if stop_event.is_set():
                    return
                yield b

        return _gen()


class TestFileWatcherLifecycle:
    def _changes(self):
        from watchfiles import Change
        return {(Change.modified, "/x/tasks.py")}

    def test_construct_requires_at_least_one_dir(self):
        with pytest.raises(ValueError, match="at least one"):
            FileWatcher(watch_dirs=(), trigger=lambda: None, is_busy=lambda: False)

    def test_trigger_fires_on_change(self):
        trigger = MagicMock()
        fake = _FakeWatch(batches=[self._changes()])
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=trigger,
            is_busy=lambda: False,
            debounce_ms=50,
        )
        with patch("watchfiles.watch", fake):
            w.start()
            # Wait for watcher exit via the public readiness signal.
            assert w.wait_until_stopped(timeout=2.0)
        assert trigger.call_count == 1
        w.stop()

    def test_trigger_suppressed_when_busy(self):
        trigger = MagicMock()
        fake = _FakeWatch(batches=[self._changes(), self._changes()])
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=trigger,
            is_busy=lambda: True,
            debounce_ms=50,
        )
        with patch("watchfiles.watch", fake):
            w.start()
            assert w.wait_until_stopped(timeout=2.0)
        assert trigger.call_count == 0
        w.stop()

    def test_trigger_exception_does_not_kill_thread(self, caplog):
        trigger = MagicMock(side_effect=RuntimeError("boom"))
        fake = _FakeWatch(batches=[self._changes(), self._changes()])
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=trigger,
            is_busy=lambda: False,
            debounce_ms=50,
        )
        with patch("watchfiles.watch", fake):
            w.start()
            assert w.wait_until_stopped(timeout=2.0)
        # Both batches must have fired the trigger — the first failure
        # must not have prevented the second call.
        assert trigger.call_count == 2
        w.stop()

    def test_watch_crash_logged_as_warning(self, caplog):
        import logging

        caplog.set_level(logging.WARNING)
        trigger = MagicMock()
        fake = _FakeWatch(batches=[], raise_on_iter=True)
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=trigger,
            is_busy=lambda: False,
        )
        with patch("watchfiles.watch", fake):
            w.start()
            assert w.wait_until_stopped(timeout=2.0)
        # Supervisor must still be alive — crash contained.
        assert trigger.call_count == 0
        assert w.is_running() is False
        w.stop()

    def test_double_start_rejected(self):
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=lambda: None,
            is_busy=lambda: False,
        )
        fake = _FakeWatch(batches=[])
        with patch("watchfiles.watch", fake):
            w.start()
            with pytest.raises(RuntimeError, match="already started"):
                w.start()
            assert w.wait_until_stopped(timeout=1.0)
        w.stop()

    def test_stop_sets_event_and_joins(self):
        event_seen = threading.Event()

        def _long_watch(*paths, stop_event, **_kw):
            # Mimic watchfiles.watch — block until stop_event fires.
            while not stop_event.wait(0.05):
                pass
            event_seen.set()
            return iter(())

        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=lambda: None,
            is_busy=lambda: False,
        )
        with patch("watchfiles.watch", _long_watch):
            w.start()
            # Wait until the watcher actually entered the loop before
            # stopping — otherwise stop_event may be set before the
            # Rust layer arms, and event_seen would never be set.
            for _ in range(40):
                if w.is_running():
                    break
                time.sleep(0.01)
            w.stop(timeout=2.0)
        assert event_seen.is_set()
        assert w.is_running() is False

    def test_wait_until_stopped_before_start_returns_true(self):
        """Public-API edge case: wait on an un-started watcher is a no-op."""
        w = FileWatcher(
            watch_dirs=("/tmp",),
            trigger=lambda: None,
            is_busy=lambda: False,
        )
        assert w.wait_until_stopped(timeout=0.1) is True
        assert w.is_running() is False
