"""Gemeinsame ProcessManager-Fixtures (Plan U21)."""

from __future__ import annotations

import signal

import pytest


@pytest.fixture
def _restore_default_signals():
    """R5.3 ce-review #10: tests that install real signal handlers
    (`_setup_signals`) must reset them afterward so other suites
    are not affected."""

    saved_hup = signal.getsignal(signal.SIGHUP)
    saved_int = signal.getsignal(signal.SIGINT)
    saved_term = signal.getsignal(signal.SIGTERM)
    saved_chld = signal.getsignal(signal.SIGCHLD)
    yield
    # ce-review pass 2 #8: SIG_DFL == 0 is falsy, so `saved if saved
    # else SIG_DFL` worked by accident. Use `is not None` for the
    # intended semantics (the only non-handler value getsignal returns).
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
