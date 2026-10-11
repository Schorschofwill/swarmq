"""Reload-Zustandsmaschine des Supervisors (Plan 2026-10-06-1811, KTD8).

Der SIGHUP-Handler setzt nur einen ausstehenden Reload; die Pruefung einer
``--log-config``-Datei startet und pollt der Monitor-Loop. Ausgelagert, weil
``supervisor.py`` am 800-Zeilen-Guard steht; ``pm: Any`` wie in
``autoscale.py``, eine Typangabe waere ein Importzyklus.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from collections.abc import Callable
from typing import Any

from swarmq.exceptions import LoggingConfigError
from swarmq.logging import _error_fields, get_logger, shutdown_logging
from swarmq.logging_file import DictConfigProbe, load_log_config

logger = get_logger(__name__)

#: Frist der Probe; wie bisher 30 s. Sie laeuft im Monitor-Loop ab, nicht in
#: einem blockierenden Aufruf.
PROBE_TIMEOUT_S = 30.0


class ReloadController:
    """Ausstehender Reload und laufende Probe; getrieben vom Monitor-Loop."""

    def __init__(
        self,
        log_config: str | None = None,
        *,
        probe_factory: Callable[[str], Any] = DictConfigProbe,
        clock: Callable[[], float] = time.monotonic,
        timeout_s: float = PROBE_TIMEOUT_S,
    ) -> None:
        self.log_config = log_config
        self._probe_factory = probe_factory
        self._clock = clock
        self._timeout_s = timeout_s
        self._deadline = 0.0
        self.pending = False
        self._probe: Any = None

    @property
    def busy(self) -> bool:
        """Reload ausstehend oder Probe laeuft (``FileWatcher.is_busy``)."""
        return self.pending or self._probe is not None

    def request(self) -> None:
        """SIGHUP: nur vormerken -- im Signal-Handler startet nichts."""
        self.pending = True

    def cancel(self) -> None:
        """SIGTERM: einen ausstehenden Reload verwerfen. Eine laufende Probe
        beendet der Loop (:meth:`abort`), nicht der Signal-Handler."""
        self.pending = False

    def abort(self) -> None:
        """Shutdown: laufende Probe beenden, kein Reload."""
        probe, self._probe = self._probe, None
        self.pending = False
        if probe is not None:
            probe.kill()

    def step(self) -> bool:
        """Ein Monitor-Tick; ``True`` = Reload beschlossen."""
        if self._probe is not None:
            return self._poll()
        if not self.pending:
            return False
        self.pending = False
        if self.log_config is None:
            return True
        try:
            self._probe = self._probe_factory(load_log_config(self.log_config))
        except LoggingConfigError as exc:
            self._log_failed(exc)
            return False
        except OSError as exc:
            self._log_failed(LoggingConfigError(self.log_config, exc))
            return False
        self._deadline = self._clock() + self._timeout_s
        return self._poll()

    def _poll(self) -> bool:
        probe = self._probe
        returncode = probe.poll()
        if returncode is None and self._clock() < self._deadline:
            return False
        self._probe = None
        # SIGHUPs waehrend der Probe sind in ihr aufgegangen (zusammengefasst).
        self.pending = False
        if returncode is None:
            # Eine Probe in ihrer Frist ist keine ungueltige Konfiguration:
            # ein Handler kann an seinem Ziel haengen (Syslog per TCP).
            probe.kill()
            logger.error(
                "log_config_reload_probe_timeout",
                path=self.log_config,
                timeout_s=self._timeout_s,
                hint="a handler may block on its target; check it and send SIGHUP again; workers keep running",
            )
            return False
        if returncode != 0:
            cause = ValueError(probe.error() or f"dictConfig exited {returncode}")
            self._log_failed(LoggingConfigError(str(self.log_config), cause))
            return False
        return True

    def _log_failed(self, exc: LoggingConfigError) -> None:
        logger.error(
            "log_config_reload_failed",
            path=self.log_config,
            **_error_fields(exc),
            hint="fix the logging config and send SIGHUP again; workers keep running",
        )


def reexec(pm: Any, retcode: int) -> int:
    """Beschlossener Reload nach dem Worker-Stopp: ``execvpe`` des Supervisors.

    Kehrt nur zurueck, wenn der Reload nicht stattfindet: ueberlebende Kinder,
    ein SIGTERM waehrend Stopp oder Leeren (KTD8) oder ein gescheitertes
    ``execvpe``.
    """
    # Orphan-guard: if any child survived Phase-3 SIGKILL (typically D-state:
    # NFS stall, dying disk), os.execvp would replace the parent image and
    # orphan the child to init/systemd. Prefer clean no-reload exit over orphan.
    alive = [w for w in pm.workers.values() if w.process.is_alive()]
    if alive:
        logger.warning(
            "reload_aborted_alive_children",
            alive_count=len(alive),
            alive_pids=[w.process.pid for w in alive],
            hint=(
                "re-send SIGHUP once all workers have exited; "
                "a D-state process (uninterruptible kernel wait) "
                "may require host-level intervention"
            ),
        )
        # Return 0 (clean exit): systemd / k8s must NOT restart us —
        # operator needs to act.
        return 0
    try:
        logger.info("process_manager_reload_execvp")
        shutdown_logging()  # R15: die Queue vor dem neuen Abbild leeren
        # Ab hier bis ``execve`` bleibt ein SIGTERM anhaengig, statt dass der
        # Handler nach der Pruefung unbemerkt Flags setzt: Maske und
        # anhaengiges Signal ueberleben ``execve``, der neue Supervisor
        # bekommt es, sobald ``ProcessManager.run`` die Signale freigibt.
        saved = _block_shutdown_signals()
        # Die Maske gilt nur fuer diesen Thread. Nimmt ein anderer Thread das
        # Signal an, liefe der Handler hier womoeglich erst nach der Pruefung;
        # er stellt es dann erneut an diesen Thread zu, wo es anhaengig bleibt.
        pm._reexec_holds_signals = saved is not None
        try:
            # KTD8: SIGTERM waehrend Stopp oder Leeren -- oder schon, waehrend
            # der Loop den Reload beschloss.
            if not pm._reload_requested or pm._shutdown_signal is not None:
                pm._log_shutdown_signal()
                return retcode
            # Use the argv AND env snapshots captured in ``__init__``.
            # execvpe closes the env side of the trust boundary so in-process
            # os.environ mutation (e.g., a library flipping PYTHONPATH) cannot
            # influence the re-exec'd interpreter's import resolution.
            os.execvpe(sys.executable, [sys.executable, *pm._startup_argv], pm._startup_env)  # noqa: S606 - Re-Exec mit eigenem argv-Snapshot, keine Shell
        finally:
            pm._reexec_holds_signals = False
            _restore_signal_mask(saved)  # nur ohne ``execve`` erreicht
    except OSError as exc:
        logger.error("reload_execvp_failed", **_error_fields(exc))
        return 1
    return retcode


_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _block_shutdown_signals() -> set[int | signal.Signals] | None:
    """Blockiert SIGTERM und SIGINT; liefert die alte Maske (``None`` ohne
    ``pthread_sigmask``)."""
    try:
        return signal.pthread_sigmask(signal.SIG_BLOCK, _SHUTDOWN_SIGNALS)
    except AttributeError, OSError:
        return None


def _restore_signal_mask(saved: set[int | signal.Signals] | None) -> None:
    """Stellt die alte Maske her; ein inzwischen anhaengiges SIGTERM wird
    dabei zugestellt."""
    if saved is not None:
        signal.pthread_sigmask(signal.SIG_SETMASK, saved)
