"""
SwarmQ Worker-Prototyp: Multiprocessing + asyncio Signal-Handling

Verifiziert:
  1. spawn-basierter Prozess-Start mit sauberem asyncio-Loop
  2. Signal-Handling: SIGTERM/SIGINT im Parent, SIGTERM im Worker
  3. SIGINT-Ignorierung in Workern (nur Parent reagiert auf Ctrl+C)
  4. Zweistufiger Graceful Shutdown (soft -> hard)
  5. Auto-Restart toter Worker mit max_restarts
  6. Zombie-Vermeidung
  7. multiprocessing.Event als Shutdown-Kanal
  8. Cooperative Shutdown via asyncio.Event
  9. Laufende Tasks sauber beenden oder NACKen
  10. SIGHUP fuer Reload

Aufruf:
  python worker_prototype.py [--workers N] [--task-duration MS]

Test-Szenarien:
  - Ctrl+C:     Graceful Shutdown aller Worker
  - kill -TERM: Graceful Shutdown
  - kill -HUP:  Worker-Reload
  - kill -9 WORKER_PID: Worker stirbt, wird automatisch neugestartet
"""

from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import logging
import multiprocessing
import os
import signal
import sys
import time
from argparse import ArgumentParser
from dataclasses import dataclass, field
from enum import IntEnum

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FORMAT = "%(asctime)s [%(processName)-12s PID=%(process)d] %(levelname)-5s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("swarmq")


# ---------------------------------------------------------------------------
# Exit-Codes
# ---------------------------------------------------------------------------


class ExitCode(IntEnum):
    SUCCESS = 0
    ERROR = 1
    RESTART_REQUESTED = 2


# ---------------------------------------------------------------------------
# Signal-Blocking (Dramatiq-Pattern)
# ---------------------------------------------------------------------------

HANDLED_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)

_libc_name = ctypes.util.find_library("c")
_libc: ctypes.CDLL | None = ctypes.CDLL(_libc_name, use_errno=True) if _libc_name else None


def block_signals() -> None:
    """Blockiere behandelte Signale via pthread_sigmask.

    Muss VOR dem Spawnen von Child-Prozessen aufgerufen werden,
    damit kein Signal in der Luecke zwischen fork und Handler-Setup verloren geht.
    Children erben die Signal-Maske und entblocken selbst.
    """
    if sys.platform == "win32":
        return
    signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)


def unblock_signals() -> None:
    """Entblocke behandelte Signale nach dem Setup."""
    if sys.platform == "win32":
        return
    signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)


# ---------------------------------------------------------------------------
# Simulierte Task-Arbeit (steht fuer echte Tasks)
# ---------------------------------------------------------------------------


async def simulate_task(task_id: int, duration_ms: float, shutdown: asyncio.Event) -> str:
    """Simuliert einen async I/O-Task. Reagiert cooperative auf Shutdown."""
    log.info(f"Task {task_id} gestartet (Dauer: {duration_ms}ms)")
    try:
        await asyncio.sleep(duration_ms / 1000)
        log.info(f"Task {task_id} abgeschlossen")
        return f"result_{task_id}"
    except asyncio.CancelledError:
        log.warning(f"Task {task_id} wurde cancelled (Hard-Shutdown)")
        raise


# ---------------------------------------------------------------------------
# Async Worker (laeuft in eigenem Prozess)
# ---------------------------------------------------------------------------


class AsyncWorker:
    """Ein einzelner async Worker mit eigenem Event-Loop.

    Enthaelt das Prefetcher/Runner-Pattern mit cooperative Shutdown.
    """

    def __init__(
        self,
        worker_id: int,
        shutdown_event: multiprocessing.Event,
        task_duration_ms: float = 500,
        max_concurrent: int = 5,
    ):
        self.worker_id = worker_id
        self.mp_shutdown = shutdown_event
        self.task_duration_ms = task_duration_ms
        self.max_concurrent = max_concurrent

        # Lokale asyncio-Events (pro Worker-Prozess)
        self._local_shutdown = asyncio.Event()
        self._active_tasks: set[asyncio.Task] = set()
        self._task_counter = 0
        self._tasks_completed = 0
        self._signal_count = 0

    async def run(self) -> None:
        """Haupteinstiegspunkt — wird via asyncio.run() aufgerufen."""
        loop = asyncio.get_running_loop()

        # Signal-Handler im Worker:
        # SIGINT ignorieren (nur Parent reagiert auf Ctrl+C)
        # SIGTERM fuer Notfall-Shutdown
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        loop.add_signal_handler(signal.SIGTERM, self._on_sigterm)

        # Monitor fuer multiprocessing.Event (nicht async-nativ)
        monitor = asyncio.create_task(self._monitor_mp_event())

        log.info(f"Worker {self.worker_id} bereit (PID={os.getpid()}, max_concurrent={self.max_concurrent})")

        try:
            await self._task_loop()
        except asyncio.CancelledError:
            pass
        finally:
            monitor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor
            await self._drain_active_tasks()
            log.info(f"Worker {self.worker_id} beendet. Tasks abgeschlossen: {self._tasks_completed}")

    def _on_sigterm(self) -> None:
        """SIGTERM-Handler: Setzt lokalen Shutdown."""
        self._signal_count += 1
        log.info(f"Worker {self.worker_id}: SIGTERM empfangen (#{self._signal_count})")
        self._local_shutdown.set()

    async def _monitor_mp_event(self) -> None:
        """Pollt das multiprocessing.Event im Executor (nicht blockierend)."""
        loop = asyncio.get_running_loop()
        while not self._local_shutdown.is_set():
            is_set = await loop.run_in_executor(
                None,
                self.mp_shutdown.wait,
                0.5,
            )
            if is_set:
                log.info(f"Worker {self.worker_id}: Shutdown-Event vom Parent empfangen")
                self._local_shutdown.set()
                return

    async def _task_loop(self) -> None:
        """Hauptloop: Simuliert Task-Annahme und -Verarbeitung."""
        sem = asyncio.Semaphore(self.max_concurrent)

        while not self._local_shutdown.is_set():
            await sem.acquire()

            if self._local_shutdown.is_set():
                sem.release()
                break

            self._task_counter += 1
            task_id = self._task_counter

            task = asyncio.create_task(
                self._run_single_task(task_id, sem),
                name=f"task-{task_id}",
            )
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

            # Kurze Pause — simuliert Broker-Fetch
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break

    async def _run_single_task(self, task_id: int, sem: asyncio.Semaphore) -> None:
        """Fuehrt einen einzelnen Task aus und released den Semaphore."""
        try:
            await simulate_task(task_id, self.task_duration_ms, self._local_shutdown)
            self._tasks_completed += 1
        except asyncio.CancelledError:
            log.warning(f"Task {task_id}: Cancelled — wuerde NACK an Broker senden")
            raise
        finally:
            sem.release()

    async def _drain_active_tasks(self) -> None:
        """Zweistufiger Shutdown fuer laufende Tasks."""
        if not self._active_tasks:
            return

        log.info(f"Worker {self.worker_id}: {len(self._active_tasks)} Tasks laufen noch. Soft-Timeout: 5s...")

        # Stufe 1: Soft — warte auf laufende Tasks
        done, pending = await asyncio.wait(
            self._active_tasks,
            timeout=5.0,
            return_when=asyncio.ALL_COMPLETED,
        )
        self._tasks_completed += len(done)

        if not pending:
            log.info(f"Worker {self.worker_id}: Alle Tasks sauber beendet")
            return

        # Stufe 2: Hard — cancel verbleibende Tasks
        log.warning(f"Worker {self.worker_id}: {len(pending)} Tasks nach Soft-Timeout. Hard-Cancel...")
        for task in pending:
            task.cancel()

        done2, still_pending = await asyncio.wait(pending, timeout=2.0)

        for task in done2:
            if not task.cancelled() and task.exception():
                log.error(f"Task-Exception beim Shutdown: {task.exception()}")

        if still_pending:
            log.error(f"Worker {self.worker_id}: {len(still_pending)} Tasks reagieren nicht auf Cancel!")


# ---------------------------------------------------------------------------
# Worker Entry-Point (wird in neuem Prozess via spawn ausgefuehrt)
# ---------------------------------------------------------------------------

import contextlib


def worker_entry(
    worker_id: int,
    shutdown_event: multiprocessing.Event,
    task_duration_ms: float,
) -> None:
    """Entry-Point fuer Worker-Prozess. Erstellt eigenen Event-Loop."""
    # Logging neu konfigurieren (spawn = frischer Prozess)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, force=True)

    # Optional: uvloop aktivieren
    try:
        import uvloop

        uvloop.install()
        log.info(f"Worker {worker_id}: uvloop aktiviert")
    except ImportError:
        pass

    worker = AsyncWorker(
        worker_id=worker_id,
        shutdown_event=shutdown_event,
        task_duration_ms=task_duration_ms,
    )

    try:
        asyncio.run(worker.run())
        sys.exit(ExitCode.SUCCESS)
    except KeyboardInterrupt:
        log.warning(f"Worker {worker_id}: KeyboardInterrupt")
        sys.exit(ExitCode.SUCCESS)
    except Exception as exc:
        log.exception(f"Worker {worker_id}: Unbehandelte Exception: {exc}")
        sys.exit(ExitCode.ERROR)


# ---------------------------------------------------------------------------
# Process Manager (Hauptprozess, KEIN Event-Loop)
# ---------------------------------------------------------------------------


@dataclass
class WorkerInfo:
    process: multiprocessing.Process
    worker_id: int
    restart_count: int = 0
    last_restart: float = field(default_factory=time.monotonic)


class ProcessManager:
    """Verwaltet Worker-Prozesse mit Auto-Restart und Signal-Handling."""

    MAX_RESTARTS = 5
    RESTART_WINDOW = 60.0
    MONITOR_INTERVAL = 0.5

    def __init__(
        self,
        num_workers: int,
        task_duration_ms: float,
    ):
        self.num_workers = num_workers
        self.task_duration_ms = task_duration_ms
        self._ctx = multiprocessing.get_context("spawn")
        self.shutdown_event = self._ctx.Event()
        self.workers: dict[int, WorkerInfo] = {}
        self._shutting_down = False
        self._reload_requested = False
        self._next_worker_id = 0

    def run(self) -> int:
        """Haupteinstiegspunkt. Blockiert bis Shutdown."""
        log.info(
            f"ProcessManager gestartet (PID={os.getpid()}, "
            f"Workers={self.num_workers}, "
            f"Task-Dauer={self.task_duration_ms}ms)"
        )

        # Signal-Blocking VOR dem Worker-Start (Dramatiq-Pattern)
        block_signals()
        self._start_all_workers()
        self._setup_signals()
        unblock_signals()

        log.info("Alle Worker gestartet. Druecke Ctrl+C fuer Graceful Shutdown.")
        log.info("Teste: kill -TERM %d | kill -HUP %d | kill -9 <WORKER_PID>", os.getpid(), os.getpid())

        try:
            retcode = self._monitor_loop()
        finally:
            self._shutdown_all_workers()
            self._collect_zombies()

        if self._reload_requested:
            log.info("Reload angefordert — starte ProcessManager neu via execvp")
            os.execvp(sys.executable, [sys.executable] + sys.argv)

        return retcode

    def _setup_signals(self) -> None:
        """Signal-Handler fuer den Parent-Prozess (KEIN Event-Loop)."""
        signal.signal(signal.SIGINT, self._on_shutdown_signal)
        signal.signal(signal.SIGTERM, self._on_shutdown_signal)
        signal.signal(signal.SIGHUP, self._on_reload_signal)
        signal.signal(signal.SIGCHLD, self._on_sigchld)

    def _on_shutdown_signal(self, signum: int, frame) -> None:
        """SIGTERM oder SIGINT empfangen — Graceful Shutdown einleiten."""
        sig_name = signal.Signals(signum).name
        log.info(f"Parent: {sig_name} empfangen — leite Shutdown ein")
        self._shutting_down = True
        self.shutdown_event.set()

    def _on_reload_signal(self, signum: int, frame) -> None:
        """SIGHUP empfangen — Worker neu laden."""
        log.info("Parent: SIGHUP empfangen — Reload angefordert")
        self._reload_requested = True
        self._shutting_down = True
        self.shutdown_event.set()

    def _on_sigchld(self, signum: int, frame) -> None:
        """SIGCHLD: Zombie-Prozesse einsammeln."""
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
            except ChildProcessError:
                break

    def _start_worker(self) -> WorkerInfo:
        """Startet einen einzelnen Worker-Prozess."""
        wid = self._next_worker_id
        self._next_worker_id += 1

        p = self._ctx.Process(
            target=worker_entry,
            args=(wid, self.shutdown_event, self.task_duration_ms),
            name=f"Worker-{wid}",
            daemon=False,
        )
        p.start()
        info = WorkerInfo(process=p, worker_id=wid)
        self.workers[p.pid] = info
        log.info(f"Worker-{wid} gestartet (PID={p.pid})")
        return info

    def _start_all_workers(self) -> None:
        """Startet alle konfigurierten Worker."""
        for _ in range(self.num_workers):
            self._start_worker()

    def _monitor_loop(self) -> int:
        """Hauptschleife: Prueft Worker-Health, restartet bei Bedarf."""
        while not self._shutting_down:
            self._check_dead_workers()

            # Fehlende Worker nachstarten
            while len(self.workers) < self.num_workers and not self._shutting_down:
                self._start_worker()

            time.sleep(self.MONITOR_INTERVAL)

        return 0

    def _check_dead_workers(self) -> None:
        """Prueft ob Worker gestorben sind und restartet sie."""
        dead: list[tuple[int, WorkerInfo]] = []

        for pid, info in list(self.workers.items()):
            if not info.process.is_alive():
                dead.append((pid, info))

        for pid, info in dead:
            exitcode = info.process.exitcode
            info.process.join(timeout=1)
            del self.workers[pid]

            log.warning(f"Worker-{info.worker_id} (PID={pid}) gestorben (Exit-Code={exitcode})")

            if self._shutting_down:
                continue

            if exitcode == ExitCode.SUCCESS:
                continue

            self._maybe_restart(info)

    def _maybe_restart(self, old_info: WorkerInfo) -> None:
        """Restart mit Limit-Pruefung."""
        now = time.monotonic()

        if now - old_info.last_restart > self.RESTART_WINDOW:
            restart_count = 0
        else:
            restart_count = old_info.restart_count + 1

        if restart_count >= self.MAX_RESTARTS:
            log.error(f"Worker-{old_info.worker_id}: {self.MAX_RESTARTS} Restarts in {self.RESTART_WINDOW}s — gebe auf")
            return

        log.info(f"Worker-{old_info.worker_id}: Restart #{restart_count + 1}/{self.MAX_RESTARTS}")
        new_info = self._start_worker()
        new_info.restart_count = restart_count
        new_info.last_restart = now

    def _shutdown_all_workers(self) -> None:
        """Dreistufiger Shutdown: Event -> SIGTERM -> SIGKILL."""
        if not self.workers:
            return

        self.shutdown_event.set()
        log.info(f"Shutdown: Warte auf {len(self.workers)} Worker (max 10s)...")

        # Stufe 1: Warte auf freiwilligen Exit via shutdown_event
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            alive = [i for i in self.workers.values() if i.process.is_alive()]
            if not alive:
                break
            time.sleep(0.3)

        # Stufe 2: SIGTERM an verbleibende Worker
        alive = [i for i in self.workers.values() if i.process.is_alive()]
        if alive:
            log.warning(f"Shutdown: {len(alive)} Worker reagieren nicht — sende SIGTERM")
            for info in alive:
                info.process.terminate()

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if not any(i.process.is_alive() for i in alive):
                    break
                time.sleep(0.3)

        # Stufe 3: SIGKILL als letztes Mittel
        alive = [i for i in self.workers.values() if i.process.is_alive()]
        if alive:
            log.error(f"Shutdown: {len(alive)} Worker reagieren nicht auf SIGTERM — SIGKILL")
            for info in alive:
                info.process.kill()

        # Zombies einsammeln
        for info in self.workers.values():
            info.process.join(timeout=2)

        log.info("Alle Worker beendet")

    def _collect_zombies(self) -> None:
        """Sammelt verbleibende Zombie-Prozesse ein."""
        for info in self.workers.values():
            try:
                info.process.join(timeout=1)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = ArgumentParser(description="SwarmQ Worker-Prototyp")
    parser.add_argument("--workers", "-w", type=int, default=3)
    parser.add_argument(
        "--task-duration", "-d", type=float, default=2000, help="Simulierte Task-Dauer in ms (Default: 2000)"
    )
    args = parser.parse_args()

    manager = ProcessManager(
        num_workers=args.workers,
        task_duration_ms=args.task_duration,
    )
    return manager.run()


if __name__ == "__main__":
    sys.exit(main())
