"""Nachweis FS-16: Im CPU-Modus ueberleben Enkelprozesse des Rechenkinds, weil
die Prozessgruppe kein SIGKILL bekommt, sobald das Kind selbst tot ist.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-16; Quelle MB-1).

``SlotPool._kill_child`` (``worker/compute/pool.py``) kehrt zurueck, sobald das
*Kind* tot ist: Endet es in der SIGTERM-Gnade, kommt das SIGKILL an die Gruppe
nie; stirbt es von selbst (OOM, Segfault, ``os._exit``), signalisiert der Pool
der Gruppe gar nicht. KTD3/AE13 (Plan 2026-09-03-001) versprechen dagegen:
"kein Prozess aus der Prozessgruppe des Kindes lebt weiter".

Aufbau: echter ``SlotPool`` mit Fork-Kontext. Nur das Kindziel ist ersetzt
(statt ``child_once``): Es bildet wie ``bind_child_process`` eine eigene
Prozessgruppe, startet einen SIGTERM-festen Enkel (Hilfsprozess mit eigenem
Handler, ``trap``-Shell o. ae.) und meldet sich per Ready-Handshake.

Echte Prozesse sind hier der Gegenstand des Befunds, deshalb Unit-Test mit
echtem Fork (kein Broker, kein Valkey). Begruendung fuer > 100 ms: Fork,
Handshake und Kill dauern rund 0,1 s. Alle erzeugten Prozesse (Kind
und Enkel) werden im ``finally`` per ``killpg``/``kill`` mit SIGKILL beendet,
auch wenn der Test fehlschlaegt; der Enkel beendet sich ausserdem selbst
nach spaetestens 10 s.

Wartezeit nach dem Kill: Der Pool sendet das SIGKILL an die Gruppe synchron,
bevor ``run`` zurueckkehrt (``_finalize``/``_kill_child``). Zustellung und
Abraeumen des Enkels laufen danach asynchron im Kernel; unter CI-Last dauerte
das schon laenger als 0,1 s (Tag-Pipeline 2964583856). E-036 sagt "die Gruppe
stirbt", keine Frist. Der Test pollt deshalb bis zum Ende der Gruppe, mit
einem Meldedeckel von ``_GROUP_GONE_DEADLINE_S``. Der Deckel liegt weit unter
der Selbstbeendigung des Enkels (``_GC_SELF_EXIT_S``) und unter dem
Test-Timeout: Ohne Kill der Gruppe lebt der Enkel bis zum Deckel und der
Test wird rot (Gegenprobe: ``_sweep_group`` als No-op).

Testliste:
# [x] Fall a: Abbruch von run() (Watchdog/Drain/Cancel), Kind endet sauber auf
#     SIGTERM -> nach run() lebt kein Mitglied der Prozessgruppe mehr
# [x] Fall b: Kind stirbt von selbst (os._exit) -> run() wirft ChildDied, danach
#     lebt kein Mitglied der Prozessgruppe mehr
"""

from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import os
import signal
import time
from pathlib import Path
from typing import Any

import pytest

from swarmq.worker.compute.envelope import ComputeJob
from swarmq.worker.compute.pool import ChildDied, SlotPool

pytestmark = [
    pytest.mark.timeout(5),
    # Der Test forkt direkt aus pytest; in der vollen Suite laufen dort noch
    # Threads anderer Tests. Das Kind fuehrt nur lock-freien Code aus, die
    # Fork-Sicherheit ist nicht Gegenstand des Nachweises.
    pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning"),
]

_GC_SELF_EXIT_S = 10.0
#: Meldedeckel fuer das Ende der Gruppe nach dem Kill (keine Zusage von E-036).
_GROUP_GONE_DEADLINE_S = 3.0


def _fs16_grandchild(report_fd: int, started_fd: int) -> None:
    """Enkel: ignoriert SIGTERM, meldet seine PID, beendet sich spaetestens nach 10 s."""
    try:
        # Wie ``subprocess.Popen(close_fds=True)``: der Enkel haelt den Rueckkanal
        # des Kindes nicht offen (sonst kaeme dessen EOF nie).
        low, high = sorted((report_fd, started_fd))
        os.closerange(3, low)
        os.closerange(low + 1, high)
        os.closerange(high + 1, os.sysconf("SC_OPEN_MAX"))
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.write(report_fd, f"{os.getpid()}\n".encode())
        os.write(started_fd, b"x")  # erst jetzt meldet sich das Kind bereit
        os.close(started_fd)
        deadline = time.monotonic() + _GC_SELF_EXIT_S
        while time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        os._exit(0)


def _fs16_exit_on_term(_signum: int, _frame: object) -> None:
    os._exit(0)


def _fs16_child(mode: str, report_fd: int, channel: Any, slot: int) -> None:
    """Ersatz fuer ``child_once``: eigene Gruppe, SIGTERM-fester Enkel, Ready."""
    os.setpgid(0, 0)
    started_r, started_w = os.pipe()
    if os.fork() == 0:
        os.close(started_r)
        _fs16_grandchild(report_fd, started_w)
    os.close(started_w)
    # Ready erst, wenn der Enkel SIGTERM ignoriert und seine PID gemeldet hat:
    # sonst trifft ein (korrekter) Gruppen-Kill ihn, bevor der Test ihn kennt.
    os.read(started_r, 1)
    os.close(started_r)
    channel.send(("ready", slot, os.getpid(), True))
    if mode == "die":
        os._exit(1)  # stirbt von selbst (wie OOM-Kill/Segfault), ohne Ergebnis
    signal.signal(signal.SIGTERM, _fs16_exit_on_term)  # endet sauber in der SIGTERM-Gnade
    deadline = time.monotonic() + _GC_SELF_EXIT_S
    while time.monotonic() < deadline:
        time.sleep(0.05)
    os._exit(0)


class _Fs16Context:
    """Fork-Kontext, dessen ``Process`` das Kindziel durch ``_fs16_child`` ersetzt."""

    def __init__(self, mode: str, report_fd: int) -> None:
        self._ctx = multiprocessing.get_context("fork")
        self._mode = mode
        self._report_fd = report_fd
        self.processes: list[Any] = []

    def Pipe(self, duplex: bool = True) -> Any:
        return self._ctx.Pipe(duplex=duplex)

    def Process(self, *, target: Any, args: tuple[Any, ...], name: str) -> Any:
        del target
        _job, send_conn, slot = args
        proc = self._ctx.Process(target=_fs16_child, args=(self._mode, self._report_fd, send_conn, slot), name=name)
        self.processes.append(proc)
        return proc


def _alive_members(pgid: int) -> list[int]:
    """PIDs der Gruppe ``pgid``, die noch laufen (Zombies zaehlen als tot)."""
    alive: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        fields = stat[stat.rindex(")") + 2 :].split()
        state, process_group = fields[0], int(fields[2])
        if process_group == pgid and state not in ("Z", "X"):
            alive.append(int(entry.name))
    return alive


def _wait_group_gone(pgid: int, timeout: float = _GROUP_GONE_DEADLINE_S) -> list[int]:
    """Auf das Ende der Gruppe nach einem bereits gesendeten SIGKILL warten.

    Kehrt zurueck, sobald kein Mitglied mehr lebt; ``timeout`` ist nur der
    Meldedeckel, nach dem die noch lebenden Mitglieder gemeldet werden."""
    deadline = time.monotonic() + timeout
    while (alive := _alive_members(pgid)) and time.monotonic() < deadline:
        time.sleep(0.005)
    return alive


async def _read_grandchild_pid(report_r: int) -> int:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[int] = loop.create_future()

    def _readable() -> None:
        loop.remove_reader(report_r)
        fut.set_result(int(os.read(report_r, 64).decode().strip()))

    loop.add_reader(report_r, _readable)
    return await fut


async def _handshake_done(pool: SlotPool) -> None:
    """Warten, bis ``run`` das Kind nach dem Ready-Handshake fuehrt (eigene Gruppe bestaetigt)."""
    deadline = time.monotonic() + 2.0
    while pool._slots[0].active is None:
        assert time.monotonic() < deadline, "Ready-Handshake blieb aus"
        await asyncio.sleep(0.005)


class _Fs16Setup:
    def __init__(self, mode: str) -> None:
        self.report_r, self.report_w = os.pipe()
        self.ctx = _Fs16Context(mode, self.report_w)
        self.pool = SlotPool(self.ctx, 1, recycle_rss_mb=None, recycle_max_tasks=None)
        self.grandchild: int | None = None

    def cleanup(self) -> None:
        for proc in self.ctx.processes:
            if proc.pid is not None:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
        if self.grandchild is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(self.grandchild, signal.SIGKILL)
        for proc in self.ctx.processes:
            proc.join(timeout=1)
        for fd in (self.report_r, self.report_w):
            with contextlib.suppress(OSError):
                os.close(fd)


def _job() -> ComputeJob:
    return ComputeJob(task_id="fs16-t1", task_name="fs16_demo", queue="q", args=(), kwargs={})


@pytest.mark.befund("FS-16")
async def test_cancelled_run_leaves_no_member_of_the_child_process_group() -> None:
    setup = _Fs16Setup("term")
    try:
        await setup.pool.start()
        run = asyncio.create_task(setup.pool.run(_job(), use_di=False))
        setup.grandchild = await _read_grandchild_pid(setup.report_r)
        pgid = setup.ctx.processes[0].pid
        assert os.getpgid(setup.grandchild) == pgid, "Voraussetzung: Enkel liegt in der Gruppe des Kindes"
        await _handshake_done(setup.pool)

        run.cancel()  # Watchdog, Cancel-Monitor oder Drain-Ende
        with pytest.raises(asyncio.CancelledError):
            await run

        assert not setup.ctx.processes[0].is_alive(), "Voraussetzung: das Kind ist tot"
        alive = _wait_group_gone(pgid)
        assert alive == [], f"Prozessgruppe {pgid} lebt nach dem Kill weiter: {alive} (Enkel {setup.grandchild})"
    finally:
        await setup.pool.aclose()
        setup.cleanup()


@pytest.mark.befund("FS-16")
async def test_child_death_leaves_no_member_of_the_child_process_group() -> None:
    setup = _Fs16Setup("die")
    try:
        await setup.pool.start()
        run = asyncio.create_task(setup.pool.run(_job(), use_di=False))
        setup.grandchild = await _read_grandchild_pid(setup.report_r)
        pgid = setup.ctx.processes[0].pid

        with pytest.raises(ChildDied):
            await run

        alive = _wait_group_gone(pgid)
        assert alive == [], f"Prozessgruppe {pgid} lebt nach dem Kindtod weiter: {alive} (Enkel {setup.grandchild})"
    finally:
        await setup.pool.aclose()
        setup.cleanup()
