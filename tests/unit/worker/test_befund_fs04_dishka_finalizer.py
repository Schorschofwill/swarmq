"""Nachweis FS-04: Ein scheiternder Dishka-Finalizer (``ExitError`` beim
Schliessen des REQUEST-Scopes) ueberspringt Lock-Freigabe und
Semaphor-Rueckgabe.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-04; Quelle N-2).

Im ``finally`` von ``_process_message`` (``worker/pipeline.py``) laeuft
``_teardown_di_scope`` vor ``_detached_lock_release`` und
``self._semaphore.release()``. Wirft der yield-Finalizer eines Providers
(etwa ``await session.commit()`` scheitert), reicht Dishka das als
``ExitError`` durch; die beiden folgenden Schritte laufen nicht. Folge: Der
LockKeeper verlaengert den Mutex, solange der Worker lebt, und jeder Fall
kostet einen Semaphor-Platz, bis der Worker nichts mehr verarbeitet.

Erwartet: Lock-Freigabe und Semaphor-Rueckgabe laufen auch dann, wenn das
Schliessen des DI-Scopes scheitert.

Absichtlich ohne ``from __future__ import annotations``: ``FromDishka`` muss
bei der Klassendefinition aufloesbar sein (wie in ``test_worker_dishka.py``).

Testliste:
# [x] Task mit lock=..., Finalizer wirft -> Lock ist frei, LockKeeper haelt
#     ihn nicht mehr
# [x] concurrency=1, Finalizer wirft -> Semaphor-Platz ist zurueckgegeben
#     (die naechste Zustellung kaeme durch)
"""

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, override

import pytest

pytest.importorskip("dishka")

from dishka import (
    FromDishka,
    Provider,
    Scope,
    from_context,
    make_async_container,
    provide,  # pyright: ignore[reportUnknownVariableType] - provide-Overload von dishka nur teilweise typisiert
)

from swarmq import dishka as di
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory.backend import InMemoryLockStore
from tests.support.inmemory.clock import FakeClock
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_LOCK = "fs04-report"
_REASON = "FS-04: offen - scheiternder DI-Finalizer ueberspringt Lock-Freigabe und Semaphor-Rueckgabe"


class Fs04Session:
    """Steht fuer eine DB-Session, deren Commit im Finalizer scheitert."""


class Fs04FailingCommitProvider(Provider):
    tc = from_context(di.TaskContext, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    async def session(self, ctx: di.TaskContext) -> AsyncGenerator[Fs04Session, BaseException | None]:
        yield Fs04Session()
        raise RuntimeError(f"commit failed for {ctx.task_id}")


class Fs04LockedDiTask(Task):
    task_info = TaskInfo(name="fs04_locked_di_task", queues=["default"], lock=_LOCK, hard_timeout=60)

    @override
    async def run(self, session: FromDishka[Fs04Session]) -> None:
        return None


class Fs04DiTask(Task):
    task_info = TaskInfo(name="fs04_di_task", queues=["default"])

    @override
    async def run(self, session: FromDishka[Fs04Session]) -> None:
        return None


# Auf Modulebene wegen FromDishka, aber nicht in der globalen Liste: Die
# Tests bauen ihre eigene TaskRegistry (_worker). Global registriert liessen
# sie jeden eingebetteten Worker-Start eines Laufs ueber alle Module an der
# DI-Pruefung scheitern (Waechter in tests/conftest.py).
Task._registered_tasks.remove(Fs04LockedDiTask)
Task._registered_tasks.remove(Fs04DiTask)


def _register_failing_finalizer_container() -> None:
    """Registriert den Container; ``tests/conftest.py`` stellt den alten
    Stand nach dem Test wieder her."""
    di.setup_dishka(make_async_container(Fs04FailingCommitProvider()))


def _worker(broker: Any, backend: Any, task: type[Task], *, concurrency: int) -> Worker:
    registry = TaskRegistry()
    registry.register(task)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=concurrency,
        queues=["default"],
    )
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


async def _deliver(worker: Worker, task_id: str, name: str) -> None:
    # Ob die Zustellung den DI-Fehler weiterreicht, ist nicht Gegenstand des
    # Nachweises; geprueft werden die Freigaben danach.
    with contextlib.suppress(Exception):
        await worker._handle_message(make_message(_default_payload(task_id=task_id, name=name, args=[], kwargs={})))


@pytest.mark.befund("FS-04")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_lock_is_released_when_the_di_finalizer_fails(broker: Any, backend: Any) -> None:
    store = InMemoryLockStore(FakeClock())
    worker = _worker(broker, backend, Fs04LockedDiTask, concurrency=1)
    worker._lock_manager = store

    _register_failing_finalizer_container()
    await _deliver(worker, "fs04-t1", "fs04_locked_di_task")

    broker.ack.assert_awaited_once()  # Voraussetzung: der Task selbst lief durch
    assert await store.current_owner(_LOCK) is None, "Lock bleibt nach scheiterndem DI-Finalizer gehalten"
    assert worker._lock_keeper.kept == 0, "LockKeeper verlaengert den Lock weiter"


@pytest.mark.befund("FS-04")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_semaphore_slot_is_returned_when_the_di_finalizer_fails(broker: Any, backend: Any) -> None:
    worker = _worker(broker, backend, Fs04DiTask, concurrency=1)

    _register_failing_finalizer_container()
    await _deliver(worker, "fs04-t2", "fs04_di_task")

    broker.ack.assert_awaited_once()  # Voraussetzung: der Task selbst lief durch
    assert not worker._semaphore.locked(), (
        "Semaphor-Platz ging verloren: bei concurrency=1 bliebe jede weitere Zustellung vor acquire() stehen"
    )
