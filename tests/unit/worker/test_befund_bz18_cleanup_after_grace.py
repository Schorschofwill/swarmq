"""Befund BZ-18: Lock-Freigabe und Leader-Übergabe liegen hinter der Gnadenfrist.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-18; E-7, F-6).

Der Worker nutzt ``shutdown_timeout`` vollständig als Drain-Frist
(``worker/lifecycle.py`` ``_drain_in_flight``) und gibt erst **danach**
übrig gebliebene Locks frei und stoppt den Scheduler (Leader-Übergabe).
Die Orchestrierung (Kubernetes-Default 30 s = Default ``shutdown_timeout``)
und der Supervisor rechnen mit derselben Frist ab SIGTERM. Hängt ein Task,
kommt das Aufräumen also immer erst nach Ablauf der Frist -- in der Praxis nach
dem SIGKILL. Folge: Mutex bis ``hard_timeout + 30 s`` gesperrt, kein
Leader-Handover (Cron-Lücke bis Lock-TTL), Guard-Marker werden Waisen.

Für die Publish-Endfrist gibt es bereits eine Reserve
(``_SHUTDOWN_PUBLISH_RESERVE_S``, höchstens die halbe Frist) -- für den Drain
nicht. Erwartet: Mit hängendem Task sind Locks freigegeben und der Scheduler
gestoppt, bevor ``shutdown_timeout`` ab Beginn des Herunterfahrens abläuft.

Die Frist ist im Test verkleinert (80 ms statt 30 s); das Verhältnis
Drain : Frist ist dasselbe wie mit Defaults.

Test-Liste:
- [x] hängender Task, Handler-Lock offen, eingebetteter Scheduler ->
      Lock-Freigabe und Scheduler-Stopp vor Ablauf der Frist (xfail, BZ-18)
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.worker import WorkerState

# Enge Zeitbudgets gegen die echte Uhr: unter paralleler Last gerissen
# (CI und Stresslauf 2026-10-16), daher im seriellen Durchgang (``timing``).
pytestmark = [pytest.mark.timeout(5), pytest.mark.timing]

_GRACE_S = 0.08


@pytest.mark.entscheidung("E-018")
async def test_locks_and_leader_are_released_within_the_grace_period(worker: Any) -> None:
    loop = asyncio.get_running_loop()
    worker._config.shutdown_timeout = _GRACE_S
    worker._state = WorkerState.RUNNING
    released_at: list[float] = []
    stopped_at: list[float] = []

    async def _release(token: LockToken) -> bool:
        released_at.append(loop.time())
        return True

    async def _stop() -> None:
        stopped_at.append(loop.time())

    worker._lock_manager = MagicMock()
    worker._lock_manager.release = _release
    worker._scheduler = MagicMock()
    worker._scheduler.stop = _stop

    hang = asyncio.Event()
    task = asyncio.create_task(hang.wait())  # langer Task im Drain
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="report", owner="tok", max_concurrency=1)]

    started = loop.time()
    worker._request_shutdown()
    await worker._execute_shutdown()

    assert released_at
    assert stopped_at
    lock_after = released_at[0] - started
    leader_after = stopped_at[0] - started
    msg = (
        f"Lock frei nach {lock_after * 1000:.0f} ms, Scheduler gestoppt nach "
        f"{leader_after * 1000:.0f} ms -- die Gnadenfrist ({_GRACE_S * 1000:.0f} ms, "
        "mit Defaults 30 s = K8s-SIGKILL) war da schon abgelaufen"
    )
    assert lock_after < _GRACE_S, msg
    assert leader_after < _GRACE_S, msg
