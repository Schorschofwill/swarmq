"""Der Worker koppelt die Publish-Deadline an seine Gnadenfrist.

Plan 2026-08-28-001, U3 (R12 / AE7).

Das Problembild: Der Referenzlauf liess sich nach dem haengenden Publish
"auch per SIGTERM nicht mehr beenden". Ein Deckel allein loest das nicht —
der Supervisor eskaliert nach seiner Gnadenfrist, waehrend der Publish noch
gegen seine eigene, deutlich laengere Frist laeuft.
"""

import asyncio

import pytest

from swarmq.worker import WorkerState
from swarmq.worker.lifecycle import _SHUTDOWN_PUBLISH_RESERVE_S


class TestShutdownCouplesTheDeadline:
    async def test_shutdown_sets_a_deadline_below_the_grace_period(
        self, worker, broker,
    ):
        """Covers AE7. Die Endfrist muss eine Reserve unter der Gnadenfrist
        liegen: Sperrenfreigabe, Scheduler-Stopp und Verbindungsschluss
        kommen NACH dem Drain und brauchen noch Budget."""
        worker._state = WorkerState.RUNNING
        deadlines: list[float | None] = []
        broker.set_publish_deadline.side_effect = deadlines.append

        loop = asyncio.get_running_loop()
        before = loop.time()
        worker._request_shutdown()
        await worker._execute_shutdown()

        assert deadlines, "das Herunterfahren hat keine Endfrist gesetzt"
        first = deadlines[0]
        assert first is not None
        budget = first - before
        grace = float(worker._config.shutdown_timeout)
        assert budget == pytest.approx(grace - _SHUTDOWN_PUBLISH_RESERVE_S, abs=0.5)
        assert budget < grace

    async def test_deadline_is_cleared_when_shutdown_completes(
        self, worker, broker,
    ):
        """Ein eingebetteter Nutzer reiht ueber dieselbe Instanz weiter Tasks
        ein. Eine liegengebliebene, laengst verstrichene Endfrist liesse
        danach jeden Publish sofort scheitern."""
        worker._state = WorkerState.RUNNING
        deadlines: list[float | None] = []
        broker.set_publish_deadline.side_effect = deadlines.append

        worker._request_shutdown()
        await worker._execute_shutdown()

        assert deadlines[-1] is None

    async def test_deadline_is_set_before_the_drain(self, worker, broker):
        """Reihenfolge zaehlt: Wird die Frist erst nach dem Drain gesetzt,
        haben die haengenden Publishes die Gnadenfrist bereits aufgebraucht."""
        worker._state = WorkerState.RUNNING
        order: list[str] = []
        broker.set_publish_deadline.side_effect = lambda at: order.append(
            "clear" if at is None else "set"
        )
        broker.cancel_consume.side_effect = lambda *a, **kw: order.append("drain")

        worker._request_shutdown()
        await worker._execute_shutdown()

        assert order.index("set") < order.index("drain")
