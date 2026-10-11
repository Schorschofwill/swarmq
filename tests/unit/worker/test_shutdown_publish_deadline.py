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
        self,
        worker,
        broker,
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
        self,
        worker,
        broker,
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

    async def test_short_grace_period_still_leaves_a_budget(self, worker, broker):
        """Eine knapp konfigurierte Gnadenfrist darf nicht auf ein Nullbudget
        fallen. Sonst scheitert im Drain jeder Retry- und Dead-Letter-Publish
        rein rechnerisch, und alle laufenden Tasks kommen als Redelivery
        zurueck statt verzoegert oder aussortiert zu werden."""
        worker._config.shutdown_timeout = 5
        assert worker._shutdown_publish_budget() == pytest.approx(2.5)

        worker._config.shutdown_timeout = 30
        assert worker._shutdown_publish_budget() == pytest.approx(25.0)

    async def test_zero_grace_period_yields_zero_budget(self, worker):
        """``shutdown_timeout=0`` heisst im Repo "sofort abbrechen" — das
        bleibt so, der anteilige Rueckfall aendert nur knappe Werte."""
        worker._config.shutdown_timeout = 0
        assert worker._shutdown_publish_budget() == 0.0

    async def test_deadline_is_cleared_even_when_shutdown_raises(
        self,
        worker,
        broker,
    ):
        """Ohne try/finally bliebe die Frist in der Vergangenheit stehen und
        ein eingebetteter Nutzer bekaeme danach bei jedem Publish sofort
        einen Fehler — auf einer voellig gesunden Verbindung."""
        worker._state = WorkerState.RUNNING
        deadlines: list[float | None] = []
        broker.set_publish_deadline.side_effect = deadlines.append
        broker.close.side_effect = RuntimeError("connection already gone")

        worker._request_shutdown()
        with pytest.raises(RuntimeError, match="connection already gone"):
            await worker._execute_shutdown()

        assert deadlines[-1] is None

    async def test_broker_without_the_contract_member_does_not_break_shutdown(
        self,
        worker,
        broker,
    ):
        """``set_publish_deadline`` kam erst mit diesem Plan in den
        Publisher-Vertrag. Ein vorher geschriebener Fremdbroker darf die
        Abbausequenz nicht mit AttributeError abbrechen — sonst bleiben
        Sperren gehalten und Verbindungen offen."""
        worker._state = WorkerState.RUNNING
        del broker.set_publish_deadline

        worker._request_shutdown()
        await worker._execute_shutdown()

        broker.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED

    async def test_deadline_is_anchored_before_the_signal_dispatch(
        self,
        worker,
        broker,
    ):
        """Die Uhr des Supervisors laeuft ab SIGTERM. Ein langsamer
        Shutdown-Handler darf die Endfrist nicht hinter dessen Eskalation
        schieben."""
        worker._state = WorkerState.RUNNING
        deadlines: list[float | None] = []
        broker.set_publish_deadline.side_effect = deadlines.append

        async def _slow_dispatch(*args, **kwargs):
            await asyncio.sleep(0.2)

        worker._signal_dispatcher.dispatch = _slow_dispatch

        loop = asyncio.get_running_loop()
        before = loop.time()
        worker._request_shutdown()
        await worker._execute_shutdown()

        budget = deadlines[0] - before
        assert budget == pytest.approx(worker._shutdown_publish_budget(), abs=0.1), (
            "die Endfrist wurde erst nach dem Dispatch verankert und liegt damit um dessen Dauer zu spaet"
        )

    async def test_deadline_is_set_before_the_drain(self, worker, broker):
        """Reihenfolge zaehlt: Wird die Frist erst nach dem Drain gesetzt,
        haben die haengenden Publishes die Gnadenfrist bereits aufgebraucht."""
        worker._state = WorkerState.RUNNING
        order: list[str] = []
        broker.set_publish_deadline.side_effect = lambda at: order.append("clear" if at is None else "set")
        broker.cancel_consume.side_effect = lambda *a, **kw: order.append("drain")

        worker._request_shutdown()
        await worker._execute_shutdown()

        assert order.index("set") < order.index("drain")
