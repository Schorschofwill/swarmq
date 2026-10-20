"""Gesamtdeckel auf jedem Publish-Vorgang.

Plan 2026-08-28-001, U2 (R1, R2, R4, R10, R11).

Der Referenzlauf vom 2026-08-28 stand 62 Minuten still, weil ein Publish
auf einem still toten Transport unbegrenzt auf sein Publisher-Confirm
wartete. Der bestehende Wiederholpfad greift dort nicht: Es fliegt keine
Ausnahme, und der Kanal gilt weiter als offen.

Testlücken Runde 4 (Plan 2026-10-20-1500, U4, Lücke E-12), virtuelle Uhr:
# [x] belegte Kanalsperre: der Verwurf gibt nach seiner Frist auf, der Aufrufer kehrt zurück
# [x] ein hängendes Schließen des verworfenen Kanals endet nach seiner eigenen Frist
# [x] ein beendetes Schließen wird nicht festgehalten (weakref + gc)
"""

import asyncio
import gc
import weakref
from unittest.mock import AsyncMock, Mock, patch

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError
from tests.support.rabbitmq_faults import hanging_publish, publish_failure
from tests.support.virtual_time import on_virtual_clock


def _broker(timeout: float = 0.05) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock()
    broker.publish_timeout_s = timeout
    return broker


class TestDeadlineEnforcement:
    async def test_hanging_publish_returns_within_deadline(self):
        """Covers AE1."""
        broker = _broker()
        with hanging_publish(broker), pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(broker.publish("tasks", b"{}"), timeout=2.0)

    async def test_error_names_the_elapsed_budget_and_target(self):
        """Ein Deckelbruch muss sich von einem gewoehnlichen
        Verbindungsabbruch unterscheiden lassen."""
        broker = _broker()
        with hanging_publish(broker), pytest.raises(BrokerConnectionError) as excinfo:
            await broker.publish("tasks", b"{}")
        message = str(excinfo.value)
        assert "0.05" in message
        assert "tasks" in message

    async def test_retries_that_together_exceed_the_deadline_abort(self):
        """Der Deckel umspannt den ganzen Vorgang, nicht den Einzelversuch:
        die fuenf Fehlversuche duerfen ihn nicht ueberdauern."""
        broker = _broker(timeout=0.02)
        with publish_failure(broker), pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(broker.publish("tasks", b"{}"), timeout=2.0)

    async def test_healthy_publish_is_untouched(self):
        broker = _broker(timeout=5.0)
        channel = AsyncMock(is_closed=False)
        broker._publish_channel = channel

        receipt = await broker.publish("tasks", b"{}")

        assert receipt.queue == "tasks"
        assert broker._publish_channel is channel


class TestChannelDiscard:
    """R10 — ohne Verwurf bliebe die tote Verbindung stehen und jeder
    Folge-Publish verbrennte erneut die volle Frist."""

    async def test_channel_is_discarded_after_deadline(self):
        broker = _broker()
        with hanging_publish(broker):
            with pytest.raises(BrokerConnectionError):
                await broker.publish("tasks", b"{}")
            assert broker._publish_channel is None

    async def test_hanging_close_does_not_hold_the_caller(self):
        """KTD3: Das Schliessen kann auf dem toten Transport selbst haengen,
        darum wird es nebenlaeufig angestossen statt abgewartet."""
        broker = _broker()
        with hanging_publish(broker, close_hangs=True):
            with pytest.raises(BrokerConnectionError):
                await asyncio.wait_for(broker.publish("tasks", b"{}"), timeout=2.0)
            assert broker._publish_channel is None

    async def test_discard_leaves_a_concurrently_opened_channel_alone(self):
        """Ohne Identitaetspruefung wuerfe der Verwurf den Kanal weg, den
        eine parallele Ausfuehrung gerade frisch geoeffnet hat."""
        broker = _broker()
        with hanging_publish(broker) as hanging:
            publish_task = asyncio.create_task(broker.publish("tasks", b"{}"))
            await hanging.default_exchange.publish_started.wait()
            fresh = AsyncMock(is_closed=False)
            broker._publish_channel = fresh

            with pytest.raises(BrokerConnectionError):
                await publish_task

            assert broker._publish_channel is fresh


class TestDiscardDeadlines:
    """Lücke E-12: Fristen und Aufräumen des Verwurfs nach einem Deadline-Bruch."""

    @pytest.mark.timeout(5)
    @on_virtual_clock
    async def test_a_busy_channel_lock_skips_the_discard_after_its_deadline(self, swarmq_logs):
        """Mutant ``connection.py:553`` ``timeout=_CHANNEL_CLOSE_TIMEOUT_S`` →
        ``timeout=None``: Der Aufrufer stünde nach seiner Frist weiter.

        Seam: die private Kanalsperre ``_channel_lock`` (Plan, Spalte
        „privat“). Sie bildet ein hängendes ``Channel.Open`` an der Grenze ab,
        das über die öffentliche API nicht gleichzeitig erreichbar ist."""
        broker = _broker(timeout=1.0)
        loop = asyncio.get_running_loop()
        with hanging_publish(broker) as hanging:
            publishing = asyncio.create_task(broker.publish("tasks", b"{}"))
            await hanging.default_exchange.publish_started.wait()
            await broker._channel_lock.acquire()
            try:
                start = loop.time()
                with pytest.raises(BrokerConnectionError):
                    await asyncio.wait_for(publishing, timeout=60.0)
                elapsed = loop.time() - start
            finally:
                broker._channel_lock.release()

        assert elapsed == pytest.approx(1.0 + 5.0)  # Publish-Frist plus Frist der Sperre
        [skipped] = [e for e in swarmq_logs if e["event"] == "publish_channel_discard_skipped"]
        assert skipped["reason"] == "lock_busy"
        assert hanging.closed is False

    @pytest.mark.timeout(5)
    @on_virtual_clock
    async def test_a_hanging_close_of_the_discarded_channel_ends_after_its_own_deadline(self, swarmq_logs):
        """Mutant ``connection.py:638`` ``asyncio.timeout(_CHANNEL_CLOSE_TIMEOUT_S)``
        → ``asyncio.timeout(None)``: Der Schließ-Task hinge bis ``close()``."""
        broker = _broker(timeout=1.0)
        with hanging_publish(broker, close_hangs=True):
            before = asyncio.all_tasks()
            with pytest.raises(BrokerConnectionError):
                await broker.publish("tasks", b"{}")
            [closer] = asyncio.all_tasks() - before

        _done, pending = await asyncio.wait({closer}, timeout=6.0)

        assert pending == set()
        [incomplete] = [e for e in swarmq_logs if e["event"] == "publish_channel_close_incomplete"]
        assert incomplete["error_type"] == "TimeoutError"

    @pytest.mark.timeout(5)
    @on_virtual_clock
    async def test_a_finished_close_of_the_discarded_channel_is_not_kept(self):
        """Mutant ``connection.py:584`` Zeile
        ``task.add_done_callback(self._pending_channel_closes.discard)``
        gestrichen: Jeder Verwurf hinterließe einen beendeten Task im Broker,
        ein Leck über die Laufzeit des Workers.

        Laufzeit: Allein ~10 ms; in der vollen Unit-Suite bis ~150 ms, weil
        ``gc.collect()`` den ganzen Prozess durchläuft. Ohne volle Sammlung
        ließe sich das Leck nicht zeigen (wie
        ``test_publish_channel_closing.py::test_a_finished_publish_child_is_not_kept_alive_by_its_channel``)."""
        broker = _broker(timeout=1.0)
        release = asyncio.Event()
        with hanging_publish(broker) as hanging:

            async def slow_close() -> None:
                await release.wait()
                hanging.closed = True

            hanging.close = slow_close  # type: ignore[method-assign]  # Test hält das Schließen an
            before = asyncio.all_tasks()
            with pytest.raises(BrokerConnectionError):
                await broker.publish("tasks", b"{}")
            [closer] = asyncio.all_tasks() - before
        finished = asyncio.Event()
        closer.add_done_callback(lambda _task: finished.set())
        closer_ref = weakref.ref(closer)
        del closer  # der Test selbst hält den Task nicht fest
        release.set()
        await finished.wait()
        assert hanging.closed is True

        gc.collect()

        assert closer_ref() is None


class TestAllPublishPaths:
    """R2 — die Regel bindet an die Broker-Naht, nicht an eine feste Liste
    von Aufrufstellen. Scheduler-Tick und Workflow-Folgeschritt gehen ueber
    dasselbe ``publish``."""

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("retry_later", {"delay_ms": 1000, "headers": {}}),
            ("dead_letter", {"headers": {}}),
        ],
    )
    async def test_worker_internal_paths_share_the_deadline(self, method, kwargs):
        broker = _broker()
        delivery = Mock(queue="tasks", body=b"{}")
        with hanging_publish(broker), pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(getattr(broker, method)(delivery, **kwargs), timeout=2.0)

    async def test_scheduler_and_workflow_use_the_same_seam(self):
        """Beide rufen ``broker.publish`` — derselbe gedeckelte Pfad wie der
        Client-Publish, ohne eigene Sonderbehandlung."""
        broker = _broker()
        with hanging_publish(broker), pytest.raises(BrokerConnectionError):
            await asyncio.wait_for(
                broker.publish("schedules", b"{}", headers={"x-swarmq-schedfire": "1"}),
                timeout=2.0,
            )


class TestTerminalDlxPathIsUntouched:
    async def test_route_via_dlx_does_not_go_through_the_capped_path(self):
        """Covers AE5. Der terminale Lock-Gate-Ausweg nackt nur — er
        veroeffentlicht nichts und laeuft daher nicht durch den Deckel.
        Die Nachricht landet ueber die Queue-Argumente in ``{queue}.XQ``
        und wird NICHT erneut ausgefuehrt; das unterscheidet ihn von der
        Redelivery aus AE2 und haelt ihn ausserhalb des Zustellbudgets.
        Siehe docs/solutions/logic-errors/
        nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md.
        """
        broker = _broker()
        delivery = Mock(queue="tasks", body=b"{}")
        message = AsyncMock()
        with (
            patch("swarmq.broker.rabbitmq.admin.message_of", return_value=message) as message_of,
            hanging_publish(broker) as hanging,
        ):
            await broker.route_via_dlx(delivery)

        assert message_of.called
        message.nack.assert_awaited_once_with(requeue=False)
        assert not hanging.default_exchange.publish_started.is_set()


class TestAckOrderingSurvives:
    async def test_dead_letter_does_not_ack_when_the_deadline_breaks(self):
        """Covers AE2. Veroeffentlichen-dann-Quittieren muss den Umbau
        ueberleben: eine unbestaetigte Nachricht wird redelivert statt
        still verloren zu gehen."""
        broker = _broker()
        delivery = Mock(queue="tasks", body=b"{}")
        with patch("swarmq.broker.rabbitmq.delivery.message_of") as message_of:
            with hanging_publish(broker), pytest.raises(BrokerConnectionError):
                await broker.dead_letter(delivery, headers={})
            message_of.return_value.ack.assert_not_called()

    async def test_retry_later_does_not_ack_when_the_deadline_breaks(self):
        broker = _broker()
        delivery = Mock(queue="tasks", body=b"{}")
        with patch("swarmq.broker.rabbitmq.delivery.message_of") as message_of:
            with hanging_publish(broker), pytest.raises(BrokerConnectionError):
                await broker.retry_later(delivery, delay_ms=1000, headers={})
            message_of.return_value.ack.assert_not_called()
