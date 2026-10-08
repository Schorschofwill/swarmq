"""Schedfire-Dedup-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle.

Test-Liste 0.9.0 (Schedfire-Claim bei Nicht-Ausfuehrung, Claim-Token):
1. Stellt ein spaeteres Prae-Pipeline-Gate (Cancel-Check) die Nachricht nach
   gewonnenem Claim zurueck, wird der Claim freigegeben.
2. Dasselbe fuer den Dead-Letter des Delivery-Count-Gates.
3. Der Claim laeuft mit einem frischen Owner-Token; ohne Claim-Header gibt es
   keine Uebernahme (``takeover=None``).
4. Traegt die Zustellung ``x-swarmq-schedfire-claim``, bietet das Gate dieses
   Token zur Uebernahme an.
5. Die zurueckgestellte Kopie traegt das Owner-Token als
   ``x-swarmq-schedfire-claim`` -- sie erkennt den Claim als ihren, auch
   wenn die Freigabe am Backend-Ausfall scheitert.
6. Die nicht-terminale Freigabe im finally ist owner-geprueft (Token).
7. Verlorener Claim bleibt Duplikat (Ack, kein Lauf) -- auch mit fremdem Token.

Test-Liste 0.9.0 (Zeitlimit):
8. Haengender Completed-Check (Blackhole) -> nach ``backend_op_timeout``
   zurueckgestellt (``stage=completed_check``).
9. Haengender In-Flight-Claim -> ebenso (``stage=inflight_claim``).

Test-Liste 0.9.0 (drittes Review):
10. Wirft ein Prae-Pipeline-Gate nach gewonnenem Claim, wird der Claim
    nicht-terminal (owner-geprueft) freigegeben und die Ausnahme weiter-
    geworfen; eine spaetere Zustellung finalisiert ihn nicht.
11. Claim mit unklarem Ausgang (``claim_fire`` wirft ``BackendConnectionError``,
    Valkey hat den Claim vielleicht geschrieben): die zurueckgestellte Kopie
    traegt das Owner-Token, das der Aufruf geschrieben haben kann.
12. Traegt die Zustellung schon ein Token, bietet die Kopie nach unklarem
    Ausgang beide an (neues zuerst) -- der Claim kann noch das alte tragen.
13. Verlorener Claim (``False``) haelt keinen Claim am Kontext.
14. Nach gewonnener Uebernahme traegt eine spaetere Kopie nur das neue Token
    (der Claim traegt sicher nur noch dieses).
15. Eine Zustellung mit mehreren Tokens bietet alle zur Uebernahme an.
16. Die Kandidatenliste ist gedeckelt (die aeltesten fallen weg).
17. Fehlerantwort beim Claim (``BackendResponseError``) -> kein Ausfall:
    Requeue an derselben Stelle, keine Zurueckstellung.
18. Pre-Start-Cancel finalisiert terminal (Completed-Marker); die nicht-
    terminale Finalisierung des Runners danach ist ein No-op -- der Claim
    wird nicht mehr freigegeben, der Marker bleibt.

Test-Liste 0.9.0 (viertes Review):
19. Ein gefaelschter oder kaputter Header mit beliebig vielen Tokens geht
    nicht ungedeckelt als Lua-ARGV an den Claim: das Gate bietet hoechstens
    ``MAX_SCHEDFIRE_CLAIM_TOKENS`` (die neuesten) an.
20. Liste 10 auch fuer ``asyncio.CancelledError`` (Shutdown bricht einen
    Handler im Gate ab): Freigabe owner-geprueft, kein Completed-Marker,
    der Abbruch propagiert.
21. Ein zweiter Abbruch waehrend dieser Freigabe unterbricht sie nicht: sie
    laeuft (durch ``backend_op_timeout`` begrenzt) zu Ende, der Abbruch
    propagiert trotzdem.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
import structlog

from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,  # noqa: F401
    _default_payload,
)

pytestmark = pytest.mark.timeout(5)


class TestSchedfireDedup:
    """Scheduled-fire messages carry an ``x-swarmq-schedfire`` header.

    The worker SETNXs a key derived from that header before entering
    the normal pipeline. Duplicates (two deliveries of the same fire)
    get ack'd and skipped silently.

    Dedup runs BEFORE cancel-check so a duplicate fire never even
    enters the cancel-scan. Both stages run BEFORE the semaphore acquire.
    """

    async def test_no_header_skips_dedup(self, broker, backend, config):

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={},  # no x-swarmq-schedfire
        )
        await w._handle_message(msg)

        backend.schedules.mark_fire_completed.assert_not_awaited()
        backend.store_result.assert_awaited_once()

    async def test_first_delivery_runs_task(self, broker, backend, config):
        """Unit 1 two-marker flow: completed=False, inflight-SETNX wins,
        task runs, finally block writes the completed marker with
        long TTL."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = True  # claim wins
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        backend.schedules.is_fire_completed.assert_awaited_once_with(
            "cron_a:1700000000000"
        )
        # Inflight TTL = scheduler_inflight_ttl * 1000 = 300 * 1000
        # (no hard_timeout on NoopTask → config value applies).
        inflight_call = backend.schedules.claim_fire.await_args
        assert inflight_call.args[0] == "cron_a:1700000000000"
        assert inflight_call.args[1] == 300_000
        # Task ran
        backend.store_result.assert_awaited_once()
        # finally-block wrote the long-TTL completed marker
        dedup_call = backend.schedules.mark_fire_completed.await_args
        assert dedup_call.args[0] == "cron_a:1700000000000"
        # dedup_ttl * 1000 = 3900 * 1000
        assert dedup_call.args[1] == 3_900_000

    async def test_inflight_ttl_covers_long_hard_timeout(
        self, broker, backend, config,
    ):
        """Härtung R2 P1-6: die fixe 300s-inflight-TTL war der einzige
        Split-Brain-Schutz — lief sie WÄHREND eines langen Laufs ab
        (hard_timeout > 300s ist erlaubt), gewann eine Stale-Leader-
        Kopie das frei gewordene SETNX → Doppelausführung desselben
        Fires. Die TTL muss die maximal erlaubte Laufzeit überdecken:
        max(scheduler_inflight_ttl, hard_timeout + 30s) — dasselbe
        Muster wie compute_lock_ttl_ms (P0-A-Fix 2026-06-02)."""

        class LongTask(Task):
            task_info = TaskInfo(
                name="long", queues=["default"], hard_timeout=600,
            )

            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(LongTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = True
        msg = make_message(
            _default_payload(name="long", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "long_cron:1700000000000"},
        )
        await w._handle_message(msg)

        inflight_call = backend.schedules.claim_fire.await_args
        # (600 + 30) * 1000 — outlives the watchdog-bounded runtime.
        assert inflight_call.args[1] == 630_000

    async def test_duplicate_when_completed_marker_set(
        self, broker, backend, config
    ):
        """Redelivery after a prior successful fire sees the completed
        marker and ack-skips without claiming in-flight."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on duplicate")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = True  # prior run
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.schedules.claim_fire.assert_not_awaited()
        backend.store_result.assert_not_awaited()

    async def test_duplicate_when_inflight_claim_lost(
        self, broker, backend, config
    ):
        """Another worker is running the same fire right now. Ack+skip
        without running. When that worker completes, the completed
        marker blocks future redeliveries; if it crashes, the in-flight
        TTL expires and the next redelivery can re-run."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on inflight-claim-lost")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = False  # someone else has it
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.store_result.assert_not_awaited()
        # No long-TTL write because we didn't own the run.
        backend.schedules.mark_fire_completed.assert_not_awaited()

    async def test_dedup_backend_error_nacks_for_requeue(
        self, broker, backend, config
    ):
        """A Valkey failure during dedup-check must requeue the message
        — we cannot assume either new or duplicate."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("must not run on backend error")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        from swarmq.exceptions import BackendConnectionError as BCE
        # First stage in the two-marker flow is completed-check;
        # fail it to exercise the nack path.
        backend.schedules.is_fire_completed.side_effect = BCE("valkey down")
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        # Zurueckstellung ueber die Delay-Queue statt Requeue an derselben
        # Stelle; die Kopie traegt den Schedfire-Schluessel weiter.
        broker.retry_later.assert_awaited_once()
        assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire"] == "cron_a:1700000000000"
        broker.redeliver.assert_not_awaited()

        # Unit 9 / Fork A2: no header mutation on the nack path — AMQP
        # nack-with-requeue does not transmit in-memory header changes
        # back to the broker, so a counter header would be broken by
        # construction. Observability is via the structured
        # schedfire_dedup_backend_error log event (visible in stderr
        # capture above) — not via headers.
        assert "x-swarmq-schedfire-dedup-count" not in msg.headers

    async def test_dedup_backend_error_does_not_touch_existing_headers(
        self, broker, backend, config
    ):
        """Unit 9 guard: pre-existing headers on the message (e.g. an
        operator-injected tracking header) pass through unchanged — we
        don't mutate the message object on the nack path at all."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop_pass", queues=["default"])
            async def run(self):
                raise AssertionError("must not run on backend error")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        from swarmq.exceptions import BackendConnectionError as BCE
        backend.schedules.mark_fire_completed.side_effect = BCE("valkey down")
        original_headers = {
            "x-swarmq-schedfire": "cron_a:1700000000000",
            "x-tracking-id": "abc-123",
        }
        msg = make_message(
            _default_payload(name="noop_pass", args=[], kwargs={}),
            headers=dict(original_headers),
        )
        await w._handle_message(msg)

        # Headers unchanged — no mutation, no counter addition
        assert msg.headers == original_headers

    async def test_claim_is_released_when_cancel_check_defers(self, worker, broker, backend):
        """Liste 1: Fiel Valkey zwischen ``claim_fire`` und Cancel-Check aus,
        blieb der Claim stehen; die zurueckgestellte Kopie sah ihn und wurde
        als Duplikat bestaetigt -- der Fire ging verloren."""
        from swarmq.exceptions import BackendConnectionError as BCE

        backend.is_cancelled.side_effect = BCE("valkey down")
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        backend.schedules.release_fire_claim.assert_awaited_once()
        assert backend.schedules.release_fire_claim.await_args.args[0] == "cron_a:1700000000000"
        backend.schedules.mark_fire_completed.assert_not_awaited()

    async def test_deferred_copy_carries_claim_token(self, worker, broker, backend):
        """Liste 5: scheitert auch die Freigabe (Valkey weg), erkennt die
        Kopie den Claim an diesem Token als ihren."""
        from swarmq.exceptions import BackendConnectionError as BCE

        backend.is_cancelled.side_effect = BCE("valkey down")
        backend.schedules.release_fire_claim.side_effect = BCE("valkey down")
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        headers = broker.retry_later.await_args.kwargs["headers"]
        assert headers["x-swarmq-schedfire-claim"] == owner
        assert headers["x-swarmq-schedfire"] == "cron_a:1700000000000"

    async def test_release_is_owner_checked(self, worker, broker, backend):
        """Liste 6: eine verspaetete Freigabe darf den Claim, den die Kopie
        inzwischen uebernommen hat, nicht loeschen."""
        from swarmq.exceptions import BackendConnectionError as BCE

        backend.is_cancelled.side_effect = BCE("valkey down")
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        assert backend.schedules.release_fire_claim.await_args.kwargs["owner"] == owner

    async def test_claim_is_released_when_delivery_count_gate_dead_letters(self, worker, broker, backend, config):
        """Liste 2: der Dead-Letter des Delivery-Count-Gates endet ebenfalls
        vor dem finally; der Claim darf nicht bis zur TTL stehen bleiben."""
        msg = make_message(
            _default_payload(),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000", "x-delivery-count": config.delivery_limit},
        )

        await worker._handle_message(msg)

        broker.dead_letter.assert_awaited_once()
        backend.schedules.release_fire_claim.assert_awaited_once()

    async def test_claim_is_released_when_a_later_gate_raises(self, worker, broker, backend, swarmq_logs):
        """Liste 10: blieb der Claim im Register liegen, konnte eine spaetere
        Zustellung mit wiederverwendeter ``id()`` ihn als erledigt markieren.
        Seit B22 entweicht der Fehler nicht mehr, er wird geloggt."""
        backend.is_cancelled.side_effect = RuntimeError("boom")
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        [failed] = [e for e in swarmq_logs if e["event"] == "delivery_handler_failed"]
        assert failed["error"] == "boom"

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        backend.schedules.release_fire_claim.assert_awaited_once_with("cron_a:1700000000000", owner=owner)
        backend.schedules.mark_fire_completed.assert_not_awaited()

    async def test_claim_is_released_when_a_later_gate_is_cancelled(self, worker, broker, backend):
        """Liste 20."""
        backend.is_cancelled.side_effect = asyncio.CancelledError()
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        with pytest.raises(asyncio.CancelledError):
            await worker._handle_message(msg)

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        backend.schedules.release_fire_claim.assert_awaited_once_with("cron_a:1700000000000", owner=owner)
        backend.schedules.mark_fire_completed.assert_not_awaited()

    async def test_second_cancel_does_not_interrupt_the_claim_release(self, worker, broker, backend):
        """Liste 21: sonst stuende der Claim bis zur TTL, und die Redelivery
        nach dem Shutdown waere ein Duplikat (verlorener Fire)."""
        release_started, may_finish, released = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def _release(key, *, owner):
            release_started.set()
            await may_finish.wait()
            released.set()

        backend.is_cancelled.side_effect = asyncio.CancelledError()
        backend.schedules.release_fire_claim.side_effect = _release
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})
        handler = asyncio.create_task(worker._handle_message(msg))
        await release_started.wait()

        handler.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handler
        may_finish.set()

        await asyncio.wait_for(released.wait(), timeout=1)

    async def test_pre_start_cancel_completes_the_fire_and_releases_nothing(self, worker, broker, backend):
        """Liste 18."""
        backend.is_cancelled.return_value = True
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        backend.schedules.mark_fire_completed.assert_awaited_once()
        assert backend.schedules.mark_fire_completed.await_args.args[0] == "cron_a:1700000000000"
        backend.schedules.release_fire_claim.assert_not_awaited()
        broker.ack.assert_awaited_once_with(msg)

    async def test_worker_start_instantiates_scheduler(
        self, broker, backend, config, monkeypatch
    ):
        """Worker.start() must construct a SchedulerLoop after the
        backend + lock-manager are ready and call scheduler.start()."""
        from swarmq.worker import lifecycle as worker_mod  # U9: SchedulerLoop lebt im Lifecycle-Modul

        class NoopTask(Task):
            task_info = TaskInfo(name="noop_sched", queues=["default"])
            async def run(self):
                return None

        registry = TaskRegistry()
        registry.register(NoopTask)

        # Patch SchedulerLoop so we can observe start() was called
        created = []
        class _FakeSched:
            def __init__(self, **kwargs):
                created.append(kwargs)
                self.started = False
                self.stopped = False
            async def start(self):
                self.started = True
            async def stop(self):
                self.stopped = True

        monkeypatch.setattr(worker_mod, "SchedulerLoop", _FakeSched)
        # Patch broker.consume so start() returns from the consume loop
        # — we shut down immediately after start() assembles the pipeline.
        async def _noop_consume(*a, **kw): return None
        broker.consume = AsyncMock(side_effect=_noop_consume)

        w = Worker(broker=broker, backend=backend, registry=registry, config=config)
        # Fire shutdown before start() blocks on _shutdown_event
        w._shutdown_event.set()

        await w.start()

        assert len(created) == 1
        assert w._scheduler is not None
        assert w._scheduler.started is True
        assert w._scheduler.stopped is True

    async def test_dedup_runs_before_cancel_check(
        self, broker, backend, config
    ):
        """Duplicate schedfire must ack+skip BEFORE cancel-check is reached.
        This guarantees the pipeline-ordering-discipline documented in
        docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-*."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = True  # prior fire
        backend.is_cancelled.side_effect = AssertionError(
            "cancel-check must not run for a duplicate schedfire"
        )
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# R3.5 Unique Tasks — Worker release (Units 10 + 11)
# ---------------------------------------------------------------------------


class TestSchedfireGateOutcomes:
    """Direkte Gate-Tests (Plan U7)."""

    def _ctx(self):
        return GateContext(task_id="abc-123", task_name="add", queue="default")

    async def test_without_header_is_continue(self, worker):
        assert await worker._schedfire_gate.apply(make_message(_default_payload()), self._ctx()) is Continue

    async def test_completed_marker_is_terminal_without_execution(self, worker, broker, backend):
        backend.schedules.is_fire_completed = AsyncMock(return_value=True)
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) == Terminal.never_release()
        broker.ack.assert_awaited_once_with(msg)
        backend.schedules.claim_fire.assert_not_awaited()

    async def test_won_claim_is_continue_and_records_claim(self, worker, backend):
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        ctx = self._ctx()
        assert await worker._schedfire_gate.apply(msg, ctx) is Continue
        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        assert ctx.schedfire_claim is not None
        assert (ctx.schedfire_claim.key, ctx.schedfire_claim.owner) == ("s:1", owner)

    async def test_claim_uses_fresh_owner_token_without_takeover(self, worker, backend):
        """Liste 3."""
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())
        kwargs = backend.schedules.claim_fire.await_args.kwargs
        assert isinstance(kwargs["owner"], str) and kwargs["owner"]
        assert kwargs["takeover"] == ()

    async def test_claim_offers_carried_token_for_takeover(self, worker, backend):
        """Liste 4: die Kopie traegt das Token ihres Vorgaengers."""
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": "tok-a"},
        )
        await worker._schedfire_gate.apply(msg, self._ctx())
        kwargs = backend.schedules.claim_fire.await_args.kwargs
        assert kwargs["takeover"] == ("tok-a",)
        assert kwargs["owner"] != "tok-a"

    async def test_inflight_ttl_grows_with_hard_timeout(self, worker, backend, config):
        worker._registry.register(LongTask)
        msg = make_message(_default_payload(name="long"), headers={"x-swarmq-schedfire": "s:2"})
        ctx = GateContext(task_id="abc-123", task_name="long", queue="default")
        await worker._schedfire_gate.apply(msg, ctx)
        ttl_ms = backend.schedules.claim_fire.await_args.args[1]
        assert ttl_ms == max(config.scheduler_inflight_ttl, 900 + 30) * 1000

    async def test_inflight_claim_backend_error_is_deferred(self, worker, broker, backend):
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Redelivered
        broker.retry_later.assert_awaited_once()
        broker.redeliver.assert_not_awaited()

    async def test_copy_after_indeterminate_claim_carries_the_attempted_owner(self, worker, broker, backend):
        """Liste 11."""
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("reply lost"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire-claim"] == owner

    async def test_copy_after_indeterminate_takeover_offers_both_tokens(self, worker, broker, backend):
        """Liste 12."""
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("reply lost"))
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": "tok-a"},
        )
        await worker._schedfire_gate.apply(msg, self._ctx())

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire-claim"] == f"{owner},tok-a"

    async def test_copy_after_won_takeover_carries_only_the_new_token(self, worker, broker, backend):
        """Liste 14."""
        from swarmq.exceptions import BackendConnectionError

        backend.is_cancelled.side_effect = BackendConnectionError("valkey down")
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": "tok-a"},
        )
        await worker._handle_message(msg)

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire-claim"] == owner

    async def test_claim_offers_every_carried_token_for_takeover(self, worker, backend):
        """Liste 15."""
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": "tok-b, tok-a,"},
        )
        await worker._schedfire_gate.apply(msg, self._ctx())

        assert backend.schedules.claim_fire.await_args.kwargs["takeover"] == ("tok-b", "tok-a")

    async def test_claim_offers_at_most_the_capped_number_of_tokens(self, worker, backend):
        """Liste 19."""
        from swarmq.worker.gates import MAX_SCHEDFIRE_CLAIM_TOKENS

        carried = [f"tok-{n}" for n in range(10_000)]
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": ",".join(carried)},
        )
        await worker._schedfire_gate.apply(msg, self._ctx())

        offered = backend.schedules.claim_fire.await_args.kwargs["takeover"]
        assert offered == tuple(carried[:MAX_SCHEDFIRE_CLAIM_TOKENS])

    async def test_copy_caps_the_carried_tokens(self, worker, broker, backend):
        """Liste 16: bei jedem Claim mit unklarem Ausgang kommt ein Token
        dazu; der Header bleibt gedeckelt, die aeltesten fallen weg."""
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("reply lost"))
        carried = [f"tok-{n}" for n in range(20)]
        msg = make_message(
            _default_payload(), headers={"x-swarmq-schedfire": "s:1", "x-swarmq-schedfire-claim": ",".join(carried)},
        )
        await worker._schedfire_gate.apply(msg, self._ctx())

        owner = backend.schedules.claim_fire.await_args.kwargs["owner"]
        tokens = broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire-claim"].split(",")
        assert tokens == [owner, *carried[:15]]

    async def test_claim_error_reply_is_requeued_not_deferred(self, worker, broker, backend):
        """Liste 17."""
        from swarmq.exceptions import BackendResponseError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendResponseError("WRONGTYPE"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Redelivered
        broker.retry_later.assert_not_awaited()
        broker.redeliver.assert_awaited_once_with(msg)

    async def test_lost_claim_holds_no_claim(self, worker, broker, backend):
        """Liste 13: ein Duplikat gibt keinen fremden Claim frei."""
        backend.schedules.claim_fire = AsyncMock(return_value=False)
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})

        await worker._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.schedules.release_fire_claim.assert_not_awaited()

    async def test_completed_check_backend_error_logs_one_deferral_line(self, worker, backend, swarmq_logs):
        """Plan 2026-10-06-0831, U9 (R17, KTD11): die Zurueckstellung erholt
        sich selbst -> WARNING; 0.9.0: eine Zeile mit Stufe und Fire-Key."""
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.is_fire_completed = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())

        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["event"] == "task_deferred_backend_unavailable" and entry["gate"] == "schedfire"
        assert entry["stage"] == "completed_check" and entry["schedfire_key"] == "s:1"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    async def test_inflight_claim_backend_error_logs_one_deferral_line(self, worker, backend, swarmq_logs):
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())

        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["event"] == "task_deferred_backend_unavailable"
        assert entry["stage"] == "inflight_claim" and entry["schedfire_key"] == "s:1"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


    @pytest.mark.timeout(5)
    async def test_hanging_completed_check_is_deferred_after_backend_op_timeout(
        self, worker, broker, backend, swarmq_logs,
    ):
        """Liste 8."""
        worker._gate_deps.config.backend_op_timeout = 0.05
        never = asyncio.Event()

        async def _hang(*_args, **_kwargs):
            await never.wait()

        backend.schedules.is_fire_completed = _hang
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Redelivered
        broker.retry_later.assert_awaited_once()
        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["stage"] == "completed_check" and entry["error_type"] == "BackendConnectionError"


    @pytest.mark.timeout(5)
    async def test_hanging_inflight_claim_is_deferred_after_backend_op_timeout(
        self, worker, broker, backend, swarmq_logs,
    ):
        """Liste 9."""
        worker._gate_deps.config.backend_op_timeout = 0.05
        never = asyncio.Event()

        async def _hang(*_args, **_kwargs):
            await never.wait()

        backend.schedules.claim_fire = _hang
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Redelivered
        broker.retry_later.assert_awaited_once()
        [entry] = [e for e in swarmq_logs if e["log_level"] == "warning"]
        assert entry["stage"] == "inflight_claim" and entry["error_type"] == "BackendConnectionError"


class LongTask(Task):
    task_info = TaskInfo(name="long", queues=["default"], hard_timeout=900)

    async def run(self) -> None:
        return None


class TestSchedfireGateLogContext:
    """Plan 2026-10-06-0831, U10 (KTD13): frueh endende Pfade hinterlassen
    keinen Task-Kontext und lassen Variablen der App stehen."""

    async def test_completed_duplicate_keeps_the_app_context(self, worker, backend, app_log_context):
        backend.schedules.is_fire_completed = AsyncMock(return_value=True)
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        assert structlog.contextvars.get_contextvars() == app_log_context
