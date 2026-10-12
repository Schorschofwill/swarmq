"""Tabellentest der Ausfuehrungsidentitaet ueber Replay-Ketten (Plan 0.12.0,
MR 12, BZ-08; Pruefbefunde A1-A3, C2).

Die Akteure sind echter Code: der DLQ-Stempel von ``RabbitMQBroker.dead_letter``
und die Weiterleitung von ``retry_dead_letter``/``list_dead_letters`` (gegen
einen gefaelschten Kanal), ``republish_headers`` fuer Kopien des Workers und
der In-Memory-Ausfuehrungsschutz (Semantik von ``guard_claim.lua``). Der
Broker-DLX ist nach den Spikes S5/S7 nachgebildet: Er baut ``x-death`` nach
einem Client-Publish neu auf (count 1) und zaehlt nur in einer reinen
Broker-Kette hoch; ein Client-Publish laesst ``x-death`` unveraendert.

Test-Liste:
1. [x] A1: Broker-DLX -> UI-Move -> Lauf scheitert -> Worker-DLQ -> CLI:
       drei verschiedene Generationen, jede laeuft genau einmal.
2. [x] A2: Worker-DLQ -> UI-Move -> Lauf -> route_via_dlx -> UI-Move: ebenso.
3. [x] A3: 0.11-DLQ ohne Generation, zweimal CLI: die CLI leitet die ID aus
       der Identitaet ab (Review MR 12, P2-2) -- derselbe Bestand laeuft
       einmal, eine neue Generation (0.11-Worker stempelt neu) laeuft.
4. [x] PublishIndeterminate-Doppelkopie: beide DLQ-Kopien tragen dieselbe
       Generations-ID; der zweite CLI-Retry wird verworfen.
5. [x] ``dlq list`` dazwischen aendert nichts: die Generation bleibt frisch.
6. [x] At-least-once-Duplikat: dieselben Header, dieselbe Identitaet, der
       zweite Lauf wird verworfen -- auch fuer ein CLI-Replay.
7. [x] Retry ueber jede Delay-Stufe und {q}.DQ: n bleibt (Base), die
       Retry-Kopie laeuft unter derselben Identitaet.
8. [x] 0.11-Replay-ID in-flight: Identitaet byte-gleich zu 0.11 (``task|H``).

Review MR 12 (P2-3):
9. [x] A2, zweite Runde: nach erneutem ``route_via_dlx`` wird ein zweiter
       UI-Move uebersprungen (``x-death`` count bleibt 1, keine Base --
       dokumentierte Grenze); ``dlq retry`` derselben Nachricht laeuft.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.identity import execution_identity, generation_id
from swarmq.queue_names import delay_tiers
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext, republish_headers
from swarmq.worker.gates.execution_guard import retry_count_of
from tests.support.inmemory import FakeClock
from tests.support.inmemory.backend import InMemoryExecutionGuardStore
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

QUEUE = "tasks"
TASK = "t-1"
BODY = json.dumps({"task": {"id": TASK, "name": "n", "queue": QUEUE}, "args": [], "kwargs": {}}).encode()


def _fake_broker(now: float) -> Any:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: now
    return broker


class World:
    """Eine Nachricht auf ihrem Weg durch Broker, Worker, UI und CLI."""

    def __init__(self) -> None:
        self.store = InMemoryExecutionGuardStore(FakeClock())
        self.now = 1_760_000_000.0
        self.runs: list[str] = []
        self.owners: dict[str, str] = {}  # Owner-Token des letzten Claims je Identitaet
        self.broker_owned = False  # x-death stammt vom Broker (reine Broker-Kette)

    def _tick(self) -> float:
        self.now += 1.0
        return self.now

    # --- Worker ---------------------------------------------------------------

    async def deliver(self, headers: dict[str, Any]) -> bool:
        """Claim des Ausfuehrungsschutzes: False = verworfen (``done``)."""
        identity, owner = execution_identity(headers, TASK), uuid.uuid4().hex
        claim = await self.store.claim(
            identity,
            owner=owner,
            retry_count=retry_count_of(headers),
            ttl_ms=60_000,
            state_ttl_ms=600_000,
            max_orphans=3,
        )
        if claim.status in ("done", "superseded"):
            return False
        if claim.status == "running":
            raise AssertionError(f"Simulation: {identity} laeuft noch (vorher ack/worker_dlq/worker_retry_copy)")
        self.runs.append(identity)
        self.owners[identity] = owner
        return True

    async def run_ok(self, headers: dict[str, Any]) -> bool:
        """Lauf mit Ack: False = verworfen."""
        if not await self.deliver(headers):
            return False
        await self.ack(headers)
        return True

    async def ack(self, headers: dict[str, Any]) -> None:
        identity = execution_identity(headers, TASK)
        await self.store.mark_done(identity, owner=self.owners[identity], state_ttl_ms=600_000)

    async def worker_dlq(self, headers: dict[str, Any]) -> dict[str, Any]:
        """Echter Stempel von ``RabbitMQBroker.dead_letter``; Done-Marker wie
        der protokollierende Wrapper."""
        broker = _fake_broker(self._tick())
        handle = Mock(consumer_tag=None)
        handle.ack = AsyncMock()
        await broker.dead_letter(Delivery(BODY, dict(headers), QUEUE, handle), headers=dict(headers))
        await self.ack(headers)
        self.broker_owned = False
        return dict(broker._publish_channel.default_exchange.publish.call_args.args[0].headers)

    async def worker_retry_copy(self, headers: dict[str, Any]) -> dict[str, Any]:
        ctx = GateContext(task_id=TASK, task_name="n", queue=QUEUE, task_info=TaskInfo(name="n", queues=[QUEUE]))
        copy = republish_headers(make_message(headers=headers), ctx)
        copy[HEADER_RETRY_COUNT] = retry_count_of(headers) + 1
        identity = execution_identity(headers, TASK)
        await self.store.mark_superseded(
            identity,
            owner=self.owners[identity],
            retry_count=copy[HEADER_RETRY_COUNT],
            state_ttl_ms=600_000,
        )
        self.broker_owned = False
        return copy

    # --- Broker ---------------------------------------------------------------

    def broker_dlx(self, headers: dict[str, Any], queue: str = QUEUE, reason: str = "rejected") -> dict[str, Any]:
        """``nack(requeue=False)`` bzw. Ablauf: ``x-death`` neu aufgebaut (S5)
        oder in der reinen Broker-Kette hochgezaehlt (S7)."""
        out = {k: v for k, v in headers.items() if k != "x-death"}
        deaths = [dict(d) for d in headers.get("x-death", [])] if self.broker_owned else []
        for death in deaths:
            if death["queue"] == queue and death["reason"] == reason:
                death["count"] += 1
                break
        else:
            deaths.insert(0, {"queue": queue, "count": 1, "reason": reason, "exchange": "", "routing-keys": [queue]})
        out["x-death"] = deaths
        self.broker_owned = True
        return out

    # --- UI und CLI -----------------------------------------------------------

    def ui_move(self, headers: dict[str, Any]) -> dict[str, Any]:
        self.broker_owned = False
        return dict(headers)

    async def _admin(self, headers: dict[str, Any], call: str) -> list[tuple[str, dict[str, Any]]]:
        broker = _fake_broker(self._tick())
        queue = Mock()
        queue.get = AsyncMock(side_effect=[Mock(body=BODY, headers=dict(headers)), None])
        broker._dlq_queue = AsyncMock(return_value=queue)
        published: list[tuple[str, dict[str, Any]]] = []

        async def publish(routing_key: str, msg: Any) -> None:
            published.append((routing_key, dict(msg.headers)))

        broker._publish_with_retry = publish
        if call == "retry":
            assert (await broker.retry_dead_letter(QUEUE, TASK))[0] is True
        else:
            await broker.list_dead_letters(QUEUE)
        self.broker_owned = False
        return published

    async def cli_retry(self, headers: dict[str, Any]) -> dict[str, Any]:
        [(routing_key, forwarded)] = await self._admin(headers, "retry")
        assert routing_key == QUEUE
        return forwarded

    async def dlq_list(self, headers: dict[str, Any]) -> dict[str, Any]:
        [(routing_key, back)] = await self._admin(headers, "list")
        assert routing_key == f"{QUEUE}.XQ"
        return back


def _legacy_dlq(replay_id: str | None = None) -> dict[str, Any]:
    """DLQ-Kopie eines 0.11-Workers: Zeitstempel und Frist, keine Generation."""
    headers: dict[str, Any] = {"x-swarmq-dead-lettered-at": 1_759_000_000_000, "x-swarmq-dead-letter-ttl-ms": "none"}
    if replay_id is not None:
        headers["x-swarmq-replay-id"] = replay_id
    return headers


async def test_a1_broker_dlx_ui_move_worker_dlq_and_cli_are_three_generations() -> None:
    w = World()
    parked = w.broker_dlx({})  # z. B. Delivery-Limit
    replay = w.ui_move(parked)
    assert await w.deliver(replay) is True
    dlq = await w.worker_dlq(replay)  # der Lauf scheiterte terminal
    assert dlq["x-swarmq-replay-id"] == generation_id(f"{TASK}#1")
    assert "x-death" not in dlq
    forwarded = await w.cli_retry(dlq)
    assert await w.run_ok(forwarded) is True
    assert w.runs == [f"{TASK}#1", f"{TASK}|{generation_id(f'{TASK}#1')}#1"]
    assert await w.run_ok(forwarded) is False  # Neuzustellung des CLI-Replays


async def test_a2_worker_dlq_ui_move_route_via_dlx_ui_move() -> None:
    w = World()
    assert await w.deliver({}) is True
    dlq = await w.worker_dlq({})
    first_replay = w.ui_move(dlq)
    assert await w.run_ok(first_replay) is True
    parked = w.broker_dlx(first_replay)  # route_via_dlx: Header bleiben, x-death waechst
    second_replay = w.ui_move(parked)
    assert await w.run_ok(second_replay) is True
    gen = generation_id(TASK)
    assert w.runs == [TASK, f"{TASK}|{gen}", f"{TASK}|{gen}#1"]
    assert await w.run_ok(w.ui_move(dlq)) is False  # das Original des Replays bleibt geschuetzt


async def test_a2_second_round_ui_move_is_skipped_dlq_retry_runs() -> None:
    w = World()
    assert await w.deliver({}) is True
    dlq = await w.worker_dlq({})
    first_replay = w.ui_move(dlq)
    assert await w.run_ok(first_replay) is True
    second_replay = w.ui_move(w.broker_dlx(first_replay))
    assert await w.run_ok(second_replay) is True
    parked_again = w.broker_dlx(second_replay)  # x-death neu aufgebaut: wieder count 1
    # Dokumentierte Grenze (Abschnitt 7): der zweite UI-Move hat dieselbe
    # Identitaet wie der erste und wird verworfen.
    assert await w.run_ok(w.ui_move(parked_again)) is False
    # ``dlq retry`` laeuft: x-death zeigt, dass die Generation verbraucht ist.
    assert await w.run_ok(await w.cli_retry(parked_again)) is True
    gen = generation_id(TASK)
    assert w.runs[:3] == [TASK, f"{TASK}|{gen}", f"{TASK}|{gen}#1"]
    assert len(set(w.runs)) == 4


async def test_a3_legacy_dlq_without_generation_replays_once_per_generation() -> None:
    w = World()
    first = await w.cli_retry(_legacy_dlq())
    assert await w.run_ok(first) is True
    again_dead = _legacy_dlq(first["x-swarmq-replay-id"])  # 0.11-Worker stempelt ohne Generation
    second = await w.cli_retry(again_dead)
    assert await w.run_ok(second) is True
    assert len(set(w.runs)) == 2
    assert all(len(r.split("|")[1]) == 32 for r in w.runs)
    # Review MR 12 (P2-2): derselbe Bestand ein zweites Mal per CLI wird verworfen.
    assert await w.run_ok(await w.cli_retry(_legacy_dlq())) is False


async def test_publish_indeterminate_double_copy_replays_once() -> None:
    w = World()
    assert await w.deliver({}) is True
    first_copy = await w.worker_dlq({})
    second_copy = await w.worker_dlq({})  # Wiederholung nach PublishIndeterminate, spaeter
    assert first_copy["x-swarmq-replay-id"] == second_copy["x-swarmq-replay-id"]
    assert first_copy["x-swarmq-dlq-gen"] != second_copy["x-swarmq-dlq-gen"]
    assert await w.run_ok(await w.cli_retry(first_copy)) is True
    assert await w.run_ok(await w.cli_retry(second_copy)) is False
    assert w.runs == [TASK, f"{TASK}|{generation_id(TASK)}"]


async def test_dlq_list_in_between_keeps_the_generation_fresh() -> None:
    w = World()
    assert await w.deliver({}) is True
    dlq = await w.worker_dlq({})
    listed = await w.dlq_list(dlq)
    assert listed == dlq
    assert await w.run_ok(await w.cli_retry(listed)) is True
    assert await w.run_ok(await w.cli_retry(await w.dlq_list(dlq))) is False
    assert w.runs == [TASK, f"{TASK}|{generation_id(TASK)}"]


async def test_at_least_once_duplicate_is_skipped_also_for_a_replay() -> None:
    w = World()
    assert await w.deliver({}) is True
    dlq = await w.worker_dlq({})
    assert await w.run_ok({}) is False
    forwarded = await w.cli_retry(dlq)
    assert await w.run_ok(forwarded) is True
    assert await w.run_ok(forwarded) is False
    assert len(w.runs) == 2


@pytest.mark.parametrize("delay_queue", [*delay_tiers(QUEUE), f"{QUEUE}.DQ"])
async def test_retry_copy_through_any_delay_queue_keeps_n(delay_queue: str) -> None:
    w = World()
    replay = w.ui_move(w.broker_dlx({}))
    assert await w.deliver(replay) is True
    copy = await w.worker_retry_copy(replay)
    assert copy["x-swarmq-xdeath-base"] == 1
    assert "x-death" not in copy
    arrived = w.broker_dlx(copy, queue=delay_queue, reason="expired")
    assert await w.deliver(arrived) is True
    assert w.runs == [f"{TASK}#1", f"{TASK}#1"]


def test_a_0_11_replay_id_in_flight_has_the_0_11_identity() -> None:
    replay_id = uuid.uuid4().hex
    assert execution_identity({"x-swarmq-replay-id": replay_id}, TASK) == f"{TASK}|{replay_id}"
