"""Gnadenfrist fuer unbekannte Tasks (Plan 0.12.0, MR 11, BZ-06).

Ein Task, den dieser Worker nicht kennt, wird innerhalb von
``unknown_task_grace_s`` (Standard 900 s) mit Backoff zurueckgestellt; der
Zustands-Header ``x-swarmq-unknown-since`` (Epoch-ms des ersten Kontakts)
reist in jeder Kopie mit (Q6). Nach Ablauf der Frist folgt der terminale
Dead-Letter des Workers (Fehlerergebnis, Workflow-Fehlmarkierung,
Unique-Freigabe, ``TASK_DLQ``, DLQ-Frist) plus ``TASK_REJECTED``.

Test-Liste:
- [x] erste Kopie traegt ``x-swarmq-unknown-since`` = jetzt (ms)
- [x] spaetere Kopie behaelt den ersten Zeitpunkt
- [x] Wartezeit: 1 s beim ersten Kontakt, dann so lang wie bisher gewartet,
      hoechstens 60 s, hoechstens bis zum Ende der Frist
- [x] Frist abgelaufen: Dead-Letter mit Fehlerergebnis, DLQ-Kopie und
      ``TASK_REJECTED`` (reason ``unregistered_task``), ERROR ``task_not_found``
- [x] Frist 0: sofort Dead-Letter
- [x] Publish der Kopie scheitert: ``redeliver``, keine Ausnahme
- [x] Cron-Fire (Schedfire-Claim): Rueckstellung schreibt keinen
      Completed-Marker und gibt das Unique-Flag nicht frei; die Kopie traegt
      das Claim-Token
- [x] Kopien der Gates behalten ``x-swarmq-unknown-since``, die fachliche
      Retry-Kopie entfernt ihn (Q6)
- [x] Signal ``TASK_UNKNOWN_DEFERRED`` je Rueckstellung
- [x] DLQ-Publish scheitert: DLX-Fallback (``route_via_dlx``) und Freigabe
- [x] ``TASK_REJECTED`` nur, wenn die DLQ-Kopie liegt (Workflow-Fehlmarkierung
      voruebergehend gescheitert -> neu zugestellt, kein Signal)
"""

from __future__ import annotations

import dataclasses
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.exceptions import BackendConnectionError
from swarmq.headers import (
    HEADER_GUARD_DEFER_COUNT,
    HEADER_RETRY_COUNT,
    HEADER_SCHEDFIRE_CLAIM,
    HEADER_UNIQUE_HASH,
    HEADER_UNIQUE_UNTIL,
    HEADER_UNKNOWN_SINCE,
    HEADER_WORKFLOW_ID,
    HEADER_WORKFLOW_STEP,
)
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext, republish_headers
from tests.support.messages import make_message
from tests.unit.worker._helpers import AddTask, _default_payload

pytestmark = pytest.mark.timeout(5)

HEADER_SCHEDFIRE = "x-swarmq-schedfire"
_NOW_S = 1_700_000_000.0
_NOW_MS = int(_NOW_S * 1000)


def _worker(broker: Any, backend: Any, config: Any, **cfg: Any) -> Worker:
    registry = TaskRegistry()
    registry.register(AddTask)
    return Worker(broker=broker, backend=backend, registry=registry, config=dataclasses.replace(config, **cfg))


def _unknown(**headers: Any) -> Any:
    message = make_message(_default_payload(task_id="unk-1", name="new_task"), headers=headers)
    message.queue = "default"
    return message


@pytest.fixture
def clock():
    with patch("swarmq.worker.gates.unknown_task.time") as fake:
        fake.time.return_value = _NOW_S
        yield fake


async def test_first_copy_carries_unknown_since_now(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config)

    await worker._handle_message(_unknown())

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert headers[HEADER_UNKNOWN_SINCE] == _NOW_MS


async def test_later_copy_keeps_the_first_contact(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config)

    await worker._handle_message(_unknown(**{HEADER_UNKNOWN_SINCE: _NOW_MS - 5_000}))

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert headers[HEADER_UNKNOWN_SINCE] == _NOW_MS - 5_000


@pytest.mark.parametrize(
    ("elapsed_ms", "expected_delay_ms"),
    [
        (0, 1_000),  # erster Kontakt
        (500, 1_000),  # unter der Basis
        (5_000, 5_000),  # so lang wie bisher gewartet (verdoppelt sich je Runde)
        (120_000, 60_000),  # Deckel 60 s (Stufe <= 1 min, Autoscaling)
        (880_000, 20_000),  # hoechstens bis zum Ende der Frist (900 s)
    ],
)
async def test_delay_grows_with_the_wait_and_stops_at_the_grace(
    broker,
    backend,
    config,
    clock,
    elapsed_ms: int,
    expected_delay_ms: int,
) -> None:
    worker = _worker(broker, backend, config)

    await worker._handle_message(_unknown(**{HEADER_UNKNOWN_SINCE: _NOW_MS - elapsed_ms}))

    assert broker.retry_later.await_args.kwargs["delay_ms"] == expected_delay_ms


def _spy_signals(worker: Worker) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    async def _dispatch(signal: Any, **kwargs: Any) -> None:
        events.append({"signal": signal, **kwargs})

    worker._signal_dispatcher.dispatch = AsyncMock(side_effect=_dispatch)  # type: ignore[method-assign]
    return events


@pytest.mark.entscheidung("E-008")
async def test_expired_grace_dead_letters_with_result_and_rejected_signal(
    broker,
    backend,
    config,
    clock,
    swarmq_logs,
) -> None:
    worker = _worker(broker, backend, config)
    events = _spy_signals(worker)

    await worker._handle_message(_unknown(**{HEADER_UNKNOWN_SINCE: _NOW_MS - 901_000}))

    broker.retry_later.assert_not_awaited()
    broker.route_via_dlx.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()
    backend.store_result.assert_awaited_once()
    assert decode_result(backend.store_result.await_args.args[1])["is_error"] is True
    assert [e["signal"] for e in events] == [Signal.TASK_DLQ, Signal.TASK_REJECTED]
    rejected = events[1]
    assert (rejected["reason"], rejected["task_id"], rejected["task_name"], rejected["queue"]) == (
        "unregistered_task",
        "unk-1",
        "new_task",
        "default",
    )
    [not_found] = [e for e in swarmq_logs if e["event"] == "task_not_found"]
    assert not_found["log_level"] == "error"


@pytest.mark.entscheidung("E-008")
async def test_grace_zero_dead_letters_at_first_contact(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config, unknown_task_grace_s=0)

    await worker._handle_message(_unknown())

    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


async def test_failed_copy_publish_redelivers_the_original(broker, backend, config, clock) -> None:
    broker.retry_later = AsyncMock(side_effect=RuntimeError("broker gone"))
    worker = _worker(broker, backend, config)

    await worker._handle_message(_unknown())

    broker.redeliver.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()


@pytest.mark.entscheidung("E-008")
async def test_deferred_cron_fire_keeps_claim_open_and_unique_flag_held(broker, backend, config, clock) -> None:
    backend.release_unique = AsyncMock(return_value=True)
    worker = _worker(broker, backend, config)
    message = _unknown(
        **{
            HEADER_SCHEDFIRE: "removed_cron:1700000000000",
            HEADER_UNIQUE_HASH: "a" * 64,
            HEADER_UNIQUE_UNTIL: "completion",
        }
    )

    await worker._handle_message(message)

    broker.retry_later.assert_awaited_once()
    # Kein Completed-Marker: die Kopie darf laufen, sobald ein Worker den
    # Task kennt; das Flag bleibt bis dahin gehalten (BZ-06, B2).
    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.schedules.release_fire_claim.assert_awaited_once()
    backend.release_unique.assert_not_awaited()
    headers = broker.retry_later.await_args.kwargs["headers"]
    assert headers[HEADER_SCHEDFIRE_CLAIM]
    assert headers[HEADER_UNIQUE_HASH] == "a" * 64


class FailsOnce(Task):
    task_info = TaskInfo(name="grace_fails_once", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise ValueError("boom")


async def test_gate_copies_keep_unknown_since_but_the_retry_copy_drops_it(broker, backend, config) -> None:
    # Q6: Zustands-Header bleiben in den Kopien der Gates (sonst setzte jede
    # fremde Rueckstellung die Frist zurueck) ...
    delivery = _unknown(**{HEADER_UNKNOWN_SINCE: _NOW_MS, HEADER_GUARD_DEFER_COUNT: 2})
    kept = republish_headers(delivery, GateContext(task_id="unk-1", task_name="new_task", queue="default"))
    assert kept[HEADER_UNKNOWN_SINCE] == _NOW_MS
    # ... die fachliche Retry-Kopie entfernt ihn: der Task lief, die naechste
    # Unbekanntheit (naechster Rollout) wird neu gemessen.
    registry = TaskRegistry()
    registry.register(FailsOnce)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(
        _default_payload(task_id="r-1", name="grace_fails_once", args=[], kwargs={}),
        headers={HEADER_UNKNOWN_SINCE: _NOW_MS},
    )
    message.queue = "default"

    await worker._handle_message(message)

    retry_headers = broker.retry_later.await_args.kwargs["headers"]
    assert retry_headers[HEADER_RETRY_COUNT] == 1
    assert HEADER_UNKNOWN_SINCE not in retry_headers


async def test_each_deferral_dispatches_task_unknown_deferred(broker, backend, config, clock) -> None:
    worker = _worker(broker, backend, config)
    events = _spy_signals(worker)

    await worker._handle_message(_unknown(**{HEADER_UNKNOWN_SINCE: _NOW_MS - 5_000}))

    assert events == [
        {
            "signal": Signal.TASK_UNKNOWN_DEFERRED,
            "task_id": "unk-1",
            "task_name": "new_task",
            "queue": "default",
            "delay": 5.0,
            "unknown_since": _NOW_MS - 5_000,
        }
    ]


async def test_failed_dlq_publish_falls_back_to_the_dlx_and_releases(broker, backend, config, clock) -> None:
    broker.dead_letter = AsyncMock(side_effect=RuntimeError("broker gone"))
    backend.release_unique = AsyncMock(return_value=True)
    worker = _worker(broker, backend, config, unknown_task_grace_s=0)
    events = _spy_signals(worker)

    await worker._handle_message(_unknown(**{HEADER_UNIQUE_HASH: "b" * 64, HEADER_UNIQUE_UNTIL: "completion"}))

    # Dauerhafte Ursache: ein Requeue liefe nur erneut hinein -- terminaler
    # DLX-Fallback wie bei den Lock-Pfaden, dann die Freigabe.
    broker.route_via_dlx.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    backend.release_unique.assert_awaited_once()
    assert [(e["signal"], e.get("reason")) for e in events] == [(Signal.TASK_REJECTED, "dlq_publish_failed")]


async def test_no_rejected_signal_when_the_workflow_fail_mark_redelivers(broker, backend, config, clock) -> None:
    """Workflow-Fehlmarkierung voruebergehend gescheitert (BZ-13): die
    Zustellung kommt neu, nichts liegt in der DLQ -- also kein ``TASK_REJECTED``."""
    worker = _worker(broker, backend, config, unknown_task_grace_s=0)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    events = _spy_signals(worker)
    message = make_message(
        _default_payload(task_id="unk-wf", name="new_task", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: "01890a5d-ac96-774b-bcce-b302099a8057", HEADER_WORKFLOW_STEP: "/1"},
    )
    message.queue = "default"

    await worker._handle_message(message)

    broker.redeliver.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()
    assert events == []
