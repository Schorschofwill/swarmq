"""U5: WorkflowAdvancer DAG logic, driven by an in-memory fake worker.

These tests exercise the full advancement recursion — chain progression
with result injection, immutable suppression, chord fan-in with ordered
results, standalone group aggregation, and the cancel gate — without any
broker/backend infrastructure. A tiny driver loop plays the role of the
worker: it drains published step messages, "runs" each (echoing a result),
stores the result, and calls advance() with the step's path.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, override
from unittest.mock import AsyncMock

import pytest

from swarmq.contracts import PublishReceipt
from swarmq.exceptions import SwarmQError, WorkflowStateConflict
from swarmq.middleware import Middleware, MiddlewareChain
from swarmq.serialization import decode_json, decode_result, encode_json, encode_result
from swarmq.task import Task, TaskInfo
from swarmq.workflow import _node_from_dict, chain, chord, group
from swarmq.workflow_enqueue import WorkflowStep
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    WorkflowAdvancer,
    WorkflowHandle,
    WorkflowStepEnqueuer,
    aggregate_result_id,
    build_initial_workflow_state,
    initial_frontier,
    workflow_step_task_id,
)
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = [pytest.mark.timeout(5)]


class Echo(Task):
    task_info = TaskInfo(name="echo", queues=["default"])

    @override
    async def run(self, *args) -> list[Any]:
        return list(args)


def _connected_backend() -> InMemoryBackend:
    """In-Memory-Stub (Plan U4) statt des früheren lokalen FakeBackend."""
    backend = InMemoryBackend(FakeClock())
    backend._connected = True  # synchron „verbunden" — connect() ist async
    return backend


def _unpack(raw: bytes):
    data = decode_result(raw)
    return data["result"]


async def _noop() -> None:
    return None


class _NoopPublisher:
    """Publisher-Seam für Tests, die den Enqueue-Pfad nicht ausüben."""

    async def pre_enqueue(self, steps) -> None:
        return None

    async def publish(self, step):
        return PublishReceipt(queue=step.queue)


class Driver:
    """Plays the worker: builds a workflow, seeds initial frontier, then
    drains published step messages — running each, storing its result, and
    advancing the workflow — until no work remains."""

    def __init__(self, workflow):
        self.backend = _connected_backend()
        self.chord = self.backend.chords
        self.published: list[tuple[str, bytes, dict[str, str]]] = []
        self.wf_id = "wf-test"
        self.advancer = WorkflowAdvancer(
            self.backend,
            self.chord,
            publisher=self,
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )
        self.workflow = workflow

    async def pre_enqueue(self, steps):
        return None

    async def publish(self, step):
        self.published.append((step.queue, encode_json(step.to_message()), step.headers))
        return PublishReceipt(queue=step.queue)

    async def _run_step(self, body: bytes) -> str:
        msg = decode_json(body)
        task_id: str = msg["task"]["id"]
        # Echo task: result is the arg list (captures injected predecessor).
        result_value = list(msg["args"])
        await self.backend.store_result(
            task_id,
            encode_result(
                {
                    "task": msg["task"],
                    "is_error": False,
                    "is_cancelled": False,
                    "error_message": "",
                    "result": result_value,
                }
            ),
            60_000,
        )
        return task_id

    async def seed(self):
        state = build_initial_workflow_state(self.wf_id, self.workflow.to_dict())
        step_tasks = {}
        for step_path, _sig in initial_frontier(self.workflow):
            tid = f"seed-{step_path}"
            step_tasks[step_path] = tid
        state["step_tasks"] = step_tasks
        await self.backend.store_workflow_state(self.wf_id, encode_json(state), 60_000)
        # "Publish" the initial frontier and run it through the driver.
        for step_path, sig in initial_frontier(self.workflow):
            tid = step_tasks[step_path]
            await self.backend.store_result(
                tid,
                encode_result(
                    {
                        "task": {"id": tid, "queue": "default", "name": sig.task_name},
                        "is_error": False,
                        "is_cancelled": False,
                        "error_message": "",
                        "result": list(sig.args),
                    }
                ),
                60_000,
            )
            await self.advancer.advance(self.wf_id, step_path, tid, list(sig.args))

    async def drain(self):
        # Process published messages FIFO until none remain.
        while self.published:
            _queue, body, headers = self.published.pop(0)
            step_path = headers["x-swarmq-workflow-step"]
            task_id = await self._run_step(body)
            raw = await self.backend.get_result(task_id)
            assert raw is not None
            result_value = decode_result(raw)["result"]
            await self.advancer.advance(self.wf_id, step_path, task_id, result_value)

    async def state(self) -> dict[str, Any]:
        raw = await self.backend.get_workflow_state(self.wf_id)
        assert raw is not None
        state: dict[str, Any] = decode_json(raw)
        return state

    async def final_value(self):
        st = await self.state()
        fid = st["final_result_task_id"]
        if fid is None:
            return None
        raw = await self.backend.get_result(fid)
        assert raw is not None
        return decode_result(raw)["result"]


# --------------------------------------------------------------------------
# Chain advancement — R4
# --------------------------------------------------------------------------


async def test_chain_advances_and_completes():
    d = Driver(chain(Echo.s("a"), Echo.s(), Echo.s()))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    # Each step enqueued exactly once: paths /0, /1, /2.
    assert set(st["step_tasks"].keys()) == {"/0", "/1", "/2"}


async def test_chain_injects_predecessor_result():
    # Echo returns its args; step 2 should receive step 1's result as arg0.
    d = Driver(chain(Echo.s("seed"), Echo.s()))
    await d.seed()
    await d.drain()
    final = await d.final_value()
    # step1 result = ["seed"]; injected into step2 → step2 result = [["seed"]].
    assert final == [["seed"]]


async def test_chain_immutable_suppresses_injection():
    d = Driver(chain(Echo.s("seed"), Echo.si("fixed")))
    await d.seed()
    await d.drain()
    final = await d.final_value()
    # immutable step keeps its own args only — no predecessor injection.
    assert final == ["fixed"]


async def test_cancel_gate_stops_advancement():
    d = Driver(chain(Echo.s("a"), Echo.s(), Echo.s()))
    await d.seed()
    await d.backend.set_workflow_cancel_flag(d.wf_id, 60_000)  # cancel before draining
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_CANCELLED


# --------------------------------------------------------------------------
# Chord & group fan-in — R5, R6
# --------------------------------------------------------------------------


async def test_chord_fires_body_with_ordered_results():
    d = Driver(chord(group(Echo.s("a"), Echo.s("b"), Echo.s("c")), Echo.s()))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    # Body receives the ordered list of header results as its injected arg0;
    # Echo wraps args in a list → [[ ["a"], ["b"], ["c"] ]].
    assert final == [[["a"], ["b"], ["c"]]]


async def test_standalone_group_completes_with_aggregate():
    d = Driver(group(Echo.s("x"), Echo.s("y")))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    assert final == [["x"], ["y"]]


# --------------------------------------------------------------------------
# B10 — zusammengesetzte Gruppenglieder (Plan 2026-08-30-002, U1)
#
# Reproduktion des Defekts: `_frontier` legt fuer ein zusammengesetztes Glied
# keinen Schluessel `<gruppe>/g<i>` an, `_materialise` schreibt die
# Aggregatkennung nie nach `step_tasks`, und `_ordered_member_ids` ueberspringt
# den fehlenden Schluessel wortlos. Am ungefixten Stand liefern beide Tests
# eine um das zusammengesetzte Glied gekuerzte Liste.
# --------------------------------------------------------------------------


async def test_group_with_chain_member_keeps_all_members():
    # R1/R2, AE1. Ungefixt: einelementige Liste [["a"]] — das Kettenglied fehlt.
    d = Driver(group(Echo.s("a"), chain(Echo.s("b"), Echo.s())))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    assert final == [["a"], [["b"]]]


async def test_chord_header_with_chain_member_injects_all_members():
    # R3, AE4. Ungefixt bekommt der Body eine einelementige Header-Liste —
    # der Fehler landet damit in der Rechnung des Bodys, nicht im Rueckgabewert.
    d = Driver(chord(group(Echo.s("a"), chain(Echo.s("b"), Echo.s())), Echo.s()))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    assert final == [[["a"], [["b"]]]]


async def test_group_with_group_member_keeps_all_members():
    # R2, AE2 — zusammengesetztes Glied ist selbst eine Gruppe.
    d = Driver(group(Echo.s("a"), group(Echo.s("b"), Echo.s("c"))))
    await d.seed()
    await d.drain()
    assert (await d.state())["status"] == STATUS_COMPLETED
    assert await d.final_value() == [["a"], [["b"], ["c"]]]


async def test_group_of_only_composite_members():
    # AE3 — kein einziges einfaches Glied; die Liste ist trotzdem vollstaendig
    # und in Definitions-, nicht Abschlussreihenfolge.
    d = Driver(group(chain(Echo.s("a"), Echo.s()), chain(Echo.s("c"), Echo.s())))
    await d.seed()
    await d.drain()
    assert await d.final_value() == [[["a"]], [["c"]]]


async def test_group_nested_two_levels_deep():
    # R4, AE5 — die Ableitung ist rekursiv, nicht auf eine Ebene beschraenkt.
    d = Driver(group(Echo.s("a"), group(Echo.s("b"), chain(Echo.s("c"), Echo.s()))))
    await d.seed()
    await d.drain()
    assert await d.final_value() == [["a"], [["b"], [["c"]]]]


async def test_every_member_result_lives_where_the_contract_says():
    """Deckungstest der tragenden Annahme (Plan KTD1 / Assumptions).

    Der Test schreibt die Kennung je Gliedform **unabhaengig** vor, statt die
    Ableitung des Codes zu befragen: Ein Blatt liegt unter seinem
    Schritt-Schluessel, eine Kette unter dem ihres letzten Schritts, ein Chord
    unter dem seines Bodys — und alles, was als Gruppe endet, unter der
    Aggregatkennung des GLIEDPFADS, weil `_materialise` genau dort und nur
    dort ablegt.

    Die letzten beiden Zeilen sind der Kern: Ein Glied, dessen Abschlussknoten
    eine Gruppe hinter einer Ketten- oder Chord-Ebene ist, hat sein Aggregat
    am Gliedpfad liegen, nicht am tiefsten Pfad. Genau das ging beim ersten
    Anlauf schief und blieb unbemerkt, weil kein Test diese Form baute.
    """
    from swarmq.workflow_orchestration import aggregate_result_id

    d = Driver(
        group(
            Echo.s("a"),  # /g0 Blatt
            chain(Echo.s("b"), Echo.s()),  # /g1 Kette
            group(Echo.s("c"), Echo.s("d")),  # /g2 Gruppe
            chord(group(Echo.s("e")), Echo.s()),  # /g3 Chord
            chain(Echo.s("f"), group(Echo.si("g"), Echo.si("h"))),  # /g4 Kette->Gruppe
            chord(group(Echo.si("i")), group(Echo.si("j"), Echo.si("k"))),  # /g5 Chord->Gruppe
        )
    )
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED

    expected = [
        ["a"],
        [["b"]],
        [["c"], ["d"]],
        [[["e"]]],
        [["g"], ["h"]],
        [["j"], ["k"]],
    ]
    assert await d.final_value() == expected

    step_tasks = st["step_tasks"]
    where = [
        step_tasks["/g0"],  # Blatt
        step_tasks["/g1/1"],  # letzter Kettenschritt
        aggregate_result_id(d.wf_id, "/g2"),  # Gruppe am Gliedpfad
        step_tasks["/g3/b"],  # Chord-Body (Blatt)
        aggregate_result_id(d.wf_id, "/g4"),  # Kette, die als Gruppe endet
        aggregate_result_id(d.wf_id, "/g5"),  # Chord mit Gruppen-Body
    ]
    for i, task_id in enumerate(where):
        raw = await d.backend.get_result(task_id)
        assert raw is not None, f"Glied {i}: kein Ergebnis unter {task_id}"
        assert decode_result(raw)["result"] == expected[i], f"Glied {i}"


async def test_group_member_ending_in_group_behind_a_chain():
    # Plan-AE5 in seiner urspruenglichen Form: eine Kette, deren letzter
    # Schritt eine Gruppe ist. Das Aggregat liegt am Gliedpfad /g1, nicht am
    # tiefsten Pfad /g1/1 — der erste Anlauf griff daneben und meldete
    # faelschlich eine abgelaufene TTL.
    d = Driver(group(Echo.s("x"), chain(Echo.s("a"), group(Echo.si("b"), Echo.si("c")))))
    await d.seed()
    await d.drain()
    assert (await d.state())["status"] == STATUS_COMPLETED
    assert await d.final_value() == [["x"], [["b"], ["c"]]]


async def test_group_member_is_chord_whose_body_is_a_group():
    d = Driver(
        group(
            Echo.s("x"),
            chord(group(Echo.si("h")), group(Echo.si("b1"), Echo.si("b2"))),
        )
    )
    await d.seed()
    await d.drain()
    assert (await d.state())["status"] == STATUS_COMPLETED
    assert await d.final_value() == [["x"], [["b1"], ["b2"]]]


async def test_chord_header_member_ending_in_group_behind_a_chain():
    d = Driver(
        chord(
            group(Echo.s("x"), chain(Echo.s("a"), group(Echo.si("b"), Echo.si("c")))),
            Echo.s(),
        )
    )
    await d.seed()
    await d.drain()
    assert (await d.state())["status"] == STATUS_COMPLETED
    assert await d.final_value() == [[["x"], [["b"], ["c"]]]]


async def test_composite_member_redelivery_leaves_result_intact():
    # R10, AE8 — erneute Zustellung eines zusammengesetzten Gliedabschlusses.
    # Nebenbei: das einfache Glied schliesst hier VOR dem Kettenglied ab, die
    # Liste steht trotzdem in Definitionsreihenfolge.
    d = Driver(group(chain(Echo.s("b"), Echo.s()), Echo.s("a")))
    await d.seed()
    assert len(d.published) == 1
    _queue, body, headers = d.published.pop(0)
    step_path = headers["x-swarmq-workflow-step"]
    task_id = await d._run_step(body)
    raw = await d.backend.get_result(task_id)
    assert raw is not None
    value = decode_result(raw)["result"]

    await d.advancer.advance(d.wf_id, step_path, task_id, value)
    await d.advancer.advance(d.wf_id, step_path, task_id, value)

    assert (await d.state())["status"] == STATUS_COMPLETED
    assert await d.final_value() == [[["b"]], ["a"]]


async def test_expired_aggregate_of_composite_member_raises():
    # R7, AE9 — laeuft das Aggregat eines zusammengesetzten Glieds vor dem
    # Fan-in ab, gilt derselbe Vertrag wie fuer jedes andere Gliedergebnis:
    # WorkflowResultExpired, kein stilles Loch und keine kuerzere Liste.
    from swarmq.exceptions import WorkflowResultExpired
    from swarmq.workflow_orchestration import aggregate_result_id

    d = Driver(group(group(Echo.s("b"), Echo.s("c")), Echo.s("a")))
    state = build_initial_workflow_state(d.wf_id, d.workflow.to_dict())
    frontier = initial_frontier(d.workflow)
    state["step_tasks"] = {sp: f"seed-{sp}" for sp, _ in frontier}
    await d.backend.store_workflow_state(d.wf_id, encode_json(state), 60_000)

    for step_path, sig in frontier:
        task_id = state["step_tasks"][step_path]
        await d.backend.store_result(
            task_id,
            encode_result(
                {
                    "task": {"id": task_id, "queue": "default", "name": sig.task_name},
                    "is_error": False,
                    "is_cancelled": False,
                    "error_message": "",
                    "result": list(sig.args),
                }
            ),
            60_000,
        )
        if step_path == "/g1":
            # Die innere Gruppe ist fertig, ihr Aggregat liegt gespeichert vor.
            # Jetzt laeuft es ab, bevor die aeussere Gruppe es einsammelt.
            await d.backend.delete_result(aggregate_result_id(d.wf_id, "/g0"))
            with pytest.raises(WorkflowResultExpired):
                await d.advancer.advance(d.wf_id, step_path, task_id, list(sig.args))
        else:
            await d.advancer.advance(d.wf_id, step_path, task_id, list(sig.args))


# --------------------------------------------------------------------------
# B10 — Vollstaendigkeitsguard (Plan 2026-08-30-002, U3)
#
# Eine kuerzere Liste als die Gruppe Glieder hat ist ein Fehler, kein
# Ergebnis — und zwar fuer JEDE Gruppe, nicht nur die verschachtelte.
# --------------------------------------------------------------------------


async def _seed_results(d, frontier, step_tasks):
    for step_path, sig in frontier:
        task_id = step_tasks[step_path]
        await d.backend.store_result(
            task_id,
            encode_result(
                {
                    "task": {"id": task_id, "queue": "default", "name": sig.task_name},
                    "is_error": False,
                    "is_cancelled": False,
                    "error_message": "",
                    "result": list(sig.args),
                }
            ),
            60_000,
        )


async def _drop_step_marker(d, drop_path):
    state = await d.state()
    state["step_tasks"] = {k: v for k, v in state["step_tasks"].items() if k != drop_path}
    await d.backend.store_workflow_state(d.wf_id, encode_json(state), 60_000)


async def test_guard_rejects_truncated_list_for_flat_group():
    # R5, AE10 — der Guard sitzt an der gemeinsamen Stelle und wirkt auch
    # dort, wo B10 nie zuschlagen konnte.
    from swarmq.exceptions import WorkflowValidationError

    d = Driver(group(Echo.s("a"), Echo.s("b")))
    frontier = initial_frontier(d.workflow)
    state = build_initial_workflow_state(d.wf_id, d.workflow.to_dict())
    state["step_tasks"] = {sp: f"seed-{sp}" for sp, _ in frontier}
    await d.backend.store_workflow_state(d.wf_id, encode_json(state), 60_000)
    await _seed_results(d, frontier, state["step_tasks"])

    await d.advancer.advance(d.wf_id, "/g0", state["step_tasks"]["/g0"], ["a"])
    await _drop_step_marker(d, "/g0")

    with pytest.raises(WorkflowValidationError) as exc:
        await d.advancer.advance(d.wf_id, "/g1", state["step_tasks"]["/g1"], ["b"])
    assert "1 of 2" in str(exc.value)


async def test_guard_fires_before_chord_body_is_enqueued():
    # R5 — beim Chord-Header waere die gekuerzte Liste die EINGABE des Bodys,
    # nicht sein Rueckgabewert. Der Guard muss davor sitzen.
    from swarmq.exceptions import WorkflowValidationError

    d = Driver(chord(group(Echo.s("a"), Echo.s("b")), Echo.s()))
    frontier = initial_frontier(d.workflow)
    state = build_initial_workflow_state(d.wf_id, d.workflow.to_dict())
    state["step_tasks"] = {sp: f"seed-{sp}" for sp, _ in frontier}
    await d.backend.store_workflow_state(d.wf_id, encode_json(state), 60_000)
    await _seed_results(d, frontier, state["step_tasks"])

    await d.advancer.advance(d.wf_id, "/h/g0", state["step_tasks"]["/h/g0"], ["a"])
    await _drop_step_marker(d, "/h/g0")

    with pytest.raises(WorkflowValidationError) as exc:
        await d.advancer.advance(d.wf_id, "/h/g1", state["step_tasks"]["/h/g1"], ["b"])
    assert "'/h'" in str(exc.value)
    assert d.published == [], "Body wurde trotz unvollstaendiger Eingabe eingereiht"


async def test_guard_catches_lost_marker_of_composite_member():
    # R5 — der realitaetsnaechste Fall: der Abschlussschritt eines
    # Kettenglieds verliert seinen Marker (publish-then-mark-Fenster).
    from swarmq.exceptions import WorkflowValidationError

    d = Driver(group(Echo.s("a"), chain(Echo.s("b"), Echo.s())))
    await d.seed()
    await _drop_step_marker(d, "/g1/1")

    with pytest.raises(WorkflowValidationError) as exc:
        await d.drain()
    assert "1 of 2" in str(exc.value)


# --------------------------------------------------------------------------
# B10 — mehrere Advancer auf gemeinsamem Zustand (Plan 2026-08-30-002, U4)
# --------------------------------------------------------------------------


class MultiDriver:
    """Wie Driver, aber jeder Schritt laeuft auf einer EIGENEN Advancer-Instanz
    ueber demselben Backend — der Mehrprozess-Fall, in dem kein Advancer den
    Zustand eines anderen im Arbeitsspeicher sieht."""

    def __init__(self, workflow):
        self.backend = _connected_backend()
        self.published: list[tuple[str, bytes, dict[str, str]]] = []
        self.wf_id = "wf-multi"
        self.workflow = workflow
        self.advancer_count = 0
        self.cas_calls = 0

    def advancer(self) -> WorkflowAdvancer:
        self.advancer_count += 1
        return WorkflowAdvancer(
            self.backend,
            self.backend.chords,
            publisher=self,
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )

    async def pre_enqueue(self, steps):
        return None

    async def publish(self, step):
        self.published.append((step.queue, encode_json(step.to_message()), step.headers))
        return PublishReceipt(queue=step.queue)

    async def _store(self, task_id, name, value):
        await self.backend.store_result(
            task_id,
            encode_result(
                {
                    "task": {"id": task_id, "queue": "default", "name": name},
                    "is_error": False,
                    "is_cancelled": False,
                    "error_message": "",
                    "result": value,
                }
            ),
            60_000,
        )

    async def seed(self) -> dict[str, Any]:
        state = build_initial_workflow_state(self.wf_id, self.workflow.to_dict())
        frontier = initial_frontier(self.workflow)
        state["step_tasks"] = {sp: f"seed-{sp}" for sp, _ in frontier}
        await self.backend.store_workflow_state(self.wf_id, encode_json(state), 60_000)
        for step_path, sig in frontier:
            await self._store(state["step_tasks"][step_path], sig.task_name, list(sig.args))
        return state

    async def run_next(self) -> None:
        """Eine veroeffentlichte Nachricht auf einer frischen Advancer-Instanz."""
        _queue, body, headers = self.published.pop(0)
        step_path = headers["x-swarmq-workflow-step"]
        msg = decode_json(body)
        task_id = msg["task"]["id"]
        value = list(msg["args"])
        await self._store(task_id, msg["task"]["name"], value)
        await self.advancer().advance(self.wf_id, step_path, task_id, value)

    async def state(self) -> dict[str, Any]:
        raw = await self.backend.get_workflow_state(self.wf_id)
        assert raw is not None
        state: dict[str, Any] = decode_json(raw)
        return state

    async def final_value(self):
        fid = (await self.state())["final_result_task_id"]
        raw = await self.backend.get_result(fid)
        assert raw is not None
        return decode_result(raw)["result"]


async def test_composite_members_across_separate_advancers():
    """AE7 — jeder Schritt auf einer eigenen Advancer-Instanz.

    Kein Advancer traegt Zustand im Arbeitsspeicher weiter; alles laeuft ueber
    das gemeinsame Backend. Genau der Fall, an dem ein Entwurf zerbricht, der
    die Aggregatkennung im Prozess haelt statt sie abzuleiten.
    """
    m = MultiDriver(group(chain(Echo.s("a"), Echo.s()), chain(Echo.s("c"), Echo.s())))
    state = await m.seed()
    for step_path, sig in initial_frontier(m.workflow):
        await m.advancer().advance(m.wf_id, step_path, state["step_tasks"][step_path], list(sig.args))
    while m.published:
        await m.run_next()

    assert m.advancer_count >= 4, "Test soll mehrere Advancer-Instanzen benutzen"
    assert (await m.state())["status"] == STATUS_COMPLETED
    assert await m.final_value() == [[["a"]], [["c"]]]


async def test_children_reports_only_enqueued_steps():
    """R8 — `children()` bleibt eine Abbildung der eingereihten SCHRITTE.

    Die Aggregatkennung eines zusammengesetzten Glieds taucht dort nicht auf;
    sie wird abgeleitet und nicht im Zustand gefuehrt. Haette die Korrektur ein
    Zustandsfeld eingefuehrt, waere das hier sichtbar geworden.
    """
    d = Driver(group(Echo.s("a"), chain(Echo.s("b"), Echo.s())))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert set(st["step_tasks"]) == {"/g0", "/g1/0", "/g1/1"}


async def test_stale_snapshot_keeps_both_step_markers():
    """AE10 / R22-R24 (Plan 2026-09-08-001, U9): Regressionstest des frueheren
    strikten xfail ``test_lost_state_update_still_truncates_group``.

    Zwei Advancer auf demselben Backend; der zweite hat beim Eintritt einen
    VERALTETEN Schnappschuss gelesen. Ohne Compare-and-Set loeschte sein
    Rueckschreiben den Schritt-Marker des ersten (verlorenes Zustands-Update,
    KTD5 des Plans 2026-08-30-002). Mit CAS scheitert die Schreibung, der
    Advancer merged seine Marker in den frischen Zustand -- beide Marker sind
    persistiert und das Gruppenergebnis vollstaendig.
    """
    m = MultiDriver(group(chain(Echo.s("a"), Echo.s()), chain(Echo.s("c"), Echo.s())))
    state = await m.seed()
    stale_raw = encode_json(state)

    # Advancer 1 schaltet die erste Kette weiter und markiert /g0/1.
    await m.advancer().advance(m.wf_id, "/g0/0", state["step_tasks"]["/g0/0"], ["a"])

    # Advancer 2 liest beim Eintritt noch den ALTEN Schnappschuss — genau das
    # Fenster, das ohne CAS offen steht — und schreibt ihn blind zurueck.
    # Kein kuenstlicher Marker: der Verlust entsteht auf dem normalen Weg.
    real_get = m.backend.get_workflow_state
    calls = {"n": 0}

    async def stale_once(wf_id, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return stale_raw
        return await real_get(wf_id, *args, **kwargs)

    m.backend.get_workflow_state = stale_once  # type: ignore[method-assign]
    try:
        await m.advancer().advance(m.wf_id, "/g1/0", state["step_tasks"]["/g1/0"], ["c"])
    finally:
        m.backend.get_workflow_state = real_get  # type: ignore[method-assign]

    # Beide Marker persistiert -- der Merge hat keinen verloren.
    assert set((await m.state())["step_tasks"]) >= {"/g0/1", "/g1/1"}

    while m.published:
        await m.run_next()

    assert (await m.state())["status"] == STATUS_COMPLETED
    assert await m.final_value() == [[["a"]], [["c"]]]


def _race_before_first_cas(m: MultiDriver, race):
    """Vor dem ERSTEN Compare-and-Set des Backends laeuft *race* -- ein
    nebenlaeufiger Schreiber, der den Zustand zwischen Lesen und Schreiben des
    Advancers veraendert. Zaehlt alle CAS-Aufrufe in ``m.cas_calls``."""
    real_cas = m.backend.compare_and_store_workflow_state
    m.cas_calls = 0

    async def racing_cas(wf_id, data, ttl_ms, *, expected_version, **kwargs):
        m.cas_calls += 1
        if m.cas_calls == 1:
            await race()
        return await real_cas(wf_id, data, ttl_ms, expected_version=expected_version, **kwargs)

    m.backend.compare_and_store_workflow_state = racing_cas  # type: ignore[method-assign]


async def test_conflict_with_fresh_failed_state_writes_nothing():
    """R23: ist der frische Zustand nach einem Konflikt terminal, endet der
    Advance ohne Schreibung und ohne Fehler."""
    m = MultiDriver(chain(Echo.s("a"), Echo.s()))
    state = await m.seed()

    async def fail_concurrently():
        await m.advancer().fail(m.wf_id, "TaskError", "boom")

    _race_before_first_cas(m, fail_concurrently)
    await m.advancer().advance(m.wf_id, "/0", state["step_tasks"]["/0"], ["a"])

    fresh = await m.state()
    assert fresh["status"] == STATUS_FAILED
    assert fresh["error"] == {"type": "TaskError", "message": "boom"}
    # Der Marker des verlierenden Advancers wurde NICHT nachgetragen.
    assert "/1" not in fresh["step_tasks"]
    assert m.cas_calls == 2  # fail() + der gescheiterte Advance; kein Retry


async def _bump_with_marker(m: MultiDriver, path: str, task_id: str) -> None:
    """Nebenlaeufiger Schreiber: traegt einen Marker nach und erhoeht die Version
    -- der Zustand bleibt laufend."""
    fresh = await m.state()
    fresh["step_tasks"][path] = task_id
    fresh["version"] = fresh.get("version", 0) + 1
    await m.backend.store_workflow_state(m.wf_id, encode_json(fresh), 60_000)


async def test_conflict_on_terminal_write_merges_terminal_fields():
    """AE10 / R23: verliert der ABSCHLIESSENDE Advancer das CAS gegen einen
    laufenden Schreiber, traegt der Merge ``status``, ``final_result_task_id``
    und ``error`` -- und ``handle.get()`` liefert das Ergebnis, nicht ``None``."""
    m = MultiDriver(Echo.s("a"))
    state = await m.seed()
    _race_before_first_cas(m, lambda: _bump_with_marker(m, "/other", "other-task"))

    await m.advancer().advance(m.wf_id, "", state["step_tasks"][""], ["a"])

    fresh = await m.state()
    assert fresh["status"] == STATUS_COMPLETED
    assert fresh["final_result_task_id"] == state["step_tasks"][""]
    assert fresh["error"] is None
    assert fresh["step_tasks"]["/other"] == "other-task"  # Bestand ueberlebt den Merge
    assert m.cas_calls == 2
    handle = WorkflowHandle(m.backend, m.wf_id, _unpack, cancel_ttl_ms=60_000)
    assert await handle.get(timeout=1.0) == ["a"]


async def test_twenty_five_conflicts_raise_workflow_state_conflict():
    """R23: die Konfliktgrenze liegt bei 25 Runden. Verliert ein Advancer jede
    davon, gilt die Nachricht als nicht abgeschlossen -- ``WorkflowStateConflict``
    (``worker/results.py`` bildet sie in U10 auf eine Redelivery ab)."""
    m = MultiDriver(chain(Echo.s("a"), Echo.s()))
    state = await m.seed()
    cas = AsyncMock(return_value=False)
    m.backend.compare_and_store_workflow_state = cas  # type: ignore[method-assign]

    with pytest.raises(WorkflowStateConflict):
        await m.advancer().advance(m.wf_id, "/0", state["step_tasks"]["/0"], ["a"])

    assert cas.await_count == 25
    assert issubclass(WorkflowStateConflict, SwarmQError)


async def test_twenty_four_conflicts_then_success_completes_the_write():
    m = MultiDriver(chain(Echo.s("a"), Echo.s()))
    state = await m.seed()
    real_cas = m.backend.compare_and_store_workflow_state
    calls = {"n": 0}

    async def flaky_cas(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 25:
            return False
        return await real_cas(*args, **kwargs)

    m.backend.compare_and_store_workflow_state = flaky_cas  # type: ignore[method-assign]
    await m.advancer().advance(m.wf_id, "/0", state["step_tasks"]["/0"], ["a"])

    assert calls["n"] == 25
    assert "/1" in (await m.state())["step_tasks"]


async def test_fail_merges_into_concurrently_advanced_state():
    """R22/R23: auch ``fail()`` schreibt per CAS; verliert es gegen einen
    laufenden Advancer, traegt der Merge FAILED + ``error`` in den frischen
    Zustand, dessen Marker bleiben erhalten."""
    m = MultiDriver(chain(Echo.s("a"), Echo.s()))
    await m.seed()
    _race_before_first_cas(m, lambda: _bump_with_marker(m, "/1", "wf-multi#/1"))

    await m.advancer().fail(m.wf_id, "TaskError", "boom")

    fresh = await m.state()
    assert fresh["status"] == STATUS_FAILED
    assert fresh["error"] == {"type": "TaskError", "message": "boom"}
    assert fresh["step_tasks"]["/1"] == "wf-multi#/1"
    assert m.cas_calls == 2


async def test_refresh_before_fan_in_honours_terminal_status():
    """R24: die frische Lesung vor dem Fan-in bleibt (Publish-vor-Persistenz-
    Fenster) und honoriert einen frischen Terminalstatus -- der Advance endet
    ohne Schreibung: kein CAS, kein Aggregat, kein Fehler."""
    m = MultiDriver(group(Echo.s("a"), Echo.s("b")))
    state = await m.seed()
    await m.advancer().advance(m.wf_id, "/g0", state["step_tasks"]["/g0"], ["a"])

    real_record = m.backend.chords.record_member

    async def record_then_fail(*args, **kwargs):
        fired = await real_record(*args, **kwargs)
        if fired:
            # Zwischen Fan-in-Zaehler und Lesung schlaegt ein anderer Worker fehl.
            await m.advancer().fail(m.wf_id, "TaskError", "boom")
        return fired

    m.backend.chords.record_member = record_then_fail  # type: ignore[method-assign]
    _race_before_first_cas(m, _noop)
    await m.advancer().advance(m.wf_id, "/g1", state["step_tasks"]["/g1"], ["b"])

    fresh = await m.state()
    assert fresh["status"] == STATUS_FAILED
    assert fresh["final_result_task_id"] is None
    assert m.cas_calls == 1, "nur fail() hat geschrieben; der Fan-in-Advance nicht"
    assert await m.backend.get_result(aggregate_result_id(m.wf_id, "")) is None


async def test_refresh_before_fan_in_adopts_fresh_version():
    """Die frische Lesung zieht neben ``step_tasks`` auch die ``version`` nach:
    der abschliessende Fan-in-Advancer mit veraltetem Eintritts-Schnappschuss
    schreibt in EINER CAS-Runde, statt erst zu verlieren und zu mergen."""
    m = MultiDriver(group(Echo.s("a"), Echo.s("b")))
    state = await m.seed()
    stale_raw = encode_json(state)
    await m.advancer().advance(m.wf_id, "/g0", state["step_tasks"]["/g0"], ["a"])
    await _bump_with_marker(m, "/other", "other-task")

    real_get = m.backend.get_workflow_state
    calls = {"n": 0}

    async def stale_once(wf_id, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return stale_raw
        return await real_get(wf_id, *args, **kwargs)

    m.backend.get_workflow_state = stale_once  # type: ignore[method-assign]
    _race_before_first_cas(m, _noop)
    try:
        await m.advancer().advance(m.wf_id, "/g1", state["step_tasks"]["/g1"], ["b"])
    finally:
        m.backend.get_workflow_state = real_get  # type: ignore[method-assign]

    fresh = await m.state()
    assert fresh["status"] == STATUS_COMPLETED
    assert fresh["step_tasks"]["/other"] == "other-task"
    assert fresh["version"] == 2
    assert m.cas_calls == 1


_GOLDEN_WORKFLOW_STATE = (
    Path(__file__).resolve().parents[1] / "integration" / "backend" / "golden" / "values" / "workflow_state.json"
)


async def test_golden_state_without_version_decodes_and_first_write_is_version_one():
    """R22: Dokumente aus 0.5.x (die Golden-Fixture) tragen keine ``version``;
    sie gelten als 0 und werden beim ersten Schreiben 1. Die Fixture bleibt
    unveraendert -- ein Bruch hier ist Stop-Condition (b) des Plans."""
    fixture = await asyncio.to_thread(_GOLDEN_WORKFLOW_STATE.read_bytes)
    golden = decode_json(fixture)
    assert "version" not in golden
    m = MultiDriver(_node_from_dict(golden["topology"]))
    m.wf_id = golden["workflow_id"]
    seeded = dict(golden, step_tasks={"/0": "seed-/0"})
    await m.backend.store_workflow_state(m.wf_id, encode_json(seeded), 60_000)
    await m._store("seed-/0", "golden.a", [1])

    await m.advancer().advance(m.wf_id, "/0", "seed-/0", [1])

    fresh = await m.state()
    assert fresh["version"] == 1
    assert set(fresh["step_tasks"]) == {"/0", "/1/h/g0", "/1/h/g1"}
    assert fresh["v"] == golden["v"]
    assert fresh["topology"] == golden["topology"]


async def test_expired_member_result_raises_workflow_result_expired():
    # R6: a chord fan-in where one (completed) member's result has expired
    # must raise WorkflowResultExpired, not silently pass None to the body.
    from swarmq.exceptions import WorkflowResultExpired

    be = _connected_backend()
    ch = be.chords
    adv = WorkflowAdvancer(
        be,
        ch,
        publisher=_NoopPublisher(),
        resolve_queue=lambda name: "default",
        make_task_id=lambda wf_id, sp: "t-x",
        unpack=_unpack,
        ttl_ms=60_000,
    )
    wf = chord(group(Echo.s("a"), Echo.s("b")), Echo.s())
    state = build_initial_workflow_state("wf", wf.to_dict())
    state["step_tasks"] = {"/h/g0": "m0", "/h/g1": "m1"}
    await be.store_workflow_state("wf", encode_json(state), 60_000)
    # Only m1's result exists; m0 expired before fan-in.
    await be.store_result(
        "m1",
        encode_result(
            {
                "task": {"id": "m1", "queue": "default", "name": "echo"},
                "is_error": False,
                "is_cancelled": False,
                "error_message": "",
                "result": ["b"],
            }
        ),
        60_000,
    )
    await adv.advance("wf", "/h/g0", "m0", ["a"])  # 1/2 — no gather yet
    with pytest.raises(WorkflowResultExpired):
        await adv.advance("wf", "/h/g1", "m1", ["b"])  # 2/2 — gathers, m0 gone


async def test_chain_with_embedded_chord():
    d = Driver(
        chain(
            Echo.s("start"),
            chord(group(Echo.s("a"), Echo.s("b")), Echo.s()),
            Echo.s(),
        )
    )
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED


# --------------------------------------------------------------------------
# Redelivery idempotency — the publish-then-mark step_tasks marker is the
# real one-shot (chord_complete.lua no longer holds a one-shot flag).
# --------------------------------------------------------------------------


async def test_chain_redelivery_does_not_double_enqueue():
    # AT_LEAST_ONCE: a redelivered chain step must not enqueue its successor
    # twice — the persisted next-enqueued marker (step_tasks) suppresses it.
    d = Driver(chain(Echo.s("a"), Echo.s()))
    await d.seed()  # advancing /0 enqueues /1
    assert len(d.published) == 1
    seed_tid = (await d.state())["step_tasks"]["/0"]
    await d.advancer.advance(d.wf_id, "/0", seed_tid, ["a"])  # redelivery
    assert len(d.published) == 1  # successor still enqueued exactly once


async def test_chord_member_redelivery_enqueues_body_once():
    # The chord Lua now reports "complete" on every post-completion call;
    # the body must still be enqueued exactly once, guarded by step_tasks["/b"].
    d = Driver(chord(group(Echo.s("a"), Echo.s("b")), Echo.s()))
    await d.seed()  # both header members advanced → body enqueued once

    def _body_msgs():
        return [p for p in d.published if p[2]["x-swarmq-workflow-step"] == "/b"]

    assert len(_body_msgs()) == 1
    last_tid = (await d.state())["step_tasks"]["/h/g1"]
    # Redeliver the last header member: der Chord-Store meldet erneut complete=True.
    await d.advancer.advance(d.wf_id, "/h/g1", last_tid, ["b"])
    assert len(_body_msgs()) == 1  # marker prevents a second body enqueue


class TestMarkEnqueuedRequiresReceipt:
    """AE5 (Plan U20): publish-then-mark ist strukturell erzwungen — die
    Markierung verlangt den ``PublishReceipt`` aus ``Publisher.publish``."""

    def _advancer(self):
        backend = _connected_backend()
        return WorkflowAdvancer(
            backend,
            backend.chords,
            publisher=_NoopPublisher(),
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )

    def test_forged_receipt_raises_type_error(self):
        advancer = self._advancer()

        class Fake:
            queue = "q"
            message_id = None

        state: dict[str, Any] = {"step_tasks": {}}
        with pytest.raises(TypeError, match="PublishReceipt"):
            advancer.mark_enqueued(state, "/1", "tid", Fake())
        assert state["step_tasks"] == {}

    def test_real_receipt_marks_the_step(self):
        advancer = self._advancer()
        state: dict[str, Any] = {"step_tasks": {}}
        advancer.mark_enqueued(state, "/1", "tid", PublishReceipt(queue="default"))
        assert state["step_tasks"] == {"/1": "tid"}


class _RecordingPublisher:
    """U11 (KTD7): Publisher-Seam des Advancers. Protokolliert die Reihenfolge
    von ``pre_enqueue`` und ``publish``, damit das Gate über die ganze Front
    prüfbar ist."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...]]] = []
        self.steps: list[WorkflowStep] = []

    async def pre_enqueue(self, steps) -> None:
        self.calls.append(("pre_enqueue", tuple(s.step_path for s in steps)))

    async def publish(self, step) -> PublishReceipt:
        self.calls.append(("publish", (step.step_path,)))
        self.steps.append(step)
        return PublishReceipt(queue=step.queue)


async def _advance_group_frontier(publisher) -> None:
    """Ein Kettenschritt endet, dessen Nachfolger eine Gruppe aus zwei
    Gliedern ist — die Front, die ``pre_enqueue`` als Ganzes sehen muss."""
    backend = _connected_backend()
    advancer = WorkflowAdvancer(
        backend,
        backend.chords,
        publisher=publisher,
        resolve_queue=lambda name: "default",
        make_task_id=workflow_step_task_id,
        unpack=_unpack,
        ttl_ms=60_000,
    )
    wf = chain(Echo.s("a"), group(Echo.s(), Echo.s()))
    state = build_initial_workflow_state("wf-front", wf.to_dict())
    state["step_tasks"] = {"/0": "t0"}
    state["version"] = 0
    await backend.store_workflow_state("wf-front", encode_json(state), 60_000)
    await backend.store_result(
        "t0",
        encode_result(
            {
                "task": {"id": "t0", "queue": "default", "name": "echo"},
                "is_error": False,
                "is_cancelled": False,
                "error_message": "",
                "result": ["a"],
            }
        ),
        60_000,
    )
    await advancer.advance("wf-front", "/0", "t0", ["a"])


async def test_pre_enqueue_gates_the_whole_frontier():
    """R15/AE6: ``pre_enqueue`` läuft für ALLE Schritte einer Front, bevor der
    erste publiziert wird — sonst könnte eine ablehnende Middleware eine
    Gruppe halb eingereiht hinterlassen, die den Fan-in nie erreicht."""
    publisher = _RecordingPublisher()
    await _advance_group_frontier(publisher)
    assert publisher.calls[0] == ("pre_enqueue", ("/1/g0", "/1/g1"))
    assert [c[0] for c in publisher.calls[1:]] == ["publish", "publish"]


async def test_middleware_rejecting_the_second_member_publishes_no_member():
    """R17/AE7: Eine Middleware, die erst das ZWEITE Glied einer Gruppen-Front
    ablehnt, darf das erste nicht durchlassen — eine halb eingereihte Gruppe
    erreicht den Fan-in nie und der Workflow haengt bis zum TTL. Die Ausnahme
    propagiert; der Result-Tail markiert den Workflow als gescheitert."""
    published: list[str] = []
    seen: list[str] = []

    class RejectSecondMember(Middleware):
        @override
        async def pre_enqueue(self, message) -> None:
            seen.append(message["step_path"])
            if message["step_path"] == "/1/g1":
                raise PermissionError("quota exceeded")

    async def _publish(queue, body, headers):
        published.append(headers["x-swarmq-workflow-step"])
        return PublishReceipt(queue=queue)

    publisher = WorkflowStepEnqueuer(
        publish=_publish,
        dispatch=AsyncMock(),
        get_chain=lambda: MiddlewareChain([RejectSecondMember()]),
    )
    with pytest.raises(PermissionError, match="quota exceeded"):
        await _advance_group_frontier(publisher)
    assert seen == ["/1/g0", "/1/g1"]  # beide gefragt
    assert published == []  # keines veroeffentlicht


async def test_post_enqueue_failure_does_not_lose_the_step_marker():
    """R15: Ein Fehler in ``post_enqueue`` ist nachgelagert — der Schritt ist
    veroeffentlicht, also muss der Marker gesetzt werden. Ohne Marker wuerde
    eine Redelivery den Schritt ein zweites Mal einreihen (die Kette schluckt
    den Fehler, wie im Client)."""
    published: list[str] = []

    class FailingPost(Middleware):
        @override
        async def post_enqueue(self, message) -> None:
            raise RuntimeError("metrics sink down")

    async def _publish(queue, body, headers):
        published.append(headers["x-swarmq-workflow-step"])
        return PublishReceipt(queue=queue)

    backend = _connected_backend()
    publisher = WorkflowStepEnqueuer(
        publish=_publish,
        dispatch=AsyncMock(),
        get_chain=lambda: MiddlewareChain([FailingPost()]),
    )
    advancer = WorkflowAdvancer(
        backend,
        backend.chords,
        publisher=publisher,
        resolve_queue=lambda name: "default",
        make_task_id=workflow_step_task_id,
        unpack=_unpack,
        ttl_ms=60_000,
    )
    wf = chain(Echo.s("a"), Echo.s())
    state = build_initial_workflow_state("wf-post", wf.to_dict())
    state["step_tasks"] = {"/0": "t0"}
    state["version"] = 0
    await backend.store_workflow_state("wf-post", encode_json(state), 60_000)
    await backend.store_result(
        "t0",
        encode_result(
            {
                "task": {"id": "t0", "queue": "default", "name": "echo"},
                "is_error": False,
                "is_cancelled": False,
                "error_message": "",
                "result": ["a"],
            }
        ),
        60_000,
    )
    await advancer.advance("wf-post", "/0", "t0", ["a"])

    assert published == ["/1"]
    raw_state = await backend.get_workflow_state("wf-post")
    assert raw_state is not None
    fresh = decode_json(raw_state)
    assert "/1" in fresh["step_tasks"]


async def test_fail_with_missing_workflow_state_logs_a_warning(swarmq_logs) -> None:
    """BZ-13 (Plan 0.12.0, MR 3): ein terminal endender Schritt, dessen
    Workflow-Zustand schon abgelaufen ist -- der Workflow endet nicht mehr
    ohne Spur."""
    backend = _connected_backend()
    advancer = WorkflowAdvancer(
        backend,
        backend.chords,
        publisher=_NoopPublisher(),
        resolve_queue=lambda _n: "default",
        make_task_id=lambda wf, path: f"{wf}:{path}",
        unpack=_unpack,
        ttl_ms=60_000,
    )

    await advancer.fail("wf-gone", "TaskError", "boom")

    [entry] = [e for e in swarmq_logs if e["event"] == "workflow_state_missing"]
    assert entry["log_level"] == "warning"
    assert entry["workflow_id"] == "wf-gone"


# P9 (Plan 2026-10-12-1105, MR 3, T13): ``fail`` speichert ``error.class`` nur
# mit Wert; ohne Keyword bleibt der Zustand exakt wie bisher (Tests oben).


async def test_fail_stores_error_class_only_when_given():
    with_class = MultiDriver(chain(Echo.s("a"), Echo.s()))
    await with_class.seed()
    await with_class.advancer().fail(with_class.wf_id, "TaskError", "", error_class="a.B")
    without = MultiDriver(chain(Echo.s("a"), Echo.s()))
    await without.seed()
    await without.advancer().fail(without.wf_id, "TaskError", "boom")

    assert (await with_class.state())["error"] == {"type": "TaskError", "message": "", "class": "a.B"}
    assert (await without.state())["error"] == {"type": "TaskError", "message": "boom"}
