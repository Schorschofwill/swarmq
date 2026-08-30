"""U5: WorkflowAdvancer DAG logic, driven by an in-memory fake worker.

These tests exercise the full advancement recursion — chain progression
with result injection, immutable suppression, chord fan-in with ordered
results, standalone group aggregation, and the cancel gate — without any
broker/backend infrastructure. A tiny driver loop plays the role of the
worker: it drains published step messages, "runs" each (echoing a result),
stores the result, and calls advance() with the step's path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.contracts import PublishReceipt
from swarmq.serialization import decode_json, decode_result, encode_json, encode_result
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    WorkflowAdvancer,
    build_initial_workflow_state,
    initial_frontier,
    workflow_step_task_id,
)
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = [pytest.mark.timeout(5)]


class Echo(Task):
    task_info = TaskInfo(name="echo", queues=["default"])

    async def run(self, *args) -> list:
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


class Driver:
    """Plays the worker: builds a workflow, seeds initial frontier, then
    drains published step messages — running each, storing its result, and
    advancing the workflow — until no work remains."""

    def __init__(self, workflow):
        self.backend = _connected_backend()
        self.chord = self.backend.chords
        self.published: list[tuple[str, bytes, dict]] = []
        self.wf_id = "wf-test"
        self.advancer = WorkflowAdvancer(
            self.backend,
            self.chord,
            publish=self._publish,
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )
        self.workflow = workflow

    async def _publish(self, queue, body, headers):
        self.published.append((queue, body, headers))
        return PublishReceipt(queue=queue)

    async def _run_step(self, body: bytes) -> str:
        msg = decode_json(body)
        task_id = msg["task"]["id"]
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

    async def state(self) -> dict:
        raw = await self.backend.get_workflow_state(self.wf_id)
        assert raw is not None
        return decode_json(raw)

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
    d = Driver(
        chord(group(Echo.s("a"), chain(Echo.s("b"), Echo.s())), Echo.s())
    )
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
    d = Driver(
        group(Echo.s("a"), group(Echo.s("b"), chain(Echo.s("c"), Echo.s())))
    )
    await d.seed()
    await d.drain()
    assert await d.final_value() == [["a"], [["b"], [["c"]]]]


async def test_derived_member_id_resolves_to_that_members_result():
    """Deckungstest der tragenden Annahme (Plan KTD1 / Assumptions).

    Fuer jede Knotenart muss die abgeleitete Kennung genau das Ergebnis
    treffen, das dieses Glied als Ganzes produziert hat. Driften Ableitung
    und ``_materialise`` auseinander, bricht die Zusammenstellung still —
    dieser Test ist die Bremse davor.
    """
    d = Driver(
        group(
            Echo.s("a"),
            chain(Echo.s("b"), Echo.s()),
            group(Echo.s("c"), Echo.s("d")),
            chord(group(Echo.s("e")), Echo.s()),
        )
    )
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED

    expected = [["a"], [["b"]], [["c"], ["d"]], [[["e"]]]]
    assert await d.final_value() == expected

    step_tasks = st["step_tasks"]
    for i, member in enumerate(d.workflow.members):
        task_id = d.advancer._terminal_result_id(
            d.wf_id, member, f"/g{i}", step_tasks
        )
        assert task_id is not None, f"Glied {i} ohne ableitbare Kennung"
        raw = await d.backend.get_result(task_id)
        assert raw is not None, f"Glied {i}: Kennung zeigt auf kein Ergebnis"
        assert decode_result(raw)["result"] == expected[i]


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
                await d.advancer.advance(
                    d.wf_id, step_path, task_id, list(sig.args)
                )
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
    state["step_tasks"] = {
        k: v for k, v in state["step_tasks"].items() if k != drop_path
    }
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
        await d.advancer.advance(
            d.wf_id, "/h/g1", state["step_tasks"]["/h/g1"], ["b"]
        )
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
        self.published: list[tuple[str, bytes, dict]] = []
        self.wf_id = "wf-multi"
        self.workflow = workflow
        self.advancer_count = 0

    def advancer(self) -> WorkflowAdvancer:
        self.advancer_count += 1
        return WorkflowAdvancer(
            self.backend,
            self.backend.chords,
            publish=self._publish,
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )

    async def _publish(self, queue, body, headers):
        self.published.append((queue, body, headers))
        return PublishReceipt(queue=queue)

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

    async def seed(self) -> dict:
        state = build_initial_workflow_state(self.wf_id, self.workflow.to_dict())
        frontier = initial_frontier(self.workflow)
        state["step_tasks"] = {sp: f"seed-{sp}" for sp, _ in frontier}
        await self.backend.store_workflow_state(
            self.wf_id, encode_json(state), 60_000
        )
        for step_path, sig in frontier:
            await self._store(
                state["step_tasks"][step_path], sig.task_name, list(sig.args)
            )
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

    async def state(self) -> dict:
        raw = await self.backend.get_workflow_state(self.wf_id)
        assert raw is not None
        return decode_json(raw)

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
        await m.advancer().advance(
            m.wf_id, step_path, state["step_tasks"][step_path], list(sig.args)
        )
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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "KTD5 des Plans 2026-08-30-002: `advance()` ist ein Read-Modify-Write "
        "ohne CAS und `store_workflow_state` ueberschreibt das ganze Dokument "
        "blind, sodass zwei nebenlaeufige Advances einander Schritt-Marker "
        "loeschen koennen. Vorbestehend und bewusst nicht in diesem Plan "
        "behoben; der Guard aus U3 macht den Verlust jetzt sichtbar, statt ihn "
        "still zu kuerzen. Schlaegt dieser Test um, ist KTD5 behoben — dann "
        "gehoert er umgeschrieben, nicht geloescht."
    ),
)
async def test_lost_state_update_still_truncates_group():
    m = MultiDriver(group(chain(Echo.s("a"), Echo.s()), chain(Echo.s("c"), Echo.s())))
    state = await m.seed()
    stale_step_tasks = dict(state["step_tasks"])

    # Advancer 1 schaltet die erste Kette weiter und markiert /g0/1.
    await m.advancer().advance(m.wf_id, "/g0/0", stale_step_tasks["/g0/0"], ["a"])

    # Advancer 2 ist von DEMSELBEN Schnappschuss ausgegangen und schreibt ihn
    # blind zurueck — der Marker /g0/1 ist damit verloren.
    clobbered = await m.state()
    clobbered["step_tasks"] = {
        **stale_step_tasks,
        "/g1/1": workflow_step_task_id(m.wf_id, "/g1/1"),
    }
    await m.backend.store_workflow_state(m.wf_id, encode_json(clobbered), 60_000)
    await m.advancer().advance(m.wf_id, "/g1/0", stale_step_tasks["/g1/0"], ["c"])

    while m.published:
        await m.run_next()

    assert await m.final_value() == [[["a"]], [["c"]]]


async def test_expired_member_result_raises_workflow_result_expired():
    # R6: a chord fan-in where one (completed) member's result has expired
    # must raise WorkflowResultExpired, not silently pass None to the body.
    from swarmq.exceptions import WorkflowResultExpired

    be = _connected_backend()
    ch = be.chords
    adv = WorkflowAdvancer(
        be,
        ch,
        publish=lambda *a: _noop(),
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


async def test_workflow_step_task_id_is_deterministic():
    # Determinism is the property that keeps a re-enqueued member's id stable
    # so the chord SADD set cannot be inflated past N under a lost-persist crash.
    assert workflow_step_task_id("wf-1", "/h/g0") == workflow_step_task_id(
        "wf-1", "/h/g0"
    )
    assert workflow_step_task_id("wf-1", "/0") != workflow_step_task_id("wf-1", "/1")


class TestMarkEnqueuedRequiresReceipt:
    """AE5 (Plan U20): publish-then-mark ist strukturell erzwungen — die
    Markierung verlangt den ``PublishReceipt`` aus ``Publisher.publish``."""

    def _advancer(self):
        backend = _connected_backend()
        return WorkflowAdvancer(
            backend, backend.chords, publish=AsyncMock(), resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id, unpack=_unpack, ttl_ms=60_000,
        )

    def test_forged_receipt_raises_type_error(self):
        advancer = self._advancer()

        class Fake:
            queue = "q"
            message_id = None

        state = {"step_tasks": {}}
        with pytest.raises(TypeError, match="PublishReceipt"):
            advancer.mark_enqueued(state, "/1", "tid", Fake())
        assert state["step_tasks"] == {}

    def test_real_receipt_marks_the_step(self):
        advancer = self._advancer()
        state = {"step_tasks": {}}
        advancer.mark_enqueued(state, "/1", "tid", PublishReceipt(queue="default"))
        assert state["step_tasks"] == {"/1": "tid"}
