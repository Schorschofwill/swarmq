"""Deterministische Abläufe der Workflow-Maschine (Plan 2026-10-17-0941, U8,
Testszenarien). Beide Stufen fahren sie: ``test_workflow_machine.py`` gegen
die Fakes, ``tests/integration/stateful/test_workflow_machine_valkey.py`` gegen
Valkey.

Jede Funktion bekommt eine frische Maschine und prüft alle Invarianten
(``verify``). Die Nachweise der Befunde (``NACHWEISE``, IDs aus
``docs/befunde/fehlersuche-runde-4-2026-10.md``) beschreiben das erwartete
Verhalten und scheitern am heutigen Stand.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, override
from unittest.mock import patch

import pytest

from swarmq.serialization import decode_result
from swarmq.workflow import WorkflowNode
from swarmq.workflow_orchestration import STATUS_COMPLETED, STATUS_FAILED, STATUS_RUNNING, WorkflowAdvancer
from tests.property.stateful.workflow_machine import TERMINAL, Msg, Spec, Wf, WorkflowMachine

CHORD3: Spec = ("chord", [("s", "a1"), ("s", "a2"), ("s", "a3")], ("s", "b"))


class ReferenceOff(WorkflowMachine):
    """Selbsttest: eine Referenz, die das Endergebnis falsch vorhersagt."""

    @override
    def start(self, spec: Spec) -> Wf:
        wf = super().start(spec)
        wf.expected = ["nicht", "das", "Ergebnis"]
        return wf


def _at(machine: WorkflowMachine, wf: Wf, path: str) -> Msg:
    """Die offene Zustellung des Schritts an *path*."""
    return next(msg for msg in machine.pending if msg.wf is wf and msg.path == path)


def _copy(machine: WorkflowMachine, msg: Msg) -> Msg:
    """Eine zweite Zustellung derselben Nachricht (Neuzustellung, Kanalverlust)."""
    return Msg(msg.seq + 1000, msg.wf, msg.path, msg.task_id, msg.body, dict(msg.headers))


def _final_value(machine: WorkflowMachine, wf: Wf) -> Any:
    state = machine.state_of(wf)
    assert state is not None
    raw = machine.run(machine.env.backend.get_result(state["final_result_task_id"]))
    assert raw is not None
    return decode_result(raw)["result"]


def chord_last_member_delivered_twice(machine: WorkflowMachine) -> None:
    """Chord mit drei Mitgliedern, das letzte zweimal zugestellt: Body einmal."""
    wf = machine.start(CHORD3)
    for path in ("/h/g0", "/h/g1", "/h/g2"):
        machine.deliver(_at(machine, wf, path))
        machine.verify()
    assert wf.publishes["/b"] == 1
    machine.redeliver(next(msg for msg in machine.done if msg.path == "/h/g2"))
    machine.verify()
    assert wf.publishes["/b"] == 1
    machine.drain()
    machine.verify()
    assert machine.status_of(wf) == STATUS_COMPLETED
    assert _final_value(machine, wf) == [[["a1"], ["a2"], ["a3"]], "b"]


def body_never_before_the_last_member(machine: WorkflowMachine) -> None:
    """Zwei von drei Mitgliedern, beliebig oft neu zugestellt: kein Body."""
    wf = machine.start(CHORD3)
    machine.deliver(_at(machine, wf, "/h/g2"))
    machine.deliver(_at(machine, wf, "/h/g0"))
    for msg in list(machine.done):
        machine.redeliver(msg)
        machine.verify()
    assert "/b" not in wf.publishes


def two_advancers_on_the_last_member(machine: WorkflowMachine) -> None:
    """Zwei Advancer überlappend auf dasselbe letzte Mitglied: ein CAS
    gewinnt, der zweite merged, der Workflow endet richtig, der Body läuft
    einmal. Publiziert wird er je nach Verschränkung ein- oder zweimal; die
    Kopie verwirft der Ausführungsschutz (E-009). Beleg zu R4Z-03 (NICHT
    HALTBAR): Zwei gleichzeitige Zustellungen derselben Nachricht verhindert
    der Running-Marker, den die Maschine nicht nachbildet; das Doppel-Enqueue
    im Crash-Fenster ist entschieden (Commit 4f8820d, Plan 2026-05-30-001)."""
    wf = machine.start(CHORD3)
    machine.deliver(_at(machine, wf, "/h/g0"))
    machine.deliver(_at(machine, wf, "/h/g1"))
    last = _at(machine, wf, "/h/g2")
    machine.concurrent(last, _copy(machine, last), ([1] * 12, [1] * 12))
    machine.verify()
    copies = wf.publishes["/b"] - 1
    assert copies in {0, 1}
    state = machine.state_of(wf)
    assert state is not None
    assert state["version"] == 2  # eine Schreibung gewann, die zweite folgte oder mergte
    discarded = machine.discarded
    machine.drain()
    machine.verify()
    assert machine.discarded == discarded + copies  # keine Body-Kopie lief
    assert machine.status_of(wf) == STATUS_COMPLETED
    assert _final_value(machine, wf) == [[["a1"], ["a2"], ["a3"]], "b"]


def member_fails_while_another_completes(machine: WorkflowMachine) -> None:
    """Ein Mitglied scheitert terminal, ein anderes wird fertig: failed, kein Body."""
    wf = machine.start(CHORD3)
    machine.deliver(_at(machine, wf, "/h/g0"))
    machine.deliver(_at(machine, wf, "/h/g1"), "dlq")
    machine.verify()
    assert machine.status_of(wf) == STATUS_FAILED  # synchron (E-040)
    machine.deliver(_at(machine, wf, "/h/g2"))
    machine.verify()
    assert machine.status_of(wf) == STATUS_FAILED
    assert "/b" not in wf.publishes


def chain_in_group_in_chord_in_any_order(machine: WorkflowMachine) -> None:
    """Chain in Group in Chord, Mitglieder rückwärts: Zähler und Ergebnis stimmen."""
    spec: Spec = (
        "chord",
        [("chain", [("s", "a1"), ("s", "a2")]), ("group", [("s", "a3"), ("si", "a4")])],
        ("chain", [("s", "b1"), ("s", "b2")]),
    )
    wf = machine.start(spec)
    while machine.pending:
        machine.deliver(machine.pending[-1])
        machine.verify()
    assert machine.status_of(wf) == STATUS_COMPLETED
    assert all(count == 1 for count in wf.publishes.values())
    assert _final_value(machine, wf) == [[[[["a1"], "a2"], [["a3"], ["a4"]]], "b1"], "b2"]


def faf_step_is_rejected_at_build(machine: WorkflowMachine) -> None:
    """E-039: FAF als Chain-Glied, Gruppenmitglied und Chord-Body."""
    machine.start_with_faf(("chain", [("s", "a1"), ("s", "a2")]), 1)
    machine.start_with_faf(("group", [("s", "a1"), ("s", "a2")]), 0)
    machine.start_with_faf(CHORD3, 3)
    assert not machine.wfs
    machine.verify()


def unroutable_successor_fails_the_workflow(machine: WorkflowMachine) -> None:
    """E-002: der Nachfolger hat keine Queue: failed, Zustellung bestätigt."""
    wf = machine.start(("chain", [("s", "a1"), ("ghost", "a2")]))
    machine.deliver(_at(machine, wf, "/0"))
    machine.verify()
    state = machine.state_of(wf)
    assert state is not None
    assert state["status"] == STATUS_FAILED
    assert state["error"]["type"] == "UnroutableMessage"
    assert not machine.pending


def unroutable_frontier_fails_at_apply(machine: WorkflowMachine) -> None:
    """E-002 beim Start: apply() wirft, der Zustand ist failed."""
    wf = machine.start(("group", [("s", "a1"), ("ghost", "a2")]))
    machine.verify()
    assert machine.status_of(wf) == STATUS_FAILED
    machine.drain()
    machine.verify()


def transient_fail_mark_is_redelivered(machine: WorkflowMachine) -> None:
    """E-040: das Fail-Mark scheitert vorübergehend: Neuzustellung, dann failed."""
    wf = machine.start(CHORD3)
    machine.backend_outage(1)
    machine.deliver(_at(machine, wf, "/h/g1"), "dlq")
    machine.verify()
    assert machine.status_of(wf) == STATUS_RUNNING
    machine.deliver(_at(machine, wf, "/h/g1"), "dlq")
    machine.verify()
    assert machine.status_of(wf) == STATUS_FAILED


def lost_cas_rounds_redeliver_and_complete(machine: WorkflowMachine) -> None:
    """25 verlorene CAS-Runden: Neuzustellung (WorkflowStateConflict), danach fertig."""
    wf = machine.start(("chain", [("s", "a1"), ("s", "a2")]))
    machine.cas_conflict(25)
    machine.deliver(_at(machine, wf, "/0"))
    machine.verify()
    assert _at(machine, wf, "/0") is not None  # neu zugestellt
    machine.drain()
    machine.verify()
    assert machine.status_of(wf) == STATUS_COMPLETED


def cancel_without_further_step_stays_running(machine: WorkflowMachine) -> None:
    """FS-32 (LÜCKE, offen): nach cancel enden die Schritte im Cancel-Gate,
    ``status()`` bleibt ``running``."""
    wf = machine.start(CHORD3)
    machine.cancel(wf)
    machine.drain()
    machine.verify()
    assert machine.status_of(wf) == STATUS_RUNNING
    assert "/b" not in wf.publishes


def reference_off_is_caught(machine: WorkflowMachine) -> None:
    """Selbsttest: W7 erkennt ein falsches Endergebnis."""
    machine.start(CHORD3)
    machine.drain()
    with pytest.raises(AssertionError, match="Endergebnis"):
        machine.verify()


# -- Nachweise der Befunde (xfail strict, scheitern am heutigen Stand) --------


def dead_letter_after_unadvanced_success(machine: WorkflowMachine) -> None:
    """F4-01 (R4Z-02): Der Schritt speichert sein Erfolgsergebnis, der Advance
    scheitert vorübergehend (Neuzustellung). Die Neuzustellung endet im
    Dead-Letter: ``fail_workflow_step`` sieht das alte Erfolgsergebnis und
    markiert nicht (E-040, Bewusste Folgen), ein Advance läuft nie mehr; der
    Workflow bleibt ``running`` bis zur TTL. Erwartet (E-040: „nie still in
    den ``get()``-Timeout“): ein terminaler Status. Deterministischer
    Nachweis ohne Maschine:
    ``tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py``."""
    wf = machine.start(("s", "a1"))
    machine.backend_write_outage()
    machine.deliver(_at(machine, wf, ""))  # Erfolg gespeichert, Abschluss scheitert
    machine.deliver(_at(machine, wf, ""), "dlq")
    assert not machine.pending
    assert machine.status_of(wf) in TERMINAL


def unmarked_member_completing_first_completes_the_workflow(machine: WorkflowMachine) -> None:
    """F4-16 (R4Z-01; E-073, Georg 2026-10-17, PF-3 Option a): Der Advance von
    ``/g1/0`` publiziert ``/g1/1``, das Compare-and-Set scheitert (Valkey kurz
    weg), die Zustellung geht zurück. ``/g1/1`` läuft vor der Neuzustellung:
    der Fan-in findet den Marker nicht und markiert den Workflow heute
    ``failed`` (``WorkflowValidationError``, „resolved 1 of 2 member results“),
    obwohl kein Schritt scheiterte. Erwartet (E-073): Der Fan-in leitet die
    Kennung des Blatts ab (``workflow_step_task_id``), der Workflow wird
    ``completed`` mit dem richtigen Endergebnis. Seit dem Fix (0.13.0) ist die
    Klammer ``deliverable`` der Maschine entfallen; der Unit-Nachweis ist
    ``test_lost_marker_of_composite_member_still_completes``
    (``tests/unit/test_workflow_advancer.py``)."""
    wf = machine.start(("group", [("s", "a1"), ("chain", [("s", "a2"), ("s", "a3")])]))
    machine.deliver(_at(machine, wf, "/g0"))
    machine.backend_write_outage()
    machine.deliver(_at(machine, wf, "/g1/0"))  # publiziert /g1/1, Neuzustellung
    machine.deliver(_at(machine, wf, "/g1/1"))  # läuft vor der Neuzustellung von /g1/0
    machine.drain()
    state = machine.state_of(wf)
    assert state is not None
    assert state["status"] == STATUS_COMPLETED, state
    machine.verify()


# -- Sabotage am Subjekt (Selbsttest der Invarianten, beide Stufen) -----------


def sabotage_chord_fires_one_member_early_is_caught(machine: WorkflowMachine) -> None:
    """Der Chord-Store meldet „vollständig“ ein Mitglied zu früh: der Fan-in
    kippt den Workflow ohne gescheiterten Schritt (W7)."""
    store = type(machine.env.backend.chords)
    original = store.record_member

    async def early(self: Any, wf_id: str, chord_id: str, member_id: str, *, expected: int, ttl_ms: int) -> bool:
        return bool(await original(self, wf_id, chord_id, member_id, expected=max(1, expected - 1), ttl_ms=ttl_ms))

    wf = machine.start(CHORD3)
    with patch.object(store, "record_member", early):
        machine.deliver(_at(machine, wf, "/h/g0"))
        machine.deliver(_at(machine, wf, "/h/g1"))
    with pytest.raises(AssertionError, match=r"\(W7\)"):
        machine.verify()


def sabotage_fail_mark_removed_is_caught(machine: WorkflowMachine) -> None:
    """``WorkflowAdvancer.fail`` tut nichts: ein Dead-Letter lässt den
    Workflow ``running`` (W4)."""

    async def no_mark(_self: WorkflowAdvancer, *_args: Any, **_kwargs: Any) -> None:
        return None

    wf = machine.start(CHORD3)
    machine.deliver(_at(machine, wf, "/h/g0"))
    with patch.object(WorkflowAdvancer, "fail", no_mark), pytest.raises(AssertionError, match=r"\(W4"):
        machine.deliver(_at(machine, wf, "/h/g1"), "dlq")


def sabotage_marker_skip_removed_is_caught(machine: WorkflowMachine) -> None:
    """``_enqueue_node`` überspringt markierte Schritte nicht mehr: die
    Neuzustellung des letzten Mitglieds publiziert den Body erneut (W2)."""
    original = WorkflowAdvancer._enqueue_node

    async def unguarded(
        self: WorkflowAdvancer, wf_id: str, state: dict[str, Any], node: WorkflowNode, node_path: str, *, inject: Any
    ) -> None:
        saved = dict(state["step_tasks"])
        for path in [p for p in saved if p.startswith(node_path)]:
            del state["step_tasks"][path]
        try:
            await original(self, wf_id, state, node, node_path, inject=inject)
        finally:
            for path, task_id in saved.items():
                state["step_tasks"].setdefault(path, task_id)

    wf = machine.start(CHORD3)
    for path in ("/h/g0", "/h/g1", "/h/g2"):
        machine.deliver(_at(machine, wf, path))
    with patch.object(WorkflowAdvancer, "_enqueue_node", unguarded):
        machine.redeliver(next(msg for msg in machine.done if msg.path == "/h/g2"))
    with pytest.raises(AssertionError, match=r"\(W2\)"):
        machine.verify()


Scenario = Callable[[WorkflowMachine], None]

SCENARIOS: dict[str, Scenario] = {
    "chord_last_member_delivered_twice": chord_last_member_delivered_twice,
    "body_never_before_the_last_member": body_never_before_the_last_member,
    "two_advancers_on_the_last_member": two_advancers_on_the_last_member,
    "member_fails_while_another_completes": member_fails_while_another_completes,
    "chain_in_group_in_chord_in_any_order": chain_in_group_in_chord_in_any_order,
    "faf_step_is_rejected_at_build": faf_step_is_rejected_at_build,
    "unroutable_successor_fails_the_workflow": unroutable_successor_fails_the_workflow,
    "unroutable_frontier_fails_at_apply": unroutable_frontier_fails_at_apply,
    "transient_fail_mark_is_redelivered": transient_fail_mark_is_redelivered,
    "lost_cas_rounds_redeliver_and_complete": lost_cas_rounds_redeliver_and_complete,
    "cancel_without_further_step_stays_running": cancel_without_further_step_stays_running,
}

SABOTAGES: dict[str, Scenario] = {
    "chord_one_member_early": sabotage_chord_fires_one_member_early_is_caught,
    "fail_mark_removed": sabotage_fail_mark_removed_is_caught,
    "marker_skip_removed": sabotage_marker_skip_removed_is_caught,
}

NACHWEISE: dict[str, Scenario] = {
    "F4-01": dead_letter_after_unadvanced_success,
    "F4-16": unmarked_member_completing_first_completes_the_workflow,
}

__all__ = ["NACHWEISE", "SABOTAGES", "SCENARIOS", "ReferenceOff", "reference_off_is_caught"]
