"""Deterministische Abläufe der Retry-Maschine (Plan 2026-10-17-0941, U9,
Testszenarien). Beide Stufen fahren sie: ``test_retry_chain_machine.py``
gegen die Fakes, ``tests/integration/stateful/test_retry_chain_machine_valkey.py``
gegen Valkey.

Jede Funktion bekommt eine frische Maschine. ``deliver`` prüft bei jeder
Zustellung K1 bis K7 gegen das Modell; ``verify`` die übrigen Invarianten.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, override

import pytest

from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.serialization import decode_result
from tests.property.stateful.retry_chain_machine import (
    Act,
    ChainCfg,
    Pending,
    RetryChainMachine,
    lenient_count,
    model_identity,
)

ERROR = Act("error")


class LenientCountModel(RetryChainMachine):
    """Selbsttest: ein Modell, das den Retry-Zähler nachsichtig liest
    (``"3"`` zählt 3) -- widerspricht E-051."""

    @staticmethod
    @override
    def _rc(pending: Pending) -> int:
        return max(lenient_count(pending.headers.get(HEADER_RETRY_COUNT)) or 0, 0)


def only(machine: RetryChainMachine) -> Pending:
    """Die einzige wartende Zustellung."""
    assert len(machine.pending) == 1, machine.pending
    return machine.pending[0]


def verify(machine: RetryChainMachine) -> None:
    machine.swarmq_copies_without_broker_counters()
    machine.one_settled_run_per_attempt()
    machine.one_guarded_dead_letter_per_identity()


def retry_copies(machine: RetryChainMachine) -> list[Any]:
    return [copy.headers.get(HEADER_RETRY_COUNT) for copy in machine.broker.copies if copy.via == "retry_later"]


def three_failures_with_two_retries_end_in_one_dead_letter(machine: RetryChainMachine) -> None:
    """``max_retries=2``: drei retry-fähige Fehler -> Zähler 0, 1, 2, zwei
    Retry-Kopien, eine DLQ-Kopie mit Fehlerergebnis (K2, K7)."""
    machine.reconfigure(ChainCfg(max_retries=2, backoff="fixed", base_delay=1.0))
    machine.publish("t1")
    seen = []
    for _ in range(3):
        pending = only(machine)
        seen.append(pending.headers.get(HEADER_RETRY_COUNT, 0))
        machine.deliver(pending, ERROR)
        machine.advance(1)
        verify(machine)
    assert seen == [0, 1, 2]
    assert retry_copies(machine) == [1, 2]
    assert [task_id for task_id, _ in machine.broker.dead] == ["t1"]
    assert machine.broker.dead[0][1][HEADER_RETRY_COUNT] == 2
    assert machine.pending == []


def broken_retry_count_header_counts_as_none(machine: RetryChainMachine) -> None:
    """``"abc"``, ``-2``, ``True``, ``2.5``: die Zustellung zählt als 0, die
    Retry-Kopie trägt 1; kein Absturz, genau ein Verb (K1, K5)."""
    machine.reconfigure(ChainCfg(max_retries=3))
    for index, value in enumerate(("abc", -2, True, 2.5), start=1):
        task_id = machine.publish(f"t{index}")
        pending = machine.set_retry_count(only(machine), value)
        expect = machine.deliver(pending, ERROR)
        assert (expect.verb, expect.copy_retry_count) == ("retry_later", 1), (value, expect)
        machine.advance(2)
        copy = only(machine)
        assert copy.task_id == task_id
        assert copy.headers[HEADER_RETRY_COUNT] == 1
        machine.deliver(copy, Act("ok"))
        verify(machine)


def negative_retry_delay_is_clamped_and_counts(machine: RetryChainMachine) -> None:
    """E-060: ``Retry(delay=-5)`` -> Kopie mit 0 ms, Zähler erhöht; bei
    erschöpftem Budget DLQ."""
    machine.reconfigure(ChainCfg(max_retries=1))
    machine.publish("t1")
    machine.deliver(only(machine), Act("retry", -5.0))
    [call] = machine.broker.calls[next(iter(machine.broker.calls))]
    assert (call.verb, call.delay_ms) == ("retry_later", 0)
    machine.advance(1)
    copy = only(machine)
    assert copy.headers[HEADER_RETRY_COUNT] == 1
    expect = machine.deliver(copy, Act("retry", -5.0))
    assert expect.verb == "dead_letter"
    verify(machine)


def hard_timeout_without_retries_goes_to_dead_letter(machine: RetryChainMachine) -> None:
    """E-037: Hard-Timeout bei ``max_retries=0`` -> direkt DLQ mit
    Fehlerergebnis, kein ``redeliver``; Kindtod ebenso."""
    machine.reconfigure(ChainCfg(max_retries=0, timeouts="hard"))
    machine.publish("t1")
    expect = machine.deliver(only(machine), Act("hang"))
    assert (expect.verb, expect.stores_error) == ("dead_letter", True)
    machine.publish("t2")
    expect = machine.deliver(only(machine), Act("child_death"))
    assert (expect.verb, expect.stores_error) == ("dead_letter", True)
    assert machine.pending == []
    assert [task_id for task_id, _ in machine.broker.dead] == ["t1", "t2"]
    verify(machine)


def soft_timeout_and_child_death_take_the_retry_path(machine: RetryChainMachine) -> None:
    """E-037/E-045: Soft-Timeout und Kindtod verbrauchen einen Retry."""
    machine.reconfigure(ChainCfg(max_retries=2, timeouts="soft"))
    machine.publish("t1")
    machine.deliver(only(machine), Act("hang"))
    machine.advance(1)
    machine.deliver(only(machine), Act("child_death"))
    machine.advance(1)
    assert only(machine).headers[HEADER_RETRY_COUNT] == 2
    verify(machine)


def replay_with_new_identity_runs_and_same_identity_is_skipped(machine: RetryChainMachine) -> None:
    """E-009: ``dlq retry`` läuft (neue Identität); eine zweite Kopie
    derselben Wiederholung und ein UI-Move ohne Replay-ID sind Duplikate."""
    machine.reconfigure(ChainCfg(max_retries=0))
    machine.publish("t1")
    machine.deliver(only(machine), ERROR)
    machine.replay("t1", "dlq_retry_twice")
    first, second = machine.pending
    assert model_identity(first.headers, "t1") != "t1"
    assert model_identity(first.headers, "t1") == model_identity(second.headers, "t1")
    assert machine.deliver(first, Act("ok")).reason == "success"
    assert machine.deliver(second, Act("ok")).reason == "done"

    machine.publish("t2")
    machine.deliver(only(machine), ERROR)
    machine.replay("t2", "ui_move_without_replay_id")
    assert machine.deliver(only(machine), Act("ok")).reason == "done"
    verify(machine)


def ui_replay_with_string_count_gets_the_full_budget(machine: RetryChainMachine) -> None:
    """E-051: UI-Replay mit ``x-swarmq-retry-count: "3"`` zählt wie 0."""
    machine.reconfigure(ChainCfg(max_retries=3))
    machine.publish("t1")
    for _ in range(4):
        machine.deliver(only(machine), ERROR)
        machine.advance(2)
    assert machine.broker.dead[0][1][HEADER_RETRY_COUNT] == 3
    machine.replay("t1", "ui_move_str")
    replayed = only(machine)
    assert replayed.headers[HEADER_RETRY_COUNT] == "3"
    expect = machine.deliver(replayed, ERROR)
    assert (expect.verb, expect.copy_retry_count) == ("retry_later", 1)
    verify(machine)


def ui_move_keeps_the_exhausted_count(machine: RetryChainMachine) -> None:
    """Gegenstück: ein UI-Move mit ganzzahligem Zähler läuft (neue Identität,
    BZ-08), hat aber kein Budget mehr und geht beim nächsten Fehler zurück
    in die DLQ."""
    machine.reconfigure(ChainCfg(max_retries=1))
    machine.publish("t1")
    machine.deliver(only(machine), ERROR)
    machine.advance(2)
    machine.deliver(only(machine), ERROR)
    machine.replay("t1", "ui_move")
    assert machine.deliver(only(machine), ERROR).reason == "exhausted"
    assert len(machine.broker.dead) == 2
    verify(machine)


def fixed_backoff_waits_min_of_base_and_cap(machine: RetryChainMachine) -> None:
    """E-023: ``fixed`` mit Basis 1 s wartet 1 000 ms, mit Basis 90 s den
    Deckel ``retry_max_delay`` (60 000 ms), ohne Jitter."""
    machine.reconfigure(ChainCfg(max_retries=3, backoff="fixed", base_delay=1.0))
    machine.publish("t1")
    machine.deliver(only(machine), ERROR)
    machine.advance(1)
    machine.reconfigure(ChainCfg(max_retries=3, backoff="fixed", base_delay=90.0))
    machine.deliver(only(machine), ERROR)
    delays = [call.delay_ms for calls in machine.broker.calls.values() for call in calls]
    assert delays == [1_000, 60_000]
    verify(machine)


def crash_defers_the_redelivery_until_the_marker_expires(machine: RetryChainMachine) -> None:
    """E-046: Absturz ohne Finish -> die Neuzustellung wird zurückgestellt
    (kein Retry-Verbrauch), nach Ablauf des Running-Markers läuft sie (Waise)."""
    machine.publish("t1")
    machine.deliver(only(machine), Act("crash"))
    redelivered = only(machine)
    assert redelivered.headers["x-delivery-count"] == 1
    expect = machine.deliver(redelivered, Act("ok"))
    assert expect.reason == "guard_defer"
    machine.advance(61)
    copy = only(machine)
    assert "x-delivery-count" not in copy.headers
    assert machine.deliver(copy, Act("ok")).reason == "success"
    assert machine.guard["t1"].orphans == 1
    verify(machine)


def three_orphans_end_in_the_dead_letter_queue(machine: RetryChainMachine) -> None:
    """E-046 Nachtrag F3-27: die Absturzschleife endet über
    ``execution_guard_max_orphans`` (3) in der DLQ."""
    machine.publish("t1")
    for _ in range(3):
        machine.deliver(only(machine), Act("crash"))
        machine.deliver(only(machine), Act("crash"))  # Neuzustellung: zurückgestellt
        machine.advance(61)
    expect = machine.deliver(only(machine), Act("ok"))
    assert (expect.verb, expect.reason) == ("dead_letter", "orphan_limit")
    verify(machine)


def rate_limit_window_defers_the_third_and_releases_later(machine: RetryChainMachine) -> None:
    """E-070, ``sliding_window_counter.lua``: zwei Läufe je Minute, der dritte
    wird mit ``retry_after_ms`` zurückgestellt und läuft nach dem Fenster."""
    machine.reconfigure(ChainCfg(rate_limited=True))
    for index in (1, 2, 3):
        machine.publish(f"t{index}")
    for _ in range(2):
        assert machine.deliver(machine.pending[0], Act("ok")).reason == "success"
    expect = machine.deliver(only(machine), Act("ok"))
    assert expect.reason == "rate_limit"
    assert expect.delay_ms is not None
    machine.advance(expect.delay_ms[0] // 1000 + 1)
    copy = only(machine)
    assert copy.headers["x-swarmq-rate-limit-count"] == 1
    assert machine.deliver(copy, Act("ok")).reason == "success"
    verify(machine)


def redeliver_loop_ends_at_the_delivery_limit(machine: RetryChainMachine) -> None:
    """BZ-29, ``gates/delivery_count.py``: scheitert der Retry-Publish
    wiederholt, zählt ``x-acquired-count``; ab 3 geht die Zustellung vor dem
    Ausführungsschutz in die DLQ (``RedeliveryLimitError``)."""
    machine.publish("t1")
    for count in range(3):
        expect = machine.deliver(only(machine), ERROR, copy_failure=True)
        assert expect.verb == "redeliver"
        assert only(machine).headers["x-acquired-count"] == count + 1
    expect = machine.deliver(only(machine), ERROR)
    assert (expect.verb, expect.reason, expect.runs) == ("dead_letter", "delivery_limit", False)
    verify(machine)


def _redelivered_at_the_limit(machine: RetryChainMachine) -> Pending:
    """Zwei weitere Kanalverluste beim Bestätigen des Duplikats: die
    Neuzustellung trägt ``x-delivery-count`` 3 (Schwelle bei Limit 4)."""
    for _ in range(2):
        expect = machine.deliver(only(machine), Act("ok"), lose_settle=True)
        assert expect.reason in ("done", "superseded"), expect
    redelivered = only(machine)
    assert redelivered.headers["x-delivery-count"] == 3
    return redelivered


def _handle_unchecked(machine: RetryChainMachine, pending: Pending) -> None:
    """Die Zustellung ohne Modellprüfung durch die Pipeline (das Modell
    meldet R4Z-04 vorab)."""
    machine.pending.remove(pending)
    machine.current = pending
    machine.act = Act("ok")
    try:
        machine.run(machine.io._handle_message(pending.delivery))
    finally:
        machine.current = None


def r4z_04_completed_task_redelivered_at_the_limit_stays_a_duplicate(machine: RetryChainMachine) -> None:
    """F4-02 (R4Z-04): Der Task lief erfolgreich, das Ack ging verloren
    (Kanal weg, Done-Marker gesetzt, ``GuardedBroker.ack``). Die
    Neuzustellungen werden als Duplikat bestätigt -- bis ``x-delivery-count``
    die Schwelle erreicht: dann schickt das Zustelllimit-Gate, das vor dem
    Ausführungsschutz liegt, die Nachricht in die DLQ und überschreibt das
    Erfolgsergebnis mit einem Fehler. Erwartet (E-009, E-043): Duplikat,
    keine DLQ-Kopie, Ergebnis bleibt Erfolg. Deterministischer Nachweis ohne
    Maschine: ``tests/unit/worker/test_befund_f4_02_delivery_limit_before_guard.py``."""
    machine.publish("t1")
    assert machine.deliver(only(machine), Act("ok"), lose_settle=True).reason == "success"
    redelivered = _redelivered_at_the_limit(machine)
    assert machine.predict(redelivered, Act("ok"), copy_failure=False).suspect == "R4Z-04"
    _handle_unchecked(machine, redelivered)
    raw = machine.run(machine.env.backend.get_result("t1"))
    assert raw is not None
    result = decode_result(raw)
    assert result["is_error"] is False, f"Erfolgsergebnis überschrieben: {result['error_message']!r}"
    assert machine.broker.dead == [], "erfolgreicher Task in der DLQ"


def r4z_04_superseded_attempt_redelivered_at_the_limit_stays_a_duplicate(machine: RetryChainMachine) -> None:
    """F4-02 (R4Z-04), zweite Form: Der Versuch scheiterte, die Retry-Kopie liegt
    (Superseded-Marker), nur das Ack des Originals ging verloren
    (``SettleFailedAfterPublish``). Am Zustelllimit geht das Original in die
    DLQ und schreibt ein Fehlerergebnis, während die Kette über die
    Retry-Kopie weiterläuft; ``get_result`` meldet den Fehler vorzeitig."""
    machine.reconfigure(ChainCfg(max_retries=3))
    machine.publish("t1")
    assert machine.deliver(only(machine), ERROR, lose_settle=True).reason == "retry"
    redelivered = _redelivered_at_the_limit(machine)
    _handle_unchecked(machine, redelivered)
    raw = machine.run(machine.env.backend.get_result("t1"))
    assert machine.broker.dead == [], "überholter Versuch in der DLQ, die Retry-Kopie läuft weiter"
    assert raw is None, "Fehlerergebnis, obwohl die Kette über die Retry-Kopie weiterläuft"


def republished_retry_copy_with_text_count_is_skipped_as_superseded(machine: RetryChainMachine) -> None:
    """E-051 (R4Z-05, ENTSCHEIDUNG; Nachtrag vorgeschlagen als PF-4): Eine
    Retry-Kopie (Zähler 1) wird über die RabbitMQ-UI ohne neue Replay-ID neu
    publiziert, die UI setzt den Header als Text ``"1"``. Streng gelesen
    zählt er 0 (E-051), der Ausführungsschutz sieht einen vom Retry überholten
    Versuch (Superseded-Marker 1, E-009, KTD5) und bestätigt die Nachricht als
    Duplikat (``duplicate_delivery_skipped``, ``reason=superseded``): kein
    Lauf, kein Ergebnis, keine DLQ-Kopie. Wiedereinspielen nur über
    ``swarmq dlq retry`` (neue Identität)."""
    machine.reconfigure(ChainCfg(max_retries=3))
    machine.publish("t1")
    machine.deliver(only(machine), ERROR)
    machine.advance(2)
    moved = machine.set_retry_count(only(machine), "1")
    runs_before = len(machine.runs)
    assert machine.deliver(moved, Act("ok")).reason == "superseded"
    assert machine.pending == []
    assert len(machine.runs) == runs_before, "UI-Republish mit Text-Zähler lief, obwohl überholt (E-051)"
    assert machine.run(machine.env.backend.get_result("t1")) is None
    assert machine.broker.dead == []


def lenient_model_is_caught(machine: RetryChainMachine) -> None:
    """Selbsttest: ``LenientCountModel`` erwartet beim UI-Replay mit ``"3"``
    den DLQ-Pfad; die Pipeline liest streng und wiederholt (K2/K5)."""
    with pytest.raises(
        AssertionError, match=r"erwartet dead_letter \(exhausted\), Broker sah \[VerbCall\(verb='retry_later'"
    ):
        ui_replay_with_string_count_gets_the_full_budget(machine)


SCENARIOS: dict[str, Callable[[RetryChainMachine], None]] = {
    fn.__name__: fn
    for fn in (
        three_failures_with_two_retries_end_in_one_dead_letter,
        broken_retry_count_header_counts_as_none,
        negative_retry_delay_is_clamped_and_counts,
        hard_timeout_without_retries_goes_to_dead_letter,
        soft_timeout_and_child_death_take_the_retry_path,
        replay_with_new_identity_runs_and_same_identity_is_skipped,
        ui_replay_with_string_count_gets_the_full_budget,
        ui_move_keeps_the_exhausted_count,
        fixed_backoff_waits_min_of_base_and_cap,
        crash_defers_the_redelivery_until_the_marker_expires,
        three_orphans_end_in_the_dead_letter_queue,
        rate_limit_window_defers_the_third_and_releases_later,
        redeliver_loop_ends_at_the_delivery_limit,
    )
}

# Nachweise des Befunds F4-02 (R4Z-04; xfail strict, scheitern am heutigen Stand).
F4_02_NACHWEISE: dict[str, Callable[[RetryChainMachine], None]] = {
    fn.__name__: fn
    for fn in (
        r4z_04_completed_task_redelivered_at_the_limit_stays_a_duplicate,
        r4z_04_superseded_attempt_redelivered_at_the_limit_stays_a_duplicate,
    )
}
