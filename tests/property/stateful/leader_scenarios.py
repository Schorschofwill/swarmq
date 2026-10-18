"""Deterministische Abläufe der Leader-Maschine (Plan 2026-10-17-0941, U7,
Testszenarien). Beide Stufen fahren sie: ``test_leader_machine.py`` gegen die
Fakes, ``tests/integration/stateful/test_leader_machine_valkey.py`` gegen
Valkey.

Jede Funktion bekommt eine frische Maschine; die Regeln prüfen ihre
Invarianten selbst, ``verify`` vergleicht Store und Modell.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest

from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.fire import SchedulerFire
from tests.property.stateful.leader_machine import (
    CLI_CRON,
    STATIC_CRON,
    CommitCall,
    LeaderMachine,
    LockCall,
    PublishAttempt,
)


def _state(machine: LeaderMachine, cand: str) -> str:
    return machine.cands[cand].loop.state


def _make_leader(machine: LeaderMachine, cand: str) -> None:
    machine.election_tick(cand)
    assert _state(machine, cand) == "leader"
    machine.adopt_tick(cand)
    machine.verify()


def _advance_ticking(machine: LeaderMachine, seconds: int, cand: str = "A") -> None:
    """Zeit vorstellen; ``cand`` taktet seine Wahl wie die Schleife (Heartbeat)."""
    while seconds > 0:
        step = min(seconds, machine.heartbeat_s)
        machine.advance(step)
        seconds -= step
        machine.election_tick(cand)


def _due_cli_cron(machine: LeaderMachine) -> int:
    machine.add_cli_cron()
    score = machine.index[CLI_CRON]
    _advance_ticking(machine, (score - machine.now_ms) // 1000 + 1)
    machine.verify()
    return score


def lock_expires_during_tick_commits_once(machine: LeaderMachine) -> None:
    """Zwei Kandidaten, das Lock geht mitten im Tick an B: A publiziert, sein
    Commit wird abgelehnt (Standby); B publiziert erneut (E-047) und schreibt
    genau einmal fest."""
    _make_leader(machine, "A")
    score = _due_cli_cron(machine)
    start = len(machine.events)

    machine.poll_tick("A", mid="takeover")

    assert _state(machine, "A") == "standby"
    assert _state(machine, "B") == "leader"
    machine.verify()
    machine.adopt_tick("B")
    machine.poll_tick("B")
    machine.verify()
    events = machine.events[start:]
    published = [e.cand for e in events if isinstance(e, PublishAttempt) and e.score == score]
    effective = [e.cand for e in events if isinstance(e, CommitCall) and e.fired == score and e.effective]
    assert published == ["A", "B"]
    assert effective == ["B"]
    assert machine.index[CLI_CRON] > score


def oneshot_with_lost_insert_reply_is_in_the_fire_index(machine: LeaderMachine) -> None:
    """E-058: Das zweite, byte-gleiche Insert meldet ``False``; der Eintrag
    steht trotzdem im Fire-Index und feuert unter seiner ``task_id``."""
    task_id = machine.schedule_oneshot(30, reply_lost=True)
    machine.verify()
    name = f"oneshot:{task_id}"
    assert machine.run(machine.env.backend.schedules.fire_ts(name)) == machine.index[name]

    _make_leader(machine, "A")
    _advance_ticking(machine, 31)
    machine.poll_tick("A")
    machine.verify()
    assert name in machine.published_oneshots
    assert name not in machine.entries


def publish_failures_over_stepdown_s_step_down_and_cool_down(machine: LeaderMachine) -> None:
    """E-013: zwei Verbindungsfehler im Abstand der Frist lassen A
    zurücktreten; 2 x TTL bewirbt sich A nicht, danach wieder."""
    _make_leader(machine, "A")
    machine.add_cli_cron()
    machine.cands["A"].publisher.fault = "connection"
    window_s = machine.stepdown_window_ms // 1000
    _advance_ticking(machine, (machine.index[CLI_CRON] - machine.now_ms) // 1000 + 1)
    machine.poll_tick("A")
    assert _state(machine, "A") == "leader"
    _advance_ticking(machine, window_s)
    machine.poll_tick("A")
    assert _state(machine, "A") == "standby"
    stepdown_at = machine.now_ms
    assert machine.run(machine.env.backend.locks.current_owner(SCHEDULER_LOCK_NAME)) is None

    start = len(machine.events)
    for _ in range(2 * machine.ttl_ms // 10_000 - 1):
        machine.advance(10)
        machine.election_tick("A")
    attempts = [e for e in machine.events[start:] if isinstance(e, LockCall) and e.op == "acquire"]
    assert attempts == []
    machine.advance(10)
    assert machine.now_ms - stepdown_at == 2 * machine.ttl_ms
    machine.election_tick("A")
    assert _state(machine, "A") == "leader"
    machine.verify()


def stepdown_with_failed_release_leaves_the_lock_to_its_ttl(machine: LeaderMachine) -> None:
    """E-013 mit Valkey-Ausfall: Die Freigabe beim Rücktritt scheitert, A ist
    trotzdem Standby; das Lock läuft per TTL ab, erst dann übernimmt B."""
    _make_leader(machine, "A")
    machine.add_cli_cron()
    machine.cands["A"].publisher.fault = "connection"
    _advance_ticking(machine, (machine.index[CLI_CRON] - machine.now_ms) // 1000 + 1)
    machine.poll_tick("A")
    _advance_ticking(machine, machine.stepdown_window_ms // 1000)
    machine.cands["A"].locks.down = True
    start = len(machine.events)

    machine.poll_tick("A")

    assert _state(machine, "A") == "standby"
    releases = [e for e in machine.events[start:] if isinstance(e, LockCall) and e.op == "release"]
    assert releases == [LockCall("A", "release", None)]
    assert machine.run(machine.env.backend.locks.current_owner(SCHEDULER_LOCK_NAME)) == machine.cands["A"].owner
    machine.election_tick("B")
    assert _state(machine, "B") == "standby"
    machine.advance(machine.ttl_ms // 1000)
    machine.election_tick("B")
    assert _state(machine, "B") == "leader"
    machine.verify()


def removed_cron_while_due_never_fires(machine: LeaderMachine) -> None:
    """Ein fälliger Cron wird entfernt: kein Fire danach, Fire-Index leer."""
    _make_leader(machine, "A")
    _due_cli_cron(machine)
    start = len(machine.events)

    machine.remove_cron(CLI_CRON)
    machine.poll_tick("A")

    machine.verify()
    assert not [e for e in machine.events[start:] if isinstance(e, PublishAttempt)]
    assert machine.run(machine.env.backend.schedules.fire_ts(CLI_CRON)) is None
    assert machine.index == {}


def unroutable_oneshot_backs_off_and_is_dropped_with_an_error_result(machine: LeaderMachine) -> None:
    """E-004: +60 s je Versuch, ``unroutable_since`` bleibt; nach der Frist
    verworfen mit Fehlerergebnis."""
    _make_leader(machine, "A")
    task_id = machine.schedule_oneshot(1)
    name = f"oneshot:{task_id}"
    machine.cands["A"].publisher.fault = "unroutable"
    _advance_ticking(machine, 1)
    first = machine.now_ms
    while name in machine.entries:
        machine.poll_tick("A")
        machine.verify()
        if name in machine.entries:
            assert machine.entries[name].unroutable_since == first
            assert machine.index[name] == machine.now_ms + 60_000
        _advance_ticking(machine, 60)
    assert name in machine.dropped_oneshots
    assert machine.run(machine.env.backend.get_result(task_id)) is not None


def renew_false_sends_the_leader_to_standby(machine: LeaderMachine) -> None:
    """E-048: Verschwindet der Leader-Key, liefert der Renew ``False``; der
    Leader geht sofort in den Standby."""
    _make_leader(machine, "A")
    machine.leader_lock_lost()
    machine.election_tick("B")
    assert _state(machine, "B") == "leader"
    machine.advance(10)
    machine.election_tick("A")
    assert _state(machine, "A") == "standby"


def settle_after_faults_fires_everything_due(machine: LeaderMachine) -> None:
    """L9: Nach Störungen auf beiden Seiten findet ``settle`` einen Leader,
    und nichts bleibt fällig."""
    _make_leader(machine, "A")
    machine.add_cli_cron()
    machine.schedule_oneshot(30)
    machine.cands["A"].publisher.fault = "indeterminate_lost"
    machine.cands["A"].locks.down = True
    machine.advance(61)
    machine.poll_tick("A")
    machine.election_tick("A")
    machine.election_tick("B")
    machine.settle()
    machine.verify()


def unroutable_cron_fire_is_committed_to_the_next_occurrence(machine: LeaderMachine) -> None:
    """E-003: Der unroutbare Cron-Fire ist verloren, wird gezählt und auf den
    nächsten Termin festgeschrieben."""
    _make_leader(machine, "A")
    machine.cands["A"].publisher.fault = "unroutable"
    score = _due_cli_cron(machine)

    machine.poll_tick("A")

    machine.verify()
    assert machine.outcomes["cron E-003"] == 1
    assert machine.index[CLI_CRON] == machine.cron_next() > score


def withdrawn_static_cron_is_held_not_dropped(machine: LeaderMachine) -> None:
    """E-014: Ein gemeldeter, dann nicht mehr gemeldeter statischer Cron wird
    nach Ablauf der Meldung zurückgehalten (``now + seen_interval``), nicht
    publiziert und nicht entfernt. Nur im Fake: Valkey führt die Meldungen
    mit Serverzeit, der Zeitsprung lässt sie nicht veralten."""
    _make_leader(machine, "A")
    machine.report_static(None)
    machine.adopt_tick("A")
    assert STATIC_CRON in machine.index
    machine.static_withdraw()
    _advance_ticking(machine, 200)
    start = len(machine.events)

    machine.adopt_tick("A")
    machine.poll_tick("A")

    machine.verify()
    assert not [e for e in machine.events[start:] if isinstance(e, PublishAttempt)]
    assert machine.outcomes["cron E-014"] == 1
    assert machine.index[STATIC_CRON] == machine.now_ms + machine.seen_ms
    assert STATIC_CRON in machine.entries


SCENARIOS: dict[str, Callable[[LeaderMachine], None]] = {
    "lock_expires_during_tick_commits_once": lock_expires_during_tick_commits_once,
    "oneshot_with_lost_insert_reply_is_in_the_fire_index": oneshot_with_lost_insert_reply_is_in_the_fire_index,
    "publish_failures_over_stepdown_s_step_down_and_cool_down": publish_failures_over_stepdown_s_step_down_and_cool_down,
    "removed_cron_while_due_never_fires": removed_cron_while_due_never_fires,
    "stepdown_with_failed_release_leaves_the_lock_to_its_ttl": (
        stepdown_with_failed_release_leaves_the_lock_to_its_ttl
    ),
    "unroutable_oneshot_backs_off_and_is_dropped_with_an_error_result": (
        unroutable_oneshot_backs_off_and_is_dropped_with_an_error_result
    ),
    "renew_false_sends_the_leader_to_standby": renew_false_sends_the_leader_to_standby,
    "settle_after_faults_fires_everything_due": settle_after_faults_fires_everything_due,
    "unroutable_cron_fire_is_committed_to_the_next_occurrence": (
        unroutable_cron_fire_is_committed_to_the_next_occurrence
    ),
}

# -- Selbsttests: eine Sabotage am Subjekt muss die Maschine rot machen ------


def sabotage_owner_check_is_caught(machine: LeaderMachine) -> None:
    """L1: ``commit_fire`` mit dem Token des aktuellen Halters (Owner-Check
    ausgehebelt) lässt den Commit eines veralteten Leaders durch."""
    original = ScheduleStore.commit_fire

    async def as_holder(
        self: ScheduleStore,
        schedule_name: str,
        next_fire_ts_ms: int,
        updated_def: ScheduleDefinition | None,
        owner_token: str,
        *,
        fired_ts_ms: int | None = None,
    ) -> bool:
        holder = await machine.env.backend.locks.current_owner(SCHEDULER_LOCK_NAME)
        return await original(
            self, schedule_name, next_fire_ts_ms, updated_def, holder or owner_token, fired_ts_ms=fired_ts_ms
        )

    _make_leader(machine, "A")
    _due_cli_cron(machine)
    with patch.object(ScheduleStore, "commit_fire", as_holder), pytest.raises(AssertionError, match=r"\(L1"):
        machine.poll_tick("A", mid="takeover")


def sabotage_kept_claim_is_caught(machine: LeaderMachine) -> None:
    """E-011: Gibt der Leader den eigenen Claim nach einem Verbindungsfehler
    nicht frei, endet der nächste Poll als Duplikat und das Fire fehlt."""

    async def keep(self: SchedulerFire, *args: Any, **kwargs: Any) -> None:
        return None

    machine.report_static(120)
    _make_leader(machine, "A")
    machine.cands["A"].publisher.fault = "connection"
    _advance_ticking(machine, (machine.index[STATIC_CRON] - machine.now_ms) // 1000 + 1)
    with patch.object(SchedulerFire, "_release_own_claim", keep):
        machine.poll_tick("A")
        machine.cands["A"].publisher.fault = None
        machine.advance(1)
        with pytest.raises(AssertionError, match=r"Duplikat ohne gehaltenen Claim"):
            machine.poll_tick("A")


def sabotage_double_publish_is_caught(machine: LeaderMachine) -> None:
    """E-047 deckt keine zwei Publishes derselben Identität in einem Takt."""
    original = SchedulerFire._publish_fire

    async def twice(
        self: SchedulerFire, schedule_name: str, score_ms: int, definition: Any, body: bytes, headers: Any, *args: Any
    ) -> bool:
        await self._broker.publish(definition.queue, body, headers=headers)
        return await original(self, schedule_name, score_ms, definition, body, headers, *args)

    _make_leader(machine, "A")
    _due_cli_cron(machine)
    with patch.object(SchedulerFire, "_publish_fire", twice), pytest.raises(AssertionError, match=r"zweimal"):
        machine.poll_tick("A")


SABOTAGES: dict[str, Callable[[LeaderMachine], None]] = {
    "owner_check": sabotage_owner_check_is_caught,
    "kept_claim": sabotage_kept_claim_is_caught,
    "double_publish": sabotage_double_publish_is_caught,
}

# Nur gegen die Fakes: Die Valkey-Stufe verschiebt die Serverzeit der
# Cron-Meldungen nicht (``tests/integration/stateful/conftest.py``).
FAKE_ONLY_SCENARIOS: dict[str, Callable[[LeaderMachine], None]] = {
    "withdrawn_static_cron_is_held_not_dropped": withdrawn_static_cron_is_held_not_dropped,
}
