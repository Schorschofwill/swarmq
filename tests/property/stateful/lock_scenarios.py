"""Deterministische Abläufe der Lock-Maschine (Plan 2026-10-17-0941, U6,
Testszenarien). Beide Stufen fahren sie: ``test_lock_machine.py`` gegen die
Fakes, ``tests/integration/stateful/test_lock_machine_valkey.py`` gegen Valkey.

Jede Funktion bekommt eine frische Maschine und prüft nach jedem Schritt alle
Invarianten (``verify``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import override

import pytest

from swarmq.contracts import LockToken
from tests.property.stateful.lock_machine import LockMachine
from tests.unit._log_capture import capture_swarmq_logs


class OwnerCheckOmitted(LockMachine):
    """Selbsttest: ein Modell, das beim Freigeben den Owner nicht vergleicht."""

    @override
    def model_release(self, token: LockToken) -> bool:
        hold = self.holder(token.name)
        if hold is None:
            return False
        del self.holds[token.name]
        return True


def two_owners_with_expiry_in_between(machine: LockMachine) -> None:
    """Zwei Owner, dazwischen läuft der Lock bei blockiertem Loop ab (E-022)."""
    first = machine.start("w1", "res-a", 33_000)
    assert first is not None
    machine.verify()
    assert machine.start("w2", "res-a", 33_000) is None  # E-007: Konflikt wartet
    machine.advance(seconds=33, blocked="w1")
    machine.verify()
    assert first.excused is not None
    assert not machine.keepers["w1"].keeps(first.token)

    second = machine.start("w2", "res-a", 33_000)

    assert second is not None
    machine.verify()
    machine.finish(first)  # fremder Halter: Freigabe ohne Wirkung
    machine.verify()
    assert machine.holder("res-a") is not None


def ticking_keeper_holds_the_lock_far_past_its_ttl(machine: LockMachine) -> None:
    """Rechtzeitige Takte halten den Lock über jede TTL (I4)."""
    handler = machine.start("w1", "res-a", 33_000)
    assert handler is not None
    for _ in range(5):
        machine.advance(seconds=40, blocked=None)
        machine.verify()
    assert machine.start("w2", "res-a", 33_000) is None
    machine.finish(handler)
    machine.verify()


def keeper_extends_before_expiry_and_reports_loss_after(machine: LockMachine) -> None:
    """Takt vor Ablauf verlängert; nach Übernahme liefert ``extend`` ``False``
    (``task_lock_lost``, stage ``running``); nach Ablauf ohne Takt fällt die
    Lease ohne Roundtrip heraus (stage ``expired``)."""
    with capture_swarmq_logs() as logs:
        taken = machine.start("w1", "res-a", 33_000)
        assert taken is not None
        machine.advance(seconds=25, blocked="w1")  # kürzer als das Fenster: der Takt rettet
        assert taken.excused is None
        assert machine.run(machine.env.backend.locks.current_owner("res-a")) == taken.token.owner

        machine.force_release("res-a")
        machine.plain_acquire(name="res-a", ttl_ms=90_000)
        machine.advance(seconds=9, blocked=None)  # Lease wird fällig, extend liefert False
        machine.verify()
        assert not machine.keepers["w1"].keeps(taken.token)

        expired = machine.start("w2", "res-b", 33_000)
        assert expired is not None
        machine.advance(seconds=33, blocked="w2")
        machine.verify()
        assert not machine.keepers["w2"].keeps(expired.token)

    stages = [(entry["lock_name"], entry["stage"]) for entry in logs if entry["event"] == "task_lock_lost"]
    assert stages == [("res-a", "running"), ("res-b", "expired")]


def foreign_tokens_change_nothing(machine: LockMachine) -> None:
    """Nur der Owner gibt frei (I2), auch nach einem Ausfall (E-052)."""
    handler = machine.start("w1", "res-a", 33_000)
    assert handler is not None
    for op in ("release", "renew", "extend"):
        machine.foreign_touch(name="res-a", op=op)
        machine.verify()
    machine.reply_lost_acquire(worker="w2", name="res-a", release_first=False)
    machine.verify()
    machine.reply_lost_acquire(worker="w2", name="res-b", release_first=True)
    machine.verify()
    assert machine.holder("res-b") is not None  # F6: verwaist bis zur TTL (E-052)
    machine.advance(seconds=9, blocked=None)
    machine.verify()
    assert machine.holder("res-b") is None


def model_without_owner_check_is_caught(machine: LockMachine) -> None:
    """Selbsttest der Invariante I2: läuft nur mit ``OwnerCheckOmitted``."""
    assert machine.start("w1", "res-a", 33_000) is not None
    with pytest.raises(AssertionError, match=r"\(I2\)"):
        machine.foreign_touch(name="res-a", op="release")


def end_before_start_frees_the_start_flag(machine: LockMachine) -> None:
    """FS-13 behoben (E-034, Nachtrag): Ende vor dem Start gibt frei."""
    first = machine.schedule(1)
    [message] = machine.messages
    assert message.mode == "start"
    machine.end_message(message)
    machine.verify()

    second = machine.schedule(1)

    assert second != first
    assert len(machine.messages) == 2
    machine.verify()


def start_mode_releases_exactly_once(machine: LockMachine) -> None:
    """E-034: Freigabe beim Start, keine zweite am Ende."""
    task_id = machine.schedule(2, unique_until="start")
    [message] = machine.messages
    machine.start_message(message)
    machine.verify()
    machine.end_message(message)
    machine.verify()
    assert machine.releases.calls == {task_id: 1}
    # Bewusste Folge E-034: nach dem Start läuft ein Duplikat durch.
    assert machine.schedule(2, unique_until="start") != task_id
    machine.verify()


def unique_for_none_never_claims(machine: LockMachine) -> None:
    """E-033: ``unique_for=None`` überstimmt den TaskInfo-Default."""
    first = machine.schedule(1)
    opted_out = machine.schedule(1, unique_for=None)
    assert opted_out != first
    assert machine.schedule(1) == first  # das Flag hält weiter der erste
    machine.end_message(machine.messages[1])
    machine.verify()
    assert machine.releases.calls == {}


def reply_lost_claim_never_touches_a_foreign_flag(machine: LockMachine) -> None:
    """E-052: Rückbau unter der frischen ``task_id``."""
    machine.reply_lost_schedule(arg=1)
    held = machine.schedule(1, unique_for=30)
    assert len(machine.messages) == 1, "zurückgebauter Claim blockiert das Flag"
    machine.reply_lost_schedule(arg=1)
    assert machine.schedule(1, unique_for=30) == held
    machine.verify()


SCENARIOS: dict[str, Callable[[LockMachine], None]] = {
    "two_owners_with_expiry_in_between": two_owners_with_expiry_in_between,
    "ticking_keeper_holds_the_lock_far_past_its_ttl": ticking_keeper_holds_the_lock_far_past_its_ttl,
    "keeper_extends_before_expiry_and_reports_loss_after": keeper_extends_before_expiry_and_reports_loss_after,
    "foreign_tokens_change_nothing": foreign_tokens_change_nothing,
    "end_before_start_frees_the_start_flag": end_before_start_frees_the_start_flag,
    "start_mode_releases_exactly_once": start_mode_releases_exactly_once,
    "unique_for_none_never_claims": unique_for_none_never_claims,
    "reply_lost_claim_never_touches_a_foreign_flag": reply_lost_claim_never_touches_a_foreign_flag,
}
